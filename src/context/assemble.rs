//! Assemble the final message list sent to the model.
//!
//! Token budgeting uses a crude 4-chars/token approximation — fine for
//! deciding what to fit; a model-native tokenizer comes later with the
//! vLLM port. Walk from the most recent message backwards, dropping
//! older ones until the budget is satisfied.

use super::{Message, SessionContext};

pub const DEFAULT_SYSTEM_PROMPT: &str =
    "You are a helpful AI assistant running on the Pinaivu network. \
     Answer concisely and accurately based on the conversation history.";

pub const APPROX_CHARS_PER_TOKEN: usize = 4;

#[derive(Debug, Clone, Copy)]
pub struct Budget {
    pub model_context_window: usize,
    pub reserved_for_output: usize,
}

impl Budget {
    pub fn for_model(model: &str) -> Self {
        // Crude per-family defaults; real values come from a config
        // map once we have more than a handful of models in play.
        let model_context_window = match model {
            m if m.starts_with("llama3.1:8b") || m.starts_with("llama3.2") => 8_192,
            m if m.starts_with("llama3.1:70b") => 32_768,
            m if m.starts_with("qwen") => 32_768,
            _ => 8_192,
        };
        Self {
            model_context_window,
            reserved_for_output: 2_048,
        }
    }

    fn input_budget_tokens(&self) -> usize {
        self.model_context_window
            .saturating_sub(self.reserved_for_output)
    }
}

fn approx_tokens(s: &str) -> usize {
    s.len().div_ceil(APPROX_CHARS_PER_TOKEN)
}

/// Build the system prompt with optional facts + summary preamble.
fn render_system_prompt(ctx: &SessionContext) -> String {
    let mut out = String::from(DEFAULT_SYSTEM_PROMPT);
    if !ctx.user_facts.is_empty() {
        out.push_str("\n\nKnown facts about the user:\n");
        for f in &ctx.user_facts {
            out.push_str("- ");
            out.push_str(f);
            out.push('\n');
        }
    }
    if let Some(s) = &ctx.summary {
        out.push_str("\nEarlier conversation summary:\n");
        out.push_str(s);
        out.push('\n');
    }
    out
}

pub struct Assembled {
    pub messages: Vec<Message>,
    pub system_prompt: String,
}

/// Produce the final message list for the model. The new user message
/// is always included; older messages are dropped (oldest first) if
/// the budget is exceeded.
pub fn build(ctx: &SessionContext, new_user_message: &str, budget: Budget) -> Assembled {
    let system_prompt = render_system_prompt(ctx);
    let budget_tokens = budget.input_budget_tokens();

    // Start with system + new user message — these are non-negotiable.
    let mut tokens_used = approx_tokens(&system_prompt) + approx_tokens(new_user_message);

    // Walk recent messages newest first; keep what fits.
    let mut keep_rev: Vec<&Message> = Vec::new();
    for m in ctx.recent_messages.iter().rev() {
        let cost = approx_tokens(&m.content) + 4; // role + bookkeeping
        if tokens_used + cost > budget_tokens {
            break;
        }
        tokens_used += cost;
        keep_rev.push(m);
    }

    let mut messages: Vec<Message> = keep_rev.into_iter().rev().cloned().collect();
    messages.push(Message {
        role: "user".to_string(),
        content: new_user_message.to_string(),
    });

    Assembled {
        messages,
        system_prompt,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(role: &str, content: &str) -> Message {
        Message {
            role: role.into(),
            content: content.into(),
        }
    }

    #[test]
    fn empty_context_yields_single_user_turn() {
        let ctx = SessionContext::default();
        let a = build(&ctx, "hi", Budget::for_model("llama3.2:1b"));
        assert_eq!(a.messages.len(), 1);
        assert_eq!(a.messages[0].role, "user");
        assert_eq!(a.messages[0].content, "hi");
    }

    #[test]
    fn facts_and_summary_render_into_system_prompt() {
        let ctx = SessionContext {
            user_facts: vec!["user prefers Rust".into()],
            summary: Some("earlier: discussed P2P".into()),
            recent_messages: vec![],
        };
        let a = build(&ctx, "go on", Budget::for_model("llama3.2:1b"));
        assert!(a.system_prompt.contains("user prefers Rust"));
        assert!(a.system_prompt.contains("discussed P2P"));
    }

    #[test]
    fn recent_messages_appear_in_chronological_order() {
        let ctx = SessionContext {
            user_facts: vec![],
            summary: None,
            recent_messages: vec![
                msg("user", "first"),
                msg("assistant", "answer-1"),
                msg("user", "second"),
                msg("assistant", "answer-2"),
            ],
        };
        let a = build(&ctx, "third", Budget::for_model("llama3.2:1b"));
        let contents: Vec<&str> = a.messages.iter().map(|m| m.content.as_str()).collect();
        assert_eq!(contents, vec!["first", "answer-1", "second", "answer-2", "third"]);
    }

    #[test]
    fn oldest_messages_dropped_when_over_budget() {
        let big = "x".repeat(20_000); // approx 5k tokens each
        let ctx = SessionContext {
            user_facts: vec![],
            summary: None,
            recent_messages: vec![
                msg("user", &big),
                msg("assistant", &big),
                msg("user", &big),
            ],
        };
        let budget = Budget {
            model_context_window: 8_192,
            reserved_for_output: 2_048,
        };
        let a = build(&ctx, "tiny new message", budget);
        // We must keep the new user message; we must NOT keep all three
        // big history messages — they would blow the budget.
        assert_eq!(a.messages.last().unwrap().content, "tiny new message");
        assert!(a.messages.len() < 4);
    }
}
