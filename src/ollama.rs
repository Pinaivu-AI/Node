//! Minimal Ollama `/api/chat` client (non-streaming).

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub struct OllamaReply {
    pub content: String,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub latency_ms: u32,
}

#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: &'a [ChatMessage<'a>],
    stream: bool,
}

#[derive(Serialize, Clone)]
pub struct ChatMessage<'a> {
    pub role: &'a str,
    pub content: &'a str,
}

#[derive(Deserialize)]
struct ChatResponse {
    message: ChatResponseMessage,
    #[serde(default)]
    prompt_eval_count: u32,
    #[serde(default)]
    eval_count: u32,
}

#[derive(Deserialize)]
struct ChatResponseMessage {
    content: String,
}

/// Run a chat completion against Ollama with an assembled message list.
pub async fn chat(ollama_url: &str, model: &str, messages: &[ChatMessage<'_>]) -> Result<OllamaReply> {
    let url = format!("{}/api/chat", ollama_url.trim_end_matches('/'));
    let body = ChatRequest {
        model,
        messages,
        stream: false,
    };
    let start = std::time::Instant::now();
    let resp: ChatResponse = reqwest::Client::new()
        .post(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?
        .error_for_status()
        .context("ollama non-2xx")?
        .json()
        .await
        .context("decode ollama response")?;
    let latency_ms = start.elapsed().as_millis() as u32;
    Ok(OllamaReply {
        content: resp.message.content,
        prompt_tokens: resp.prompt_eval_count,
        completion_tokens: resp.eval_count,
        latency_ms,
    })
}
