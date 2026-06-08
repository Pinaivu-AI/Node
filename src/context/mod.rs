//! Per-session context layer.
//!
//! Loads conversation history from Postgres + Walrus, assembles the
//! sliding window for the model, then writes the updated history back
//! after the model returns. Session state is keyed by the
//! coordinator-signed `session_id` in the dispatch token; the
//! `session_key` rides the HTTPS body and is wiped on completion.

pub mod assemble;
pub mod fetch;
pub mod persist;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: String,
}

/// What `fetch` returns to `assemble`. Empty for cold-start turns.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SessionContext {
    /// Long-term facts about the user, injected into the system prompt.
    #[serde(default)]
    pub user_facts: Vec<String>,
    /// Rolling summary of older messages, replaces them in the window.
    #[serde(default)]
    pub summary: Option<String>,
    /// Verbatim recent turns, most recent last.
    #[serde(default)]
    pub recent_messages: Vec<Message>,
}

/// Stored verbatim on Walrus as an encrypted blob. The protocol version
/// lets us reshape this struct later without breaking older blobs.
#[derive(Debug, Serialize, Deserialize)]
pub struct SessionBlob {
    pub version: u32,
    pub messages: Vec<Message>,
}

impl SessionBlob {
    pub const CURRENT_VERSION: u32 = 1;

    pub fn new(messages: Vec<Message>) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            messages,
        }
    }
}
