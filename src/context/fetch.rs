//! Load a session's prior history from Postgres + Walrus.
//!
//! For a brand-new session (no row in `sessions`) we return an empty
//! [`SessionContext`]. For an existing one we fetch the latest user
//! facts, the most recent summary, and the encrypted message-history
//! blob; the caller decrypts with the session_key supplied on the
//! HTTPS body.

use anyhow::{anyhow, Context, Result};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::cipher;
use crate::walrus::WalrusClient;

use super::{SessionBlob, SessionContext};

const RECENT_FACTS_LIMIT: i64 = 50;

pub struct LoadedSession {
    pub context: SessionContext,
    /// `Some(blob_id)` if the session already exists on Walrus, so
    /// [`persist`](super::persist) can chain `prev_blob_id`.
    pub prev_blob_id: Option<String>,
    /// `true` when the session already had a row in Postgres; `false`
    /// for cold starts (the persist step will INSERT).
    pub existed: bool,
}

pub async fn load(
    pg: &PgPool,
    walrus: &WalrusClient,
    session_id: Uuid,
    session_key: &[u8; 32],
    user_address: &str,
) -> Result<LoadedSession> {
    let row = sqlx::query("SELECT walrus_blob_id FROM sessions WHERE session_id = $1")
        .bind(session_id)
        .fetch_optional(pg)
        .await
        .context("query sessions row")?;

    let Some(row) = row else {
        return Ok(LoadedSession {
            context: SessionContext::default(),
            prev_blob_id: None,
            existed: false,
        });
    };

    let prev_blob_id: Option<String> = row.try_get("walrus_blob_id").ok();

    let facts: Vec<String> = sqlx::query_scalar(
        "SELECT fact FROM user_facts
         WHERE user_address = $1 AND is_active = TRUE
         ORDER BY updated_at DESC LIMIT $2",
    )
    .bind(user_address)
    .bind(RECENT_FACTS_LIMIT)
    .fetch_all(pg)
    .await
    .context("query user_facts")?;

    let summary: Option<String> = sqlx::query_scalar(
        "SELECT summary_text FROM session_summaries
         WHERE session_id = $1
         ORDER BY created_at DESC LIMIT 1",
    )
    .bind(session_id)
    .fetch_optional(pg)
    .await
    .context("query session_summaries")?;

    let recent_messages = match prev_blob_id.as_deref() {
        None => Vec::new(),
        Some(blob_id) => {
            let bytes = walrus
                .get_blob(blob_id)
                .await
                .with_context(|| format!("walrus get_blob {blob_id}"))?
                .ok_or_else(|| anyhow!("walrus blob {blob_id} missing"))?;
            let plaintext = cipher::open(session_key, &bytes)
                .map_err(|e| anyhow!("decrypt session blob: {e}"))?;
            let blob: SessionBlob = serde_json::from_slice(&plaintext)
                .context("decode session blob")?;
            blob.messages
        }
    };

    Ok(LoadedSession {
        context: SessionContext {
            user_facts: facts,
            summary,
            recent_messages,
        },
        prev_blob_id,
        existed: true,
    })
}
