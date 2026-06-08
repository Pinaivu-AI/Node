//! Write a completed turn back to Postgres + Walrus.
//!
//! Inserts the `turns` row, upserts the parent `sessions` row, encrypts
//! the updated message history and pushes it to Walrus, then records
//! the warm-node mapping so the coordinator can route subsequent turns
//! back to us.

use anyhow::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

use crate::cipher;
use crate::walrus::WalrusClient;

use super::{Message, SessionBlob};

pub struct CommitInput<'a> {
    pub session_id: Uuid,
    pub request_id: Uuid,
    pub user_address: &'a str,
    pub model_id: &'a str,
    pub node_peer_id: &'a str,
    pub new_user_message: &'a str,
    pub assistant_reply: &'a str,
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub latency_ms: u32,
    pub cost_nanox: u64,
    pub prior_messages: Vec<Message>,
    pub prev_blob_id: Option<String>,
    /// `true` if `sessions` already had a row (skip the INSERT).
    pub session_existed: bool,
}

pub struct CommitOutput {
    pub walrus_blob_id: String,
    pub turn_id: Uuid,
}

pub async fn commit(
    pg: &PgPool,
    walrus: &WalrusClient,
    session_key: &[u8; 32],
    input: CommitInput<'_>,
) -> Result<CommitOutput> {
    // Build the new message log (old + new turn).
    let mut messages = input.prior_messages;
    messages.push(Message {
        role: "user".to_string(),
        content: input.new_user_message.to_string(),
    });
    messages.push(Message {
        role: "assistant".to_string(),
        content: input.assistant_reply.to_string(),
    });

    // Encrypt + upload the new session blob.
    let blob = SessionBlob::new(messages);
    let plaintext = serde_json::to_vec(&blob).context("encode session blob")?;
    let ciphertext = cipher::seal(session_key, &plaintext);
    let walrus_blob_id = walrus
        .put_blob(&ciphertext)
        .await
        .context("walrus put_blob")?;

    let total_tokens = input.input_tokens as i64 + input.output_tokens as i64;

    let mut tx = pg.begin().await.context("begin tx")?;

    if !input.session_existed {
        sqlx::query(
            "INSERT INTO sessions
                (session_id, user_address, model_id, walrus_blob_id,
                 turn_count, total_tokens, total_cost_nanox)
             VALUES ($1, $2, $3, $4, 1, $5, $6)
             ON CONFLICT (session_id) DO UPDATE SET
                walrus_blob_id   = EXCLUDED.walrus_blob_id,
                turn_count       = sessions.turn_count + 1,
                total_tokens     = sessions.total_tokens + EXCLUDED.total_tokens,
                total_cost_nanox = sessions.total_cost_nanox + EXCLUDED.total_cost_nanox,
                last_updated     = NOW(),
                prev_blob_id     = sessions.walrus_blob_id",
        )
        .bind(input.session_id)
        .bind(input.user_address)
        .bind(input.model_id)
        .bind(&walrus_blob_id)
        .bind(total_tokens)
        .bind(input.cost_nanox as i64)
        .execute(&mut *tx)
        .await
        .context("insert sessions")?;
    } else {
        sqlx::query(
            "UPDATE sessions SET
                walrus_blob_id   = $2,
                prev_blob_id     = $3,
                turn_count       = turn_count + 1,
                total_tokens     = total_tokens + $4,
                total_cost_nanox = total_cost_nanox + $5,
                last_updated     = NOW()
             WHERE session_id = $1",
        )
        .bind(input.session_id)
        .bind(&walrus_blob_id)
        .bind(&input.prev_blob_id)
        .bind(total_tokens)
        .bind(input.cost_nanox as i64)
        .execute(&mut *tx)
        .await
        .context("update sessions")?;
    }

    let turn_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO turns
            (turn_id, session_id, user_address, request_id, node_peer_id,
             input_tokens, output_tokens, latency_ms, cost_nanox)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(turn_id)
    .bind(input.session_id)
    .bind(input.user_address)
    .bind(input.request_id.to_string())
    .bind(input.node_peer_id)
    .bind(input.input_tokens as i32)
    .bind(input.output_tokens as i32)
    .bind(input.latency_ms as i32)
    .bind(input.cost_nanox as i64)
    .execute(&mut *tx)
    .await
    .context("insert turns")?;

    // Coordinator routing optimisation — mark this node as warm for
    // the session so the next auction prefers us.
    sqlx::query(
        "INSERT INTO node_session_cache (node_peer_id, session_id, last_served_at, cache_tier)
         VALUES ($1, $2, NOW(), 'gpu')
         ON CONFLICT (node_peer_id, session_id) DO UPDATE SET
            last_served_at = NOW(),
            cache_tier     = 'gpu'",
    )
    .bind(input.node_peer_id)
    .bind(input.session_id)
    .execute(&mut *tx)
    .await
    .context("upsert node_session_cache")?;

    tx.commit().await.context("commit tx")?;

    Ok(CommitOutput {
        walrus_blob_id,
        turn_id,
    })
}
