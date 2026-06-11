//! Axum HTTP server — accepts `POST /v1/inference` from clients.
//!
//! Flow per request:
//!   1. Verify the dispatch token's signature under the coordinator's
//!      pubkey (discovered at startup).
//!   2. Defensive: confirm we bid on this request_id; body session_id
//!      must match the signed token's.
//!   3. If a Postgres pool + Walrus client are configured, fetch the
//!      session's prior messages, decrypt with `session_key`, and
//!      assemble the full sliding window. Otherwise fall back to
//!      single-message stateless mode.
//!   4. Call Ollama.
//!   5. Persist the new turn (Postgres rows + encrypted Walrus blob).
//!   6. Return the answer to the client.
//!   7. Spawn the CompletionAck fire-and-forget so the client doesn't
//!      wait on the coordinator libp2p round-trip.

use std::sync::Arc;

use anyhow::Result;
use axum::{extract::State as AxumState, response::IntoResponse, routing::post, Json, Router};
use ed25519_dalek::SigningKey;
use pinaivu_protocol::DispatchToken;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use uuid::Uuid;
use zeroize::Zeroize;

use crate::completion::{build_completion_ack, ProofInputs};
use crate::context::{self, assemble::Budget};
use crate::inflight::Inflight;
use crate::mesh;
use crate::ollama;
use crate::walrus::WalrusClient;

#[derive(Clone)]
pub struct State {
    pub ollama_url: String,
    pub model: String,
    pub identity: SigningKey,
    pub node_peer_id: String,
    pub price_per_1k_nanox: u64,
    pub coord_pubkey: [u8; 32],
    pub mesh: mpsc::Sender<mesh::Command>,
    pub inflight: Arc<Inflight>,
    /// Postgres pool + Walrus client. When both are `Some` the node
    /// runs in **stateful mode** (Phase 16); when both are `None` it
    /// stays stateless (legacy single-turn mode).
    pub pg: Option<PgPool>,
    pub walrus: Option<Arc<WalrusClient>>,
}

#[derive(Deserialize)]
struct InferenceReq {
    /// The user's new message for this turn. Older client builds may
    /// still send this under the legacy `prompt` key.
    #[serde(alias = "prompt")]
    new_user_message: String,
    dispatch_token: DispatchToken,
    /// Session this turn belongs to. Must match `dispatch_token.session_id`.
    session_id: Uuid,
    /// AES-256 key used to encrypt/decrypt the session blob on Walrus.
    /// Required when the node is configured with Postgres + Walrus —
    /// without it we can't read prior turns or persist this one.
    /// Wiped from memory after the turn.
    #[serde(default, with = "session_key_b64")]
    session_key: Option<[u8; 32]>,
    /// Cross-session memory facts from the chat-relayer's own pgvector
    /// store. When present, prepended into the system prompt before the
    /// intra-session history. Absent for direct API callers.
    #[serde(default)]
    memwal_context: Option<String>,
}

mod session_key_b64 {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<[u8; 32]>, D::Error> {
        let opt: Option<String> = Option::deserialize(d)?;
        match opt {
            None => Ok(None),
            Some(s) => {
                let raw = STANDARD.decode(s.as_bytes()).map_err(serde::de::Error::custom)?;
                let arr: [u8; 32] = raw
                    .as_slice()
                    .try_into()
                    .map_err(|_| serde::de::Error::custom("session_key must be 32 bytes"))?;
                Ok(Some(arr))
            }
        }
    }
}

#[derive(Serialize)]
struct InferenceResp {
    request_id: Uuid,
    session_id: Uuid,
    content: String,
    input_tokens: u32,
    output_tokens: u32,
    latency_ms: u32,
}

pub async fn serve(addr: &str, state: State) -> Result<()> {
    let app = Router::new()
        .route("/v1/inference", post(handle_inference))
        .with_state(state);
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(listening = %listener.local_addr()?, "node http ready");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_inference(
    AxumState(state): AxumState<State>,
    Json(mut req): Json<InferenceReq>,
) -> Result<Json<InferenceResp>, ErrorResponse> {
    // 1. Verify the dispatch token came from the coordinator we trust.
    if req.dispatch_token.coordinator_pubkey != state.coord_pubkey {
        return Err(ErrorResponse::unauthorized(
            "dispatch token signed by unknown coordinator",
        ));
    }
    req.dispatch_token
        .verify()
        .map_err(|e| ErrorResponse::unauthorized(format!("dispatch token verify failed: {e}")))?;

    let request_id = req.dispatch_token.request_id;
    let session_id = req.dispatch_token.session_id;

    // 2. Body session_id must agree with the signed dispatch token, and
    //    we must have bid on this request.
    if req.session_id != session_id {
        return Err(ErrorResponse::bad_request(
            "session_id does not match dispatch_token.session_id",
        ));
    }
    if !state.inflight.contains(&request_id) {
        return Err(ErrorResponse::bad_request(
            "no matching bid for this request_id",
        ));
    }

    let client_address = hex::encode(req.dispatch_token.client_pubkey);

    // 3. Fetch + assemble context (stateful mode) or build a one-message
    //    array (stateless fallback).
    let stateful = state.pg.as_ref().zip(state.walrus.as_ref());
    let (assembled, loaded) = if let Some((pg, walrus)) = stateful {
        let key = req.session_key.ok_or_else(|| {
            ErrorResponse::bad_request("session_key is required for stateful turns")
        })?;
        let loaded = context::fetch::load(pg, walrus, session_id, &key, &client_address)
            .await
            .map_err(|e| ErrorResponse::internal(format!("context fetch: {e}")))?;
        let assembled = context::assemble::build(
            &loaded.context,
            &req.new_user_message,
            Budget::for_model(&state.model),
            req.memwal_context.as_deref(),
        );
        (assembled, Some(loaded))
    } else {
        // Stateless: just the user message, default system prompt.
        let assembled = context::assemble::build(
            &context::SessionContext::default(),
            &req.new_user_message,
            Budget::for_model(&state.model),
            req.memwal_context.as_deref(),
        );
        (assembled, None)
    };

    // 4. Run inference with the assembled message list.
    let ollama_msgs: Vec<ollama::ChatMessage<'_>> =
        std::iter::once(ollama::ChatMessage {
            role: "system",
            content: &assembled.system_prompt,
        })
        .chain(
            assembled
                .messages
                .iter()
                .map(|m| ollama::ChatMessage {
                    role: m.role.as_str(),
                    content: m.content.as_str(),
                }),
        )
        .collect();
    let reply = ollama::chat(&state.ollama_url, &state.model, &ollama_msgs)
        .await
        .map_err(|e| ErrorResponse::internal(format!("ollama: {e}")))?;

    // 5. Persist (only in stateful mode).
    if let (Some(pg), Some(walrus), Some(key), Some(loaded)) = (
        state.pg.as_ref(),
        state.walrus.as_ref(),
        req.session_key.as_ref(),
        loaded,
    ) {
        let prior = loaded.context.recent_messages.clone();
        let prev_blob_id = loaded.prev_blob_id.clone();
        let session_existed = loaded.existed;
        let input = context::persist::CommitInput {
            session_id,
            request_id,
            user_address: &client_address,
            model_id: &state.model,
            node_peer_id: &state.node_peer_id,
            new_user_message: &req.new_user_message,
            assistant_reply: &reply.content,
            input_tokens: reply.prompt_tokens,
            output_tokens: reply.completion_tokens,
            latency_ms: reply.latency_ms,
            cost_nanox: state.price_per_1k_nanox,
            prior_messages: prior,
            prev_blob_id,
            session_existed,
        };
        if let Err(e) = context::persist::commit(pg, walrus, key, input).await {
            // Don't fail the client response on persistence errors —
            // the inference still happened and the coordinator gets a
            // proof. Log loudly so we can investigate.
            tracing::error!(error = %e, session_id = %session_id,
                "context persist failed; turn served but history not saved");
        }
    }

    // 6. Build the client response.
    let resp = InferenceResp {
        request_id,
        session_id,
        content: reply.content.clone(),
        input_tokens: reply.prompt_tokens,
        output_tokens: reply.completion_tokens,
        latency_ms: reply.latency_ms,
    };

    // Wipe the session key before we hand control off to the spawned task.
    if let Some(mut k) = req.session_key.take() {
        k.zeroize();
    }

    // 7. Fire-and-forget CompletionAck so the client doesn't wait on
    //    the coordinator's libp2p round-trip.
    let inputs = ProofInputs {
        request_id,
        session_id,
        client_address: client_address.clone(),
        model_id: state.model.clone(),
        prompt: req.new_user_message,
        output: reply.content,
        input_tokens: reply.prompt_tokens,
        output_tokens: reply.completion_tokens,
        latency_ms: reply.latency_ms,
        price_paid_nanox: state.price_per_1k_nanox,
        node_peer_id: state.node_peer_id.clone(),
    };
    let ack = build_completion_ack(&inputs, &state.identity);
    let mesh_tx = state.mesh.clone();
    let inflight = state.inflight.clone();
    tokio::spawn(async move {
        if let Err(e) = mesh_tx.send(mesh::Command::SendCompletionAck(ack)).await {
            tracing::error!(error = %e, "send CompletionAck command");
        }
        inflight.forget(&request_id);
    });

    Ok(Json(resp))
}

struct ErrorResponse {
    status: axum::http::StatusCode,
    message: String,
}

impl ErrorResponse {
    fn unauthorized(msg: impl Into<String>) -> Self {
        Self {
            status: axum::http::StatusCode::UNAUTHORIZED,
            message: msg.into(),
        }
    }
    fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: axum::http::StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }
    fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.into(),
        }
    }
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> axum::response::Response {
        let body = serde_json::json!({ "error": self.message });
        (self.status, Json(body)).into_response()
    }
}
