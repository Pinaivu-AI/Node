//! Axum HTTP server — accepts `POST /v1/inference` from clients.
//!
//! Flow per request:
//!   1. Verify the dispatch token's signature under the coordinator's
//!      pubkey (discovered at startup).
//!   2. Defensive: confirm we bid on this request_id.
//!   3. Call Ollama.
//!   4. Return the answer to the client immediately.
//!   5. Spawn a background task that builds + signs the CompletionAck
//!      and asks the mesh task to send it.

use std::sync::Arc;

use anyhow::Result;
use axum::{extract::State as AxumState, response::IntoResponse, routing::post, Json, Router};
use ed25519_dalek::SigningKey;
use pinaivu_protocol::DispatchToken;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::completion::{build_completion_ack, ProofInputs};
use crate::inflight::Inflight;
use crate::mesh;
use crate::ollama;

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
    /// AES-256 key used to encrypt/decrypt the session blob on Walrus
    /// and KV-cache blocks. Transmitted in the HTTPS body so the node
    /// can fetch and decrypt history; wiped from memory after the turn.
    /// Base64-encoded for JSON transport. Consumed in Phase 16 Step 3.
    #[serde(default, with = "session_key_b64")]
    #[allow(dead_code)]
    session_key: Option<[u8; 32]>,
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
    Json(req): Json<InferenceReq>,
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

    // 2a. Body session_id must agree with the signed dispatch token.
    if req.session_id != session_id {
        return Err(ErrorResponse::bad_request(
            "session_id does not match dispatch_token.session_id",
        ));
    }

    // 2b. Defensive: confirm we bid on this. The signed token is the
    // real authority, but if we don't have a matching bid record then
    // something is off — refuse.
    if !state.inflight.contains(&request_id) {
        return Err(ErrorResponse::bad_request(
            "no matching bid for this request_id",
        ));
    }

    // 3. Run inference.
    //    Phase 16 Step 3 will fetch history from Postgres + Walrus
    //    (decrypted with `req.session_key`) and assemble the full
    //    sliding window before this call. For now we send the single
    //    new user message and the node sees just this turn.
    let reply = ollama::chat(&state.ollama_url, &state.model, &req.new_user_message)
        .await
        .map_err(|e| ErrorResponse::internal(format!("ollama: {e}")))?;

    // 4. Build the response we hand the client.
    let resp = InferenceResp {
        request_id,
        content: reply.content.clone(),
        input_tokens: reply.prompt_tokens,
        output_tokens: reply.completion_tokens,
        latency_ms: reply.latency_ms,
    };

    // 5. Fire-and-forget CompletionAck so the client doesn't wait on
    // the coordinator's libp2p round-trip.
    let inputs = ProofInputs {
        request_id,
        session_id,
        client_address: hex::encode(req.dispatch_token.client_pubkey),
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
