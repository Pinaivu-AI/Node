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
    prompt: String,
    dispatch_token: DispatchToken,
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

    // 2. Defensive: confirm we bid on this. The signed token is the
    // real authority, but if we don't have a matching bid record then
    // something is off — refuse.
    if !state.inflight.contains(&request_id) {
        return Err(ErrorResponse::bad_request(
            "no matching bid for this request_id",
        ));
    }

    // 3. Run inference.
    let reply = ollama::chat(&state.ollama_url, &state.model, &req.prompt)
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
        session_id: Uuid::new_v4(),
        client_address: hex::encode(req.dispatch_token.client_pubkey),
        model_id: state.model.clone(),
        prompt: req.prompt,
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
