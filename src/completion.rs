//! Build the `ProofOfInference` for a completed job and wrap it in a
//! `CompletionAck` signed with the node's identity key.

use ed25519_dalek::SigningKey;
use pinaivu_protocol::mesh::CompletionAck;
use pinaivu_protocol::{NanoX, NodePeerId, ProofOfInference};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub struct ProofInputs {
    pub request_id: Uuid,
    pub session_id: Uuid,
    pub client_address: String,
    pub model_id: String,
    pub prompt: String,
    pub output: String,
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub latency_ms: u32,
    pub price_paid_nanox: u64,
    pub node_peer_id: String,
}

pub fn build_completion_ack(inputs: &ProofInputs, identity: &SigningKey) -> CompletionAck {
    let input_hash: [u8; 32] = Sha256::digest(inputs.prompt.as_bytes()).into();
    let output_hash: [u8; 32] = Sha256::digest(inputs.output.as_bytes()).into();

    let proof = ProofOfInference {
        request_id: inputs.request_id,
        session_id: inputs.session_id,
        node_peer_id: NodePeerId(inputs.node_peer_id.clone()),
        client_address: inputs.client_address.clone(),
        model_id: inputs.model_id.clone(),
        input_tokens: inputs.input_tokens,
        output_tokens: inputs.output_tokens,
        latency_ms: inputs.latency_ms,
        price_paid_nanox: NanoX(inputs.price_paid_nanox),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        input_hash,
        output_hash,
        settlement_id: "free".to_string(),
        escrow_tx_id: None,
        node_pubkey: [0u8; 32],
        signature: Vec::new(),
    }
    .sign(identity);

    CompletionAck {
        request_id: inputs.request_id,
        proofs: vec![proof],
        aggregated_output_hash: output_hash,
        primary_pubkey: [0u8; 32],
        signature: Vec::new(),
    }
    .sign(identity)
}
