//! Decide whether to bid on an incoming `InferenceRequest`, and build
//! the `InferenceBid` payload.
//!
//! v1: bid on everything for our configured model; price/latency/rep
//! pulled from CLI config (no live capacity check, no rate limiting —
//! those land in a later slice).

use pinaivu_protocol::{InferenceBid, InferenceRequest, NanoX, NodePeerId};

pub struct BidConfig {
    pub model: String,
    pub node_peer_id: String,
    pub price_per_1k_nanox: u64,
    pub http_endpoint: String,
}

pub fn build_bid(req: &InferenceRequest, cfg: &BidConfig) -> Option<InferenceBid> {
    if req.model != cfg.model {
        return None;
    }
    Some(InferenceBid {
        request_id: req.request_id,
        node_peer_id: NodePeerId(cfg.node_peer_id.clone()),
        price_per_1k: NanoX(cfg.price_per_1k_nanox),
        latency_ms: 300,
        reputation: 0.9,
        http_endpoint: cfg.http_endpoint.clone(),
    })
}
