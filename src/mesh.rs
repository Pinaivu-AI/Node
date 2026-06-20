//! libp2p swarm — drives the gossipsub bid loop and handles outbound
//! `CompletionAck` request-response messages to the coordinator.
//!
//! Spawned once at startup; returns a [`Handle`] the HTTP layer uses
//! to send commands into the event loop.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use ed25519_dalek::SigningKey;
use futures::StreamExt;
use libp2p::{
    gossipsub::{self, IdentTopic},
    multiaddr::Protocol,
    request_response::{self, OutboundRequestId, ResponseChannel},
    swarm::SwarmEvent,
    Multiaddr, PeerId, Swarm,
};
use pinaivu_protocol::mesh::{
    behaviour::{libp2p_identity_from_ed25519_secret, PinaivuBehaviour, PinaivuBehaviourEvent},
    completion_proto::{CompletionAck, CompletionResponse},
    inference_proto::{InferenceDispatch, InferenceReply},
    topics::{ANNOUNCE, BIDS, INFERENCE_ANY},
};
use pinaivu_protocol::{InferenceRequest, NodeCapabilities, NodePeerId};
use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::bidder::{build_bid, BidConfig};
use crate::http::{self, run_inference_job};
use crate::inflight::Inflight;
use crate::walrus::WalrusClient;

pub struct Config {
    pub identity: SigningKey,
    pub coordinator_addr: Multiaddr,
    pub listen_addr: Multiaddr,
    pub model: String,
    pub price_per_1k_nanox: u64,
    pub advertise_url: String,
    pub inflight: Arc<Inflight>,
    /// Sui address advertised in every InferenceBid as the destination
    /// for on-chain settlement payouts. Plumbed from CLI.
    pub payout_address: String,
    /// Everything else `http::State` needs, so the event loop can run
    /// inbound `InferenceDispatch` jobs (the NAT-safe libp2p path)
    /// using the exact same pipeline as the HTTP `/v1/inference` path.
    pub ollama_url: String,
    pub coord_pubkey: [u8; 32],
    pub pg: Option<PgPool>,
    pub walrus: Option<Arc<WalrusClient>>,
}

pub struct Handle {
    pub peer_id: PeerId,
    pub cmd_tx: mpsc::Sender<Command>,
    /// Fully-built state, shared with the HTTP server so both paths
    /// (libp2p and direct-dial HTTP) run identical inference logic.
    pub state: http::State,
}

pub enum Command {
    /// Send a `CompletionAck` to the coordinator. Fire-and-forget at the
    /// channel level; the event loop awaits the libp2p response and
    /// logs it but the caller doesn't block on it.
    SendCompletionAck(CompletionAck),
    /// Reply to an inbound `InferenceDispatch` once the (possibly slow)
    /// inference job spawned off the event loop has finished.
    SendInferenceReply {
        channel: ResponseChannel<InferenceReply>,
        reply: InferenceReply,
    },
}

pub async fn spawn(cfg: Config) -> Result<Handle> {
    let secret = cfg.identity.to_bytes();
    let identity = libp2p_identity_from_ed25519_secret(&secret)?;
    let local_peer_id = PeerId::from(identity.public());

    let mut swarm: Swarm<PinaivuBehaviour> = libp2p::SwarmBuilder::with_existing_identity(identity)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )
        .map_err(|e| anyhow!("tcp transport: {e}"))?
        .with_behaviour(|key| {
            PinaivuBehaviour::new(key)
                .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()))
        })
        .map_err(|e| anyhow!("compose behaviour: {e}"))?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    // Subscribe to the marketplace topics the coordinator publishes on.
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&IdentTopic::new(INFERENCE_ANY))
        .map_err(|e| anyhow!("subscribe {INFERENCE_ANY}: {e}"))?;
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&IdentTopic::new(BIDS))
        .map_err(|e| anyhow!("subscribe {BIDS}: {e}"))?;
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&IdentTopic::new(ANNOUNCE))
        .map_err(|e| anyhow!("subscribe {ANNOUNCE}: {e}"))?;

    swarm
        .listen_on(cfg.listen_addr.clone())
        .map_err(|e| anyhow!("listen_on: {e}"))?;

    // Extract coordinator's peer id from the multiaddr so we can route
    // request-response messages to it later.
    let coord_peer = peer_id_from_multiaddr(&cfg.coordinator_addr)
        .ok_or_else(|| anyhow!("coordinator_addr must end with /p2p/<peer_id>"))?;
    swarm
        .behaviour_mut()
        .kademlia
        .add_address(&coord_peer, cfg.coordinator_addr.clone());
    swarm
        .dial(cfg.coordinator_addr.clone())
        .with_context(|| format!("dial {}", cfg.coordinator_addr))?;
    tracing::info!(coordinator = %coord_peer, "dialing coordinator");

    let bid_cfg = BidConfig {
        model: cfg.model.clone(),
        node_peer_id: local_peer_id.to_string(),
        price_per_1k_nanox: cfg.price_per_1k_nanox,
        http_endpoint: cfg.advertise_url.clone(),
        payout_address: cfg.payout_address.clone(),
    };

    let (cmd_tx, cmd_rx) = mpsc::channel(32);

    let state = http::State {
        ollama_url: cfg.ollama_url,
        model: cfg.model,
        identity: cfg.identity,
        node_peer_id: local_peer_id.to_string(),
        price_per_1k_nanox: cfg.price_per_1k_nanox,
        coord_pubkey: cfg.coord_pubkey,
        mesh: cmd_tx.clone(),
        inflight: cfg.inflight,
        pg: cfg.pg,
        walrus: cfg.walrus,
    };

    tokio::spawn(run_event_loop(
        swarm,
        cmd_rx,
        bid_cfg,
        state.inflight.clone(),
        coord_peer,
        state.clone(),
        cmd_tx.clone(),
    ));

    Ok(Handle {
        peer_id: local_peer_id,
        cmd_tx,
        state,
    })
}

fn peer_id_from_multiaddr(addr: &Multiaddr) -> Option<PeerId> {
    for p in addr.iter() {
        if let Protocol::P2p(peer) = p {
            return Some(peer);
        }
    }
    None
}

async fn run_event_loop(
    mut swarm: Swarm<PinaivuBehaviour>,
    mut cmd_rx: mpsc::Receiver<Command>,
    bid_cfg: BidConfig,
    inflight: Arc<Inflight>,
    coord_peer: PeerId,
    state: http::State,
    self_cmd_tx: mpsc::Sender<Command>,
) {
    // Track in-flight outbound completion requests so we can log their
    // results without blocking the HTTP path.
    let mut pending_acks: std::collections::HashMap<OutboundRequestId, uuid::Uuid> =
        std::collections::HashMap::new();
    let mut announced = false;
    let mut announce_after: Option<tokio::time::Instant> = None;
    let mut announce_interval = tokio::time::interval(Duration::from_secs(30));
    announce_interval.tick().await;

    loop {
        if let Some(deadline) = announce_after {
            if tokio::time::Instant::now() >= deadline {
                announce_after = None;
                let caps = NodeCapabilities {
                    peer_id: NodePeerId(bid_cfg.node_peer_id.clone()),
                    models: vec![bid_cfg.model.clone()],
                    max_concurrent_jobs: 4,
                };
                if let Ok(payload) = serde_json::to_vec(&caps) {
                    match swarm.behaviour_mut().gossipsub.publish(IdentTopic::new(ANNOUNCE), payload) {
                        Ok(_) => {
                            announced = true;
                            tracing::info!("announced capabilities to coordinator");
                            // Warm up the BIDS gossipsub mesh by publishing a
                            // heartbeat bid. Without this the first real bid
                            // gets dropped because the mesh isn't grafted yet.
                            let warmup_bid = pinaivu_protocol::InferenceBid {
                                request_id: uuid::Uuid::nil(),
                                node_peer_id: NodePeerId(bid_cfg.node_peer_id.clone()),
                                price_per_1k: pinaivu_protocol::NanoX(bid_cfg.price_per_1k_nanox),
                                latency_ms: 0,
                                reputation: 0.0,
                                http_endpoint: bid_cfg.http_endpoint.clone(),
                                payout_address: bid_cfg.payout_address.clone(),
                                node_x25519_pubkey: None,
                            };
                            if let Ok(bid_payload) = serde_json::to_vec(&warmup_bid) {
                                let _ = swarm.behaviour_mut().gossipsub.publish(IdentTopic::new(BIDS), bid_payload);
                                tracing::info!("sent warmup bid to graft BIDS mesh");
                            }
                        }
                        Err(e) => tracing::warn!(error = %e, "failed to announce capabilities"),
                    }
                }
            }
        }

        tokio::select! {
            _ = announce_interval.tick(), if announced => {
                let caps = NodeCapabilities {
                    peer_id: NodePeerId(bid_cfg.node_peer_id.clone()),
                    models: vec![bid_cfg.model.clone()],
                    max_concurrent_jobs: 4,
                };
                if let Ok(payload) = serde_json::to_vec(&caps) {
                    let _ = swarm.behaviour_mut().gossipsub.publish(IdentTopic::new(ANNOUNCE), payload);
                }
            }
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { break };
                match cmd {
                    Command::SendCompletionAck(ack) => {
                        let req_id = ack.request_id;
                        let out_id = swarm
                            .behaviour_mut()
                            .completion
                            .send_request(&coord_peer, ack);
                        pending_acks.insert(out_id, req_id);
                        tracing::info!(request_id = %req_id, "sent CompletionAck");
                    }
                    Command::SendInferenceReply { channel, reply } => {
                        let _ = swarm.behaviour_mut().inference.send_response(channel, reply);
                    }
                }
            }
            ev = swarm.select_next_some() => match ev {
                SwarmEvent::NewListenAddr { address, .. } => {
                    tracing::info!(addr = %address, "listening");
                }
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    tracing::info!(peer = %peer_id, "connection established");
                    if peer_id == coord_peer && !announced {
                        announce_after = Some(tokio::time::Instant::now() + Duration::from_secs(3));
                    }
                }
                SwarmEvent::Behaviour(PinaivuBehaviourEvent::Gossipsub(
                    gossipsub::Event::Message { message, .. }
                )) => {
                    if message.topic == IdentTopic::new(INFERENCE_ANY).hash() {
                        match serde_json::from_slice::<InferenceRequest>(&message.data) {
                            Ok(req) => {
                                tracing::info!(request_id = %req.request_id, model = %req.model, "received inference request");
                                if let Some(bid) = build_bid(&req, &bid_cfg) {
                                    inflight.record_bid(req.request_id);
                                    let payload = match serde_json::to_vec(&bid) {
                                        Ok(p) => p,
                                        Err(e) => {
                                            tracing::error!(error = %e, "serialize bid");
                                            continue;
                                        }
                                    };
                                    if let Err(e) = swarm
                                        .behaviour_mut()
                                        .gossipsub
                                        .publish(IdentTopic::new(BIDS), payload)
                                    {
                                        tracing::warn!(error = %e, "publish bid");
                                    } else {
                                        tracing::info!(request_id = %req.request_id, "published bid");
                                    }
                                }
                            }
                            Err(e) => tracing::warn!(error = %e, "decode InferenceRequest"),
                        }
                    }
                }
                SwarmEvent::Behaviour(PinaivuBehaviourEvent::Completion(
                    request_response::Event::Message { message, .. }
                )) => {
                    if let request_response::Message::Response { request_id, response } = message {
                        let req = pending_acks.remove(&request_id);
                        log_completion_response(req, response);
                    }
                }
                SwarmEvent::Behaviour(PinaivuBehaviourEvent::Completion(
                    request_response::Event::OutboundFailure { request_id, error, .. }
                )) => {
                    let req = pending_acks.remove(&request_id);
                    tracing::warn!(?req, ?error, "completion outbound failure");
                }
                SwarmEvent::Behaviour(PinaivuBehaviourEvent::Inference(
                    request_response::Event::Message {
                        message: request_response::Message::Request { request, channel, .. },
                        ..
                    }
                )) => {
                    tracing::info!(
                        request_id = %request.dispatch_token.request_id,
                        "received inference dispatch over libp2p"
                    );
                    let state = state.clone();
                    let reply_tx = self_cmd_tx.clone();
                    tokio::spawn(async move {
                        let reply = run_dispatch(state, request).await;
                        let _ = reply_tx.send(Command::SendInferenceReply { channel, reply }).await;
                    });
                }
                _ => {}
            }
        }
    }
}

/// Run an inbound `InferenceDispatch` through the same pipeline the
/// HTTP `/v1/inference` handler uses, and build the `InferenceReply`
/// to send back. Runs off the event loop (spawned by the caller) since
/// inference can take seconds and must not block the swarm.
async fn run_dispatch(state: http::State, dispatch: InferenceDispatch) -> InferenceReply {
    let request_id = dispatch.dispatch_token.request_id;
    let session_id = dispatch.dispatch_token.session_id;

    let session_key = if dispatch.session_key.is_empty() {
        None
    } else {
        match B64.decode(dispatch.session_key.as_bytes()) {
            Ok(raw) => match <[u8; 32]>::try_from(raw.as_slice()) {
                Ok(arr) => Some(arr),
                Err(_) => {
                    return InferenceReply {
                        request_id,
                        session_id,
                        content: String::new(),
                        input_tokens: 0,
                        output_tokens: 0,
                        latency_ms: 0,
                        error: Some("session_key must decode to 32 bytes".into()),
                    };
                }
            },
            Err(e) => {
                return InferenceReply {
                    request_id,
                    session_id,
                    content: String::new(),
                    input_tokens: 0,
                    output_tokens: 0,
                    latency_ms: 0,
                    error: Some(format!("session_key not valid base64: {e}")),
                };
            }
        }
    };

    match run_inference_job(
        &state,
        dispatch.dispatch_token,
        session_key,
        dispatch.new_user_message,
        dispatch.memwal_context,
    )
    .await
    {
        Ok(out) => InferenceReply {
            request_id: out.request_id,
            session_id: out.session_id,
            content: out.content,
            input_tokens: out.input_tokens,
            output_tokens: out.output_tokens,
            latency_ms: out.latency_ms,
            error: None,
        },
        Err(e) => InferenceReply {
            request_id,
            session_id,
            content: String::new(),
            input_tokens: 0,
            output_tokens: 0,
            latency_ms: 0,
            error: Some(e.message),
        },
    }
}

fn log_completion_response(req: Option<uuid::Uuid>, resp: CompletionResponse) {
    if resp.accepted {
        tracing::info!(?req, "coordinator accepted CompletionAck");
    } else {
        tracing::warn!(?req, reason = ?resp.reason, "coordinator rejected CompletionAck");
    }
}
