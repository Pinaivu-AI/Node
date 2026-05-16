//! libp2p swarm — drives the gossipsub bid loop and handles outbound
//! `CompletionAck` request-response messages to the coordinator.
//!
//! Spawned once at startup; returns a [`Handle`] the HTTP layer uses
//! to send commands into the event loop.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use ed25519_dalek::SigningKey;
use futures::StreamExt;
use libp2p::{
    gossipsub::{self, IdentTopic},
    multiaddr::Protocol,
    request_response::{self, OutboundRequestId},
    swarm::SwarmEvent,
    Multiaddr, PeerId, Swarm,
};
use pinaivu_protocol::mesh::{
    behaviour::{libp2p_identity_from_ed25519_secret, PinaivuBehaviour, PinaivuBehaviourEvent},
    completion_proto::{CompletionAck, CompletionResponse},
    topics::{BIDS, INFERENCE_ANY},
};
use pinaivu_protocol::InferenceRequest;
use tokio::sync::mpsc;

use crate::bidder::{build_bid, BidConfig};
use crate::inflight::Inflight;

pub struct Config {
    pub identity: SigningKey,
    pub coordinator_addr: Multiaddr,
    pub listen_addr: Multiaddr,
    pub model: String,
    pub price_per_1k_nanox: u64,
    pub advertise_url: String,
    pub inflight: Arc<Inflight>,
}

pub struct Handle {
    pub peer_id: PeerId,
    pub cmd_tx: mpsc::Sender<Command>,
}

pub enum Command {
    /// Send a `CompletionAck` to the coordinator. Fire-and-forget at the
    /// channel level; the event loop awaits the libp2p response and
    /// logs it but the caller doesn't block on it.
    SendCompletionAck(CompletionAck),
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
    };

    let (cmd_tx, cmd_rx) = mpsc::channel(32);

    tokio::spawn(run_event_loop(swarm, cmd_rx, bid_cfg, cfg.inflight, coord_peer));

    Ok(Handle {
        peer_id: local_peer_id,
        cmd_tx,
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
) {
    // Track in-flight outbound completion requests so we can log their
    // results without blocking the HTTP path.
    let mut pending_acks: std::collections::HashMap<OutboundRequestId, uuid::Uuid> =
        std::collections::HashMap::new();

    loop {
        tokio::select! {
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
                }
            }
            ev = swarm.select_next_some() => match ev {
                SwarmEvent::NewListenAddr { address, .. } => {
                    tracing::info!(addr = %address, "listening");
                }
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    tracing::info!(peer = %peer_id, "connection established");
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
                _ => {}
            }
        }
    }
}

fn log_completion_response(req: Option<uuid::Uuid>, resp: CompletionResponse) {
    if resp.accepted {
        tracing::info!(?req, "coordinator accepted CompletionAck");
    } else {
        tracing::warn!(?req, reason = ?resp.reason, "coordinator rejected CompletionAck");
    }
}
