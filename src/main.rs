//! Pinaivu GPU node daemon.
//!
//! Joins the coordinator's libp2p mesh, bids on inference auctions,
//! serves jobs via HTTP, and sends a signed CompletionAck back to the
//! coordinator after each completed job.

mod bidder;
mod cipher;
mod completion;
mod context;
mod http;
mod identity;
mod inflight;
mod mesh;
mod ollama;
mod walrus;

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use libp2p::Multiaddr;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug, Clone)]
#[command(version, about = "Pinaivu GPU node daemon")]
struct Args {
    /// Coordinator's libp2p multiaddr including the /p2p/<peer_id> suffix.
    #[arg(long, env = "COORDINATOR_ADDR")]
    coordinator_addr: Multiaddr,

    /// Coordinator's HTTP base URL — used at startup to fetch its
    /// `/enclave_health` so we can verify dispatch tokens against the
    /// same signing key.
    #[arg(long, env = "COORDINATOR_HTTP", default_value = "http://127.0.0.1:4000")]
    coordinator_http: String,

    /// HTTP address this node binds. Also the URL advertised to clients
    /// in our `InferenceBid.http_endpoint` unless `--advertise-url` is
    /// set to something else (useful behind NAT / port-forwarding).
    #[arg(long, default_value = "127.0.0.1:5000")]
    listen: String,

    /// URL to advertise to clients in bids. Defaults to `http://{listen}`.
    #[arg(long)]
    advertise_url: Option<String>,

    /// Ollama base URL.
    #[arg(long, env = "OLLAMA_URL", default_value = "http://localhost:11434")]
    ollama_url: String,

    /// Model to advertise + serve. Must already be `ollama pull`'d.
    #[arg(long, default_value = "llama3")]
    model: String,

    /// Asking price in MIST per 1k tokens (1 MIST = 1 Sui base unit).
    #[arg(long, default_value_t = 1_000_000)]
    price_per_1k_nanox: u64,

    /// Sui address advertised in every InferenceBid as the payout
    /// destination. The coordinator names this address in the on-chain
    /// vault::settle call when the job completes. Optional in dev.
    #[arg(long, default_value = "")]
    payout_address: String,


    /// Persisted Ed25519 identity file. Created on first run; reused
    /// thereafter so this node has a stable PeerId.
    #[arg(long)]
    identity_file: Option<std::path::PathBuf>,

    /// Postgres URL for the context layer (sessions, turns, facts,
    /// summaries, warm-node tracking). When unset the node runs in
    /// stateless single-turn mode.
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("pinaivu_node=info")))
        .init();

    let args = Args::parse();

    let identity_path = args
        .identity_file
        .clone()
        .or_else(|| dirs::home_dir().map(|h| h.join(".pinaivu-node").join("identity.key")))
        .context("could not resolve identity file path")?;

    let identity = identity::load_or_create(&identity_path)?;
    tracing::info!(
        identity_file = %identity_path.display(),
        node_pubkey = %hex::encode(identity.verifying_key().to_bytes()),
        "node identity ready"
    );

    let advertise_url = args
        .advertise_url
        .clone()
        .unwrap_or_else(|| format!("http://{}", args.listen));

    let inflight = Arc::new(inflight::Inflight::new());

    // Fetch the coordinator's signing pubkey so we can verify dispatch
    // tokens. Dispatch tokens that don't verify under this key get
    // rejected by the HTTP layer.
    let coord_pubkey = identity::fetch_coordinator_pubkey(&args.coordinator_http).await?;
    tracing::info!(
        coordinator_pubkey = %hex::encode(coord_pubkey),
        "coordinator pubkey discovered"
    );

    // Build the context-layer plumbing if both Postgres and Walrus are
    // configured. Either one alone is a mis-config — fail loudly so
    // operators notice instead of silently dropping history.
    let walrus_configured =
        std::env::var_os("WALRUS_LOCAL_DIR").is_some() || std::env::var_os("WALRUS_PUBLISHER_URL").is_some();
    let (pg, walrus) = match (&args.database_url, walrus_configured) {
        (Some(url), true) => {
            let pg = sqlx::postgres::PgPoolOptions::new()
                .max_connections(8)
                .connect(url)
                .await
                .context("connect Postgres for context layer")?;
            let walrus = walrus::WalrusClient::from_env().context("init Walrus client")?;
            tracing::info!("context layer enabled (Postgres + Walrus)");
            (Some(pg), Some(Arc::new(walrus)))
        }
        (None, false) => {
            tracing::warn!(
                "stateless mode — DATABASE_URL and WALRUS_* unset, conversation history will not be retained"
            );
            (None, None)
        }
        _ => {
            anyhow::bail!(
                "context layer requires BOTH DATABASE_URL and WALRUS_LOCAL_DIR (or WALRUS_PUBLISHER_URL+WALRUS_AGGREGATOR_URL)"
            );
        }
    };

    // Spawn the libp2p mesh; returns a handle the HTTP layer uses to
    // send CompletionAck via request-response, plus the fully-built
    // `http::State` (the event loop needs the same state to run
    // inbound `InferenceDispatch` jobs that arrive over libp2p).
    let mesh_handle = mesh::spawn(mesh::Config {
        identity: identity.clone(),
        coordinator_addr: args.coordinator_addr.clone(),
        listen_addr: "/ip4/0.0.0.0/tcp/0".parse().unwrap(),
        model: args.model.clone(),
        price_per_1k_nanox: args.price_per_1k_nanox,
        advertise_url: advertise_url.clone(),
        payout_address: args.payout_address.clone(),
        inflight: inflight.clone(),
        ollama_url: args.ollama_url.clone(),
        coord_pubkey,
        pg,
        walrus,
    })
    .await?;

    http::serve(&args.listen, mesh_handle.state).await?;
    Ok(())
}
