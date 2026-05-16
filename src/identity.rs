//! Ed25519 keypair persisted to disk so the node has a stable PeerId
//! across restarts. Also fetches the coordinator's signing pubkey at
//! startup so dispatch tokens can be verified locally.

use std::path::Path;

use anyhow::{Context, Result};
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use serde::Deserialize;

/// Load the Ed25519 identity from `path`, or create + persist a new
/// one if the file doesn't exist.
pub fn load_or_create(path: &Path) -> Result<SigningKey> {
    if path.exists() {
        let bytes = std::fs::read(path).with_context(|| format!("read identity {}", path.display()))?;
        let arr: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("identity file is not 32 bytes"))?;
        return Ok(SigningKey::from_bytes(&arr));
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create {}", parent.display()))?;
    }
    let key = SigningKey::generate(&mut OsRng);
    std::fs::write(path, key.to_bytes())
        .with_context(|| format!("write identity {}", path.display()))?;
    Ok(key)
}

#[derive(Deserialize)]
struct EnclaveHealth {
    public_key_hex: String,
}

/// Fetch the coordinator's signing pubkey via `GET /enclave_health`.
/// We use this to verify dispatch tokens we receive over HTTP before
/// running any inference.
pub async fn fetch_coordinator_pubkey(coordinator_http: &str) -> Result<[u8; 32]> {
    let url = format!("{}/enclave_health", coordinator_http.trim_end_matches('/'));
    let resp: EnclaveHealth = reqwest::get(&url)
        .await
        .with_context(|| format!("GET {url}"))?
        .json()
        .await
        .context("decode enclave_health json")?;
    let bytes = hex::decode(&resp.public_key_hex).context("decode pubkey hex")?;
    bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("coordinator pubkey is not 32 bytes"))
}
