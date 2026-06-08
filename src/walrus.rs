//! Walrus client for session blobs + (later) KV-cache blocks.
//!
//! Walrus is content-addressed — `put_blob(bytes)` returns a `blob_id`
//! and `get_blob(blob_id)` retrieves it. The `(user_address, session_id)
//! → blob_id` mapping is kept in Postgres (`sessions.walrus_blob_id`),
//! not in Walrus itself, so this module does not need to know about
//! either identifier.
//!
//! Two backends:
//! * [`WalrusClient::Http`] talks to a real Walrus publisher/aggregator
//!   pair (configured via `WALRUS_PUBLISHER_URL` + `WALRUS_AGGREGATOR_URL`).
//! * [`WalrusClient::LocalDir`] writes blobs as files under a directory
//!   keyed by `sha256(bytes)`. Used in tests and local dev so the loop
//!   can run without a Walrus instance.

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::path::PathBuf;

#[derive(Clone)]
pub enum WalrusClient {
    Http {
        publisher: String,
        aggregator: String,
        http: reqwest::Client,
        epochs: u32,
    },
    LocalDir {
        root: PathBuf,
    },
}

impl WalrusClient {
    /// Build from environment.
    ///
    /// * If `WALRUS_LOCAL_DIR` is set, use the filesystem mock.
    /// * Else require `WALRUS_PUBLISHER_URL` + `WALRUS_AGGREGATOR_URL`.
    pub fn from_env() -> Result<Self> {
        if let Ok(dir) = std::env::var("WALRUS_LOCAL_DIR") {
            let root = PathBuf::from(dir);
            std::fs::create_dir_all(&root)
                .with_context(|| format!("create WALRUS_LOCAL_DIR {root:?}"))?;
            return Ok(Self::LocalDir { root });
        }
        let publisher = std::env::var("WALRUS_PUBLISHER_URL")
            .context("WALRUS_PUBLISHER_URL not set (and no WALRUS_LOCAL_DIR)")?;
        let aggregator = std::env::var("WALRUS_AGGREGATOR_URL")
            .context("WALRUS_AGGREGATOR_URL not set")?;
        let epochs = std::env::var("WALRUS_EPOCHS")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(5);
        Ok(Self::Http {
            publisher,
            aggregator,
            http: reqwest::Client::new(),
            epochs,
        })
    }

    pub fn local_dir(root: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root)?;
        Ok(Self::LocalDir { root })
    }

    /// Upload `bytes` and return the resulting `blob_id`.
    pub async fn put_blob(&self, bytes: &[u8]) -> Result<String> {
        match self {
            Self::LocalDir { root } => {
                let id = local_blob_id(bytes);
                let path = root.join(&id);
                tokio::fs::write(&path, bytes)
                    .await
                    .with_context(|| format!("write {path:?}"))?;
                Ok(id)
            }
            Self::Http {
                publisher,
                http,
                epochs,
                ..
            } => {
                let url = format!("{}/v1/blobs?epochs={}", publisher.trim_end_matches('/'), epochs);
                let resp = http
                    .put(&url)
                    .body(bytes.to_vec())
                    .send()
                    .await
                    .context("walrus put_blob send")?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(anyhow!("walrus put_blob HTTP {status}: {body}"));
                }
                let body: PutResponse = resp.json().await.context("walrus put_blob decode")?;
                body.blob_id()
                    .ok_or_else(|| anyhow!("walrus put_blob: no blobId in response"))
            }
        }
    }

    /// Fetch the blob behind `blob_id`. Returns `Ok(None)` if Walrus
    /// reports the blob is unknown / expired.
    pub async fn get_blob(&self, blob_id: &str) -> Result<Option<Vec<u8>>> {
        match self {
            Self::LocalDir { root } => {
                let path = root.join(blob_id);
                match tokio::fs::read(&path).await {
                    Ok(bytes) => Ok(Some(bytes)),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(e).with_context(|| format!("read {path:?}")),
                }
            }
            Self::Http { aggregator, http, .. } => {
                let url = format!(
                    "{}/v1/blobs/{}",
                    aggregator.trim_end_matches('/'),
                    blob_id
                );
                let resp = http.get(&url).send().await.context("walrus get_blob send")?;
                if resp.status() == reqwest::StatusCode::NOT_FOUND {
                    return Ok(None);
                }
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(anyhow!("walrus get_blob HTTP {status}: {body}"));
                }
                Ok(Some(resp.bytes().await?.to_vec()))
            }
        }
    }
}

fn local_blob_id(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

// Walrus publisher returns one of two shapes:
//   { "newlyCreated":   { "blobObject": { "blobId": "..." }, ... } }
//   { "alreadyCertified": { "blobId": "...", ... } }
#[derive(Debug, Deserialize)]
struct PutResponse {
    #[serde(rename = "newlyCreated")]
    newly_created: Option<NewlyCreated>,
    #[serde(rename = "alreadyCertified")]
    already_certified: Option<AlreadyCertified>,
}

#[derive(Debug, Deserialize)]
struct NewlyCreated {
    #[serde(rename = "blobObject")]
    blob_object: BlobObject,
}

#[derive(Debug, Deserialize)]
struct BlobObject {
    #[serde(rename = "blobId")]
    blob_id: String,
}

#[derive(Debug, Deserialize)]
struct AlreadyCertified {
    #[serde(rename = "blobId")]
    blob_id: String,
}

impl PutResponse {
    fn blob_id(self) -> Option<String> {
        self.newly_created
            .map(|nc| nc.blob_object.blob_id)
            .or_else(|| self.already_certified.map(|ac| ac.blob_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_dir_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let w = WalrusClient::local_dir(tmp.path().to_path_buf()).unwrap();
        let id = w.put_blob(b"the quick brown fox").await.unwrap();
        let back = w.get_blob(&id).await.unwrap().unwrap();
        assert_eq!(back, b"the quick brown fox");
    }

    #[tokio::test]
    async fn local_dir_missing_blob_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let w = WalrusClient::local_dir(tmp.path().to_path_buf()).unwrap();
        assert!(w.get_blob("nonexistent").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn local_dir_dedupes_identical_payloads() {
        let tmp = tempfile::tempdir().unwrap();
        let w = WalrusClient::local_dir(tmp.path().to_path_buf()).unwrap();
        let a = w.put_blob(b"same bytes").await.unwrap();
        let b = w.put_blob(b"same bytes").await.unwrap();
        assert_eq!(a, b, "content-addressed local mock should be stable");
    }
}
