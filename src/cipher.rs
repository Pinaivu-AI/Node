//! AES-256-GCM symmetric encryption used for Walrus-stored session
//! blobs and KV-cache blocks.
//!
//! The key is supplied by the client on every turn (via the HTTPS POST
//! body, see `http::InferenceReq.session_key`). The node uses it only
//! for the duration of the turn and zeroizes it on completion.
//!
//! Wire format: `nonce_12_bytes || ciphertext_with_tag`. A fresh 12-byte
//! nonce is sampled per `seal()` call from the OS RNG.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use rand::RngCore;
use thiserror::Error;

const NONCE_LEN: usize = 12;

#[derive(Debug, Error)]
pub enum CipherError {
    #[error("ciphertext too short to contain a nonce")]
    TooShort,
    #[error("AEAD authentication failed (wrong key or tampered ciphertext)")]
    AuthFailed,
}

/// Encrypt `plaintext` with `key` under a fresh random nonce. Returns
/// `nonce || ciphertext_with_tag`.
pub fn seal(key: &[u8; 32], plaintext: &[u8]) -> Vec<u8> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let mut nonce_bytes = [0u8; NONCE_LEN];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ct = cipher
        .encrypt(nonce, plaintext)
        .expect("AES-GCM encrypt is infallible for valid key + nonce");
    let mut out = Vec::with_capacity(NONCE_LEN + ct.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ct);
    out
}

/// Decrypt `nonce || ciphertext_with_tag` previously produced by [`seal`].
pub fn open(key: &[u8; 32], blob: &[u8]) -> Result<Vec<u8>, CipherError> {
    if blob.len() < NONCE_LEN {
        return Err(CipherError::TooShort);
    }
    let (nonce_bytes, ct) = blob.split_at(NONCE_LEN);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    cipher
        .decrypt(Nonce::from_slice(nonce_bytes), ct)
        .map_err(|_| CipherError::AuthFailed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let key = [7u8; 32];
        let pt = b"hello pinaivu";
        let ct = seal(&key, pt);
        assert_ne!(&ct[NONCE_LEN..], pt);
        let got = open(&key, &ct).unwrap();
        assert_eq!(got, pt);
    }

    #[test]
    fn wrong_key_fails() {
        let ct = seal(&[1u8; 32], b"secret");
        assert!(matches!(open(&[2u8; 32], &ct), Err(CipherError::AuthFailed)));
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let key = [3u8; 32];
        let mut ct = seal(&key, b"don't change me");
        let last = ct.len() - 1;
        ct[last] ^= 0x01;
        assert!(matches!(open(&key, &ct), Err(CipherError::AuthFailed)));
    }

    #[test]
    fn short_blob_rejected() {
        assert!(matches!(open(&[0u8; 32], b"too short"), Err(CipherError::TooShort)));
    }

    #[test]
    fn fresh_nonce_per_call() {
        let key = [9u8; 32];
        let a = seal(&key, b"same plaintext");
        let b = seal(&key, b"same plaintext");
        assert_ne!(a, b, "nonce reuse would produce identical ciphertexts");
    }
}
