//! In-memory map of request_ids we bid on. Lets the HTTP layer reject
//! dispatch tokens for requests we never bid on (defensive — the token
//! signature is the real authority).

use std::collections::HashSet;
use std::sync::Mutex;

use uuid::Uuid;

pub struct Inflight {
    set: Mutex<HashSet<Uuid>>,
}

impl Inflight {
    pub fn new() -> Self {
        Self { set: Mutex::new(HashSet::new()) }
    }
    pub fn record_bid(&self, id: Uuid) {
        self.set.lock().unwrap().insert(id);
    }
    pub fn contains(&self, id: &Uuid) -> bool {
        self.set.lock().unwrap().contains(id)
    }
    pub fn forget(&self, id: &Uuid) {
        self.set.lock().unwrap().remove(id);
    }
}

impl Default for Inflight {
    fn default() -> Self {
        Self::new()
    }
}
