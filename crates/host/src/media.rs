use bytes::Bytes;
use liazo_core::{protocol::SourceKey, wire::ImageMeta};
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use uuid::Uuid;

const BUDGET: usize = 64 * 1024 * 1024;
const MAX_ITEMS: usize = 64;
const TTL: Duration = Duration::from_secs(30);

struct Item {
    bytes: Bytes,
    mime: &'static str,
    expires: Instant,
}
#[derive(Clone, Default)]
pub(crate) struct Media(Arc<Mutex<HashMap<Uuid, Item>>>);

#[derive(Serialize)]
pub struct MediaRef {
    pub id: Uuid,
    pub source: SourceKey,
    pub sequence: u64,
    pub content_type: &'static str,
    pub length: usize,
}

impl Media {
    pub fn publish(&self, metadata: &ImageMeta, bytes: Bytes) -> Option<MediaRef> {
        let mut items = self.0.lock().unwrap_or_else(|e| e.into_inner());
        items.retain(|_, item| item.expires > Instant::now());
        let used: usize = items.values().map(|item| item.bytes.len()).sum();
        if items.len() >= MAX_ITEMS || bytes.len() > BUDGET.saturating_sub(used) {
            return None;
        }
        let id = Uuid::new_v4();
        let reference = MediaRef {
            id,
            source: metadata.source.clone(),
            sequence: metadata.sequence,
            content_type: metadata.format.mime_type(),
            length: bytes.len(),
        };
        items.insert(
            id,
            Item {
                bytes,
                mime: reference.content_type,
                expires: Instant::now() + TTL,
            },
        );
        Some(reference)
    }
    pub fn get(&self, id: Uuid) -> Option<(Bytes, &'static str)> {
        let mut items = self.0.lock().unwrap_or_else(|e| e.into_inner());
        items.retain(|_, item| item.expires > Instant::now());
        items.get(&id).map(|item| (item.bytes.clone(), item.mime))
    }
    pub fn trim(&self) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, item| item.expires > Instant::now());
    }
}
