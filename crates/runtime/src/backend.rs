use async_trait::async_trait;
use liazo_core::{
    catalog::SourceSnapshot,
    control::Failure,
    protocol::{ImageSubscription, SourceKey},
    wire::Frame,
};
use uuid::Uuid;

#[async_trait]
pub trait CaptureBackend: Send + Sync + 'static {
    fn platform(&self) -> &'static str;
    fn clock_ns(&self) -> u64;
    async fn discover(&self) -> Result<SourceSnapshot, Failure>;
    async fn image(
        &self,
        settings: &ImageSubscription,
        previous: Option<[u8; 32]>,
        sequence: u64,
        revision: u64,
    ) -> Result<Frame, Failure>;
    async fn audio(&self, source: &SourceKey, stream: Uuid)
    -> Result<Box<dyn AudioTrack>, Failure>;
}

#[async_trait]
pub trait AudioTrack: Send {
    async fn next(&mut self) -> Result<Option<Frame>, Failure>;
    async fn stop(self: Box<Self>);
}
