use async_trait::async_trait;
use liazo_core::{
    catalog::{SourceRegistry, SourceResolutionError, SourceSnapshot},
    control::{Failure, FailureCode},
    protocol::{ImageSubscription, SourceKey},
    wire::Frame,
};
use liazo_runtime::backend::{AudioTrack, CaptureBackend};
use std::sync::Mutex;
use uuid::Uuid;

#[derive(Default)]
pub struct MacCapture {
    registry: std::sync::Arc<Mutex<SourceRegistry>>,
    discovery: std::sync::Arc<tokio::sync::Mutex<()>>,
}

fn native(error: liazo_macos::NativeError) -> Failure {
    Failure::new(FailureCode::Unavailable, error.to_string())
}

impl MacCapture {
    fn resolve(&self, source: &SourceKey) -> Result<(), Failure> {
        self.registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .resolve(source)
            .map(|_| ())
            .map_err(|error| {
                let code = match error {
                    SourceResolutionError::Missing => FailureCode::SourceMissing,
                    SourceResolutionError::StaleGeneration => FailureCode::SourceChanged,
                    SourceResolutionError::PermissionRequired => FailureCode::PermissionRequired,
                    SourceResolutionError::Unavailable => FailureCode::Unavailable,
                };
                Failure::new(code, error.to_string())
            })
    }
}

#[async_trait]
impl CaptureBackend for MacCapture {
    fn platform(&self) -> &'static str {
        "macos"
    }
    fn clock_ns(&self) -> u64 {
        liazo_macos::clock_ns()
    }
    async fn discover(&self) -> Result<SourceSnapshot, Failure> {
        let guard = self.discovery.clone().lock_owned().await;
        let registry = self.registry.clone();
        // Keep the native callback and its serialization guard alive if a session closes.
        tokio::spawn(async move {
            let _guard = guard;
            let discovery = liazo_macos::discover_sources().await.map_err(native)?;
            discovery
                .apply(&mut registry.lock().unwrap_or_else(|e| e.into_inner()))
                .map_err(native)
        })
        .await
        .map_err(|_| Failure::new(FailureCode::Internal, "source discovery failed"))?
    }
    async fn image(
        &self,
        settings: &ImageSubscription,
        previous: Option<[u8; 32]>,
        sequence: u64,
        revision: u64,
    ) -> Result<Frame, Failure> {
        self.resolve(&settings.source)?;
        liazo_macos::screenshot(settings, previous)
            .await
            .map_err(native)?
            .into_image_frame(settings.source.clone(), sequence, revision)
            .map_err(native)
    }
    async fn audio(
        &self,
        source: &SourceKey,
        stream: Uuid,
    ) -> Result<Box<dyn AudioTrack>, Failure> {
        self.resolve(source)?;
        let capture = liazo_macos::AudioStream::start(&source.id)
            .await
            .map_err(native)?;
        Ok(Box::new(MacAudio {
            capture,
            source: source.clone(),
            stream,
        }))
    }
}

struct MacAudio {
    capture: liazo_macos::AudioStream,
    source: SourceKey,
    stream: Uuid,
}
#[async_trait]
impl AudioTrack for MacAudio {
    async fn next(&mut self) -> Result<Option<Frame>, Failure> {
        self.capture
            .next()
            .await
            .map(|packet| {
                packet
                    .map_err(native)?
                    .into_audio_frame(self.source.clone(), self.stream)
                    .map_err(native)
            })
            .transpose()
    }
    async fn stop(self: Box<Self>) {
        self.capture.stop().await;
    }
}
