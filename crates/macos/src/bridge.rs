use std::{
    ffi::{CString, c_void},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use liazo_core::protocol::{ImageFormat, ImageSubscription, MAX_MEDIA_BYTES};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

#[allow(
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    dead_code
)]
mod ffi {
    include!(concat!(env!("OUT_DIR"), "/capture.rs"));
}

static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, thiserror::Error)]
pub enum NativeError {
    #[error("{0}")]
    Failure(String),
    #[error("native capture request was cancelled")]
    Cancelled,
}

pub struct NativePacket {
    pub metadata: Value,
    pub payload: Bytes,
}

fn failure(message: &str) -> NativeError {
    NativeError::Failure(message.into())
}

// C guarantees the buffers remain valid until the callback returns.
unsafe fn packet(
    status: i32,
    meta: *const u8,
    meta_len: usize,
    bytes: *const u8,
    len: usize,
) -> Result<NativePacket, NativeError> {
    if !matches!(status, 0 | 1)
        || meta_len == 0
        || meta_len > 64 * 1024
        || meta.is_null()
        || len > MAX_MEDIA_BYTES
        || (len > 0 && bytes.is_null())
    {
        return Err(failure("invalid native response bounds"));
    }
    let metadata: Value =
        serde_json::from_slice(unsafe { std::slice::from_raw_parts(meta, meta_len) })
            .map_err(|_| failure("invalid native metadata"))?;
    if !metadata.is_object() {
        return Err(failure("invalid native metadata shape"));
    }
    if status == 1 {
        return Err(failure(
            metadata
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("native capture failed"),
        ));
    }
    let payload = if len == 0 {
        Bytes::new()
    } else {
        Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(bytes, len) })
    };
    Ok(NativePacket { metadata, payload })
}

type ReplySender = oneshot::Sender<Result<NativePacket, NativeError>>;

unsafe extern "C" fn once(
    user: *mut c_void,
    _: u64,
    status: i32,
    meta: *const u8,
    meta_len: usize,
    bytes: *const u8,
    len: usize,
) {
    let sender = unsafe { Box::from_raw(user.cast::<ReplySender>()) };
    let _ = sender.send(unsafe { packet(status, meta, meta_len, bytes, len) });
}

fn reply() -> (
    *mut c_void,
    oneshot::Receiver<Result<NativePacket, NativeError>>,
) {
    let (sender, receiver) = oneshot::channel();
    (Box::into_raw(Box::new(sender)).cast(), receiver)
}

pub async fn sources() -> Result<Value, NativeError> {
    let (user, receiver) = reply();
    unsafe {
        ffi::liazo_native_sources(
            NEXT_REQUEST.fetch_add(1, Ordering::Relaxed),
            Some(once),
            user,
        )
    };
    Ok(receiver
        .await
        .map_err(|_| NativeError::Cancelled)??
        .metadata)
}

pub async fn screenshot(
    settings: &ImageSubscription,
    previous: Option<[u8; 32]>,
) -> Result<NativePacket, NativeError> {
    settings.validate().map_err(|e| failure(&e.to_string()))?;
    let source =
        CString::new(settings.source.id.as_str()).map_err(|_| failure("invalid source"))?;
    let mut options = ffi::LiazoImageOptions {
        max_width: settings.max_width.unwrap_or(0),
        format: if settings.format == ImageFormat::Png {
            0
        } else {
            1
        },
        quality: settings.quality,
        include_cursor: u8::from(settings.include_cursor),
        has_previous: u8::from(previous.is_some()),
        previous: previous.unwrap_or([0; 32]),
        ..Default::default()
    };
    if let Some(crop) = settings.crop {
        options.has_crop = 1;
        options.crop_x = crop.x;
        options.crop_y = crop.y;
        options.crop_width = crop.width;
        options.crop_height = crop.height;
    }
    let (user, receiver) = reply();
    unsafe {
        ffi::liazo_native_image(
            NEXT_REQUEST.fetch_add(1, Ordering::Relaxed),
            source.as_ptr(),
            &options,
            Some(once),
            user,
        )
    };
    receiver.await.map_err(|_| NativeError::Cancelled)?
}

struct AudioContext {
    sender: mpsc::Sender<Result<NativePacket, NativeError>>,
    ready: Mutex<Option<oneshot::Sender<Result<(), NativeError>>>>,
    overrun: AtomicBool,
}

unsafe extern "C" fn audio_callback(
    user: *mut c_void,
    _: u64,
    status: i32,
    meta: *const u8,
    meta_len: usize,
    bytes: *const u8,
    len: usize,
) {
    let context = unsafe { &*user.cast::<AudioContext>() };
    if status == 2 {
        if let Some(ready) = context
            .ready
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = ready.send(Ok(()));
        }
        return;
    }
    if context.sender.is_closed() {
        return;
    }
    if status == 0 && context.sender.capacity() == 0 {
        context.overrun.store(true, Ordering::Release);
        return;
    }
    let mut result = unsafe { packet(status, meta, meta_len, bytes, len) };
    if status == 1 {
        if let Some(ready) = context
            .ready
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let message = result
                .as_ref()
                .err()
                .map(ToString::to_string)
                .unwrap_or_default();
            let _ = ready.send(Err(NativeError::Failure(message)));
        }
    }
    let discontinuity = context.overrun.swap(false, Ordering::AcqRel);
    if discontinuity {
        if let Ok(packet) = &mut result {
            packet.metadata["discontinuity"] = Value::Bool(true);
        }
    }
    if context.sender.try_send(result).is_err() {
        context.overrun.store(true, Ordering::Release);
    }
}

struct StopContext {
    _native: Arc<AudioContext>,
    complete: Option<oneshot::Sender<()>>,
}

unsafe extern "C" fn stopped(
    user: *mut c_void,
    _: u64,
    _: i32,
    _: *const u8,
    _: usize,
    _: *const u8,
    _: usize,
) {
    let mut context = unsafe { Box::from_raw(user.cast::<StopContext>()) };
    if let Some(sender) = context.complete.take() {
        let _ = sender.send(());
    }
}

pub struct AudioStream {
    id: u64,
    context: Arc<AudioContext>,
    receiver: mpsc::Receiver<Result<NativePacket, NativeError>>,
    active: bool,
}

impl AudioStream {
    pub async fn start(source: &str) -> Result<Self, NativeError> {
        let source = CString::new(source).map_err(|_| failure("invalid audio source"))?;
        let (sender, receiver) = mpsc::channel(16);
        let (ready, readiness) = oneshot::channel();
        let context = Arc::new(AudioContext {
            sender,
            ready: Mutex::new(Some(ready)),
            overrun: AtomicBool::new(false),
        });
        let id = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
        let native = Arc::into_raw(context.clone()).cast_mut().cast();
        let stream = Self {
            id,
            context,
            receiver,
            active: true,
        };
        unsafe { ffi::liazo_native_audio_start(id, source.as_ptr(), Some(audio_callback), native) };
        readiness.await.map_err(|_| NativeError::Cancelled)??;
        Ok(stream)
    }

    pub async fn next(&mut self) -> Option<Result<NativePacket, NativeError>> {
        self.receiver.recv().await
    }

    pub async fn stop(mut self) {
        let (sender, receiver) = oneshot::channel();
        self.stop_inner(Some(sender));
        let _ = receiver.await;
    }

    fn stop_inner(&mut self, complete: Option<oneshot::Sender<()>>) {
        if !self.active {
            return;
        }
        self.active = false;
        // Take over the strong reference owned by C. The stop acknowledgement occurs
        // after every data callback has returned, then releases this last native owner.
        let native = unsafe { Arc::from_raw(Arc::as_ptr(&self.context)) };
        let user = Box::into_raw(Box::new(StopContext {
            _native: native,
            complete,
        }))
        .cast();
        unsafe { ffi::liazo_native_audio_stop(self.id, Some(stopped), user) };
    }
}

impl Drop for AudioStream {
    fn drop(&mut self) {
        self.stop_inner(None);
    }
}

pub fn run_main_loop() {
    unsafe { ffi::liazo_native_runloop() };
}
pub fn stop_main_loop() {
    unsafe { ffi::liazo_native_stoploop() };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_data_is_owned_before_native_buffers_disappear() {
        let mut metadata = br#"{"width":12}"#.to_vec();
        let mut payload = [1, 2, 3];
        let result = unsafe {
            packet(
                0,
                metadata.as_ptr(),
                metadata.len(),
                payload.as_ptr(),
                payload.len(),
            )
        }
        .unwrap();
        metadata.fill(0);
        payload.fill(0);
        assert_eq!(result.metadata["width"], 12);
        assert_eq!(result.payload.as_ref(), &[1, 2, 3]);
    }

    #[test]
    fn invalid_callback_status_is_not_published_as_data() {
        let metadata = b"{}";
        assert!(unsafe { packet(99, metadata.as_ptr(), 2, std::ptr::null(), 0) }.is_err());
    }

    #[test]
    fn dropped_audio_is_reported_on_the_next_delivered_chunk() {
        let (sender, mut receiver) = mpsc::channel(1);
        let context = Arc::new(AudioContext {
            sender,
            ready: Mutex::new(None),
            overrun: AtomicBool::new(false),
        });
        let user = Arc::as_ptr(&context).cast_mut().cast();
        let metadata = br#"{"discontinuity":false}"#;
        unsafe {
            audio_callback(
                user,
                1,
                0,
                metadata.as_ptr(),
                metadata.len(),
                std::ptr::null(),
                0,
            );
            audio_callback(
                user,
                1,
                0,
                metadata.as_ptr(),
                metadata.len(),
                std::ptr::null(),
                0,
            );
        }
        receiver.try_recv().unwrap().unwrap();
        unsafe {
            audio_callback(
                user,
                1,
                0,
                metadata.as_ptr(),
                metadata.len(),
                std::ptr::null(),
                0,
            );
        }
        let received = receiver.try_recv().unwrap().unwrap();
        assert_eq!(received.metadata["discontinuity"], true);
    }
}
