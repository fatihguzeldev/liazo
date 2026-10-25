pub(crate) fn with_cpu_fallback<T, E>(
    gpu: bool,
    mut attempt: impl FnMut(bool) -> Result<T, E>,
) -> Result<(T, bool), E> {
    match attempt(gpu) {
        Ok(value) => Ok((value, gpu)),
        Err(_) if gpu => attempt(false).map(|value| (value, false)),
        Err(error) => Err(error),
    }
}

#[cfg(any(feature = "metal", feature = "vulkan", test))]
#[derive(Clone, Copy)]
enum DeviceKind {
    Discrete,
    Integrated,
    Other,
}

#[cfg(any(feature = "metal", feature = "vulkan", test))]
fn preferred_gpu_ordinal(devices: impl IntoIterator<Item = DeviceKind>) -> Option<i32> {
    let mut ordinal = 0_i32;
    let mut integrated = None;
    for device in devices {
        match device {
            DeviceKind::Discrete => return Some(ordinal),
            DeviceKind::Integrated => {
                integrated.get_or_insert(ordinal);
            }
            DeviceKind::Other => continue,
        }
        ordinal = ordinal.checked_add(1)?;
    }
    integrated
}

#[cfg(any(feature = "metal", feature = "vulkan"))]
pub(crate) fn gpu_device() -> Option<i32> {
    use whisper_rs::whisper_rs_sys as native;
    // SAFETY: GGML's process-lifetime registry owns these devices. Indexes are
    // bounded by its reported count; this borrows pointers without freeing them.
    let devices = unsafe {
        (0..native::ggml_backend_dev_count()).map(|index| {
            let device = native::ggml_backend_dev_get(index);
            if device.is_null() {
                return DeviceKind::Other;
            }
            match native::ggml_backend_dev_type(device) {
                native::ggml_backend_dev_type_GGML_BACKEND_DEVICE_TYPE_GPU => DeviceKind::Discrete,
                native::ggml_backend_dev_type_GGML_BACKEND_DEVICE_TYPE_IGPU => {
                    DeviceKind::Integrated
                }
                _ => DeviceKind::Other,
            }
        })
    };
    preferred_gpu_ordinal(devices)
}

#[cfg(not(any(feature = "metal", feature = "vulkan")))]
pub(crate) fn gpu_device() -> Option<i32> {
    None
}

#[cfg(test)]
mod tests {
    use super::{DeviceKind, preferred_gpu_ordinal, with_cpu_fallback};

    #[test]
    fn a_discrete_gpu_is_preferred_using_whispers_gpu_only_ordinal() {
        use DeviceKind::*;
        assert_eq!(
            preferred_gpu_ordinal([Other, Integrated, Other, Discrete]),
            Some(1)
        );
    }

    #[test]
    fn an_integrated_gpu_is_used_when_no_discrete_gpu_is_available() {
        use DeviceKind::*;
        assert_eq!(
            preferred_gpu_ordinal([Other, Integrated, Integrated]),
            Some(0)
        );
    }

    #[test]
    fn a_cpu_only_host_has_no_gpu_ordinal() {
        assert_eq!(preferred_gpu_ordinal([DeviceKind::Other]), None);
    }

    #[test]
    fn a_gpu_initialization_error_retries_cpu_once() {
        let mut attempts = Vec::new();
        let result = with_cpu_fallback(true, |gpu| {
            attempts.push(gpu);
            if gpu { Err("gpu unavailable") } else { Ok(7) }
        });
        assert_eq!(result, Ok((7, false)));
        assert_eq!(attempts, [true, false]);
    }

    #[test]
    fn a_successful_gpu_initialization_never_repeats_work() {
        let mut attempts = Vec::new();
        let result = with_cpu_fallback(true, |gpu| {
            attempts.push(gpu);
            Ok::<_, &str>(11)
        });
        assert_eq!(result, Ok((11, true)));
        assert_eq!(attempts, [true]);
    }

    #[test]
    fn cpu_initialization_errors_do_not_restart_work() {
        let mut attempts = Vec::new();
        let result = with_cpu_fallback(false, |gpu| {
            attempts.push(gpu);
            Err::<(), _>("cpu failure")
        });
        assert_eq!(result, Err("cpu failure"));
        assert_eq!(attempts, [false]);
    }

    #[test]
    fn a_failed_cpu_retry_is_returned_without_another_gpu_attempt() {
        let mut attempts = Vec::new();
        let result = with_cpu_fallback(true, |gpu| {
            attempts.push(gpu);
            Err::<(), _>(if gpu { "gpu failure" } else { "cpu failure" })
        });
        assert_eq!(result, Err("cpu failure"));
        assert_eq!(attempts, [true, false]);
    }
}
