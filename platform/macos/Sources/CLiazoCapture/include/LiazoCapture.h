#ifndef LIAZO_CAPTURE_H
#define LIAZO_CAPTURE_H
#include <stdint.h>
uint64_t liazo_native_clock_ns(void);
#include <stddef.h>

typedef struct {
    uint32_t max_width;
    uint32_t crop_x, crop_y, crop_width, crop_height;
    uint8_t has_crop, format, quality, include_cursor;
    uint8_t has_previous;
    uint8_t previous[32];
} LiazoImageOptions;

/* Buffers are borrowed for the duration of the callback. Status 0 is data/completion,
   1 is an error, 2 acknowledges a started audio stream. Metadata is UTF-8 JSON.
   Each source/image/stop request completes once. Audio callbacks stop before stop completes.
   Callbacks must return promptly. All strings/options are copied before a function returns. */
typedef void (*LiazoCallback)(void *user, uint64_t request_id, int32_t status,
    const uint8_t *metadata, size_t metadata_len, const uint8_t *payload, size_t payload_len);

void liazo_native_sources(uint64_t request_id, LiazoCallback callback, void *user);
void liazo_native_image(uint64_t request_id, const char *source, const LiazoImageOptions *options,
    LiazoCallback callback, void *user);
void liazo_native_audio_start(uint64_t stream_id, const char *source, LiazoCallback callback, void *user);
void liazo_native_audio_stop(uint64_t stream_id, LiazoCallback callback, void *user);
void liazo_native_runloop(void);
void liazo_native_stoploop(void);
#endif
