#pragma once

#include <stdbool.h>
#include <stdint.h>

#define MUSE_APPROVAL_TEXT_MAX 4096
typedef struct {
    char text[MUSE_APPROVAL_TEXT_MAX + 1];
    char error[128];
    char token[33];
    uint32_t version;
    bool pending, can_approve, sending;
} muse_nanomuse_approval_t;

/* Snapshot only when changed. The displayed version binds a tap to its card. */
bool muse_nanomuse_approval(muse_nanomuse_approval_t *out);
bool muse_nanomuse_decide(uint32_t version, bool approved);

#define MUSE_NOTIFICATIONS_MAX 6
typedef struct {
    char token[33], kind[17], title[129], text[481];
} muse_nanomuse_notification_t;
typedef struct {
    muse_nanomuse_notification_t items[MUSE_NOTIFICATIONS_MAX];
    uint32_t version;
    unsigned count;
    bool sending;
} muse_nanomuse_notifications_t;

bool muse_nanomuse_notifications(muse_nanomuse_notifications_t *out);
unsigned muse_nanomuse_notification_count(void);
/* A read receipt only dismisses a notification; it never approves a tool. */
bool muse_nanomuse_notification_read(uint32_t version, unsigned index);

/* UI task only: take a complete preview after playback, with its byte order
 * still RGB565 big endian. Caller owns/frees the returned PSRAM buffer. */
uint8_t *muse_nanomuse_image_take(int *width, int *height);
/* Bytes waiting to display, for connection diagnostics. */
unsigned muse_nanomuse_image_bytes(void);

/* Reply presentation within the existing voice pipeline's THINKING mode. */
typedef enum {
    MUSE_NANOMUSE_WAITING,
    MUSE_NANOMUSE_PREPARING_AUDIO,
    MUSE_NANOMUSE_READING,
    MUSE_NANOMUSE_AWAITING_APPROVAL,
} muse_nanomuse_reply_phase_t;

muse_nanomuse_reply_phase_t muse_nanomuse_reply_phase(void);
