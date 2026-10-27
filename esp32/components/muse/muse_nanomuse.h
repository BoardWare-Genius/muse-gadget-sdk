#pragma once

/* Reply presentation within the existing voice pipeline's THINKING mode. */
typedef enum {
    MUSE_NANOMUSE_WAITING,
    MUSE_NANOMUSE_PREPARING_AUDIO,
    MUSE_NANOMUSE_READING,
} muse_nanomuse_reply_phase_t;

muse_nanomuse_reply_phase_t muse_nanomuse_reply_phase(void);
