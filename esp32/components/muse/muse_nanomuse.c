/* nanoMuse LAN transport, implementing the existing UI/voice chat API.
 * Audio is buffered in PSRAM, then uploaded after release so network stalls
 * cannot block I2S capture. Only the worker owns the WebSocket send side. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "cJSON.h"
#include "esp_crt_bundle.h"
#include "esp_heap_caps.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "esp_websocket_client.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"
#include "muse_chat.h"
#include "muse_chat_priv.h"
#include "muse_link.h"
#include "muse_nanomuse.h"
#include "muse_settings.h"
#include "muse_state.h"
#include "muse_ui.h"
#include "muse_wifi.h"

#define MAX_FRAMES (16000 * 16)
#define TEXT_MAX 4000
#define REPLY_MAX 6000
#define RX_MAX 8192
#define SPEECH_MAX_FRAMES (16000 * 60)
#define TURN_TIMEOUT_US (1200LL * 1000000)
#define TURN_IDLE_TIMEOUT_US (180LL * 1000000)
#define LOCK(...) do { xSemaphoreTake(s_lock, portMAX_DELAY); __VA_ARGS__; xSemaphoreGive(s_lock); } while (0)
typedef struct { uint32_t id; int16_t *pcm; size_t frames; char *text; } job_t;
static SemaphoreHandle_t s_lock;
static TaskHandle_t s_worker;
static esp_websocket_client_handle_t s_ws;
static bool s_ready, s_active, s_recording, s_typed, s_finished, s_gateway_tts;
static uint32_t s_id = 1, s_config_gen;
static job_t s_queued;
static int16_t *s_record;
static size_t s_recorded;
static int16_t *s_speech;
static size_t s_speech_frames, s_speech_received, s_speech_read;
static bool s_speech_ready, s_speech_stream;
#define CAPTION_CUES_MAX 1024
#define CAPTION_TEXT_MAX (REPLY_MAX * 2 + CAPTION_CUES_MAX)
typedef struct {
    struct { size_t frame, offset; } cues[CAPTION_CUES_MAX];
    char text[CAPTION_TEXT_MAX];
} captions_t;
static captions_t *s_captions;
static size_t s_caption_count, s_caption_bytes;
static char *s_reply, *s_rx;
static size_t s_rx_n;
static bool s_rx_bad;
static int s_rx_kind;
static unsigned s_events;
static char s_heard[96], s_error[96];
static int64_t s_reply_at;
static int64_t s_activity_at;
static uint32_t s_progress_seq;
static char s_detail[48] = "Connecting to gateway";
static muse_nanomuse_approval_t s_approval;
static bool s_decision_queued, s_decision_approved;
static muse_nanomuse_notifications_t s_notifications;
static bool s_subscribe_queued;
static char s_notification_read[33];
static int64_t s_notification_retry_at;
static uint8_t *s_image_pending;
static size_t s_image_received, s_image_bytes;
static int s_image_w, s_image_h, s_display_w, s_display_h;
static bool s_image_complete;
static int64_t s_image_deadline;
#define EV(e) (1U << (e))

static void clear_image_locked(void)
{
    free(s_image_pending); s_image_pending = NULL;
    s_image_bytes = s_image_received = 0;
    s_image_complete = false;
    s_image_deadline = 0;
}

static void release_job(job_t *j) { free(j->pcm); free(j->text); memset(j, 0, sizeof(*j)); }
static void clear_approval_locked(void)
{
    uint32_t version = s_approval.version + 1;
    memset(&s_approval, 0, sizeof(s_approval));
    s_approval.version = version;
    s_decision_queued = false;
}
/* Lock held. End the previous generation before freeing its buffers. */
static void reset_turn(void)
{
    clear_image_locked();
    s_display_w = s_display_h = 0;
    clear_approval_locked();
    if (s_typed && s_active && !s_finished) {
        muse_hatch_console("done", NULL, "\"messages\":0,\"complete\":false");
        muse_state_set_mode(MUSE_MODE_IDLE);
        muse_state_set_caption("%s", "");
    }
    if (++s_id == 0) ++s_id;
    free(s_record); s_record = NULL; s_recorded = 0;
    free(s_speech); s_speech = NULL;
    free(s_captions); s_captions = NULL; s_caption_count = s_caption_bytes = 0;
    s_speech_frames = s_speech_received = s_speech_read = 0; s_speech_ready = false; s_speech_stream = false;
    release_job(&s_queued);
    s_recording = s_active = s_typed = s_finished = false;
    s_events = 0;
    if (s_reply) s_reply[0] = 0;
    s_error[0] = s_heard[0] = 0;
    s_reply_at = 0;
    s_activity_at = esp_timer_get_time(); s_progress_seq = 0;
}
static void fail_locked(const char *why)
{
    clear_image_locked();
    if (!s_active || s_finished) return;
    clear_approval_locked();
    strlcpy(s_error, why, sizeof(s_error));
    s_events |= EV(MUSE_HATCH_EV_ERROR);
    s_finished = true;
    if (s_typed) {
        muse_hatch_console("error", why, NULL);
        muse_state_set_mode(MUSE_MODE_IDLE);
        muse_state_set_caption("%s", why);
    }
}
static void fail(uint32_t id, const char *why) { LOCK(if (id == s_id) fail_locked(why);); }
static bool current(uint32_t id) { bool b; LOCK(b = s_active && s_id == id && !s_finished;); return b; }
static bool turn_expired(uint32_t id, int64_t started)
{
    bool expired = false;
    int64_t now = esp_timer_get_time();
    LOCK({
        int64_t idle = TURN_IDLE_TIMEOUT_US + (s_approval.pending ? 120LL * 1000000 : 0);
        expired = id == s_id && (now - started >= TURN_TIMEOUT_US || now - s_activity_at >= idle);
    });
    return expired;
}

static void clear_notifications_locked(void)
{
    uint32_t version = s_notifications.version + 1;
    memset(&s_notifications, 0, sizeof(s_notifications));
    s_notifications.version = version;
    s_notification_read[0] = 0;
    s_subscribe_queued = false;
    s_notification_retry_at = 0;
}

static void receive_notifications_locked(const cJSON *root)
{
    const cJSON *items = cJSON_GetObjectItemCaseSensitive(root, "items");
    int count = cJSON_GetArraySize(items);
    if (!cJSON_IsArray(items) || count > MUSE_NOTIFICATIONS_MAX) return;
    for (int n = 0; n < count; ++n) {
        const cJSON *item = cJSON_GetArrayItem(items, n);
        const cJSON *token = cJSON_GetObjectItemCaseSensitive(item, "token");
        const cJSON *kind = cJSON_GetObjectItemCaseSensitive(item, "kind");
        const cJSON *title = cJSON_GetObjectItemCaseSensitive(item, "title");
        const cJSON *text = cJSON_GetObjectItemCaseSensitive(item, "text");
        if (!cJSON_IsString(token) || strlen(token->valuestring) != 32
            || strspn(token->valuestring, "0123456789abcdef") != 32
            || !cJSON_IsString(kind) || strlen(kind->valuestring) > 16
            || !cJSON_IsString(title) || strlen(title->valuestring) > 128
            || !cJSON_IsString(text) || strlen(text->valuestring) > 480) return;
        for (int j = 0; j < n; ++j) {
            const cJSON *prior = cJSON_GetObjectItemCaseSensitive(cJSON_GetArrayItem(items, j), "token");
            if (!strcmp(prior->valuestring, token->valuestring)) return;
        }
    }
    bool wake = false, read_pending = false;
    for (int n = 0; n < count; ++n) {
        const cJSON *token = cJSON_GetObjectItemCaseSensitive(cJSON_GetArrayItem(items, n), "token");
        bool known = false;
        for (unsigned j = 0; j < s_notifications.count; ++j)
            known |= !strcmp(s_notifications.items[j].token, token->valuestring);
        wake |= !known;
        read_pending |= !strcmp(s_notification_read, token->valuestring);
    }
    for (int n = 0; n < count; ++n) {
        const cJSON *item = cJSON_GetArrayItem(items, n);
        muse_nanomuse_notification_t *out = &s_notifications.items[n];
        strlcpy(out->token, cJSON_GetObjectItemCaseSensitive(item, "token")->valuestring, sizeof(out->token));
        strlcpy(out->kind, cJSON_GetObjectItemCaseSensitive(item, "kind")->valuestring, sizeof(out->kind));
        strlcpy(out->title, cJSON_GetObjectItemCaseSensitive(item, "title")->valuestring, sizeof(out->title));
        strlcpy(out->text, cJSON_GetObjectItemCaseSensitive(item, "text")->valuestring, sizeof(out->text));
    }
    s_notifications.count = count;
    if (!read_pending) s_notification_read[0] = 0;
    s_notifications.sending = s_notification_read[0] != 0;
    ++s_notifications.version;
    if (wake) muse_state_set_asleep(false);
}

static void receive_json(void)
{
    cJSON *root = cJSON_Parse(s_rx);
    if (!root) return;
    const cJSON *t = cJSON_GetObjectItemCaseSensitive(root, "type");
    const cJSON *i = cJSON_GetObjectItemCaseSensitive(root, "id");
    const cJSON *v = cJSON_GetObjectItemCaseSensitive(root, "text");
    const char *text = cJSON_IsString(v) ? v->valuestring : "";
    if (!cJSON_IsString(t)) { cJSON_Delete(root); return; }
    LOCK({
        if (!strcmp(t->valuestring, "hello")) {
            const cJSON *p = cJSON_GetObjectItemCaseSensitive(root, "protocol");
            s_gateway_tts = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(root, "tts"));
            s_ready = cJSON_IsNumber(p) && p->valueint == 1;
            strlcpy(s_detail, s_ready ? "Gateway ready" : "Gateway protocol mismatch", sizeof(s_detail));
            muse_link_set_state(s_ready ? MUSE_LINK_ONLINE : MUSE_LINK_ERROR);
            if (s_ready && cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(root, "notifications"))) {
                s_subscribe_queued = true;
                s_notification_retry_at = 0;
            } else clear_notifications_locked();
        } else if (!strcmp(t->valuestring, "notifications") && s_ready) {
            receive_notifications_locked(root);
        } else if (cJSON_IsNumber(i) && i->valuedouble == (double)s_id && s_ready
                   && !s_recording && !s_error[0] && !strcmp(t->valuestring, "image")) {
            /* Image delivery can finish after audio_end/DONE and playback.
             * Its generation must still match the last submitted turn. */
            const cJSON *w = cJSON_GetObjectItemCaseSensitive(root, "width");
            const cJSON *h = cJSON_GetObjectItemCaseSensitive(root, "height");
            const cJSON *bytes = cJSON_GetObjectItemCaseSensitive(root, "bytes");
            const cJSON *format = cJSON_GetObjectItemCaseSensitive(root, "format");
            clear_image_locked();
            if (s_display_w > 0 && s_display_h > 0 && cJSON_IsNumber(w) && cJSON_IsNumber(h)
                && w->valuedouble == s_display_w && h->valuedouble == s_display_h
                && cJSON_IsNumber(bytes) && bytes->valuedouble == (double)s_display_w * s_display_h * 2
                && cJSON_IsString(format) && !strcmp(format->valuestring, "rgb565be")) {
                s_image_w = s_display_w; s_image_h = s_display_h;
                s_image_bytes = (size_t)s_image_w * s_image_h * 2;
                s_image_pending = heap_caps_malloc(s_image_bytes, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
                if (!s_image_pending) clear_image_locked();
                else s_image_deadline = esp_timer_get_time() + 15000000;
            }
        } else if (cJSON_IsNumber(i) && i->valuedouble == (double)s_id
                   && !strcmp(t->valuestring, "image_end")) {
            if (s_image_pending && s_image_received == s_image_bytes
                && esp_timer_get_time() <= s_image_deadline) {
                s_image_complete = true;
                muse_state_set_asleep(false);
            } else clear_image_locked();
        } else if (cJSON_IsNumber(i) && i->valuedouble == (double)s_id && s_active && !s_finished) {
            if (!strcmp(t->valuestring, "progress")) {
                const cJSON *seq = cJSON_GetObjectItemCaseSensitive(root, "seq");
                if (cJSON_IsNumber(seq) && seq->valuedouble > s_progress_seq
                    && seq->valuedouble <= UINT32_MAX && seq->valuedouble == (double)(uint32_t)seq->valuedouble) {
                    s_progress_seq = (uint32_t)seq->valuedouble;
                    s_activity_at = esp_timer_get_time();
                }
            } else if (!strcmp(t->valuestring, "approval")) {
                const cJSON *token = cJSON_GetObjectItemCaseSensitive(root, "token");
                const cJSON *error = cJSON_GetObjectItemCaseSensitive(root, "error");
                if (cJSON_IsString(token) && strlen(token->valuestring) == 32
                    && strspn(token->valuestring, "0123456789abcdef") == 32
                    && text[0] && strlen(text) <= MUSE_APPROVAL_TEXT_MAX) {
                    clear_approval_locked();
                    strlcpy(s_approval.token, token->valuestring, sizeof(s_approval.token));
                    strlcpy(s_approval.text, text, sizeof(s_approval.text));
                    if (cJSON_IsString(error)) strlcpy(s_approval.error, error->valuestring, sizeof(s_approval.error));
                    s_approval.can_approve = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(root, "can_approve"));
                    s_approval.pending = true;
                    s_activity_at = esp_timer_get_time();
                    muse_state_set_asleep(false);
                }
            } else if (!strcmp(t->valuestring, "approval_closed")) {
                const cJSON *token = cJSON_GetObjectItemCaseSensitive(root, "token");
                if (cJSON_IsString(token) && !strcmp(token->valuestring, s_approval.token)) {
                    clear_approval_locked();
                    s_activity_at = esp_timer_get_time();
                }
            } else if (!strcmp(t->valuestring, "sent")) {
                s_activity_at = esp_timer_get_time();
                s_events |= EV(MUSE_HATCH_EV_SENT);
                if (s_typed) muse_hatch_console("sent", NULL, "\"bytes\":0");
            } else if (!strcmp(t->valuestring, "heard")) {
                if (strcmp(s_heard, text)) s_activity_at = esp_timer_get_time();
                strlcpy(s_heard, text, sizeof(s_heard)); s_events |= EV(MUSE_HATCH_EV_HEARD);
            } else if (!strcmp(t->valuestring, "reply")) {
                if (strcmp(s_reply, text)) s_activity_at = esp_timer_get_time();
                strlcpy(s_reply, text, REPLY_MAX + 1);
                s_reply_at = esp_timer_get_time(); s_events |= EV(MUSE_HATCH_EV_REPLY);
                if (s_typed) {
                    muse_hatch_console("text", s_reply, "\"msg\":1");
                    muse_hatch_console("message_done", NULL, "\"msg\":1,\"bytes\":%u", (unsigned)strlen(s_reply));
                    muse_state_set_caption("%s", s_reply);
                }
            } else if (!strcmp(t->valuestring, "done")) {
                clear_approval_locked();
                if (s_speech && !s_speech_ready) fail_locked("INCOMPLETE REPLY AUDIO");
                else {
                    s_finished = true;
                    /* Voice DONE waits for playback or text pagination. */
                    if (s_typed) {
                        muse_hatch_console("done", NULL, "\"messages\":1,\"complete\":true");
                        muse_state_set_mode(MUSE_MODE_IDLE);
                    }
                }
            } else if (!strcmp(t->valuestring, "audio")) {
                const cJSON *rate = cJSON_GetObjectItemCaseSensitive(root, "rate");
                const cJSON *channels = cJSON_GetObjectItemCaseSensitive(root, "channels");
                const cJSON *format = cJSON_GetObjectItemCaseSensitive(root, "format");
                const cJSON *frames = cJSON_GetObjectItemCaseSensitive(root, "frames");
                if (s_typed || s_speech || !cJSON_IsNumber(rate) || rate->valuedouble != 16000
                    || !cJSON_IsNumber(channels) || channels->valuedouble != 1
                    || !cJSON_IsString(format) || strcmp(format->valuestring, "s16le")
                    || !cJSON_IsNumber(frames) || frames->valuedouble < 1
                    || frames->valuedouble > SPEECH_MAX_FRAMES || frames->valuedouble != (double)frames->valueint) {
                    fail_locked("INVALID REPLY AUDIO");
                } else {
                    s_speech_stream = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(root, "stream"));
                    s_speech_frames = (size_t)frames->valueint;
                    s_speech = heap_caps_malloc(s_speech_frames * sizeof(int16_t), MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
                    if (!s_speech) fail_locked("NO PLAYBACK MEMORY");
                    else s_activity_at = esp_timer_get_time();
                }
            } else if (!strcmp(t->valuestring, "caption")) {
                /* Optional phrase cues are ordered with PCM on the same WS.
                 * Bad/oversized metadata must not break otherwise valid audio. */
                const cJSON *frame = cJSON_GetObjectItemCaseSensitive(root, "frame");
                size_t len = strlen(text);
                if (s_speech_stream && s_speech && !s_speech_ready && len && len < MUSE_CAPTION_MAX
                    && cJSON_IsNumber(frame) && frame->valuedouble == (double)s_speech_received
                    && (!s_caption_count || s_speech_received > s_captions->cues[s_caption_count - 1].frame)
                    && s_caption_count < CAPTION_CUES_MAX && len + 1 <= CAPTION_TEXT_MAX - s_caption_bytes) {
                    if (!s_captions) s_captions = heap_caps_calloc(1, sizeof(*s_captions), MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
                    if (s_captions) {
                        s_captions->cues[s_caption_count].frame = s_speech_received;
                        s_captions->cues[s_caption_count++].offset = s_caption_bytes;
                        memcpy(s_captions->text + s_caption_bytes, text, len + 1);
                        s_caption_bytes += len + 1;
                    }
                }
            } else if (!strcmp(t->valuestring, "audio_end")) {
                if (!s_speech || s_speech_ready || !s_speech_received
                    || (!s_speech_stream && s_speech_received != s_speech_frames))
                    fail_locked("INCOMPLETE REPLY AUDIO");
                else {
                    clear_approval_locked();
                    s_speech_frames = s_speech_received;
                    s_speech_ready = true;
                    /* audio_end closes the whole reply stream, after the
                     * final text and every phrase. Do not require a second
                     * done message to release the UI once PCM is drained. */
                    s_finished = true;
                }
            } else if (!strcmp(t->valuestring, "warning")) {
                /* A failed TTS request keeps the already received text usable. */
                muse_hatch_console("warning", text, NULL);
            } else if (!strcmp(t->valuestring, "error")) fail_locked(text[0] ? text : "GATEWAY ERROR");
        }
    });
    cJSON_Delete(root);
}
static uint32_t read_le32(const uint8_t *p)
{
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

static void receive_audio(void)
{
    const uint8_t *p = (const uint8_t *)s_rx;
    if (s_rx_n >= 4 && read_le32(p) == 0) {
        /* A reserved zero ID keeps image bytes out of the PCM decoder. */
        if (s_rx_n < 18 || s_rx_n > 2064 || memcmp(p + 4, "IMG1", 4) || s_rx_n % 2) return;
        uint32_t id = read_le32(p + 8), offset = read_le32(p + 12);
        LOCK({
            if (id == s_id && s_image_pending && !s_image_complete) {
                size_t n = s_rx_n - 16;
                if (offset != s_image_received || n > s_image_bytes - s_image_received
                    || esp_timer_get_time() > s_image_deadline) clear_image_locked();
                else { memcpy(s_image_pending + offset, p + 16, n); s_image_received += n; }
            }
        });
        return;
    }
    if (s_rx_n < 6 || s_rx_n > 2052 || (s_rx_n - 4) % 2) {
        LOCK(fail_locked("INVALID AUDIO PACKET");); return;
    }
    uint32_t id = (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
    size_t frames = (s_rx_n - 4) / 2;
    LOCK({
        if (s_active && !s_finished && id == s_id) {
            if (!s_speech || s_speech_ready || frames > s_speech_frames - s_speech_received)
                fail_locked("INVALID AUDIO PACKET");
            else {
                memcpy(s_speech + s_speech_received, s_rx + 4, frames * 2);
                s_speech_received += frames;
                s_activity_at = esp_timer_get_time();
            }
        }
    });
}
static void ws_event(void *arg, esp_event_base_t base, int32_t event, void *data)
{
    (void)arg; (void)base;
    esp_websocket_event_data_t *e = data;
    if (event == WEBSOCKET_EVENT_CONNECTED) {
        s_rx_n = 0; s_rx_bad = false;
        LOCK(s_ready = false; strlcpy(s_detail, "Waiting for gateway", sizeof(s_detail)););
    } else if (event == WEBSOCKET_EVENT_DISCONNECTED || event == WEBSOCKET_EVENT_ERROR
               || event == WEBSOCKET_EVENT_CLOSED) {
        LOCK({
            s_ready = false;
            clear_image_locked();
            clear_approval_locked();
            strlcpy(s_detail, "Check gateway/address/key", sizeof(s_detail));
            fail_locked("GATEWAY DISCONNECTED");
            /* The client handles both clean-close and transport reconnects.
             * Only a new hello marks the replacement connection ready. */
        });
        muse_link_set_state(MUSE_LINK_OFFLINE);
        xTaskNotifyGive(s_worker);
    } else if (event == WEBSOCKET_EVENT_DATA && (e->op_code == 1 || e->op_code == 2 || e->op_code == 0)) {
        if ((e->op_code == 1 || e->op_code == 2) && e->payload_offset == 0) {
            s_rx_n = 0; s_rx_bad = false; s_rx_kind = e->op_code;
        }
        if (e->data_len < 0 || s_rx_n + (size_t)e->data_len > RX_MAX) s_rx_bad = true;
        if (!s_rx_bad) { memcpy(s_rx + s_rx_n, e->data_ptr, e->data_len); s_rx_n += e->data_len; }
        if (e->fin && e->payload_offset + e->data_len == e->payload_len) {
            if (!s_rx_bad) {
                if (s_rx_kind == 2) receive_audio();
                else { s_rx[s_rx_n] = 0; receive_json(); }
            }
            else LOCK(fail_locked("GATEWAY MESSAGE TOO LARGE"););
        }
    }
}
static bool send_control(const char *type, uint32_t id, const char *text)
{
    if (!s_ws || !esp_websocket_client_is_connected(s_ws)) return false;
    cJSON *j = cJSON_CreateObject();
    if (!j) return false;
    cJSON_AddStringToObject(j, "type", type); cJSON_AddNumberToObject(j, "id", id);
    if (!strcmp(type, "begin") || !strcmp(type, "text")) {
        cJSON_AddBoolToObject(j, "approvals", true);
        int w, h;
        if (muse_ui_image_size(&w, &h) && w > 0 && h > 0 && w <= 640 && h <= 640 && w * h <= 320 * 480) {
            cJSON *image = cJSON_AddObjectToObject(j, "images");
            if (image) {
                cJSON_AddNumberToObject(image, "width", w);
                cJSON_AddNumberToObject(image, "height", h);
                LOCK(if (id == s_id) { s_display_w = w; s_display_h = h; });
            }
        }
    }
    if (text) cJSON_AddStringToObject(j, "text", text);
    if (!strcmp(type, "begin")) {
        cJSON_AddNumberToObject(j, "rate", 16000); cJSON_AddNumberToObject(j, "channels", 1);
        cJSON_AddStringToObject(j, "format", "s16le");
        cJSON_AddBoolToObject(j, "tts", true);
        cJSON_AddBoolToObject(j, "streaming", true);
    }
    char *s = cJSON_PrintUnformatted(j);
    bool ok = s && esp_websocket_client_send_text(s_ws, s, strlen(s), pdMS_TO_TICKS(2000)) == (int)strlen(s);
    free(s); cJSON_Delete(j); return ok;
}
/* Only the network worker sends. Touch callbacks enqueue without blocking LVGL. */
static bool send_approval_decision(uint32_t id)
{
    char token[33] = ""; bool approved = false;
    LOCK({
        if (s_decision_queued && s_active && !s_finished && s_id == id && s_approval.pending) {
            strlcpy(token, s_approval.token, sizeof(token));
            approved = s_decision_approved;
            s_decision_queued = false;
        }
    });
    if (!token[0]) return true;
    if (!s_ws || !esp_websocket_client_is_connected(s_ws)) return false;
    cJSON *j = cJSON_CreateObject();
    if (!j) return false;
    cJSON_AddStringToObject(j, "type", "approval_decision");
    cJSON_AddNumberToObject(j, "id", id);
    cJSON_AddStringToObject(j, "token", token);
    cJSON_AddBoolToObject(j, "approved", approved);
    char *wire = cJSON_PrintUnformatted(j);
    bool ok = wire && esp_websocket_client_send_text(s_ws, wire, strlen(wire), pdMS_TO_TICKS(2000)) == (int)strlen(wire);
    free(wire); cJSON_Delete(j);
    return ok;
}

static void send_inbox_controls(void)
{
    bool subscribe = false; char token[33] = "";
    LOCK({
        if (s_ready && esp_timer_get_time() >= s_notification_retry_at) {
            subscribe = s_subscribe_queued;
            if (!subscribe) strlcpy(token, s_notification_read, sizeof(token));
            if (subscribe || token[0]) s_notification_retry_at = esp_timer_get_time() + 2000000;
        }
    });
    if ((!subscribe && !token[0]) || !s_ws || !esp_websocket_client_is_connected(s_ws)) return;
    cJSON *j = cJSON_CreateObject();
    if (!j) return;
    cJSON_AddStringToObject(j, "type", subscribe ? "subscribe" : "notification_read");
    if (subscribe) cJSON_AddBoolToObject(j, "notifications", true);
    else cJSON_AddStringToObject(j, "token", token);
    char *wire = cJSON_PrintUnformatted(j);
    bool ok = wire && esp_websocket_client_send_text(s_ws, wire, strlen(wire), pdMS_TO_TICKS(2000)) == (int)strlen(wire);
    free(wire); cJSON_Delete(j);
    if (ok && subscribe) LOCK(s_subscribe_queued = false; s_notification_retry_at = 0;);
}
static void worker(void *arg)
{
    (void)arg;
    uint32_t config_gen = UINT32_MAX;
    char url[MUSE_GATEWAY_URL_MAX + 1], key[MUSE_GATEWAY_KEY_MAX + 1], headers[180];
    for (;;) {
        uint32_t gen;
        LOCK(gen = s_config_gen;);
        if (gen != config_gen) {
            if (s_ws) { esp_websocket_client_stop(s_ws); esp_websocket_client_destroy(s_ws); s_ws = NULL; }
            LOCK(s_ready = false; clear_notifications_locked(););
            muse_settings_gateway(url, key);
            config_gen = gen;
            bool valid = (!strncmp(url, "ws://", 5) || !strncmp(url, "wss://", 6))
                && !strpbrk(url, "@?#\r\n ") && strlen(key) >= 16 && !strpbrk(key, "\r\n ");
            if (valid) {
                snprintf(headers, sizeof(headers), "Authorization: Bearer %s\r\n", key);
                esp_websocket_client_config_t cfg = {
                    .uri = url, .headers = headers, .buffer_size = 2048, .task_stack = 6144,
                    .reconnect_timeout_ms = 5000, .network_timeout_ms = 3000,
                    .enable_close_reconnect = true,
                    .crt_bundle_attach = esp_crt_bundle_attach,
                };
                s_ws = esp_websocket_client_init(&cfg);
                if (s_ws) {
                    esp_websocket_register_events(s_ws, WEBSOCKET_EVENT_ANY, ws_event, NULL);
                    if (esp_websocket_client_start(s_ws) != ESP_OK) {
                        esp_websocket_client_destroy(s_ws); s_ws = NULL;
                    }
                }
                if (!s_ws) { LOCK(strlcpy(s_detail, "Gateway client failed", sizeof(s_detail));); }
            } else { LOCK(strlcpy(s_detail, "Set gateway URL and key", sizeof(s_detail));); }
            memset(key, 0, sizeof(key)); memset(headers, 0, sizeof(headers));
        }
        send_inbox_controls();
        job_t job = {0}; bool ready;
        LOCK({
            if (s_image_pending && !s_image_complete && esp_timer_get_time() > s_image_deadline)
                clear_image_locked();
            ready = s_ready;
            if (s_queued.id) { job = s_queued; memset(&s_queued, 0, sizeof(s_queued)); }
        });
        if (!job.id) { ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(100)); continue; }
        if (!ready || !s_ws || !esp_websocket_client_is_connected(s_ws)) {
            fail(job.id, "GATEWAY NOT CONNECTED"); release_job(&job); continue;
        }
        bool ok = current(job.id) && send_control(job.text ? "text" : "begin", job.id, job.text);
        if (!job.text) {
            uint8_t packet[2052];
            int64_t upload_deadline = esp_timer_get_time() + 30000000;
            for (size_t at = 0; ok && at < job.frames;) {
                if (!current(job.id) || !esp_websocket_client_is_connected(s_ws)
                    || esp_timer_get_time() > upload_deadline) { ok = false; break; }
                size_t n = job.frames - at; if (n > 1024) n = 1024;
                for (int i = 0; i < 4; ++i) packet[i] = (uint8_t)(job.id >> (8 * i));
                memcpy(packet + 4, job.pcm + at, n * 2);
                ok = esp_websocket_client_send_bin(s_ws, (char *)packet, 4 + n * 2, pdMS_TO_TICKS(2000)) == (int)(4 + n * 2);
                at += n;
            }
            if (ok) ok = send_control("end", job.id, NULL);
        }
        if (!ok) fail(job.id, "GATEWAY SEND FAILED");
        int64_t started = esp_timer_get_time();
        LOCK(if (s_id == job.id) s_activity_at = started;);
        while (ok && current(job.id) && !turn_expired(job.id, started)) {
            send_inbox_controls();
            if (!send_approval_decision(job.id)) { fail(job.id, "GATEWAY SEND FAILED"); break; }
            ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(50));
        }
        if (ok && current(job.id)) fail(job.id, "NANOMUSE RESPONSE TIMEOUT");
        bool completed;
        LOCK(completed = s_id == job.id && s_finished && !s_error[0];);
        if (!completed && esp_websocket_client_is_connected(s_ws)) send_control("cancel", job.id, NULL);
        release_job(&job);
    }
}
void muse_hatch_start(void)
{
    if (s_lock) return;
    s_lock = xSemaphoreCreateMutex();
    s_reply = heap_caps_calloc(1, REPLY_MAX + 1, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    s_rx = heap_caps_malloc(RX_MAX + 1, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    ESP_ERROR_CHECK(s_lock && s_reply && s_rx ? ESP_OK : ESP_ERR_NO_MEM);
    ESP_ERROR_CHECK(xTaskCreate(worker, "nanomuse", 6144, NULL, 4, &s_worker) == pdPASS ? ESP_OK : ESP_ERR_NO_MEM);
}
void muse_hatch_status(muse_hatch_status_t *out)
{
    memset(out, 0, sizeof(*out));
    if (!s_lock) { out->state = MUSE_HATCH_UNTESTED; return; }
    char url[MUSE_GATEWAY_URL_MAX + 1], key[MUSE_GATEWAY_KEY_MAX + 1];
    muse_settings_gateway(url, key);
    LOCK({
        out->state = !url[0] || !key[0] ? MUSE_HATCH_NOT_SET : !muse_wifi_connected() ? MUSE_HATCH_OFFLINE
            : s_ready ? MUSE_HATCH_REACHABLE : MUSE_HATCH_UNREACHABLE;
        strlcpy(out->detail, s_detail, sizeof(out->detail));
    });
    memset(key, 0, sizeof(key));
}
const char *muse_hatch_state_name(muse_hatch_state_t state)
{
    static const char *const names[] = { "Set up gateway", "No Wi-Fi", "Starting", "Connecting", "Connected", "Can't connect" };
    return (unsigned)state < sizeof(names) / sizeof(*names) ? names[state] : "Unknown";
}
void muse_hatch_config_changed(void)
{
    if (!s_lock) return;
    LOCK({
        bool voice = s_active && !s_typed;
        reset_turn();
        /* The voice task may still be waiting for reply events, including
         * while draining completed audio. Wake it with a terminal event. */
        if (voice) { s_active = true; fail_locked("GATEWAY CONFIGURATION CHANGED"); }
        ++s_config_gen;
    });
    xTaskNotifyGive(s_worker);
}
void muse_hatch_test(void) { muse_hatch_config_changed(); }
void muse_hatch_set_resting(bool rest) { (void)rest; }
bool muse_hatch_ready(void) { if (!s_lock) return false; bool ready; LOCK(ready = s_ready;); return ready && muse_wifi_connected(); }
uint8_t *muse_nanomuse_image_take(int *width, int *height)
{
    if (!s_lock || !width || !height) return NULL;
    uint8_t *pixels = NULL;
    LOCK({
        if (s_image_complete && s_finished && !s_recording && !s_error[0]
            && (!s_speech || s_speech_read == s_speech_frames)) {
            pixels = s_image_pending; s_image_pending = NULL;
            *width = s_image_w; *height = s_image_h;
            clear_image_locked();
        }
    });
    return pixels;
}

unsigned muse_nanomuse_image_bytes(void)
{
    unsigned bytes = 0;
    if (s_lock) { LOCK(bytes = s_image_received;); }
    return bytes;
}

bool muse_nanomuse_approval(muse_nanomuse_approval_t *out)
{
    if (!s_lock || !out) return false;
    bool changed = false;
    LOCK(if (out->version != s_approval.version) { *out = s_approval; changed = true; });
    return changed;
}
bool muse_nanomuse_decide(uint32_t version, bool approved)
{
    if (!s_lock) return false;
    bool queued = false;
    LOCK({
        if (s_ready && s_active && !s_finished && s_approval.pending && !s_approval.sending
            && version == s_approval.version && (!approved || s_approval.can_approve)) {
            s_approval.sending = true; ++s_approval.version;
            s_decision_queued = true; s_decision_approved = approved; queued = true;
        }
    });
    if (queued) xTaskNotifyGive(s_worker);
    return queued;
}
unsigned muse_nanomuse_notification_count(void)
{
    unsigned count = 0;
    if (s_lock) LOCK(count = s_notifications.count;);
    return count;
}

bool muse_nanomuse_notifications(muse_nanomuse_notifications_t *out)
{
    if (!s_lock || !out) return false;
    bool changed = false;
    LOCK(if (out->version != s_notifications.version) { *out = s_notifications; changed = true; });
    return changed;
}

bool muse_nanomuse_notification_read(uint32_t version, unsigned index)
{
    if (!s_lock) return false;
    bool ok = false;
    LOCK({
        if (s_ready && version == s_notifications.version && index < s_notifications.count
            && !s_notification_read[0]) {
            strlcpy(s_notification_read, s_notifications.items[index].token, sizeof(s_notification_read));
            s_notifications.sending = true;
            ++s_notifications.version;
            s_notification_retry_at = 0;
            ok = true;
        }
    });
    if (ok) xTaskNotifyGive(s_worker);
    return ok;
}

muse_nanomuse_reply_phase_t muse_nanomuse_reply_phase(void)
{
    muse_nanomuse_reply_phase_t phase = MUSE_NANOMUSE_WAITING;
    if (!s_lock) return phase;
    LOCK({
        if (s_approval.pending) phase = MUSE_NANOMUSE_AWAITING_APPROVAL;
        else if (s_active && !s_error[0] && s_reply[0]) {
            phase = !s_typed && s_gateway_tts && !s_finished && !s_speech_ready
                ? MUSE_NANOMUSE_PREPARING_AUDIO : MUSE_NANOMUSE_READING;
        }
    });
    return phase;
}
void muse_hatch_turn_cancel(void) { if (!s_lock) return; LOCK(reset_turn();); xTaskNotifyGive(s_worker); }
void muse_hatch_turn_begin(void)
{
    if (!s_lock) return;
    LOCK({
        reset_turn(); s_active = true;
        s_record = heap_caps_malloc(MAX_FRAMES * 2, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        if (!s_record) fail_locked("NO RECORDING MEMORY"); else s_recording = true;
    });
    xTaskNotifyGive(s_worker);
}
size_t muse_hatch_turn_audio_wait(const int16_t *pcm, size_t frames, int wait_ms)
{
    (void)wait_ms; size_t n = 0;
    if (!s_lock) return 0;
    LOCK({
        if (s_recording && !s_finished) {
            n = frames < MAX_FRAMES - s_recorded ? frames : MAX_FRAMES - s_recorded;
            memcpy(s_record + s_recorded, pcm, n * 2); s_recorded += n;
            if (!n && frames) fail_locked("RECORDING TOO LONG");
        }
    });
    return n;
}
void muse_hatch_turn_audio(const int16_t *pcm, size_t frames) { (void)muse_hatch_turn_audio_wait(pcm, frames, 0); }
void muse_hatch_turn_end(void)
{
    if (!s_lock) return;
    LOCK({
        if (s_recording && !s_finished && s_recorded) {
            s_queued = (job_t){ .id = s_id, .pcm = s_record, .frames = s_recorded };
            s_record = NULL; s_recording = false;
        } else fail_locked("NO RECORDING");
    });
    xTaskNotifyGive(s_worker);
}
void muse_hatch_text_turn(char *text)
{
    if (!s_lock) { free(text); return; }
    LOCK({
        if ((s_active && !s_typed) || !text[0] || strlen(text) > TEXT_MAX) {
            muse_hatch_console("error", "BUSY OR TEXT TOO LONG (4000 BYTES)", NULL); free(text);
        } else {
            reset_turn(); s_active = s_typed = true; s_queued = (job_t){ .id = s_id, .text = text };
            muse_state_set_mode(MUSE_MODE_THINKING); muse_state_set_caption("WAITING FOR NANOMUSE");
        }
    });
    xTaskNotifyGive(s_worker);
}
void muse_hatch_text_cancel(void) { bool typed = false; if (s_lock) { LOCK(typed = s_typed;); if (typed) muse_hatch_turn_cancel(); } }
/* UTF-8 reading pace: 12 characters/s; preserve complete codepoints. */
static size_t read_at(int64_t now)
{
    size_t chars = (now - s_reply_at) * 12 / 1000000, at = 0;
    while (s_reply[at] && chars--) { ++at; while (((unsigned char)s_reply[at] & 0xc0) == 0x80) ++at; }
    return at;
}
muse_hatch_ev_t muse_hatch_turn_event(char *text, size_t cap)
{
    if (!s_lock) return MUSE_HATCH_EV_NONE;
    muse_hatch_ev_t ev = MUSE_HATCH_EV_NONE;
    LOCK({
        if (s_typed) { /* Typed replies belong to the serial console, not the voice task. */ }
        else if (s_events & EV(MUSE_HATCH_EV_ERROR)) { ev = MUSE_HATCH_EV_ERROR; strlcpy(text, s_error, cap); s_events = 0; s_active = false; }
        else {
            const muse_hatch_ev_t order[] = { MUSE_HATCH_EV_HEARD, MUSE_HATCH_EV_SENT, MUSE_HATCH_EV_REPLY };
            for (int j = 0; j < 3; ++j) if (s_events & EV(order[j])) { ev = order[j]; s_events &= ~EV(ev); break; }
            if (ev != MUSE_HATCH_EV_NONE) strlcpy(text, ev == MUSE_HATCH_EV_HEARD ? s_heard : ev == MUSE_HATCH_EV_REPLY ? s_reply : "", cap);
            else if (s_finished && s_active && (s_speech_ready ? s_speech_read == s_speech_frames
                     : (!s_reply[0] || (read_at(esp_timer_get_time()) >= strlen(s_reply)
                     && esp_timer_get_time() - s_reply_at > 2500000)))) {
                ev = MUSE_HATCH_EV_DONE; s_active = false; if (cap) text[0] = 0;
                /* Completion includes draining playback/text pagination. Do
                 * not leave the displayed mode dependent on caller cleanup. */
                muse_state_set_mode(MUSE_MODE_IDLE);
            }
        }
    });
    return ev;
}
bool muse_hatch_turn_caption(size_t played, char *out, size_t cap)
{
    if (!s_lock) return false;
    bool ok = false;
    LOCK({
        if (s_speech_stream && s_caption_count) {
            size_t position = played ? played - 1 : 0;
            size_t cue = 0;
            while (cue + 1 < s_caption_count && s_captions->cues[cue + 1].frame <= position) ++cue;
            strlcpy(out, s_captions->text + s_captions->cues[cue].offset, cap);
            ok = true;
        } else { size_t len = strlen(s_reply); if (len) {
        size_t frames = s_speech_stream ? s_speech_received : s_speech_frames;
        size_t at = frames ? (size_t)((uint64_t)played * len / frames) : read_at(esp_timer_get_time());
        if (at >= len) at = len - 1;
        while (at && ((unsigned char)s_reply[at] & 0xc0) == 0x80) --at;
        ok = muse_hatch_caption_at(s_reply, at, out, cap);
    } } });
    return ok;
}
size_t muse_hatch_turn_read(int16_t *pcm, size_t frames, int wait_ms)
{
    if (!s_lock || !pcm || !frames) return 0;
    size_t n = 0;
    LOCK({
        if (s_active && !s_typed && !s_error[0] && (s_speech_ready || s_speech_stream)) {
            n = s_speech_received - s_speech_read;
            if (n > frames) n = frames;
            memcpy(pcm, s_speech + s_speech_read, n * sizeof(int16_t));
            s_speech_read += n;
        }
    });
    if (n) return n;
    if (wait_ms > 0) vTaskDelay(pdMS_TO_TICKS(wait_ms) ? pdMS_TO_TICKS(wait_ms) : 1);
    return 0;
}
size_t muse_hatch_mp3_selftest(int16_t **pcm) { *pcm = NULL; return 0; }
