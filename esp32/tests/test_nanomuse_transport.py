"""Run the actual LAN transport parser and turn buffer against host fakes."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
CJSON = ROOT / "managed_components/espressif__cjson/cJSON"


class TransportTests(unittest.TestCase):
    def test_turn_generations_fragmentation_buffer_and_terminal_events(self):
        if not (CJSON / "cJSON.c").exists():
            self.skipTest("run an ESP-IDF build to fetch cJSON first")
        with tempfile.TemporaryDirectory() as folder:
            out = Path(folder)
            (out / "freertos").mkdir()
            headers = {
                "sdkconfig.h": '#pragma once\n',
                "esp_err.h": '#pragma once\n#include <assert.h>\ntypedef int esp_err_t;\n#define ESP_OK 0\n#define ESP_ERR_NO_MEM 1\n#define ESP_ERROR_CHECK(x) assert((x)==0)\n',
                "esp_log.h": '#pragma once\n',
                "esp_crt_bundle.h": '#pragma once\nstatic inline int esp_crt_bundle_attach(void *p) { return 0; }\n',
                "esp_timer.h": '#pragma once\n#include <stdint.h>\nextern int64_t test_now;\nstatic inline int64_t esp_timer_get_time(void) { return test_now; }\n',
                "esp_heap_caps.h": '#pragma once\n#include <stdlib.h>\n#define MALLOC_CAP_SPIRAM 1\n#define MALLOC_CAP_8BIT 2\n#define heap_caps_malloc(n,c) malloc(n)\n#define heap_caps_calloc(n,s,c) calloc(n,s)\n',
                "freertos/FreeRTOS.h": '#pragma once\n#include <stdint.h>\n#define portMAX_DELAY 0xffffffff\n#define pdMS_TO_TICKS(n) (n)\n#define pdTRUE 1\n#define pdPASS 1\ntypedef unsigned TickType_t;\n',
                "freertos/semphr.h": '#pragma once\n#include <assert.h>\ntypedef int *SemaphoreHandle_t;\nstatic int test_lock;\nstatic inline SemaphoreHandle_t xSemaphoreCreateMutex(void) { return &test_lock; }\nstatic inline void xSemaphoreTake(int *p, unsigned t) { assert(!*p); *p=1; }\nstatic inline void xSemaphoreGive(int *p) { assert(*p); *p=0; }\n',
                "freertos/task.h": '#pragma once\ntypedef void *TaskHandle_t;\nstatic inline int xTaskCreate(void (*f)(void*), const char *n, int z, void *a, int p, TaskHandle_t *t) { *t=(void*)1; return 1; }\nstatic inline void xTaskNotifyGive(TaskHandle_t t) {}\nstatic inline void vTaskDelay(unsigned t) {}\nstatic inline void ulTaskNotifyTake(int a, unsigned t) {}\n',
                "esp_websocket_client.h": '''#pragma once
#include <stdbool.h>
#include <stddef.h>
typedef void *esp_websocket_client_handle_t;
typedef const char *esp_event_base_t;
typedef struct { const char *data_ptr; int data_len; bool fin; int op_code; int payload_offset; int payload_len; } esp_websocket_event_data_t;
typedef struct { const char *uri, *headers; int buffer_size, task_stack, reconnect_timeout_ms, network_timeout_ms; bool enable_close_reconnect; int (*crt_bundle_attach)(void*); } esp_websocket_client_config_t;
enum { WEBSOCKET_EVENT_CONNECTED, WEBSOCKET_EVENT_DISCONNECTED, WEBSOCKET_EVENT_ERROR, WEBSOCKET_EVENT_DATA, WEBSOCKET_EVENT_CLOSED, WEBSOCKET_EVENT_ANY };
static bool test_connected = true;
static int test_sends;
static char test_last_send[8193];
static inline void *esp_websocket_client_init(void *p) { return NULL; }
static inline int esp_websocket_client_start(void *p) { return 0; }
static inline int esp_websocket_client_stop(void *p) { return 0; }
static inline int esp_websocket_client_destroy(void *p) { return 0; }
#define esp_websocket_register_events(...) ((void)0)
static inline int esp_websocket_client_send_text(void *p, const char *s, int n, unsigned t) { test_sends++; snprintf(test_last_send,sizeof(test_last_send),"%.*s",n,s); return n; }
static inline int esp_websocket_client_send_bin(void *p, const char *s, int n, unsigned t) { return n; }
static inline bool esp_websocket_client_is_connected(void *p) { return test_connected; }
''',
            }
            for name, text in headers.items():
                (out / name).write_text(text)
            source = out / "transport.c"
            source.write_text(r'''
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <stdint.h>
static size_t strlcpy(char *d, const char *s, size_t n) { size_t l=strlen(s); if(n) { size_t a=l<n-1?l:n-1; memcpy(d,s,a); d[a]=0; } return l; }
int64_t test_now = 1000000;
#include "muse_nanomuse.c"
static int console_done;
static muse_mode_t mode;
void muse_settings_gateway(char *url, char *key) { strcpy(url,"ws://192.168.1.2:8765/device"); if(key) strcpy(key,"test-key-at-least-16"); }
bool muse_wifi_connected(void) { return true; }
bool muse_ui_image_size(int *w, int *h) { *w=4; *h=2; return true; }
void muse_link_set_state(muse_link_state_t s) {}
void muse_state_set_mode(muse_mode_t s) { mode=s; }
void muse_state_set_caption(const char *fmt, ...) {}
void muse_state_set_asleep(bool asleep) {}
void muse_hatch_console(const char *type, const char *text, const char *fields, ...) { if(!strcmp(type,"done")) console_done++; }
bool muse_hatch_caption_at(const char *text, size_t at, char *out, size_t cap) { strlcpy(out,text+at,cap); return true; }
static void json(const char *text) {
    size_t n=strlen(text), a=n/2;
    esp_websocket_event_data_t e = {.data_ptr=text, .data_len=a, .op_code=1, .payload_len=n, .payload_offset=0, .fin=true};
    ws_event(NULL,NULL,WEBSOCKET_EVENT_DATA,&e);
    e.data_ptr=text+a; e.data_len=n-a; e.payload_offset=a;
    ws_event(NULL,NULL,WEBSOCKET_EVENT_DATA,&e);
}
static void audio(uint32_t id, const int16_t *pcm, size_t frames) {
    unsigned char data[2052]; assert(frames<=1024);
    for(int i=0;i<4;i++) data[i]=(unsigned char)(id>>(i*8));
    memcpy(data+4,pcm,frames*2);
    /* Binary data split over three transport callbacks. */
    esp_websocket_event_data_t e = {.data_ptr=(char*)data, .data_len=2, .op_code=2,
        .payload_len=4+frames*2, .payload_offset=0, .fin=true};
    ws_event(NULL,NULL,WEBSOCKET_EVENT_DATA,&e);
    e.data_ptr=(char*)data+2; e.data_len=3; e.payload_offset=2;
    ws_event(NULL,NULL,WEBSOCKET_EVENT_DATA,&e);
    e.data_ptr=(char*)data+5; e.data_len=4+frames*2-5; e.payload_offset=5;
    ws_event(NULL,NULL,WEBSOCKET_EVENT_DATA,&e);
}
static void image_header(uint32_t id, int w, int h, int bytes) {
    char meta[256];
    snprintf(meta,sizeof(meta),"{\"type\":\"image\",\"id\":%u,\"width\":%d,\"height\":%d,\"bytes\":%d,\"format\":\"rgb565be\"}",id,w,h,bytes);
    json(meta);
}
static void image_end(uint32_t id) {
    char meta[80]; snprintf(meta,sizeof(meta),"{\"type\":\"image_end\",\"id\":%u}",id); json(meta);
}
static void image_chunk(uint32_t id, uint32_t offset, const uint8_t *pixels, size_t bytes) {
    uint8_t data[2064] = {0}; assert(bytes<=2048);
    memcpy(data+4,"IMG1",4);
    for(int n=0;n<4;n++) { data[8+n]=id>>(n*8); data[12+n]=offset>>(n*8); }
    memcpy(data+16,pixels,bytes);
    esp_websocket_event_data_t e={.data_ptr=(char*)data,.data_len=7,.op_code=2,
        .payload_len=16+bytes,.payload_offset=0,.fin=false};
    ws_event(NULL,NULL,WEBSOCKET_EVENT_DATA,&e);
    e.data_ptr=(char*)data+7; e.data_len=9+bytes; e.payload_offset=7; e.op_code=0; e.fin=true;
    ws_event(NULL,NULL,WEBSOCKET_EVENT_DATA,&e);
}
int main(void) {
    muse_hatch_start();
    assert(muse_nanomuse_reply_phase()==MUSE_NANOMUSE_WAITING);
    json("{\"type\":\"hello\",\"protocol\":1}");
    assert(muse_hatch_ready());
    /* Idle notifications have no turn ID and never change the voice state. */
    json("{\"type\":\"hello\",\"protocol\":1,\"notifications\":true}");
    s_ws=(void*)1;
    send_inbox_controls();
    assert(strstr(test_last_send,"subscribe") && !s_subscribe_queued);
    const char *notice="{\"type\":\"notifications\",\"items\":[{\"token\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"kind\":\"background\",\"title\":\"Task finished\",\"text\":\"Result ready\"}]}";
    json(notice);
    muse_nanomuse_notifications_t notices={0};
    assert(muse_nanomuse_notifications(&notices) && notices.count==1 && !s_active);
    assert(muse_nanomuse_notification_count()==1);
    assert(!muse_nanomuse_notifications(&notices));
    unsigned before_notice=test_sends;
    assert(!muse_nanomuse_notification_read(notices.version-1,0));
    assert(!muse_nanomuse_notification_read(notices.version,1));
    assert(muse_nanomuse_notification_read(notices.version,0));
    assert(test_sends==before_notice);
    send_inbox_controls();
    assert(test_sends==before_notice+1 && strstr(test_last_send,"notification_read"));
    assert(!strstr(test_last_send,"approved"));
    send_inbox_controls(); assert(test_sends==before_notice+1); /* bounded retries */
    json("{\"type\":\"notifications\",\"items\":[{\"token\":\"bad\"}]}");
    assert(s_notifications.count==1);
    muse_hatch_turn_begin();
    assert(s_notifications.count==1); /* inbox survives a new chat */
    reset_turn();
    json("{\"type\":\"notifications\",\"items\":[]}");
    assert(muse_nanomuse_notifications(&notices) && !notices.count && !notices.sending);
    assert(!s_notification_read[0]);
    assert(muse_nanomuse_notification_count()==0);
    json(notice);
    ws_event(NULL,NULL,WEBSOCKET_EVENT_DISCONNECTED,NULL);
    assert(s_notifications.count==1); /* cached across a temporary disconnect */
    assert(!muse_nanomuse_notification_read(s_notifications.version,0));
    json("{\"type\":\"hello\",\"protocol\":1}");
    assert(!s_notifications.count); /* old gateways remain compatible */
    test_sends=0;
    /* Consent is bound to the displayed card and turn; sends run on the worker. */
    muse_hatch_turn_begin();
    char card[512];
    snprintf(card,sizeof(card),"{\"type\":\"approval\",\"id\":%u,\"token\":\"0123456789abcdef0123456789abcdef\",\"text\":\"Review operation\",\"can_approve\":true}",s_id);
    json(card);
    muse_nanomuse_approval_t view = {0};
    assert(muse_nanomuse_approval(&view) && view.pending && view.can_approve);
    uint32_t displayed = view.version;
    assert(!muse_nanomuse_approval(&view));
    assert(!muse_nanomuse_decide(displayed-1,true));
    assert(muse_nanomuse_decide(displayed,true));
    assert(test_sends==0 && !muse_nanomuse_decide(displayed,false));
    assert(muse_nanomuse_approval(&view) && view.sending);
    s_ws=(void*)1;
    assert(send_approval_decision(s_id) && test_sends==1);
    cJSON *sent = cJSON_Parse(test_last_send);
    assert(!strcmp(cJSON_GetObjectItem(sent,"type")->valuestring,"approval_decision"));
    assert(cJSON_IsTrue(cJSON_GetObjectItem(sent,"approved")));
    assert(cJSON_GetObjectItem(sent,"id")->valueint==(int)s_id);
    cJSON_Delete(sent);
    assert(send_approval_decision(s_id) && test_sends==1);
    snprintf(card,sizeof(card),"{\"type\":\"approval_closed\",\"id\":%u,\"token\":\"11111111111111111111111111111111\"}",s_id);
    json(card); assert(s_approval.pending);
    snprintf(card,sizeof(card),"{\"type\":\"approval_closed\",\"id\":%u,\"token\":\"0123456789abcdef0123456789abcdef\"}",s_id);
    json(card); assert(muse_nanomuse_approval(&view) && !view.pending);
    assert(!muse_nanomuse_decide(displayed,true));
    snprintf(card,sizeof(card),"{\"type\":\"approval\",\"id\":%u,\"token\":\"0123456789abcdef0123456789abcdef\",\"text\":\"Web review required\",\"can_approve\":false}",s_id);
    json(card); assert(muse_nanomuse_approval(&view) && !view.can_approve);
    assert(!muse_nanomuse_decide(view.version,true));
    assert(muse_nanomuse_decide(view.version,false));
    muse_hatch_turn_cancel();
    assert(!s_approval.pending && !s_decision_queued);
    assert(send_approval_decision(s_id) && test_sends==1);
    test_sends=0;
    muse_hatch_turn_begin(); uint32_t old=s_id;
    int16_t pcm[1024]; for(int i=0;i<1024;i++) pcm[i]=i-512;
    assert(muse_hatch_turn_audio_wait(pcm,1024,0)==1024);
    muse_hatch_turn_end(); assert(s_queued.frames==1024 && !memcmp(s_queued.pcm,pcm,sizeof(pcm)));
    muse_hatch_turn_cancel(); assert(!s_queued.id && !s_active);
    char message[256], text[100];
    snprintf(message,sizeof(message),"{\"type\":\"reply\",\"id\":%u,\"text\":\"stale\"}",old);
    json(message); assert(!s_reply[0]);
    muse_hatch_turn_begin();
    for(int i=0;i<MAX_FRAMES/1024;i++) assert(muse_hatch_turn_audio_wait(pcm,1024,0)==1024);
    size_t left=MAX_FRAMES%1024;
    assert(muse_hatch_turn_audio_wait(pcm,1024,0)==left);
    assert(muse_hatch_turn_audio_wait(pcm,1,0)==0);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_ERROR);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_NONE);
    muse_hatch_turn_begin();
    snprintf(message,sizeof(message),"{\"type\":\"sent\",\"id\":%u}",s_id); json(message);
    snprintf(message,sizeof(message),"{\"type\":\"reply\",\"id\":%u,\"text\":\"你好🙂\"}",s_id); json(message);
    snprintf(message,sizeof(message),"{\"type\":\"done\",\"id\":%u}",s_id); json(message);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_SENT);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_REPLY);
    assert(!strcmp(text,"你好🙂"));
    assert(muse_nanomuse_reply_phase()==MUSE_NANOMUSE_READING);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_NONE);
    test_now+=3000000;
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_DONE);
    assert(mode==MUSE_MODE_IDLE);
    assert(muse_nanomuse_reply_phase()==MUSE_NANOMUSE_WAITING);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_NONE);
    muse_hatch_text_turn(strdup("hello"));
    assert(s_typed && s_queued.text && mode==MUSE_MODE_THINKING);
    snprintf(message,sizeof(message),"{\"type\":\"reply\",\"id\":%u,\"text\":\"typed\"}",s_id); json(message);
    snprintf(message,sizeof(message),"{\"type\":\"done\",\"id\":%u}",s_id); json(message);
    assert(console_done==1);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_NONE);
    muse_hatch_text_cancel(); assert(!s_typed && !s_queued.id);
    /* Text is ready while TTS may take much longer. Reading a text-only
       fallback must not keep displaying the model's thinking phase. */
    json("{\"type\":\"hello\",\"protocol\":1,\"tts\":true}");
    muse_hatch_turn_begin(); mode=MUSE_MODE_THINKING;
    snprintf(message,sizeof(message),"{\"type\":\"reply\",\"id\":%u,\"text\":\"answer\"}",s_id); json(message);
    assert(muse_nanomuse_reply_phase()==MUSE_NANOMUSE_PREPARING_AUDIO);
    test_now+=3000000;
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_REPLY);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_NONE);
    snprintf(message,sizeof(message),"{\"type\":\"warning\",\"id\":%u,\"text\":\"TTS failed\"}",s_id); json(message);
    snprintf(message,sizeof(message),"{\"type\":\"done\",\"id\":%u}",s_id); json(message);
    assert(muse_nanomuse_reply_phase()==MUSE_NANOMUSE_READING);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_DONE);
    assert(mode==MUSE_MODE_IDLE);
    /* Reply audio is buffered fully before playback, and DONE waits for drain. */
    muse_hatch_turn_begin(); uint32_t speech_id=s_id; mode=MUSE_MODE_THINKING;
    snprintf(message,sizeof(message),"{\"type\":\"audio\",\"id\":%u,\"rate\":16000,\"channels\":1,\"format\":\"s16le\",\"frames\":1024}",s_id); json(message);
    audio(s_id-1,pcm,1024); assert(s_speech_received==0);
    audio(s_id,pcm,1024); assert(s_speech_received==1024);
    int16_t out[1024]; assert(muse_hatch_turn_read(out,1024,0)==0);
    snprintf(message,sizeof(message),"{\"type\":\"audio_end\",\"id\":%u}",s_id); json(message);
    snprintf(message,sizeof(message),"{\"type\":\"done\",\"id\":%u}",s_id); json(message);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_NONE);
    /* Server completion still owns the voice turn until playback drains.
       A typed request must not replace it and strand the voice task. */
    muse_hatch_text_turn(strdup("do not interrupt playback"));
    assert(!s_typed && s_id==speech_id && s_speech_ready);
    assert(muse_hatch_turn_read(out,320,0)==320 && !memcmp(out,pcm,640));
    mode=MUSE_MODE_SPEAKING;
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_NONE);
    assert(mode==MUSE_MODE_SPEAKING);
    assert(muse_hatch_turn_read(out,1024,0)==704 && !memcmp(out,pcm+320,1408));
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_DONE);
    assert(mode==MUSE_MODE_IDLE);
    assert(muse_hatch_turn_read(out,1024,0)==0);
    muse_hatch_turn_cancel(); assert(!s_speech);
    /* Reconnect/settings changes notify a waiting voice consumer, even if
       the server already completed and playback was still draining. */
    for(int finished=0;finished<2;finished++) {
        muse_hatch_turn_begin(); uint32_t before_reconnect=s_id;
        s_finished=finished;
        muse_hatch_config_changed();
        assert(s_id!=before_reconnect);
        assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_ERROR);
        assert(!strcmp(text,"GATEWAY CONFIGURATION CHANGED"));
        assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_NONE);
        assert(muse_nanomuse_reply_phase()==MUSE_NANOMUSE_WAITING);
    }
    muse_hatch_turn_begin(); audio(speech_id,pcm,1024); assert(!s_speech_received);
    /* Streaming plays received PCM before audio_end, survives underflow,
       and waits for both the end marker and drain before DONE. */
    muse_hatch_turn_begin();
    snprintf(message,sizeof(message),"{\"type\":\"audio\",\"id\":%u,\"rate\":16000,\"channels\":1,\"format\":\"s16le\",\"frames\":4096,\"stream\":true}",s_id); json(message);
    audio(s_id,pcm,512);
    assert(muse_hatch_turn_read(out,1024,0)==512);
    assert(muse_hatch_turn_read(out,1024,0)==0);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_NONE);
    audio(s_id,pcm,512);
    snprintf(message,sizeof(message),"{\"type\":\"audio_end\",\"id\":%u}",s_id); json(message);
    assert(s_speech_frames==1024 && s_finished);
    /* No separate done packet: audio_end is sufficient, but never finishes
       before the remaining samples have been consumed. */
    mode=MUSE_MODE_SPEAKING;
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_NONE);
    assert(muse_hatch_turn_read(out,1024,0)==512);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_DONE);
    assert(mode==MUSE_MODE_IDLE);
    snprintf(message,sizeof(message),"{\"type\":\"done\",\"id\":%u}",s_id); json(message);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_NONE);
    assert(mode==MUSE_MODE_IDLE);
    muse_hatch_turn_cancel();
    /* Phrase cues stay anchored to audio offsets as more text/PCM arrives. */
    muse_hatch_turn_begin();
    snprintf(message,sizeof(message),"{\"type\":\"audio\",\"id\":%u,\"rate\":16000,\"channels\":1,\"format\":\"s16le\",\"frames\":4096,\"stream\":true}",s_id); json(message);
    snprintf(message,sizeof(message),"{\"type\":\"caption\",\"id\":%u,\"frame\":0,\"text\":\"第一小句🙂\"}",s_id); json(message);
    audio(s_id,pcm,1024);
    assert(muse_hatch_turn_caption(1,text,sizeof(text)) && !strcmp(text,"第一小句🙂"));
    snprintf(message,sizeof(message),"{\"type\":\"caption\",\"id\":%u,\"frame\":1024,\"text\":\"第二小句\"}",s_id); json(message);
    audio(s_id,pcm,1024);
    snprintf(message,sizeof(message),"{\"type\":\"reply\",\"id\":%u,\"text\":\"a much longer final answer that must not jump the current caption ahead\"}",s_id); json(message);
    assert(muse_hatch_turn_caption(512,text,sizeof(text)) && !strcmp(text,"第一小句🙂"));
    assert(muse_hatch_turn_caption(1024,text,sizeof(text)) && !strcmp(text,"第一小句🙂"));
    assert(muse_hatch_turn_caption(1025,text,sizeof(text)) && !strcmp(text,"第二小句"));
    /* Stale, reversed and nonintegral cues cannot replace the current phrase. */
    snprintf(message,sizeof(message),"{\"type\":\"caption\",\"id\":%u,\"frame\":2048,\"text\":\"stale\"}",s_id-1); json(message);
    snprintf(message,sizeof(message),"{\"type\":\"caption\",\"id\":%u,\"frame\":1,\"text\":\"bad\"}",s_id); json(message);
    snprintf(message,sizeof(message),"{\"type\":\"caption\",\"id\":%u,\"frame\":2048.5,\"text\":\"bad\"}",s_id); json(message);
    assert(s_caption_count==2);
    muse_hatch_turn_cancel(); assert(!s_captions && !s_caption_count && !s_caption_bytes);
    /* Bad metadata, overflow and incomplete messages cannot reach playback. */
    muse_hatch_turn_begin();
    snprintf(message,sizeof(message),"{\"type\":\"audio\",\"id\":%u,\"rate\":16000,\"channels\":1,\"format\":\"s16le\",\"frames\":960001}",s_id); json(message);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_ERROR);
    muse_hatch_turn_begin();
    snprintf(message,sizeof(message),"{\"type\":\"audio\",\"id\":%u,\"rate\":16000,\"channels\":1,\"format\":\"s16le\",\"frames\":512}",s_id); json(message);
    audio(s_id,pcm,1024);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_ERROR);
    assert(muse_hatch_turn_read(out,1024,0)==0);
    muse_hatch_turn_begin();
    snprintf(message,sizeof(message),"{\"type\":\"audio\",\"id\":%u,\"rate\":16000,\"channels\":1,\"format\":\"s16le\",\"frames\":1024}",s_id); json(message);
    snprintf(message,sizeof(message),"{\"type\":\"audio_end\",\"id\":%u}",s_id); json(message);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_ERROR);
    muse_hatch_turn_begin();
    snprintf(message,sizeof(message),"{\"type\":\"audio\",\"id\":%u,\"rate\":16000,\"channels\":1,\"format\":\"s16le\",\"frames\":1024}",s_id); json(message);
    audio(s_id,pcm,512); assert(s_speech_received==512);
    uint32_t cancelled=s_id;
    muse_hatch_turn_cancel(); assert(!s_speech && !s_speech_received);
    muse_hatch_turn_begin(); audio(cancelled,pcm,512); assert(!s_speech_received);
    assert(muse_hatch_turn_read(out,1024,0)==0);
    muse_hatch_turn_cancel(); assert(!s_speech);
    /* A normal server close invalidates readiness. The upgraded client now
       handles reconnects; a fresh hello is required before retrying notes. */
    muse_hatch_turn_begin();
    uint32_t generation=s_config_gen;
    ws_event(NULL,NULL,WEBSOCKET_EVENT_CLOSED,NULL);
    assert(!muse_hatch_ready() && s_config_gen==generation);
    assert(muse_hatch_turn_event(text,sizeof(text))==MUSE_HATCH_EV_ERROR);
    assert(!strcmp(text,"GATEWAY DISCONNECTED"));
    ws_event(NULL,NULL,WEBSOCKET_EVENT_CONNECTED,NULL);
    assert(!muse_hatch_ready());
    json("{\"type\":\"hello\",\"protocol\":1}");
    assert(muse_hatch_ready());
    /* A transport failure relies on ESP-IDF's automatic reconnect path. */
    generation=s_config_gen;
    ws_event(NULL,NULL,WEBSOCKET_EVENT_DISCONNECTED,NULL);
    assert(!muse_hatch_ready() && s_config_gen==generation);
    s_ws=(void*)1; test_connected=false;
    assert(!send_control("begin",s_id,NULL) && test_sends==0);
    test_connected=true;
    assert(send_control("begin",s_id,NULL) && test_sends==1);
    /* Image frames are distinct from PCM; never expose an incomplete preview
     * or cover speech before the final PCM has drained. */
    json("{\"type\":\"hello\",\"protocol\":1}");
    muse_hatch_turn_begin();
    assert(muse_hatch_turn_audio_wait(pcm,2,0)==2); muse_hatch_turn_end();
    assert(send_control("begin",s_id,NULL));
    assert(strstr(test_last_send,"images") && s_display_w==4 && s_display_h==2);
    snprintf(message,sizeof(message),"{\"type\":\"audio\",\"id\":%u,\"rate\":16000,\"channels\":1,\"format\":\"s16le\",\"frames\":2}",s_id);
    json(message); audio(s_id,pcm,2);
    const uint8_t pixels[16]={0xf8,0,0x07,0xe0,0,0x1f,0xff,0xff,0xf8,0,0x07,0xe0,0,0x1f,0xff,0xff};
    image_header(s_id,4,2,16);
    image_chunk(s_id,0,pixels,8);
    int iw=0,ih=0;
    assert(!muse_nanomuse_image_take(&iw,&ih) && muse_nanomuse_image_bytes()==8);
    image_chunk(s_id,8,pixels+8,8); image_end(s_id);
    assert(!s_error[0] && s_image_complete && s_speech_received==2);
    assert(!muse_nanomuse_image_take(&iw,&ih));
    snprintf(message,sizeof(message),"{\"type\":\"audio_end\",\"id\":%u}",s_id); json(message);
    assert(!muse_nanomuse_image_take(&iw,&ih));
    assert(muse_hatch_turn_read(pcm,2,0)==2);
    uint8_t *preview=muse_nanomuse_image_take(&iw,&ih);
    assert(preview && iw==4 && ih==2 && !memcmp(preview,pixels,16)); free(preview);
    assert(!muse_nanomuse_image_take(&iw,&ih) && !muse_nanomuse_image_bytes());
    /* Bad images never fail a valid audio/text turn. */
    image_header(s_id,640,640,819200); assert(!s_image_pending && !s_error[0]);
    image_header(s_id,4,2,15); assert(!s_image_pending);
    image_header(s_id,4,2,16); image_chunk(s_id,4,pixels,8);
    assert(!s_image_pending && !s_error[0]);
    image_header(s_id,4,2,16); image_chunk(s_id,0,pixels,8); image_end(s_id);
    assert(!s_image_pending && !s_error[0]);
    image_header(s_id,4,2,16); image_chunk(s_id,0,pixels,16);
    test_now+=16000000; image_end(s_id); assert(!s_image_pending);
    image_header(s_id,4,2,16);
    uint32_t image_old=s_id;
    muse_hatch_turn_cancel(); image_chunk(image_old,0,pixels,16); image_end(image_old);
    assert(!s_image_pending && !s_error[0]);
    /* A late preview may arrive after DONE/playback, while the same generation
     * is idle. A different or cancelled turn still cannot display it. */
    muse_hatch_text_turn(strdup("Show picture")); assert(send_control("text",s_id,"Show picture"));
    snprintf(message,sizeof(message),"{\"type\":\"done\",\"id\":%u}",s_id); json(message);
    image_header(s_id,4,2,16); image_chunk(s_id,0,pixels,16); image_end(s_id);
    preview=muse_nanomuse_image_take(&iw,&ih); assert(preview && mode==MUSE_MODE_IDLE); free(preview);
    image_header(s_id,4,2,16);
    ws_event(NULL,NULL,WEBSOCKET_EVENT_DISCONNECTED,NULL); assert(!s_image_pending);
    muse_hatch_turn_cancel();
    /* Virtual long-running turn: genuine progress renews idle wait, while
     * duplicate/foreign notifications and polling cannot bypass either cap. */
    json("{\"type\":\"hello\",\"protocol\":1}");
    muse_hatch_turn_begin();
    int64_t beginning=test_now;
    test_now=beginning+100LL*1000000;
    snprintf(message,sizeof(message),"{\"type\":\"progress\",\"id\":%u,\"seq\":1}",s_id); json(message);
    int64_t activity=s_activity_at;
    test_now=beginning+200LL*1000000; assert(!turn_expired(s_id,beginning));
    json(message); assert(s_activity_at==activity); /* duplicate sequence */
    snprintf(message,sizeof(message),"{\"type\":\"progress\",\"id\":%u,\"seq\":2}",s_id-1); json(message);
    assert(s_activity_at==activity); /* cancelled/foreign generation */
    test_now=beginning+281LL*1000000; assert(turn_expired(s_id,beginning));
    s_approval.pending=true; assert(!turn_expired(s_id,beginning));
    s_approval.pending=false;
    for(unsigned seq=2;seq<=12;seq++) {
        test_now=beginning+(int64_t)seq*100*1000000;
        snprintf(message,sizeof(message),"{\"type\":\"progress\",\"id\":%u,\"seq\":%u}",s_id,seq);json(message);
        assert(turn_expired(s_id,beginning)==(seq==12));
    }
    /* Ordinary duplicate reply text does not reset activity; changed text does. */
    muse_hatch_turn_cancel();muse_hatch_turn_begin();
    snprintf(message,sizeof(message),"{\"type\":\"reply\",\"id\":%u,\"text\":\"first\"}",s_id);json(message);
    activity=s_activity_at;test_now+=1000000;json(message);assert(s_activity_at==activity);
    snprintf(message,sizeof(message),"{\"type\":\"reply\",\"id\":%u,\"text\":\"changed\"}",s_id);json(message);
    assert(s_activity_at==test_now);
    muse_hatch_turn_cancel();
    return 0;
}
''')
            command = [os.environ.get("CC", "cc"), "-std=gnu11", "-O1", "-g", "-fsanitize=address,undefined",
                       "-Wno-unused-parameter", "-DCONFIG_MUSE_NANOMUSE=1", "-I" + str(out),
                       "-I" + str(ROOT / "components/muse"), "-I" + str(CJSON), str(source), str(CJSON / "cJSON.c"),
                       "-lm", "-o", str(out / "transport")]
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            result = subprocess.run([str(out / "transport")], capture_output=True, text=True,
                           env={**os.environ, "ASAN_OPTIONS": "detect_leaks=0"})
            self.assertEqual(result.returncode, 0, result.stderr)
