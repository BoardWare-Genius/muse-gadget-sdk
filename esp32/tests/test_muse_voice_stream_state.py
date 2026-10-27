"""Exercise the real voice reply loop through a streaming underrun and resume."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

MUSE = Path(__file__).resolve().parents[1] / "components/muse"


class StreamStateTests(unittest.TestCase):
    def test_pause_resume_and_completion(self):
        source = (MUSE / "muse_voice.c").read_text()
        loop = source[source.index("static bool hatch_reply("):source.index("static void go_idle(const char *caption)\n{")]
        chat = (MUSE / "muse_chat.h").read_text()
        enum_at = chat.rfind("typedef enum", 0, chat.index("MUSE_HATCH_EV_NONE"))
        events = chat[enum_at:chat.index("} muse_hatch_ev_t;", enum_at) + len("} muse_hatch_ev_t;")]
        harness = r'''
#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#define MUSE_AUDIO_RATE 16000
#define MUSE_AUDIO_CHUNK 320
#define MUSE_CAPTION_MAX 400
#define MUSE_MODE_IDLE 1
#define MUSE_MODE_THINKING 3
#define MUSE_MODE_SPEAKING 4
#define MUSE_PTT_DOWN 1
#define ESP_LOGI(...) ((void)0)
#define ESP_LOGW(...) ((void)0)
#define pdMS_TO_TICKS(x) (x)
static int mode, reads;
static bool paused, resumed;
static int64_t now;
static int64_t esp_timer_get_time(void) { return now; }
static void muse_state_set_mode(int m) {
    if (mode==MUSE_MODE_SPEAKING && m==MUSE_MODE_THINKING) paused=true;
    if (paused && m==MUSE_MODE_SPEAKING) resumed=true;
    mode=m;
}
static void muse_state_set_caption(const char *f, ...) {}
static void muse_state_set_level(float f) {}
static float muse_audio_level(const int16_t *p,size_t n) { return 0; }
static void muse_audio_write(const int16_t *p,size_t n) { now+=20000; }
static void vTaskDelay(unsigned n) { now+=n*1000; }
static bool got_event(int e) { return false; }
static void muse_hatch_turn_cancel(void) {}
static bool muse_hatch_turn_caption(size_t n,char *p,size_t cap) { return false; }
static size_t muse_hatch_turn_read(int16_t *p,size_t n,int wait) {
    ++reads;
    assert(reads<20);
    now+=250000;
    return reads==1 || reads==7 ? 4 : 0;
}
static void go_idle(const char *p) { muse_state_set_mode(MUSE_MODE_IDLE); }
'''
        harness += events + r'''
static muse_hatch_ev_t muse_hatch_turn_event(char *p,size_t n) {
    static bool done;
    if (reads>=7 && !done) { done=true; return MUSE_HATCH_EV_DONE; }
    return MUSE_HATCH_EV_NONE;
}
'''
        harness += loop + r'''
int main(void) {
    bool delivered;
    assert(!hatch_reply(&delivered));
    assert(delivered && paused && resumed && mode==MUSE_MODE_IDLE);
    return 0;
}
'''
        with tempfile.TemporaryDirectory() as folder:
            code, binary = Path(folder) / "loop.c", Path(folder) / "loop"
            code.write_text(harness)
            subprocess.run([os.environ.get("CC", "cc"), "-std=c11", str(code), "-o", str(binary)], check=True)
            subprocess.run([str(binary)], check=True, timeout=5)
