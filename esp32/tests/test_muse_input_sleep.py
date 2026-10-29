"""Exercise the actual auto-sleep policy and mode transitions with a virtual clock."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class SleepTests(unittest.TestCase):
    def test_busy_time_is_not_idle_and_interaction_still_delays_sleep(self):
        state = (ROOT / 'components/muse/muse_state.c').read_text()
        inputs = (ROOT / 'components/muse/muse_input.c').read_text()
        def function(source, start, end):
            return source[source.index(start):source.index(end, source.index(start))]
        actual = function(state, 'static float secs_since(', '\nvoid muse_state_init(')
        actual += function(state, 'void muse_state_set_mode(', '\nvoid muse_state_set_level(')
        actual += function(inputs, 'static void check_sleep(', '\nstatic void set_cpu_low(')
        fixture = r'''
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
typedef enum {MUSE_MODE_BOOT,MUSE_MODE_IDLE,MUSE_MODE_THINKING,MUSE_MODE_SPEAKING,MUSE_MODE_OFF} muse_mode_t;
static int64_t now,s_mode_since_us,last_poke;
static muse_mode_t s_mode=MUSE_MODE_BOOT;
static bool asleep,prompt;
static int after=120,sleeps;
typedef struct {int passkey;} muse_ble_status_t;
#define MUSE_LINK_CONFIRM 1
static int64_t esp_timer_get_time(void) {return now;}
static void muse_ble_status(muse_ble_status_t *b) {b->passkey=prompt;}
static int muse_link_state(void) {return 0;}
static int muse_settings_sleep_s(void) {return after;}
static bool muse_state_asleep(void) {return asleep;}
static float muse_state_idle_secs(void) {return (float)(now-last_poke)/1000000;}
static void set_asleep(bool b,const char *why) {if(b&&!asleep)sleeps++;asleep=b;}
'''
        checks = r'''
int main(void) {
 for(int busy=MUSE_MODE_THINKING;busy<=MUSE_MODE_SPEAKING;busy++) {
  now=0;last_poke=0;asleep=false;s_mode=MUSE_MODE_IDLE;sleeps=0;
  muse_state_set_mode(busy);now=600LL*1000000;check_sleep();assert(!asleep);
  muse_state_set_mode(MUSE_MODE_IDLE);check_sleep();assert(!asleep);
  now+=119LL*1000000;check_sleep();assert(!asleep);
  now+=2LL*1000000;check_sleep();assert(asleep&&sleeps==1);
 }
 /* Touching an already-idle device still starts a new idle interval. */
 asleep=false;now=1000LL*1000000;last_poke=now;s_mode_since_us=0;
 check_sleep();assert(!asleep);now+=119LL*1000000;check_sleep();assert(!asleep);
 now+=2LL*1000000;check_sleep();assert(asleep);
 /* A disabled timeout and an active pairing prompt retain their behavior. */
 asleep=false;after=0;now+=10000LL*1000000;check_sleep();assert(!asleep);
 after=120;prompt=true;check_sleep();assert(!asleep);
 prompt=false;check_sleep();assert(asleep);
 return 0;
}
'''
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / 'sleep.c'
            binary = Path(folder) / 'sleep'
            source.write_text(fixture + actual + checks)
            result = subprocess.run([os.environ.get('CC','cc'), '-std=c11', '-Wall', '-Werror',
                                     '-Wno-unused-parameter', str(source), '-o', str(binary)],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            result = subprocess.run([str(binary)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
