/* Local nanoMuse startup. Cloud provisioning, BLE, Noise and OTA never start. */
#include <stdio.h>
#include <string.h>
#include "esp_log.h"
#include "esp_wifi.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"
#include "nvs_flash.h"
#include "muse_board.h"
#include "muse_input.h"
#include "muse_link.h"
#include "muse_settings.h"
#include "wifi_mgr.h"

#define SCAN_MAX 24
static SemaphoreHandle_t s_lock;
static TaskHandle_t s_radio;
static bool s_scan_pending, s_scanning, s_nap, s_failed;
static muse_wifi_ap_t s_aps[SCAN_MAX];
static int s_count;
static uint32_t s_scan_gen;
#define LOCK(...) do { xSemaphoreTake(s_lock, portMAX_DELAY); __VA_ARGS__; xSemaphoreGive(s_lock); } while (0)

static void kick(void) { if (s_radio) xTaskNotifyGive(s_radio); }
static void apply(void) { LOCK(s_failed = false;); kick(); }
static void nap(bool on) { LOCK(s_nap = on;); kick(); }
static void wifi_status(muse_wifi_status_t *out)
{
    memset(out, 0, sizeof(*out));
    muse_settings_wifi(out->ssid, NULL);
    if (wifi_mgr_is_connected()) {
        out->state = MUSE_WIFI_CONNECTED;
        esp_netif_ip_info_t ip;
        if (esp_netif_get_ip_info(wifi_mgr_get_netif(), &ip) == ESP_OK)
            snprintf(out->ip, sizeof(out->ip), IPSTR, IP2STR(&ip.ip));
        wifi_ap_record_t ap;
        if (esp_wifi_sta_get_ap_info(&ap) == ESP_OK) out->rssi = ap.rssi;
    } else if (!muse_settings_wifi_on()) out->state = MUSE_WIFI_OFF;
    else if (!out->ssid[0]) out->state = MUSE_WIFI_NO_NETWORK;
    else {
        bool failed;
        LOCK(failed = s_failed;);
        out->state = failed ? MUSE_WIFI_FAILED : MUSE_WIFI_CONNECTING;
        strlcpy(out->detail, failed ? "Check network/password" : "Joining...", sizeof(out->detail));
    }
}
static esp_err_t scan(void)
{
    LOCK(s_scan_pending = true; s_scanning = true;);
    kick();
    return ESP_OK;
}
static bool scanning(void) { bool b; LOCK(b = s_scanning;); return b; }
static int scan_results(muse_wifi_ap_t *out, int max, uint32_t *gen)
{
    int n;
    LOCK({ n = max < s_count ? max : s_count; memcpy(out, s_aps, n * sizeof(*out)); *gen = s_scan_gen; });
    return n;
}
static int saved(muse_wifi_saved_t *out, int max)
{
    char ssid[MUSE_SSID_MAX + 1];
    muse_settings_wifi(ssid, NULL);
    if (!ssid[0] || max < 1) return 0;
    memset(out, 0, sizeof(*out));
    strlcpy(out->ssid, ssid, sizeof(out->ssid));
    return 1;
}
static void forget(const char *ssid)
{
    char current[MUSE_SSID_MAX + 1];
    muse_settings_wifi(current, NULL);
    if (!ssid[0] || !strcmp(current, ssid)) muse_settings_set_wifi("", "");
}
static bool configured(void)
{
    char url[MUSE_GATEWAY_URL_MAX + 1], key[MUSE_GATEWAY_KEY_MAX + 1];
    muse_settings_gateway(url, key);
    return url[0] && key[0];
}
static const muse_link_ops_t s_ops = {
    .wifi_status = wifi_status, .wifi_apply = apply, .wifi_nap = nap,
    .wifi_scan = scan, .wifi_scanning = scanning, .wifi_scan_results = scan_results,
    .wifi_saved = saved, .wifi_forget = forget, .hatch_linked = configured,
};
static void radio_task(void *arg)
{
    (void)arg;
    char joined[MUSE_SSID_MAX + 1] = "", joined_pass[MUSE_PASS_MAX + 1] = "";
    for (;;) {
        bool do_scan, napping;
        LOCK({ do_scan = s_scan_pending; s_scan_pending = false; napping = s_nap; });
        if (do_scan) {
            wifi_scan_entry_t found[SCAN_MAX];
            int n = wifi_mgr_scan(found, SCAN_MAX, 0, NULL);
            LOCK({
                if (n >= 0) {
                    s_count = n;
                    for (int i = 0; i < n; ++i) {
                        strlcpy(s_aps[i].ssid, found[i].ssid, sizeof(s_aps[i].ssid));
                        s_aps[i].rssi = found[i].rssi;
                        s_aps[i].secure = found[i].auth_mode != WIFI_AUTH_OPEN;
                    }
                    ++s_scan_gen;
                }
                s_scanning = false;
            });
        }
        char ssid[MUSE_SSID_MAX + 1], pass[MUSE_PASS_MAX + 1];
        muse_settings_wifi(ssid, pass);
        bool enabled = muse_settings_wifi_on() && !napping && ssid[0];
        bool changed = strcmp(joined, ssid) || strcmp(joined_pass, pass);
        if ((!enabled || changed) && wifi_mgr_is_connected()) wifi_mgr_disconnect();
        if (enabled && !wifi_mgr_is_connected()) {
            bool ok = wifi_mgr_connect(ssid, pass, 10000);
            LOCK(s_failed = !ok;);
            strlcpy(joined, ssid, sizeof(joined));
            strlcpy(joined_pass, pass, sizeof(joined_pass));
        }
        memset(pass, 0, sizeof(pass));
        ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(5000));
    }
}
void nanomuse_start(void)
{
    muse_input_power_init();
    ESP_ERROR_CHECK(nvs_flash_init());
    s_lock = xSemaphoreCreateMutex();
    ESP_ERROR_CHECK(s_lock ? ESP_OK : ESP_ERR_NO_MEM);
    wifi_mgr_init();
    muse_link_register(&s_ops);
    muse_link_set_state(MUSE_LINK_CONNECTING);
    ESP_LOGI("link.nanomuse", "CoreS3 local gateway mode; cloud setup disabled");
    muse_app_run(muse_board_get());
    ESP_ERROR_CHECK(xTaskCreate(radio_task, "local_wifi", 6144, NULL, 4, &s_radio) == pdPASS ? ESP_OK : ESP_ERR_NO_MEM);
}
