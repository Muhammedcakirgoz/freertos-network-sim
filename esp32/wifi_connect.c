#include "wifi_connect.h"
#include "app_config.h"
#include <string.h>
#include "freertos/FreeRTOS.h"
#include "freertos/event_groups.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_netif.h"
#include "esp_wifi.h"
#include "nvs_flash.h"

static EventGroupHandle_t events;
static const char *TAG = "WiFi";
#define GOT_IP BIT0

static void wifi_event(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    (void)arg;
    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) {
        xEventGroupClearBits(events, GOT_IP);
        esp_wifi_connect();
    } else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *event = data;
        ESP_LOGI(TAG, "IP alindi: " IPSTR, IP2STR(&event->ip_info.ip));
        xEventGroupSetBits(events, GOT_IP);
    }
}

bool wifi_baglan(void)
{
    if (!strcmp(WIFI_SSID, "WIFI_ADINI_YAZ")) {
        ESP_LOGE(TAG, "main/app_config.h icindeki WiFi ve broker ayarlarini doldur.");
        return false;
    }
    if (strlen(WIFI_SSID) > 32 || strlen(WIFI_SIFRE) > 64) return false;
    esp_err_t err = nvs_flash_init();
    if (err == ESP_ERR_NVS_NO_FREE_PAGES || err == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        err = nvs_flash_init();
    }
    ESP_ERROR_CHECK(err);
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    events = xEventGroupCreate();
    configASSERT(events);
    configASSERT(esp_netif_create_default_wifi_sta());
    wifi_init_config_t init = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&init));
    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, wifi_event, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, wifi_event, NULL));
    wifi_config_t config = {0};
    memcpy(config.sta.ssid, WIFI_SSID, strlen(WIFI_SSID));
    memcpy(config.sta.password, WIFI_SIFRE, strlen(WIFI_SIFRE));
    ESP_ERROR_CHECK(esp_wifi_set_storage(WIFI_STORAGE_RAM));
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &config));
    ESP_ERROR_CHECK(esp_wifi_start());
    while (!(xEventGroupWaitBits(events, GOT_IP, pdFALSE, pdFALSE, pdMS_TO_TICKS(30000)) & GOT_IP)) {
        ESP_LOGW(TAG, "IP bekleniyor. SSID, sifre ve 2.4 GHz agini kontrol et.");
    }
    return true;
}
