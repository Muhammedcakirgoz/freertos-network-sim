idf_component_register(SRCS "main.c" "net_port_esp32.c" "wifi_connect.c"
    INCLUDE_DIRS "."
    REQUIRES esp_wifi nvs_flash esp_http_client esp_netif lwip esp_event
             esp_timer mbedtls heap esp_hw_support)
