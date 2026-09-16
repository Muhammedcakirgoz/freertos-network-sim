#ifndef WEATHER_H
#define WEATHER_H

#include <stdbool.h>
#include "FreeRTOS.h"
#include "task.h"
#include "semphr.h"
/* =======================================================================
 * weather.h
 *
 * Open-Meteo API entegrasyonu (anlik hava durumu) modulunun PUBLIC
 * arayuzu. Diger dosyalar, HTTPS/JSON detaylarina hic bakmadan, sadece
 * bu fonksiyonlari kullanir.
 * ===================================================================== */

typedef struct
{
    bool  basarili;
    float sicaklik;
    float enlem;
    float boylam;
} AnlikHavaSonucu_t;

/* Bir sehrin anlik sicakligini getirir (geocoding + forecast, iki
 * asamali sorgu). Mutex ile korunur - ayni anda tek cagri islenir. */
AnlikHavaSonucu_t Weather_SehirSorgula( const char *pcSehirAdi );

/* Bir sonucu JSON'a cevirip tum subscriber'lara yayinlar. */
void Weather_Yayinla( const char *pcSehirAdi, const AnlikHavaSonucu_t *pxSonuc );

/* Bir sonucu anlik_hava_log.csv'ye ekler (append). */
void Weather_Kaydet( const char *pcSehirAdi, const AnlikHavaSonucu_t *pxSonuc );

/* Periyodik yayin gorevi - xTaskCreate ile baslatilir (sadece broker). */
void vAnlikHavaTask( void *pvParameters );

/* Bu handle, runtime komut sonrasi periyodik gorevin sayacini
 * sifirlamak icin main.c'den erisiliyor. */
extern TaskHandle_t xAnlikHavaTaskHandle;

/* Su anki secili sehir - config'ten okunur, runtime komutla degisir.
 * main.c (config yukleme) ve weather.c (sorgu) tarafindan paylasilir. */
extern char cSuankiSehir[ 64 ];
extern SemaphoreHandle_t xSehirMutex;

#endif /* WEATHER_H */