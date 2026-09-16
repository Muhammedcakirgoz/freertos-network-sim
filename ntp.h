#ifndef NTP_H
#define NTP_H

#include <stdbool.h>
#include <time.h>

/* =======================================================================
 * ntp.h
 *
 * NTP (Network Time Protocol) modulunun PUBLIC arayuzu. Diger dosyalar
 * (main.c, weather.c, health.c) bu modulun ICINE (DNS cozumleme, UDP
 * paket formati, ofset hesabi) hic bakmaz - sadece bu iki fonksiyonu
 * kullanir.
 * ===================================================================== */

/* Scheduler baslamadan ONCE, bir kez bloklayici sekilde cagrilir - ilk
 * senkronizasyonu garanti altina alir (bkz. main.c/app_main). */
bool Ntp_BaslangicSenkronizasyonuYap( void );

/* Periyodik yeniden senkronizasyon gorevi - xTaskCreate ile baslatilir. */
void vNtpSyncTask( void *pvParameters );

/* "Su an kac" sorusunun cevabi - Unix zaman damgasi (saniye). NTP hic
 * senkronize olmadiysa 0 doner (cagiran taraf buna gore davranmali). */
time_t Ntp_SuankiZaman( void );

#endif /* NTP_H */