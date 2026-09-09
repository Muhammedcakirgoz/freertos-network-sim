#ifndef NET_PORT_H
#define NET_PORT_H

/* =======================================================================
 * net_port.h
 *
 * PLATFORMDAN BAGIMSIZ AG (NETWORK) ARAYUZU
 *
 * Bu dosya, uygulama kodunun (main.c'deki task'lar, JSON mantigi vb.)
 * kullanacagi TEK ag arayuzudur. Uygulama kodu, ASLA dogrudan
 * platforma ozel fonksiyonlari (Winsock'un socket()/send()'i, lwIP'nin
 * kendi cagrilari vb.) cagirmaz - sadece burada tanimli Net_*
 * fonksiyonlarini kullanir.
 *
 * Her platform icin, bu arayuzun AYRI BIR IMPLEMENTASYONU yazilir:
 *   - net_port_windows.c  -> Winsock kullanir
 *   - net_port_esp32.c    -> lwIP kullanir
 *
 * Bu, FreeRTOS kernel'inin kendi "portable" mimarisiyle AYNI
 * prensiptir: kernel mantigi (task'lar, scheduler) her platformda
 * ayni kalir, sadece port katmani degisir.
 * ===================================================================== */

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

/* Soket tanimlayicisi. Gercek tipi platforma gore degisir (Windows'ta
 * SOCKET, lwIP'de int) - uygulama kodu bu tipin ICINE hic bakmaz,
 * sadece Net_* fonksiyonlarina gecirip alir. */
typedef intptr_t NetSocket_t;

/* Gecersiz/hatali soketi temsil eden deger. */
#define NET_INVALID_SOCKET   ( (NetSocket_t) -1 )

/* Net_Al() / Net_Gonder() donus degerleri icin ozel durumlar. */
#define NET_SONUC_HATA            ( -1 )   /* gercek hata - baglanti sonlandirilmali */
#define NET_SONUC_VERI_YOK        ( -2 )   /* non-blocking modda "su an veri yok" - NORMAL durum */
#define NET_SONUC_BAGLANTI_KAPANDI ( 0 )   /* karsi taraf baglantiyi duzgunce kapatti */


/* --- YASAM DONGUSU ---------------------------------------------------
 * Windows'ta WSAStartup/WSACleanup'a karsilik gelir. ESP32'de bu
 * fonksiyonlar muhtemelen bos olacak (lwIP'de boyle bir baslatma
 * gerekmiyor) ama arayuz ayni kalacagi icin uygulama kodu degismez. */
bool Net_Baslat( void );
void Net_Temizle( void );


/* --- TCP: SUNUCU (BROKER) TARAFI ------------------------------------- */

/* Belirtilen portta dinlemeye baslar (socket + bind + listen adimlarini
 * TEK BIR cagriya toplar - uygulama kodunun bu detaylari bilmesine
 * gerek yok). Basarisizlikta NET_INVALID_SOCKET doner. */
NetSocket_t Net_DinlemeBaslat( int xPort );

/* Bekleyen bir baglanti varsa kabul eder, yoksa HEMEN
 * NET_INVALID_SOCKET doner (bloklanmaz - non-blocking). */
NetSocket_t Net_BaglantiKabulEt( NetSocket_t xDinleyenSoket );


/* --- TCP: ISTEMCI (PUBLISHER/SUBSCRIBER) TARAFI ---------------------- */

/* Belirtilen IP/porta baglanir (socket + connect). Basarisizlikta
 * NET_INVALID_SOCKET doner - cagiran taraf isterse tekrar dener. */
NetSocket_t Net_Baglan( const char *pcIp, int xPort );


/* --- TCP: ORTAK VERI ISLEMLERI --------------------------------------- */

/* Veri gonderir. Gonderilen byte sayisini, hatada NET_SONUC_HATA doner. */
int Net_Gonder( NetSocket_t xSoket, const char *pcVeri, int xUzunluk );

/* Veri okur. Okunan byte sayisini doner. Ozel durumlar:
 *   NET_SONUC_BAGLANTI_KAPANDI -> karsi taraf kapatti
 *   NET_SONUC_VERI_YOK         -> su an veri yok (NORMAL, hata degil)
 *   NET_SONUC_HATA             -> gercek hata */
int Net_Al( NetSocket_t xSoket, char *pcBuffer, int xBoyut );

/* Soketi non-blocking moda alir (Windows'ta ioctlsocket, lwIP'de
 * fcntl kullanilir - uygulama bu farki gormez). */
bool Net_NonBlockingYap( NetSocket_t xSoket );

/* Soketi kapatir. */
void Net_Kapat( NetSocket_t xSoket );


/* --- UDP ------------------------------------------------------------- */

/* UDP soketi olusturup belirtilen porta bind eder. */
NetSocket_t Net_UdpDinlemeBaslat( int xPort );

/* UDP soketi olusturur (bind ETMEDEN - istemci tarafi icin, orn. NTP
 * sorgusu gonderirken). */
NetSocket_t Net_UdpSocketOlustur( void );

/* Gonderenin adresini tutan, platformdan bagimsiz opak yapi. Uygulama
 * kodu icerigine bakmaz, sadece Net_UdpAl'dan alip Net_UdpGonder'e
 * geri verir (cevap gondermek icin). */
typedef struct
{
    uint8_t ucVeri[ 32 ];   /* platforma ozel adres yapisi icin yeterli alan */
    int     xUzunluk;
} NetAdres_t;

/* UDP veri alir. Gonderenin adresini pxGonderen'e yazar (cevap
 * gonderebilmek icin). Donus degerleri Net_Al ile ayni. */
int Net_UdpAl( NetSocket_t xSoket, char *pcBuffer, int xBoyut, NetAdres_t *pxGonderen );

/* Belirtilen adrese UDP verisi gonderir. */
int Net_UdpGonder( NetSocket_t xSoket, const char *pcVeri, int xUzunluk, const NetAdres_t *pxHedef );

/* Soket icin okuma zaman asimi ayarlar (NTP sorgusunun sonsuza kadar
 * beklememesi icin). */
bool Net_ZamanAsimiAyarla( NetSocket_t xSoket, int xMilisaniye );


/* --- DNS ------------------------------------------------------------- */

/* Bir alan adini (orn. "pool.ntp.org") coozumleyip, sonucu dogrudan
 * Net_UdpGonder'e verilebilecek bir NetAdres_t'ye yazar. */
bool Net_AdresCozumle( const char *pcHostAdi, int xPort, NetAdres_t *pxSonuc );


/* --- ZAMAN ----------------------------------------------------------- */

/* Sistem acilisindan beri gecen milisaniye. Windows'ta
 * GetTickCount64(), ESP32'de esp_timer_get_time() kullanilir.
 * NOT: Bu, FreeRTOS'un xTaskGetTickCount()'undan FARKLIDIR - scheduler
 * baslamadan once de calisir ve ayni makinedeki TUM process'ler icin
 * ortak bir referanstir (cross-instance gecikme olcumu icin gerekli). */
uint64_t Net_SistemZamaniMs( void );

#endif /* NET_PORT_H */
