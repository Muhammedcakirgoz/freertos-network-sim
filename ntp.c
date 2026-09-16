#include "ntp.h"
#include "net_port.h"

#include <stdio.h>
#include <string.h>
#include <stdint.h>

#include "FreeRTOS.h"
#include "task.h"

#define NTP_SERVER              "pool.ntp.org"
#define NTP_PORT                123
#define NTP_SYNC_INTERVAL_MS    ( 5 * 60 * 1000 )
#define NTP_UNIX_EPOCH_FARKI    2208988800UL

typedef struct
{
    uint8_t  li_vn_mode;
    uint8_t  stratum;
    uint8_t  poll;
    uint8_t  precision;
    uint32_t rootDelay;
    uint32_t rootDispersion;
    uint32_t refId;
    uint32_t refTm_s;
    uint32_t refTm_f;
    uint32_t origTm_s;
    uint32_t origTm_f;
    uint32_t rxTm_s;
    uint32_t rxTm_f;
    uint32_t txTm_s;
    uint32_t txTm_f;
} NtpPaketi_t;

static volatile time_t xUnixZamanOfseti = 0;
static volatile bool bNtpSenkronize = false;

static bool prvNtpSorgula( time_t *pxSonucUnixZaman )
{
    NetAdres_t xSunucuAdresi;

    if( !Net_AdresCozumle( NTP_SERVER, NTP_PORT, &xSunucuAdresi ) )
    {
        printf( "[NTP] HATA: DNS cozumleme basarisiz (%s).\n", NTP_SERVER );
        return false;
    }

    printf( "[NTP] DNS cozumlendi: %s\n", NTP_SERVER );

    NetSocket_t xNtpSoket = Net_UdpSocketOlustur();

    if( xNtpSoket == NET_INVALID_SOCKET )
    {
        printf( "[NTP] HATA: soket olusturulamadi.\n" );
        return false;
    }

    Net_ZamanAsimiAyarla( xNtpSoket, 3000 );

    NtpPaketi_t paket;
    memset( &paket, 0, sizeof( paket ) );
    paket.li_vn_mode = 0x1B;

    int gonderilen = Net_UdpGonder( xNtpSoket, (const char *) &paket,
                                     sizeof( paket ), &xSunucuAdresi );

    if( gonderilen < 0 )
    {
        printf( "[NTP] HATA: veri gonderilemedi.\n" );
        Net_Kapat( xNtpSoket );
        return false;
    }

    int alinan = Net_Al( xNtpSoket, (char *) &paket, sizeof( paket ) );
    Net_Kapat( xNtpSoket );

    if( alinan != (int) sizeof( paket ) )
    {
        printf( "[NTP] HATA: gecersiz cevap (beklenen %d byte, alinan %d byte).\n",
                (int) sizeof( paket ), alinan );
        return false;
    }

    uint8_t *pucByte = (uint8_t *) &paket.txTm_s;
    uint32_t txTm_s = ( (uint32_t) pucByte[ 0 ] << 24 ) |
                      ( (uint32_t) pucByte[ 1 ] << 16 ) |
                      ( (uint32_t) pucByte[ 2 ] << 8  ) |
                      ( (uint32_t) pucByte[ 3 ] );

    *pxSonucUnixZaman = (time_t) ( txTm_s - NTP_UNIX_EPOCH_FARKI );

    return true;
}

bool Ntp_BaslangicSenkronizasyonuYap( void )
{
    time_t ilkSenkronZamani;

    printf( "[NTP] Baslangic senkronizasyonu yapiliyor...\n" );

    if( prvNtpSorgula( &ilkSenkronZamani ) )
    {
        xUnixZamanOfseti = ilkSenkronZamani;
        bNtpSenkronize = true;
        printf( "[NTP] Baslangic senkronizasyonu basarili.\n" );
        return true;
    }

    printf( "[NTP] UYARI: Baslangic senkronizasyonu basarisiz - "
            "vNtpSyncTask periyodik olarak tekrar deneyecek.\n" );
    return false;
}

void vNtpSyncTask( void *pvParameters )
{
    ( void ) pvParameters;

    for( ;; )
    {
        time_t sunucuZamani;

        if( prvNtpSorgula( &sunucuZamani ) )
        {
            TickType_t suankiTick = xTaskGetTickCount();
            time_t tickSaniye = (time_t) ( ( (uint64_t) suankiTick * portTICK_PERIOD_MS ) / 1000 );

            xUnixZamanOfseti = sunucuZamani - tickSaniye;
            bNtpSenkronize = true;

            printf( "[NTP] Senkronize edildi. Sunucu zamani (Unix): %lld\n",
                    (long long) sunucuZamani );
        }
        else
        {
            printf( "[NTP] Senkronizasyon basarisiz, %d saniye sonra tekrar denenecek.\n",
                    NTP_SYNC_INTERVAL_MS / 1000 );
        }

        vTaskDelay( pdMS_TO_TICKS( NTP_SYNC_INTERVAL_MS ) );
    }
}

time_t Ntp_SuankiZaman( void )
{
    TickType_t suankiTick = xTaskGetTickCount();
    time_t tickSaniye = (time_t) ( ( (uint64_t) suankiTick * portTICK_PERIOD_MS ) / 1000 );

    return xUnixZamanOfseti + tickSaniye;
}