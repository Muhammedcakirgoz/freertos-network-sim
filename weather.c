#include "weather.h"
#include "net_port.h"
#include "ntp.h"
#include "subscribers.h"
#include "cJSON/cJSON.h"

#include <stdio.h>
#include <string.h>

#include "FreeRTOS.h"
#include "task.h"
#include "semphr.h"

#define ANLIK_HAVA_SORGU_ARALIGI_MS   ( 60 * 1000 )

static SemaphoreHandle_t xHttpMutex = NULL;

static void prvHttpMutexGarantiEt( void )
{
    if( xHttpMutex == NULL )
    {
        xHttpMutex = xSemaphoreCreateMutex();
        configASSERT( xHttpMutex != NULL );
    }
}

static void prvUrlEncode( const char *pcKaynak, char *pcHedef, size_t xHedefBoyutu )
{
    static const char *hexRakamlar = "0123456789ABCDEF";
    size_t j = 0;

    for( size_t i = 0; pcKaynak[ i ] != '\0' && j < xHedefBoyutu - 4; i++ )
    {
        unsigned char c = (unsigned char) pcKaynak[ i ];

        if( ( c >= 'A' && c <= 'Z' ) || ( c >= 'a' && c <= 'z' ) ||
            ( c >= '0' && c <= '9' ) || c == '-' || c == '_' || c == '.' || c == '~' )
        {
            pcHedef[ j++ ] = (char) c;
        }
        else
        {
            pcHedef[ j++ ] = '%';
            pcHedef[ j++ ] = hexRakamlar[ c >> 4 ];
            pcHedef[ j++ ] = hexRakamlar[ c & 0x0F ];
        }
    }

    pcHedef[ j ] = '\0';
}

AnlikHavaSonucu_t Weather_SehirSorgula( const char *pcSehirAdi )
{
    prvHttpMutexGarantiEt();

    AnlikHavaSonucu_t sonuc;
    memset( &sonuc, 0, sizeof( sonuc ) );

    if( xSemaphoreTake( xHttpMutex, pdMS_TO_TICKS( 10000 ) ) != pdTRUE )
    {
        printf( "[HavaAPI] HATA: HTTP mutex'i alinamadi (baska bir istek "
                "cok uzun suruyor).\n" );
        return sonuc;
    }

    static char cevapBuffer[ 4096 ];
    char yolBuffer[ 256 ];
    char sehirKodlanmis[ 128 ];

    prvUrlEncode( pcSehirAdi, sehirKodlanmis, sizeof( sehirKodlanmis ) );

    snprintf( yolBuffer, sizeof( yolBuffer ),
              "/v1/search?name=%s&count=1&language=tr&format=json",
              sehirKodlanmis );

    if( !Net_HttpsGet( "geocoding-api.open-meteo.com", yolBuffer,
                        cevapBuffer, sizeof( cevapBuffer ) ) )
    {
        printf( "[HavaAPI] HATA: Geocoding istegi basarisiz (%s).\n", pcSehirAdi );
        goto cikis;
    }

    {
        cJSON *geoJson = cJSON_Parse( cevapBuffer );

        if( geoJson == NULL )
        {
            printf( "[HavaAPI] HATA: Geocoding cevabi gecersiz JSON.\n" );
            goto cikis;
        }

        cJSON *sonuclar = cJSON_GetObjectItem( geoJson, "results" );

        if( sonuclar == NULL || !cJSON_IsArray( sonuclar ) || cJSON_GetArraySize( sonuclar ) == 0 )
        {
            printf( "[HavaAPI] HATA: '%s' icin sonuc bulunamadi.\n", pcSehirAdi );
            cJSON_Delete( geoJson );
            goto cikis;
        }

        cJSON *ilkSonuc = cJSON_GetArrayItem( sonuclar, 0 );
        cJSON *enlemItem = cJSON_GetObjectItem( ilkSonuc, "latitude" );
        cJSON *boylamItem = cJSON_GetObjectItem( ilkSonuc, "longitude" );

        if( enlemItem == NULL || boylamItem == NULL )
        {
            printf( "[HavaAPI] HATA: koordinat alanlari eksik.\n" );
            cJSON_Delete( geoJson );
            goto cikis;
        }

        sonuc.enlem = (float) enlemItem->valuedouble;
        sonuc.boylam = (float) boylamItem->valuedouble;

        cJSON_Delete( geoJson );
    }

    printf( "[HavaAPI] '%s' icin koordinat bulundu: (%.4f, %.4f)\n",
            pcSehirAdi, sonuc.enlem, sonuc.boylam );

    snprintf( yolBuffer, sizeof( yolBuffer ),
              "/v1/forecast?latitude=%.4f&longitude=%.4f&current_weather=true",
              sonuc.enlem, sonuc.boylam );

    if( !Net_HttpsGet( "api.open-meteo.com", yolBuffer,
                        cevapBuffer, sizeof( cevapBuffer ) ) )
    {
        printf( "[HavaAPI] HATA: Forecast istegi basarisiz.\n" );
        goto cikis;
    }

    {
        cJSON *havaJson = cJSON_Parse( cevapBuffer );

        if( havaJson == NULL )
        {
            printf( "[HavaAPI] HATA: Forecast cevabi gecersiz JSON.\n" );
            goto cikis;
        }

        cJSON *anlikHava = cJSON_GetObjectItem( havaJson, "current_weather" );

        if( anlikHava == NULL )
        {
            printf( "[HavaAPI] HATA: 'current_weather' alani bulunamadi.\n" );
            cJSON_Delete( havaJson );
            goto cikis;
        }

        cJSON *sicaklikItem = cJSON_GetObjectItem( anlikHava, "temperature" );

        if( sicaklikItem == NULL )
        {
            printf( "[HavaAPI] HATA: 'temperature' alani bulunamadi.\n" );
            cJSON_Delete( havaJson );
            goto cikis;
        }

        sonuc.sicaklik = (float) sicaklikItem->valuedouble;
        sonuc.basarili = true;

        cJSON_Delete( havaJson );
    }

    printf( "[HavaAPI] '%s' anlik sicaklik: %.1f C\n", pcSehirAdi, sonuc.sicaklik );

cikis:
    xSemaphoreGive( xHttpMutex );
    return sonuc;
}

void Weather_Yayinla( const char *pcSehirAdi, const AnlikHavaSonucu_t *pxSonuc )
{
    cJSON *havaRoot = cJSON_CreateObject();
    cJSON_AddStringToObject( havaRoot, "topic", "sensor/anlik_sicaklik" );

    char sicaklikStr[ 16 ];
    snprintf( sicaklikStr, sizeof( sicaklikStr ), "%.1f", pxSonuc->sicaklik );
    cJSON_AddStringToObject( havaRoot, "payload", sicaklikStr );
    cJSON_AddStringToObject( havaRoot, "sehir", pcSehirAdi );
    cJSON_AddNumberToObject( havaRoot, "zaman", (double) Ntp_SuankiZaman() );

    char *havaJsonStr = cJSON_PrintUnformatted( havaRoot );
    char gonderilecekHava[ 300 ];
    snprintf( gonderilecekHava, sizeof( gonderilecekHava ), "%s\n", havaJsonStr );

    xSemaphoreTake( xSubscriberListMutex, portMAX_DELAY );
    for( int i = 0; i < xSubscriberCount; i++ )
    {
        Net_Gonder( xSubscriberSockets[ i ], gonderilecekHava, (int) strlen( gonderilecekHava ) );
    }
    xSemaphoreGive( xSubscriberListMutex );

    cJSON_free( havaJsonStr );
    cJSON_Delete( havaRoot );
}

void Weather_Kaydet( const char *pcSehirAdi, const AnlikHavaSonucu_t *pxSonuc )
{
    time_t zaman = Ntp_SuankiZaman();

    FILE *kontrolFp = fopen( "anlik_hava_log.csv", "r" );
    bool dosyaVarMi = ( kontrolFp != NULL );
    if( kontrolFp != NULL )
    {
        fclose( kontrolFp );
    }

    FILE *fp = fopen( "anlik_hava_log.csv", "a" );

    if( fp == NULL )
    {
        printf( "[AnlikHavaLog] UYARI: log dosyasi acilamadi.\n" );
        return;
    }

    if( !dosyaVarMi )
    {
        fprintf( fp, "zaman,sehir,enlem,boylam,sicaklik\n" );
    }

    fprintf( fp, "%lld,%s,%.4f,%.4f,%.1f\n",
             (long long) zaman, pcSehirAdi, pxSonuc->enlem, pxSonuc->boylam, pxSonuc->sicaklik );

    fclose( fp );

    printf( "[AnlikHavaLog] Kaydedildi: %s (%.4f, %.4f) = %.1f C\n",
            pcSehirAdi, pxSonuc->enlem, pxSonuc->boylam, pxSonuc->sicaklik );
}

void vAnlikHavaTask( void *pvParameters )
{
    ( void ) pvParameters;

    vTaskDelay( pdMS_TO_TICKS( 5000 ) );

    for( ;; )
    {
        uint32_t xBildirimSayisi = ulTaskNotifyTake( pdTRUE, pdMS_TO_TICKS( ANLIK_HAVA_SORGU_ARALIGI_MS ) );

        if( xBildirimSayisi > 0 )
        {
            continue;
        }

        char sehirKopyasi[ 64 ];

        if( xSemaphoreTake( xSehirMutex, pdMS_TO_TICKS( 100 ) ) == pdTRUE )
        {
            strncpy( sehirKopyasi, cSuankiSehir, sizeof( sehirKopyasi ) - 1 );
            sehirKopyasi[ sizeof( sehirKopyasi ) - 1 ] = '\0';
            xSemaphoreGive( xSehirMutex );
        }
        else
        {
            strncpy( sehirKopyasi, "Ankara", sizeof( sehirKopyasi ) - 1 );
        }

        AnlikHavaSonucu_t sonuc = Weather_SehirSorgula( sehirKopyasi );

        if( sonuc.basarili )
        {
            Weather_Yayinla( sehirKopyasi, &sonuc );

            printf( "[AnlikHava] Periyodik yayin: %s = %.1f C\n", sehirKopyasi, sonuc.sicaklik );

            Weather_Kaydet( sehirKopyasi, &sonuc );
        }
    }
}