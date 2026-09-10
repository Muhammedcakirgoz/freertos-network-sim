#include "cJSON.h"
#include "net_port.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include <stdbool.h>
#include "freertos/timers.h"


#include "app_config.h"
#include "wifi_connect.h"
#include "esp_heap_caps.h"
#include "esp_random.h"
#include <sys/time.h>
#include <time.h>        /* time_t icin */
#include "driver/uart.h"
#include "driver/uart_vfs.h"


#define MAX_SICAKLIK_KAYIT 1100   


/* Standart 48 byte'lik NTP paket formati (RFC 5905). */
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
    uint32_t txTm_s;   /* bizim ilgilendigimiz alan - sunucunun cevabi gonderdigi an */
    uint32_t txTm_f;
} NtpPaketi_t;

typedef struct
{
    bool  basarili;
    float sicaklik;
    float enlem;
    float boylam;
} AnlikHavaSonucu_t;

static AnlikHavaSonucu_t prvSehirAnlikSicaklikGetir( const char *pcSehirAdi );


typedef struct
{
    char sehir[ 24 ];
    char tarih[ 16 ];
    float sicaklik;
    char durum[ 40 ];
} SicaklikKaydi_t;



typedef enum
{
    ROLE_UNDEFINED = 0,
    ROLE_BROKER,
    ROLE_PUBLISHER,
    ROLE_SUBSCRIBER
} SystemRole_t;

static SystemRole_t xMyRole = ROLE_UNDEFINED;

typedef struct
{
    char topic[ 64 ];
    char payload[ 128 ];  /* ONCEDEN 64 idi - nested JSON payload'lar icin buyutuldu */
} SensorData_t;

/* MQTT Publisher Task'tan Network Task'a GIDECEK veriyi tasiyan yapi.
 * SensorData_t'den ayri tutuyoruz cunku farkli bir yonde, farkli bir
 * amac icin kullaniliyor - mesaj_no alani da sadece bu yonde var. */
typedef struct
{
    char topic[ 64 ];
    char payload[ 16 ];
    int  mesaj_no;
    char sehir[ 24 ];
    char tarih[ 16 ];
    char durum[ 40 ];
} PublishData_t;

static QueueHandle_t xInternalCommQueue = NULL;
static QueueHandle_t xPublishQueue = NULL;



/* ---------------------------------------------------------------------
 * TASK ONCELIKLERI
 * ------------------------------------------------------------------- */
#define PRIORITY_HEALTH            ( tskIDLE_PRIORITY + 5 )
#define PRIORITY_INTERNAL_COMM     ( tskIDLE_PRIORITY + 4 )
#define PRIORITY_NETWORK           ( tskIDLE_PRIORITY + 3 )
#define PRIORITY_MQTT_PUBLISHER    ( tskIDLE_PRIORITY + 2 )
#define PRIORITY_MQTT_SUBSCRIBER   ( tskIDLE_PRIORITY + 2 )
#define PRIORITY_CLIENT_HANDLER    ( tskIDLE_PRIORITY + 3 )
#define PRIORITY_UDP_COMMAND       ( tskIDLE_PRIORITY + 2 )
#define PRIORITY_ANLIK_HAVA        ( tskIDLE_PRIORITY + 2 )

#define STACK_SIZE_HEALTH 4096 /* ESP-IDF: bytes */
#define STACK_SIZE_INTERNAL_COMM 3072 /* ESP-IDF: bytes */
#define STACK_SIZE_NETWORK 8192 /* ESP-IDF: bytes */
#define STACK_SIZE_MQTT 4096 /* ESP-IDF: bytes */
#define STACK_SIZE_CLIENT_HANDLER 16384 /* ESP-IDF: bytes */
#define MAX_CLIENTS                 5
#define SHARED_AUTH_TOKEN           "gizli_sifre123"
#define DEFAULT_PORT         ESP32_PORT
#define DEFAULT_BROKER_IP    ESP32_BROKER_IP
#define STACK_SIZE_UDP_COMMAND 4096 /* ESP-IDF: bytes */
#define IDLE_TIMEOUT_MS   30000   /* 30 saniye hic baglanti gelmezse kapan */

#define NTP_SERVER              "pool.ntp.org"
#define NTP_PORT                123
#define NTP_SYNC_INTERVAL_MS    ( 5 * 60 * 1000 )   /* 5 dakikada bir yeniden senkronize et */
#define NTP_UNIX_EPOCH_FARKI    2208988800UL         /* 1900-1970 arasi saniye farki */

#define STACK_SIZE_ANLIK_HAVA 16384 /* ESP-IDF: bytes */
#define ANLIK_HAVA_SORGU_ARALIGI_MS   ( 60 * 1000 )   /* 60 saniyede bir sorgula */


/* ---------------------------------------------------------------------
 * TASK HANDLE'LARI VE PROTOTIPLERI
 * ------------------------------------------------------------------- */

/* ---------------------------------------------------------------------
 * BROKER ICIN SUBSCRIBER LISTESI
 * Bagli subscriber'larin soketlerini burada tutuyoruz. Birden fazla
 * ClientHandlerTask (her biri farkli bir client icin calisan) bu listeye
 * AYNI ANDA erisebilir - bu yuzden bir MUTEX ile korumak zorundayiz.
 * ------------------------------------------------------------------- */
static NetSocket_t xSubscriberSockets[ MAX_CLIENTS ];
static int    xSubscriberCount = 0;
static SemaphoreHandle_t xSubscriberListMutex = NULL;


static TaskHandle_t xHealthTaskHandle          = NULL;
static TaskHandle_t xInternalCommTaskHandle    = NULL;
static TaskHandle_t xNetworkTaskHandle         = NULL;
static TaskHandle_t xMqttPublisherTaskHandle   = NULL;
static TaskHandle_t xMqttSubscriberTaskHandle  = NULL;
/* Broker durum yayini icin software timer. */
static TimerHandle_t xStatusTimer = NULL;
static TickType_t xSonBaglantiZamani = 0;
static volatile int xAktifClientSayisi = 0;
/* Komut satirindan override edilebilen ag ayarlari. Varsayilan
 * degerlerle baslar, prvParseNetworkArgsFromArgs() cagrildiginda
 * kullanici argüman verdiyse guncellenir. */
static int  xPortNumarasi = DEFAULT_PORT;
static char cBrokerIP[ 64 ] = DEFAULT_BROKER_IP;

/* NTP'den alinan zaman ile yerel tick sayaci arasindaki fark (saniye).
 * Bu ofset, periyodik olarak NTP ile yeniden senkronize edilir; aradaki
 * surede ise projenin kendi "real-time clock"u gibi calisir - her an
 * icin agdan tekrar sormaya gerek kalmadan hesaplanabilir. */
/* Su anki secili sehir - hem config dosyasindan hem runtime komuttan
 * (subscriber'dan gelen cmd/sehir_sorgu ile) degistirilebiliyor. Iki
 * farkli task/context'ten erisildigi icin mutex ile koruyoruz. */
static char cSuankiSehir[ 64 ] = "Ankara";
static SemaphoreHandle_t xSehirMutex = NULL;


static void vHealthTask( void *pvParameters );
static void vInternalCommTask( void *pvParameters );
static void vNetworkTask( void *pvParameters );
static void vEspPublisherNetwork(void);
static void vMqttPublisherTask( void *pvParameters );
static void vMqttSubscriberTask( void *pvParameters );
static void vClientHandlerTask( void *pvParameters );

static void prvCreateTasksForRole( SystemRole_t xRole );
static void vStatusBroadcastCallback( TimerHandle_t xTimer );
static void vUdpCommandTask( void *pvParameters );

static bool prvNtpSorgula( time_t *pxSonucUnixZaman );
static void vNtpSyncTask( void *pvParameters );
static time_t prvSuankiUnixZaman( void );

static void vAnlikHavaTask( void *pvParameters );
static void prvAnlikHavaKaydet( const char *pcSehirAdi, const AnlikHavaSonucu_t *pxSonuc, time_t zaman );
static void prvUrlEncode( const char *pcKaynak, char *pcHedef, size_t xHedefBoyutu );
static void prvKonsolBaslat(void);


/* =======================================================================
 * vSehirSorguTask()
 *
 * ESP32'nin seri portu (monitor ekrani) uzerinden interaktif sehir
 * sorgusu. Kullanici bir sehir yazip Enter'a basinca, o sehrin anlik
 * sicakligini ve koordinatlarini Open-Meteo'dan cekip ekrana basar.
 * Kalici olarak calisir (kendini SILMEZ) - test_subscriber.py'nin
 * ESP32 uzerindeki karsiligi gibi dusunulebilir.
 * ===================================================================== */
static void vSehirSorguTask(void *pvParameters)
{
    (void) pvParameters;

    vTaskDelay(pdMS_TO_TICKS(6000));

    char satir[64];
    size_t uzunluk;

    for( ;; )
    {
        printf("\nSorgulanacak sehir (yazip Enter'a bas): ");
        fflush(stdout);

        uzunluk = 0;
        satir[0] = '\0';

        for( ;; )
        {
            int ch = getchar();

            if( ch == '\r' || ch == '\n' )
            {
                printf("\n");
                break;
            }
            else if( ch == 127 || ch == '\b' )
            {
                if( uzunluk > 0 )
                {
                    uzunluk--;
                    printf("\b \b");
                    fflush(stdout);
                }
            }
            else if( ch >= 32 && ch < 127 && uzunluk < sizeof(satir) - 1 )
            {
                satir[uzunluk++] = (char) ch;
                putchar(ch);
                fflush(stdout);
            }
        }

        satir[uzunluk] = '\0';

        if( uzunluk == 0 )
        {
            continue;
        }

        /* Broker'a KISA SURELI, AYRI bir baglanti ac - sadece komut
         * gondermek icin. Asil (surekli acik) subscriber baglantimiz
         * (vNetworkTask icinde calisan) cevabi ZATEN otomatik alacak -
         * mentorumun istedigi "broker herkese yayinlar" akisi budur. */
        printf("[SehirSorgu] '%s' icin broker'a komut gonderiliyor...\n", satir);

        NetSocket_t komutSoket = Net_Baglan( cBrokerIP, xPortNumarasi );

        if( komutSoket == NET_INVALID_SOCKET )
        {
            printf("[SehirSorgu] HATA: broker'a baglanilamadi.\n");
            continue;
        }

        char kimlikMesaji[128];
        snprintf(kimlikMesaji, sizeof(kimlikMesaji), "AUTH:%s|ROLE:SUBSCRIBER\n", SHARED_AUTH_TOKEN);
        Net_Gonder(komutSoket, kimlikMesaji, (int) strlen(kimlikMesaji));

        vTaskDelay(pdMS_TO_TICKS(300));   /* authentication'in islenmesi icin kisa bekleme */

        cJSON *komutRoot = cJSON_CreateObject();
        cJSON_AddStringToObject(komutRoot, "topic", "cmd/sehir_sorgu");
        cJSON_AddStringToObject(komutRoot, "payload", satir);

        char *komutJsonStr = cJSON_PrintUnformatted(komutRoot);
        char gonderilecekKomut[128];
        snprintf(gonderilecekKomut, sizeof(gonderilecekKomut), "%s\n", komutJsonStr);

        Net_Gonder(komutSoket, gonderilecekKomut, (int) strlen(gonderilecekKomut));

        cJSON_free(komutJsonStr);
        cJSON_Delete(komutRoot);

        vTaskDelay(pdMS_TO_TICKS(300));   /* mesajin gonderilmesini garanti altina al */
        Net_Kapat(komutSoket);

        printf("[SehirSorgu] Komut gonderildi - cevabi yukaridaki log akisinda ara "
               "(InternalComm topic: sensor/anlik_sicaklik).\n");
    }
}
void app_main(void)
{
    prvKonsolBaslat();   /* EN BASTA - digerlerinden once */

    setvbuf(stdout, NULL, _IONBF, 0);
    xMyRole = ESP32_ROL;
    if (xMyRole < ROLE_BROKER || xMyRole > ROLE_SUBSCRIBER) {
        printf("[main] Gecersiz ESP32_ROL.\n");
        return;
    }
    if (!wifi_baglan() || !Net_Baslat()) return;
    printf("[main] Rol: %s | Broker: %s:%d\n",
           xMyRole == ROLE_PUBLISHER ? "PUBLISHER" :
           xMyRole == ROLE_SUBSCRIBER ? "SUBSCRIBER" : "BROKER",
           cBrokerIP, xPortNumarasi);
    xSubscriberListMutex = xSemaphoreCreateMutex();
    xSehirMutex = xSemaphoreCreateMutex();
    xInternalCommQueue = xQueueCreate(10, sizeof(SensorData_t));
    xPublishQueue = xQueueCreate(10, sizeof(PublishData_t));
    configASSERT(xSubscriberListMutex && xSehirMutex && xInternalCommQueue && xPublishQueue);
    time_t now;
    if (prvNtpSorgula(&now)) {
        struct timeval tv = { .tv_sec = now, .tv_usec = 0 };
        settimeofday(&tv, NULL);
        printf("[NTP] Sistem saati ayarlandi.\n");
    } else {
        printf("[NTP] Saat alinamadi; arka planda tekrar denenecek.\n");
    }
    /* ESP-IDF scheduler zaten calisiyor. Tum kaynaklar task'lardan once hazir. */
    prvCreateTasksForRole(xMyRole);
    if (xMyRole == ROLE_BROKER) {
        xStatusTimer = xTimerCreate("StatusTimer", pdMS_TO_TICKS(2000), pdTRUE,
                                   NULL, vStatusBroadcastCallback);
        configASSERT(xStatusTimer);
        configASSERT(xTimerStart(xStatusTimer, 0) == pdPASS);
    }
    /* Interaktif sehir sorgu task'i - kalici, hic silinmiyor. TLS/HTTPS
    * icin genis stack (16KB) gerekiyor - mbedtls'in derin fonksiyon
    * cagrilari nedeniyle. */
    xTaskCreate(vSehirSorguTask, "SehirSorgu", 16384, NULL, tskIDLE_PRIORITY + 1, NULL);
   
}
/* =======================================================================
 * prvKonsolBaslat()
 *
 * ESP-IDF'in varsayilan konsol surucusu, stdin'i BLOKLAYICI okumaya
 * uygun sekilde yapilandirilmamis oluyor - bu yuzden fgets() hemen,
 * BOS bir sonucla donuyor, sonsuz hizli bir donguye yol aciyordu. Bu
 * fonksiyon, UART surucusunu dogru sekilde kurup VFS'e (dosya sistemi
 * arayuzune) baglayarak, fgets()'in GERCEKTEN kullanici Enter'a
 * basana kadar beklemesini sagliyor.
 * ===================================================================== */
static void prvKonsolBaslat(void)
{
    setvbuf(stdin, NULL, _IONBF, 0);

    uart_vfs_dev_port_set_rx_line_endings(CONFIG_ESP_CONSOLE_UART_NUM, ESP_LINE_ENDINGS_CR);
    uart_vfs_dev_port_set_tx_line_endings(CONFIG_ESP_CONSOLE_UART_NUM, ESP_LINE_ENDINGS_CRLF);

    const uart_config_t uart_config = {
        .baud_rate = CONFIG_ESP_CONSOLE_UART_BAUDRATE,
        .data_bits = UART_DATA_8_BITS,
        .parity    = UART_PARITY_DISABLE,
        .stop_bits = UART_STOP_BITS_1,
        .source_clk = UART_SCLK_DEFAULT,
    };

    uart_driver_install(CONFIG_ESP_CONSOLE_UART_NUM, 256, 0, 0, NULL, 0);
    uart_param_config(CONFIG_ESP_CONSOLE_UART_NUM, &uart_config);
    uart_vfs_dev_use_driver(CONFIG_ESP_CONSOLE_UART_NUM);
}



/* =======================================================================
 * prvUrlEncode()
 *
 * URL'ye konacak metindeki HARF/RAKAM DISI her byte'i (bosluk, Turkce
 * ozel karakterler gibi) "%XX" (hex) formatina cevirir - HTTP/URL
 * standardinin gerektirdigi gibi. Bu, hem dogru bir HTTP istegi
 * olusturmamizi saglar, hem de UTF-8 cok baytli karakterlerin
 * prvAsciiToWide tarafindan YANLIS parcalanmasini ONLER - cunku
 * kodlama sonrasi metin, sadece ASCII (harf, rakam, %) icerir.
 * ===================================================================== */
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
            /* Bu karakterler URL'de guvenli, kodlamaya gerek yok. */
            pcHedef[ j++ ] = (char) c;
        }
        else
        {
            /* Diger her sey (bosluk, Turkce karakterlerin UTF-8
             * byte'lari vb.) "%XX" formatina cevriliyor. */
            pcHedef[ j++ ] = '%';
            pcHedef[ j++ ] = hexRakamlar[ c >> 4 ];
            pcHedef[ j++ ] = hexRakamlar[ c & 0x0F ];
        }
    }

    pcHedef[ j ] = '\0';
}


/* =======================================================================
 * prvSehirAnlikSicaklikGetir()
 *
 * IKI ASAMALI sorgu:
 * 1) Geocoding: sehir ADI -> enlem/boylam (Open-Meteo Geocoding API)
 * 2) Forecast: enlem/boylam -> ANLIK sicaklik (Open-Meteo Forecast API)
 *
 * Ikisi de HTTPS uzerinden, cJSON ile ayristirilarak.
 * ===================================================================== */
static AnlikHavaSonucu_t prvSehirAnlikSicaklikGetir( const char *pcSehirAdi )
{
    AnlikHavaSonucu_t sonuc;
    memset( &sonuc, 0, sizeof( sonuc ) );

    static char cevapBuffer[ 4096 ];
    char yolBuffer[ 256 ];

    /* --- ASAMA 1: GEOCODING (sehir adi -> koordinat) --- */
    char sehirKodlanmis[ 128 ];
    prvUrlEncode( pcSehirAdi, sehirKodlanmis, sizeof( sehirKodlanmis ) );

    snprintf( yolBuffer, sizeof( yolBuffer ),
          "/v1/search?name=%s&count=1&language=tr&format=json",
          sehirKodlanmis );

    if( !Net_HttpsGet( "geocoding-api.open-meteo.com", yolBuffer,
                    cevapBuffer, sizeof( cevapBuffer ) ) )
    {
        printf( "[HavaAPI] HATA: Geocoding istegi basarisiz (%s).\n", pcSehirAdi );
        return sonuc;
    }

    cJSON *geoJson = cJSON_Parse( cevapBuffer );

    if( geoJson == NULL )
    {
        printf( "[HavaAPI] HATA: Geocoding cevabi gecersiz JSON.\n" );
        return sonuc;
    }

    cJSON *sonuclar = cJSON_GetObjectItem( geoJson, "results" );

    if( sonuclar == NULL || !cJSON_IsArray( sonuclar ) || cJSON_GetArraySize( sonuclar ) == 0 )
    {
        printf( "[HavaAPI] HATA: '%s' icin sonuc bulunamadi.\n", pcSehirAdi );
        cJSON_Delete( geoJson );
        return sonuc;
    }

    cJSON *ilkSonuc = cJSON_GetArrayItem( sonuclar, 0 );
    cJSON *enlemItem = cJSON_GetObjectItem( ilkSonuc, "latitude" );
    cJSON *boylamItem = cJSON_GetObjectItem( ilkSonuc, "longitude" );

    if( enlemItem == NULL || boylamItem == NULL )
    {
        printf( "[HavaAPI] HATA: koordinat alanlari eksik.\n" );
        cJSON_Delete( geoJson );
        return sonuc;
    }

    sonuc.enlem = (float) enlemItem->valuedouble;
    sonuc.boylam = (float) boylamItem->valuedouble;

    cJSON_Delete( geoJson );

    printf( "[HavaAPI] '%s' icin koordinat bulundu: (%.4f, %.4f)\n",
            pcSehirAdi, sonuc.enlem, sonuc.boylam );

    /* --- ASAMA 2: FORECAST (koordinat -> ANLIK sicaklik) --- */
    snprintf( yolBuffer, sizeof( yolBuffer ),
          "/v1/forecast?latitude=%.4f&longitude=%.4f&current_weather=true",
          sonuc.enlem, sonuc.boylam );

    if( !Net_HttpsGet( "api.open-meteo.com", yolBuffer,
                    cevapBuffer, sizeof( cevapBuffer ) ) )
    {
        printf( "[HavaAPI] HATA: Forecast istegi basarisiz.\n" );
        return sonuc;
    }

    cJSON *havaJson = cJSON_Parse( cevapBuffer );

    if( havaJson == NULL )
    {
        printf( "[HavaAPI] HATA: Forecast cevabi gecersiz JSON.\n" );
        return sonuc;
    }

    cJSON *anlikHava = cJSON_GetObjectItem( havaJson, "current_weather" );

    if( anlikHava == NULL )
    {
        printf( "[HavaAPI] HATA: 'current_weather' alani bulunamadi.\n" );
        cJSON_Delete( havaJson );
        return sonuc;
    }

    cJSON *sicaklikItem = cJSON_GetObjectItem( anlikHava, "temperature" );

    if( sicaklikItem == NULL )
    {
        printf( "[HavaAPI] HATA: 'temperature' alani bulunamadi.\n" );
        cJSON_Delete( havaJson );
        return sonuc;
    }

    sonuc.sicaklik = (float) sicaklikItem->valuedouble;
    sonuc.basarili = true;

    cJSON_Delete( havaJson );

    printf( "[HavaAPI] '%s' anlik sicaklik: %.1f C\n", pcSehirAdi, sonuc.sicaklik );

    return sonuc;
}

/* =======================================================================
 * prvSehirConfigYukle()
 *
 * Baslangicta, config JSON dosyasindan varsayilan sehri okur. cJSON
 * zaten projede kullanildigi icin ekstra bir parse mekanizmasina
 * ihtiyac duymuyoruz - tutarlilik ve ileride kolay genisletilebilirlik
 * (orn. sorgu araligi gibi ek ayarlar) icin JSON secildi.
 * ===================================================================== */

/* =======================================================================
 * vAnlikHavaTask()
 *
 * Periyodik olarak (60 saniyede bir), o anki secili sehrin ANLIK
 * sicakligini Open-Meteo'dan cekip "sensor/anlik_sicaklik" topic'iyle
 * TUM subscriber'lara yayinlar. Sadece BROKER rolunde calisir.
 * ===================================================================== */
static void vAnlikHavaTask( void *pvParameters )
{
    ( void ) pvParameters;

    /* Ilk sorgu icin biraz bekle - NTP/DNS gibi diger baslangic
     * islemlerine firsat taniyoruz. */
    vTaskDelay( pdMS_TO_TICKS( 5000 ) );

    for( ;; )
    {
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

        AnlikHavaSonucu_t sonuc = prvSehirAnlikSicaklikGetir( sehirKopyasi );

        if( sonuc.basarili )
        {
            cJSON *havaRoot = cJSON_CreateObject();
            cJSON_AddStringToObject( havaRoot, "topic", "sensor/anlik_sicaklik" );

            char sicaklikStr[ 16 ];
            snprintf( sicaklikStr, sizeof( sicaklikStr ), "%.1f", sonuc.sicaklik );
            cJSON_AddStringToObject( havaRoot, "payload", sicaklikStr );
            cJSON_AddStringToObject( havaRoot, "sehir", sehirKopyasi );
            cJSON_AddNumberToObject( havaRoot, "zaman", (double) prvSuankiUnixZaman() );

            char *havaJsonStr = cJSON_PrintUnformatted( havaRoot );
            char gonderilecekHava[ 300 ];
            snprintf( gonderilecekHava, sizeof( gonderilecekHava ), "%s\n", havaJsonStr );

            xSemaphoreTake( xSubscriberListMutex, portMAX_DELAY );
            for( int i = 0; i < xSubscriberCount; i++ )
            {
                Net_Gonder( xSubscriberSockets[ i ], gonderilecekHava, (int) strlen( gonderilecekHava ));
            }
            xSemaphoreGive( xSubscriberListMutex );

            cJSON_free( havaJsonStr );
            cJSON_Delete( havaRoot );

            printf( "[AnlikHava] Periyodik yayin: %s = %.1f C\n", sehirKopyasi, sonuc.sicaklik );
            prvAnlikHavaKaydet( sehirKopyasi, &sonuc, prvSuankiUnixZaman() );
        }

        vTaskDelay( pdMS_TO_TICKS( ANLIK_HAVA_SORGU_ARALIGI_MS ) );
    }
}

/* =======================================================================
 * prvAnlikHavaKaydet()
 *
 * Her basarili anlik hava sorgusunu, CSV formatinda bir dosyaya EKLER
 * (append). Zamanla, program calistikca, sistemin KENDI gerceklestirdigi
 * gercek API sorgularindan olusan, buyuyen bir veri gunlugu birikir -
 * mentorumun bahsettigi "tablo" fikrinin, sistemin kendisi tarafindan
 * otomatik olarak tutulan hali.
 * ===================================================================== */
static void prvAnlikHavaKaydet(const char *pcSehirAdi, const AnlikHavaSonucu_t *pxSonuc, time_t zaman)
{
    /* Ilk asamada dosya sistemi yok: kayit seri porttan okunur. */
    printf("[AnlikHavaLog] %lld,%s,%.4f,%.4f,%.1f\n", (long long)zaman,
           pcSehirAdi, pxSonuc->enlem, pxSonuc->boylam, pxSonuc->sicaklik);
}





/* =======================================================================
 * prvNtpSorgula()
 *
 * TEK BIR NTP sorgusu yapar: DNS ile sunucuyu bulur, UDP ile 48 byte'lik
 * istek paketi gonderir, cevabi okuyup Unix zamanina cevirir.
 * ===================================================================== */
static bool prvNtpSorgula( time_t *pxSonucUnixZaman )
{
    NetAdres_t xSunucuAdresi;

    /* --- DNS COZUMLEME (platformdan bagimsiz) --- */
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

    /* Sonsuza kadar beklemesin diye 3 saniyelik zaman asimi. */
    if (!Net_ZamanAsimiAyarla(xNtpSoket, 3000)) { Net_Kapat(xNtpSoket); return false; }

    NtpPaketi_t paket;
    memset( &paket, 0, sizeof( paket ) );
    paket.li_vn_mode = 0x1B;   /* LI=0, VN=3 (NTPv3), Mode=3 (client istegi) */

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

    /* txTm_s: sunucunun cevabi GONDERDIGI andaki, 1900'den beri gecen
     * saniye. Network byte order'dan (buyuk-endian) makinemizin byte
     * order'ina ceviriyoruz, sonra 1970 referansina kaydiriyoruz.
     *
     * NOT: ntohl() yerine ELLE bit kaydirma kullaniyoruz - boylece
     * bu kod, Winsock'a (ya da baska bir platform kutuphanesine)
     * BAGIMLI OLMADAN, her platformda AYNI sekilde calisiyor. */
    uint8_t *pucByte = (uint8_t *) &paket.txTm_s;
    uint32_t txTm_s = ( (uint32_t) pucByte[ 0 ] << 24 ) |
                      ( (uint32_t) pucByte[ 1 ] << 16 ) |
                      ( (uint32_t) pucByte[ 2 ] << 8  ) |
                      ( (uint32_t) pucByte[ 3 ] );

    if ((paket.li_vn_mode & 7) != 4 || (paket.li_vn_mode >> 6) == 3 ||
        paket.stratum == 0 || paket.stratum > 15 || txTm_s < NTP_UNIX_EPOCH_FARKI) return false;
    *pxSonucUnixZaman = (time_t) ( txTm_s - NTP_UNIX_EPOCH_FARKI );

    return true;
}

/* =======================================================================
 * vNtpSyncTask()
 *
 * Periyodik olarak (5 dakikada bir) NTP sunucusuyla senkronize olur.
 * Basarili her senkronizasyonda, "NTP zamani - yerel tick zamani"
 * farkini (offset) gunceller - boylece aradaki surede aga gitmeden,
 * dogrudan tick sayacindan gercek zaman hesaplanabilir.
 * ===================================================================== */
static void vNtpSyncTask(void *pvParameters)
{
    (void)pvParameters;
    for (;;) {
        time_t now;
        bool ok = prvNtpSorgula(&now);
        if (ok) {
            struct timeval tv = { .tv_sec = now, .tv_usec = 0 };
            settimeofday(&tv, NULL);
            printf("[NTP] Senkronize: %lld\n", (long long)now);
        }
        vTaskDelay(pdMS_TO_TICKS(ok ? NTP_SYNC_INTERVAL_MS : 15000));
    }
}

/* =======================================================================
 * prvSuankiUnixZaman()
 *
 * "Su an kac" sorusunun cevabi - aga hic gitmeden, kaydedilen offset ve
 * o anki tick sayacindan hesaplar. NTP hic senkronize olmadiysa (henuz
 * ilk sorgu yapilmadiysa), offset 0'dir, donen deger sadece "tick
 * sayacinin saniyeye cevrilmis hali" olur (anlamli bir gercek zaman
 * DEGILDIR, bilgi amaclidir).
 * ===================================================================== */
static time_t prvSuankiUnixZaman(void)
{
    return time(NULL);
}







/*-----------------------------------------------------------*/
/* =======================================================================
 * prvParseNetworkArgsFromArgs()
 *
 * Komut satirindan OPSIYONEL port ve broker IP argumanlarini okur:
 *   argv[2] -> port numarasi (hem broker hem client icin)
 *   argv[3] -> broker'in IP adresi (sadece publisher/subscriber icin
 *              anlamli - broker kendi IP'sini dinlemez, INADDR_ANY
 *              kullanir)
 *
 * Bu sayede, ayni .exe dosyasi FARKLI PORTLARDA birden fazla kez
 * calistirilabilir - ornegin ayni bilgisayarda iki ayri broker
 * instance'i (8080 ve 9090 gibi) es zamanli calisabilir.
 * ===================================================================== */




/* =======================================================================
 * prvPrintUsage()
 * ===================================================================== */


/* =======================================================================
 * prvCreateTasksForRole()
 * ===================================================================== */
static void prvCreateTasksForRole( SystemRole_t xRole )
{
    BaseType_t xResult;

    /* --- Her rolde ortak olan task'lar --- */

    xResult = xTaskCreate( vHealthTask,
                            "Health",
                            STACK_SIZE_HEALTH,
                            NULL,
                            PRIORITY_HEALTH,
                            &xHealthTaskHandle );
    configASSERT( xResult == pdPASS );

    xResult = xTaskCreate( vInternalCommTask,
                            "InternalComm",
                            STACK_SIZE_INTERNAL_COMM,
                            NULL,
                            PRIORITY_INTERNAL_COMM,
                            &xInternalCommTaskHandle );
    configASSERT( xResult == pdPASS );

    xResult = xTaskCreate( vNetworkTask,
                            "Network",
                            STACK_SIZE_NETWORK,
                            (void *)(uintptr_t) xRole,
                            PRIORITY_NETWORK,
                            &xNetworkTaskHandle );
    configASSERT( xResult == pdPASS );

    /* MQTT'DEN BAGIMSIZ komut kanali - her rolde calisir, TCP/JSON
     * altyapisina hic dokunmaz, kendi UDP soketini ve basit metin
     * protokolunu kullanir. */
    {
        TaskHandle_t xUdpCmdHandle = NULL;
        xResult = xTaskCreate( vUdpCommandTask,
                                "UdpCmd",
                                STACK_SIZE_UDP_COMMAND,
                                NULL,
                                PRIORITY_UDP_COMMAND,
                                &xUdpCmdHandle );
        configASSERT( xResult == pdPASS );
    }
    /* NTP senkronizasyon task'i - her rolde calisir, gercek zamani
    * global bir sunucudan alip yerel olarak isletir. */
    {
        TaskHandle_t xNtpTaskHandle = NULL;
        xResult = xTaskCreate( vNtpSyncTask,
                                "NtpSync",
                                STACK_SIZE_UDP_COMMAND,   /* benzer boyut yeterli */
                                NULL,
                                PRIORITY_UDP_COMMAND,      /* benzer oncelik yeterli */
                                &xNtpTaskHandle );
        configASSERT( xResult == pdPASS );
    }

    /* --- Role ozel task'lar --- */

    switch( xRole )
    {
        case ROLE_BROKER:
        {
            TaskHandle_t xAnlikHavaHandle = NULL;
            xResult = xTaskCreate( vAnlikHavaTask,
                                    "AnlikHava",
                                    STACK_SIZE_ANLIK_HAVA,
                                    NULL,
                                    PRIORITY_ANLIK_HAVA,
                                    &xAnlikHavaHandle );
            configASSERT( xResult == pdPASS );
        }
        break;

        case ROLE_PUBLISHER:
            xResult = xTaskCreate( vMqttPublisherTask,
                                    "MqttPub",
                                    STACK_SIZE_MQTT,
                                    NULL,
                                    PRIORITY_MQTT_PUBLISHER,
                                    &xMqttPublisherTaskHandle );
            configASSERT( xResult == pdPASS );
            break;

        case ROLE_SUBSCRIBER:
            xResult = xTaskCreate( vMqttSubscriberTask,
                                    "MqttSub",
                                    STACK_SIZE_MQTT,
                                    NULL,
                                    PRIORITY_MQTT_SUBSCRIBER,
                                    &xMqttSubscriberTaskHandle );
            configASSERT( xResult == pdPASS );
            break;

        default:
            configASSERT( pdFALSE );
            break;
    }

    ( void ) xResult;
}

/* =======================================================================
 * TASK IMPLEMENTASYONLARI 
 * ===================================================================== */

/* Task durumunu insan okunur metne ceviren yardimci fonksiyon. */
static const char * prvTaskDurumuStr( eTaskState eDurum )
{
    switch( eDurum )
    {
        case eRunning:   return "CALISIYOR";
        case eReady:     return "HAZIR";
        case eBlocked:   return "BLOKE";
        case eSuspended: return "SUSPEND";
        case eDeleted:   return "SILINMIS";
        default:         return "BILINMIYOR";
    }
}

static void vHealthTask( void *pvParameters )
{
    ( void ) pvParameters;
    TickType_t xLastWakeTime = xTaskGetTickCount();

    for( ;; )
    {
        /* --- 1) HEAP ANALIZI --- */
        size_t bosHeap    = xPortGetFreeHeapSize();
        size_t minBosHeap = xPortGetMinimumEverFreeHeapSize();
        size_t toplamHeap = heap_caps_get_total_size(MALLOC_CAP_8BIT);
        int dolulukYuzdesi = toplamHeap ? (int)(100 - bosHeap * 100 / toplamHeap) : 0;
        /* Zaman damgasini BURADA, tum printf'lerden ONCE yakaliyoruz -
        * boylece olcum, sadece "ag + islem" gecikmesini yansitir,
        * konsol yazma suresini DEGIL. */
        

        printf( "\n[Health] ===== SISTEM SAGLIK RAPORU =====\n" );
        printf( "[Health] Heap: %u byte bos / %u toplam (doluluk: %%%d)\n",
                (unsigned int) bosHeap,
                (unsigned int) toplamHeap,
                dolulukYuzdesi );
        printf( "[Health] Heap en dusuk seviye: %u byte (leak gostergesi: surekli dusuyorsa sizinti var)\n",
                (unsigned int) minBosHeap );

        /* --- 2) TASK ANALIZI: durum + stack high water mark --- */
        UBaseType_t uxTaskSayisi = uxTaskGetNumberOfTasks();
        TaskStatus_t *pxDurumlar = pvPortMalloc( uxTaskSayisi * sizeof( TaskStatus_t ) );

        if( pxDurumlar != NULL )
        {
            UBaseType_t uxAlinan = uxTaskGetSystemState( pxDurumlar, uxTaskSayisi, NULL );

            printf( "[Health] %-18s %-10s %s\n", "TASK", "DURUM", "STACK BOS (byte)" );
            for( UBaseType_t i = 0; i < uxAlinan; i++ )
            {
                printf( "[Health] %-18s %-10s %u\n",
                        pxDurumlar[ i ].pcTaskName,
                        prvTaskDurumuStr( pxDurumlar[ i ].eCurrentState ),
                        (unsigned int) pxDurumlar[ i ].usStackHighWaterMark );

                /* Stack tasmasina yaklasan task'lari OZEL OLARAK uyar. */
                if( pxDurumlar[ i ].usStackHighWaterMark < 512 )
                {
                    printf( "[Health] !!! UYARI: '%s' task'inin stack'i tasma sinirina yaklasiyor!\n",
                            pxDurumlar[ i ].pcTaskName );
                }
            }

            vPortFree( pxDurumlar );
        }
        printf( "[Health] =================================\n\n" );
        /* --- HEALTH VERISINI JSON OLARAK YAYINLA (sadece BROKER'da,
        * subscriber'lar varsa) --- */
        if( xMyRole == ROLE_BROKER && xSubscriberListMutex != NULL )
        {
            if( xSemaphoreTake( xSubscriberListMutex, pdMS_TO_TICKS( 100 ) ) == pdTRUE )
            {
                unsigned long long xOlcumZamani = (unsigned long long) Net_SistemZamaniMs();
                cJSON *healthRoot = cJSON_CreateObject();
                cJSON_AddStringToObject( healthRoot, "topic", "system/health" );

                /* payload'i artik DUZ STRING degil, GERCEK NESTED JSON nesnesi
                * olarak olusturuyoruz. */
                cJSON *healthPayload = cJSON_CreateObject();
                cJSON_AddNumberToObject( healthPayload, "heap", (double) bosHeap );
                cJSON_AddNumberToObject( healthPayload, "min_heap", (double) minBosHeap );
                cJSON_AddNumberToObject( healthPayload, "doluluk", dolulukYuzdesi );
                cJSON_AddNumberToObject( healthPayload, "task_sayisi", (double) uxTaskSayisi );
                cJSON_AddNumberToObject( healthPayload, "ts", (double) xOlcumZamani );

                /* YENI: NTP ile senkronize edilmis GERCEK zaman (Unix timestamp).
                * "ts" alani (GetTickCount64 tabanli) sadece AYNI MAKINEDEKI
                * instance'lar arasi gecikme olcumu icin - "zaman" ise NTP'den gelen,
                * DUNYA CAPINDA anlamli, gercek zamandir. Ikisi FARKLI amaclar icin,
                * ikisini de tutuyoruz. */
                cJSON_AddNumberToObject( healthPayload, "zaman", (double) prvSuankiUnixZaman() );


                /* cJSON_AddItemToObject: healthPayload'i healthRoot'a "tasir" -
                * healthPayload'i ayrica cJSON_Delete etmemize GEREK YOK, healthRoot
                * silinince otomatik silinir (parent-child sahiplik ilişkisi). */
                cJSON_AddItemToObject( healthRoot, "payload", healthPayload );

                char *healthJson = cJSON_PrintUnformatted( healthRoot );
                char gonderilecek[ 256 ];
                snprintf( gonderilecek, sizeof( gonderilecek ), "%s\n", healthJson );

                for( int i = 0; i < xSubscriberCount; i++ )
                {
                    Net_Gonder( xSubscriberSockets[ i ], gonderilecek, (int) strlen( gonderilecek ));
                }

                cJSON_free( healthJson );
                cJSON_Delete( healthRoot );

                xSemaphoreGive( xSubscriberListMutex );
            }
            else
            {
                printf( "[Health] UYARI: Health verisi icin mutex 100ms icinde alinamadi, bu tur atlaniyor.\n" );
            }
        }

        vTaskDelayUntil( &xLastWakeTime, pdMS_TO_TICKS( 25000 ) );
    }
}

static void vInternalCommTask( void *pvParameters )
{
    ( void ) pvParameters;
    SensorData_t alinanVeri;

     for( ;; )
    {
        /* Queue'da veri gelene kadar BEKLE (portMAX_DELAY).
         * Bu, vTaskDelay'e gerek birakmiyor - task zaten queue
         * bos oldugu surece CPU'yu kullanmiyor (Blocked durumda). */
        if( xQueueReceive( xInternalCommQueue, &alinanVeri, portMAX_DELAY ) == pdPASS )
        {
            printf( "[InternalComm] Veri islendi -> topic: %s, payload: %s\n",
                    alinanVeri.topic, alinanVeri.payload );
        }
    }
}

static void vNetworkTask( void *pvParameters )
{
    SystemRole_t xRole = (SystemRole_t)(uintptr_t) pvParameters;
    if (xRole == ROLE_PUBLISHER) { vEspPublisherNetwork(); vTaskDelete(NULL); }

    if( xRole == ROLE_BROKER )
    {
        printf( "[Network] BROKER modu: baglanti dinlemeye hazirlaniliyor...\n" );

        /* Soket olusturma, bind, listen ve non-blocking ayari - TUMU
        * Net_DinlemeBaslat() icinde, platformdan bagimsiz sekilde yapiliyor. */
        NetSocket_t listenSocket = Net_DinlemeBaslat( xPortNumarasi );

        if( listenSocket == NET_INVALID_SOCKET )
        {
            printf( "[Network] HATA: dinleme baslatilamadi.\n" );
            vTaskDelete( NULL );
        }

        printf( "[Network] Broker port %d'de dinlemede...\n", xPortNumarasi );

        NetSocket_t clientSocket = NET_INVALID_SOCKET;

        /* BROKER'a ozel sonsuz dongu - accept() burada, if bloğunun İÇİNDE */
        for( ;; )
        {
            clientSocket = Net_BaglantiKabulEt( listenSocket );;

            if( clientSocket != NET_INVALID_SOCKET  )
            {       
                    xSonBaglantiZamani = xTaskGetTickCount();  /* idle-timeout sayacini sifirla */
                    printf( "[Network] Yeni bir client baglandi! Kendi task'i olusturuluyor...\n" );

                    TaskHandle_t xClientHandle = NULL;
                    BaseType_t xResult = xTaskCreate( vClientHandlerTask,
                                        "ClientHandler",
                                        STACK_SIZE_CLIENT_HANDLER,
                                        (void *)(uintptr_t) clientSocket,
                                        PRIORITY_CLIENT_HANDLER,
                                        &xClientHandle );

                if( xResult != pdPASS )
                {
                    printf( "[Network] HATA: Client task'i olusturulamadi, baglanti reddediliyor.\n" );
                    Net_Kapat( clientSocket );
                }

                clientSocket = NET_INVALID_SOCKET ;
}
            else
            {
                /* Bekleyen baglanti yok, normal durum. */
            }

            vTaskDelay( pdMS_TO_TICKS( 100 ) );
        }
    }
    else
    {
        printf( "[Network] CLIENT modu: broker'a baglanmaya hazirlaniliyor...\n" );

        /* Baglanana kadar tekrar dene - soket olusturma, adres cozumleme ve
        * connect adimlarinin TAMAMI Net_Baglan() icinde, platformdan
        * bagimsiz sekilde yapiliyor. */
        NetSocket_t clientSocket = NET_INVALID_SOCKET;

        while( clientSocket == NET_INVALID_SOCKET )
        {
            clientSocket = Net_Baglan( cBrokerIP, xPortNumarasi );

            if( clientSocket == NET_INVALID_SOCKET )
            {
                printf( "[Network] Broker'a baglanilamadi, 2 saniye sonra tekrar denenecek...\n" );
                vTaskDelay( pdMS_TO_TICKS( 2000 ) );
            }
        }

            

            /* Test amacli: periyodik olarak basit bir mesaj gonder.
            * Boylece TCP baglantisi uzerinden GERCEKTEN veri aktigini
            * gozlemleyebilecegiz. Faz 4'te bu, gercek JSON verisiyle
            * degistirilecek. */
           
            printf( "[Network] Broker'a basariyla baglanildi!\n" );

            /* Kendimizi broker'a tanitiyoruz - ilk mesaj olarak rol bilgimizi
            * gonderiyoruz. Broker bu mesaji okuyup bizi subscriber listesine
            * ekleyip eklemeyecegine karar verecek. */
            char kimlikMesaji[ 128 ];
            const char *rolString = ( xRole == ROLE_PUBLISHER ) ? "PUBLISHER" : "SUBSCRIBER";
            snprintf( kimlikMesaji, sizeof( kimlikMesaji ), "AUTH:%s|ROLE:%s\n", SHARED_AUTH_TOKEN, rolString );

            Net_Gonder( clientSocket, kimlikMesaji, (int) strlen( kimlikMesaji ));
            printf( "[Network] Kimlik bildirildi: %s\n", kimlikMesaji );


        if( xRole == ROLE_PUBLISHER )
        {
            /* Artik veri URETMIYORUZ - MQTT Publisher Task'in queue'ya
            * koydugu veriyi BEKLIYORUZ (portMAX_DELAY sayesinde veri
            * gelene kadar CPU'yu kullanmadan Blocked durumda kaliyoruz). */
            PublishData_t gelenVeri;

            for( ;; )
            {
                if( xQueueReceive( xPublishQueue, &gelenVeri, portMAX_DELAY ) == pdPASS )
                {
                    /* --- JSON nesnesi olustur --- */
                    cJSON *root = cJSON_CreateObject();
                    cJSON_AddStringToObject( root, "topic", gelenVeri.topic );
                    cJSON_AddStringToObject( root, "payload", gelenVeri.payload );
                    cJSON_AddNumberToObject( root, "mesaj_no", gelenVeri.mesaj_no );
                    cJSON_AddStringToObject( root, "sehir", gelenVeri.sehir );
                    cJSON_AddStringToObject( root, "tarih", gelenVeri.tarih );
                    cJSON_AddStringToObject( root, "durum", gelenVeri.durum );
                    cJSON_AddNumberToObject( root, "zaman", (double) prvSuankiUnixZaman() );

                    char *jsonString = cJSON_PrintUnformatted( root );

                    char gonderilecekVeri[ 256 ];
                    snprintf( gonderilecekVeri, sizeof( gonderilecekVeri ), "%s\n", jsonString );

                    Net_Gonder( clientSocket, gonderilecekVeri, (int) strlen( gonderilecekVeri ));
                    printf( "[Network] JSON mesaj gonderildi: %s\n", jsonString );

                    cJSON_free( jsonString );
                    cJSON_Delete( root );
                }
            }
        }
        else
        {
            
            Net_NonBlockingYap( clientSocket );

            char recvBuffer[ 256 ];
            char mesajBuffer[ 1024 ] = { 0 };  /* framing icin biriktirme buffer'i */
            int mesajBufferUzunluk = 0;

            for( ;; )
            {
                int bytesReceived = Net_Al( clientSocket, recvBuffer, sizeof( recvBuffer ) - 1 );
                if( bytesReceived > 0 )
                {
                    recvBuffer[ bytesReceived ] = '\0';

                    /* Gelen veriyi kalici buffer'a ekle (broker tarafiyla ayni mantik) */
                    if( mesajBufferUzunluk + bytesReceived < (int) sizeof( mesajBuffer ) - 1 )
                    {
                        memcpy( mesajBuffer + mesajBufferUzunluk, recvBuffer, bytesReceived );
                        mesajBufferUzunluk += bytesReceived;
                        mesajBuffer[ mesajBufferUzunluk ] = '\0';
                    }

                    /* Buffer icinde tam mesaj(lar) var mi diye kontrol et */
                    char *newlinePos;
                    while( ( newlinePos = strchr( mesajBuffer, '\n' ) ) != NULL )
                    {
                        *newlinePos = '\0';

                        /* --- JSON DOGRULAMASI (broker ile ayni mantik) --- */
                        cJSON *parsedJson = cJSON_Parse( mesajBuffer );

                        if( parsedJson == NULL )
                        {
                            printf( "[Network] UYARI: Gecersiz JSON alindi, yok sayiliyor. "
                                    "Gelen: %s\n", mesajBuffer );
                        }
                        else
                        {
                            cJSON *topicItem   = cJSON_GetObjectItem( parsedJson, "topic" );
                            cJSON *payloadItem = cJSON_GetObjectItem( parsedJson, "payload" );

                            bool bPayloadGecerli = ( payloadItem != NULL ) &&
                        ( cJSON_IsString( payloadItem ) || cJSON_IsObject( payloadItem ) );

                            if( topicItem != NULL && cJSON_IsString( topicItem ) && bPayloadGecerli )
                            {
                                char payloadMetni[ 128 ];

                                if( cJSON_IsObject( payloadItem ) )
                                {
                                    /* system/health gibi nested JSON payload'lar icin - objeyi
                                    * tekrar kompakt bir string'e ceviriyoruz, boylece mevcut
                                    * SensorData_t (sabit boyutlu char[] tutan) yapisiyla uyumlu
                                    * kaliyoruz. */
                                    char *tempStr = cJSON_PrintUnformatted( payloadItem );
                                    strncpy( payloadMetni, tempStr, sizeof( payloadMetni ) - 1 );
                                    payloadMetni[ sizeof( payloadMetni ) - 1 ] = '\0';
                                    cJSON_free( tempStr );
                                }
                                else
                                {
                                    strncpy( payloadMetni, payloadItem->valuestring, sizeof( payloadMetni ) - 1 );
                                    payloadMetni[ sizeof( payloadMetni ) - 1 ] = '\0';
                                }

                                /* --- CROSS-INSTANCE HEALTH PERFORMANS OLCUMU (guncellendi) ---
                                * Artik ts alanini strstr ile string icinde ARAMIYORUZ - ts,
                                * nested obje icinde GERCEK bir sayisal alan, dogrudan okuyoruz. */
                                if( strcmp( topicItem->valuestring, "system/health" ) == 0 &&
                                    cJSON_IsObject( payloadItem ) )
                                {
                                    cJSON *tsItem = cJSON_GetObjectItem( payloadItem, "ts" );

                                    if( tsItem != NULL && cJSON_IsNumber( tsItem ) )
                                    {
                                        printf("[HealthPeer] Uzak cihaz saglik verisi alindi; cihazlarin acilis saatleri karsilastirilmaz.\n");
                                    }
                                }

                                SensorData_t veri;
                                strncpy( veri.topic, topicItem->valuestring, sizeof( veri.topic ) - 1 );
                                veri.topic[ sizeof( veri.topic ) - 1 ] = '\0';
                                strncpy( veri.payload, payloadMetni, sizeof( veri.payload ) - 1 );
                                veri.payload[ sizeof( veri.payload ) - 1 ] = '\0';

                                if( xQueueSend( xInternalCommQueue, &veri, portMAX_DELAY ) != pdPASS )
                                {
                                    printf( "[Network] UYARI: Veri Internal Comm queue'suna gonderilemedi.\n" );
                                }
                            }
                            else
                            {
                                printf( "[Network] UYARI: JSON gecerli ama gerekli alanlar eksik.\n" );
                            }

                            cJSON_Delete( parsedJson );
                        }

                        /* Islenen mesaji buffer'dan cikar, kalani basa kaydir */
                        int islenenUzunluk = (int)( newlinePos - mesajBuffer ) + 1;
                        int kalanUzunluk = mesajBufferUzunluk - islenenUzunluk;

                        memmove( mesajBuffer, newlinePos + 1, kalanUzunluk );
                        mesajBufferUzunluk = kalanUzunluk;
                        mesajBuffer[ mesajBufferUzunluk ] = '\0';
                    }
                }
                else if( bytesReceived == NET_SONUC_BAGLANTI_KAPANDI )
                {
                    printf( "[Network] Broker baglantiyi kapatti.\n" );
                    break;
                }
                else if( bytesReceived == NET_SONUC_HATA )
                {
                    printf( "[Network] HATA: veri alinamadi.\n" );
                    break;
                }

                                vTaskDelay( pdMS_TO_TICKS( 100 ) );
                            }
                        }
                    }
                    vTaskDelete(NULL);
}

static void vClientHandlerTask( void *pvParameters )
{
    NetSocket_t clientSocket = (NetSocket_t)(uintptr_t) pvParameters;
    bool bIsSubscriber = false;
    bool bIsAuthenticated = false;

    printf( "[ClientHandler] Yeni client task'i basladi (socket: %d)\n",
            (int) clientSocket );
            xAktifClientSayisi++;

    
    Net_NonBlockingYap( clientSocket );

    char recvBuffer[ 256 ];
    char mesajBuffer[ 1024 ] = { 0 };
    int mesajBufferUzunluk = 0;

    /* Authentication icin makul bir zaman asimi - sonsuza kadar
     * bekleme, 5 saniyede gelmezse baglantiyi kapat. */
    TickType_t xBaglantiBaslangic = xTaskGetTickCount();

    for( ;; )
    {
        int bytesReceived = Net_Al( clientSocket, recvBuffer, sizeof( recvBuffer ) - 1 );
        if( bytesReceived > 0 )
        {
            recvBuffer[ bytesReceived ] = '\0';

            if( mesajBufferUzunluk + bytesReceived < (int) sizeof( mesajBuffer ) - 1 )
            {
                memcpy( mesajBuffer + mesajBufferUzunluk, recvBuffer, bytesReceived );
                mesajBufferUzunluk += bytesReceived;
                mesajBuffer[ mesajBufferUzunluk ] = '\0';
            }

            char *newlinePos;
            while( ( newlinePos = strchr( mesajBuffer, '\n' ) ) != NULL )
            {
                *newlinePos = '\0';
                int mesajUzunlugu = (int) strlen( mesajBuffer );

                if( !bIsAuthenticated )
                {
                    /* --- ILK SATIR: AUTHENTICATION MESAJI ---
                     * ARTIK ayri bir recv() DEGIL, mesajBuffer/framing
                     * mekanizmasinin BIR PARCASI. Boylece client, AUTH
                     * mesajindan hemen sonra baska mesajlar gonderse
                     * bile (ayni TCP paketinde birlesmis olsalar
                     * bile), her biri DOGRU sekilde, sirayla
                     * ayristiriliyor. */
                    char bufferKopyasi[ 256 ];
                    strncpy( bufferKopyasi, mesajBuffer, sizeof( bufferKopyasi ) - 1 );
                    bufferKopyasi[ sizeof( bufferKopyasi ) - 1 ] = '\0';

                    char *authKismi = strtok( bufferKopyasi, "|" );
                    char *roleKismi = strtok( NULL, "|" );

                    bool bAuthBasarili = false;

                    if( authKismi != NULL && strncmp( authKismi, "AUTH:", 5 ) == 0 )
                    {
                        const char *gelenToken = authKismi + 5;
                        if( strcmp( gelenToken, SHARED_AUTH_TOKEN ) == 0 )
                        {
                            bAuthBasarili = true;
                        }
                    }

                    if( !bAuthBasarili )
                    {
                        printf( "[ClientHandler] YETKISIZ BAGLANTI! Token dogrulanamadi, baglanti reddediliyor.\n" );
                        Net_Kapat( clientSocket );
                        vTaskDelete( NULL );
                    }

                    printf( "[ClientHandler] Authentication basarili.\n" );
                    bIsAuthenticated = true;

                    if( roleKismi != NULL && strcmp( roleKismi, "ROLE:SUBSCRIBER" ) == 0 )
                    {
                        bIsSubscriber = true;
                        printf( "[ClientHandler] Bu client bir SUBSCRIBER.\n" );

                        xSemaphoreTake( xSubscriberListMutex, portMAX_DELAY );
                        if( xSubscriberCount < MAX_CLIENTS )
                        {
                            xSubscriberSockets[ xSubscriberCount ] = clientSocket;
                            xSubscriberCount++;
                        }
                        xSemaphoreGive( xSubscriberListMutex );
                    }
                    else if( roleKismi != NULL && strcmp( roleKismi, "ROLE:PUBLISHER" ) == 0 )
                    {
                        printf( "[ClientHandler] Bu client bir PUBLISHER.\n" );
                    }
                    else
                    {
                        printf( "[ClientHandler] UYARI: Bilinmeyen rol, baglanti kapatiliyor.\n" );
                        Net_Kapat( clientSocket );
                        vTaskDelete( NULL );
                    }
                }
                else if( mesajUzunlugu == 0 )
                {
                    printf( "[ClientHandler] UYARI: Bos mesaj alindi, yok sayiliyor.\n" );
                }
                else if( mesajUzunlugu > 200 )
                {
                    printf( "[ClientHandler] UYARI: Anormal uzunlukta mesaj (%d byte), "
                            "reddediliyor.\n", mesajUzunlugu );
                }
                else
                {
                    cJSON *parsedJson = cJSON_Parse( mesajBuffer );

                    if( parsedJson == NULL )
                    {
                        printf( "[ClientHandler] UYARI: Gecersiz JSON alindi, reddediliyor. "
                                "Gelen: %s\n", mesajBuffer );
                    }
                    else
                    {
                        cJSON *topicItem   = cJSON_GetObjectItem( parsedJson, "topic" );
                        cJSON *payloadItem = cJSON_GetObjectItem( parsedJson, "payload" );

                        bool bGecerliMesaj = true;

                        if( topicItem == NULL || !cJSON_IsString( topicItem ) )
                        {
                            printf( "[ClientHandler] UYARI: 'topic' alani eksik veya hatali tipte.\n" );
                            bGecerliMesaj = false;
                        }

                        if( payloadItem == NULL || !cJSON_IsString( payloadItem ) )
                        {
                            printf( "[ClientHandler] UYARI: 'payload' alani eksik veya hatali tipte.\n" );
                            bGecerliMesaj = false;
                        }

                        if( bIsSubscriber && topicItem != NULL && cJSON_IsString( topicItem ) &&
                            strcmp( topicItem->valuestring, "cmd/sehir_sorgu" ) == 0 &&
                            payloadItem != NULL && cJSON_IsString( payloadItem ) )
                        {
                            /* OZEL ISTISNA: subscriber'dan gelen bir "sehir sorgu" KOMUTU -
                            * normal veri yayinlama yasaginin istisnasi. Bu sayede
                            * subscriber'lar runtime'da sehir talep edebiliyor. */
                            printf( "[ClientHandler] Sehir sorgu komutu alindi: %s\n",
                                    payloadItem->valuestring );

                            if( xSemaphoreTake( xSehirMutex, pdMS_TO_TICKS( 100 ) ) == pdTRUE )
                            {
                                strncpy( cSuankiSehir, payloadItem->valuestring, sizeof( cSuankiSehir ) - 1 );
                                cSuankiSehir[ sizeof( cSuankiSehir ) - 1 ] = '\0';
                                xSemaphoreGive( xSehirMutex );
                            }

                            /* Bu bloklayici bir HTTPS cagrisi - ama SADECE bu client'in kendi
                            * task'ini bloklar, diger client'lari ETKILEMEZ (her client kendi
                            * ClientHandlerTask'inda calisiyor). */
                            AnlikHavaSonucu_t sonuc = prvSehirAnlikSicaklikGetir( cSuankiSehir );

                            if( sonuc.basarili )
                            {
                                cJSON *havaRoot = cJSON_CreateObject();
                                cJSON_AddStringToObject( havaRoot, "topic", "sensor/anlik_sicaklik" );

                                char sicaklikStr[ 16 ];
                                snprintf( sicaklikStr, sizeof( sicaklikStr ), "%.1f", sonuc.sicaklik );
                                cJSON_AddStringToObject( havaRoot, "payload", sicaklikStr );
                                cJSON_AddStringToObject( havaRoot, "sehir", cSuankiSehir );
                                cJSON_AddNumberToObject( havaRoot, "zaman", (double) prvSuankiUnixZaman() );

                                char *havaJsonStr = cJSON_PrintUnformatted( havaRoot );
                                char gonderilecekHava[ 300 ];
                                snprintf( gonderilecekHava, sizeof( gonderilecekHava ), "%s\n", havaJsonStr );

                                xSemaphoreTake( xSubscriberListMutex, portMAX_DELAY );
                                for( int i = 0; i < xSubscriberCount; i++ )
                                {
                                    Net_Gonder( xSubscriberSockets[ i ], gonderilecekHava, (int) strlen( gonderilecekHava ));
                                }
                                xSemaphoreGive( xSubscriberListMutex );

                                cJSON_free( havaJsonStr );
                                cJSON_Delete( havaRoot );

                                printf( "[ClientHandler] Anlik hava yayinlandi: %s = %.1f C\n",
                                        cSuankiSehir, sonuc.sicaklik );
                                prvAnlikHavaKaydet( cSuankiSehir, &sonuc, prvSuankiUnixZaman() );
                            }
                            else
                            {
                                printf( "[ClientHandler] HATA: '%s' icin anlik hava alinamadi.\n", cSuankiSehir );
                            }
                        }
                        else if( bIsSubscriber )
                        {
                            printf( "[ClientHandler] YETKI IHLALI: Subscriber veri gondermeye "
                                    "calisti, veri reddediliyor. Gelen: %s\n", mesajBuffer );
                        }
                        else if( bGecerliMesaj )
                        {
                            printf( "[ClientHandler] Gecerli JSON alindi -> topic: %s, payload: %s\n",
                                    topicItem->valuestring, payloadItem->valuestring );

                            xSemaphoreTake( xSubscriberListMutex, portMAX_DELAY );
                            for( int i = 0; i < xSubscriberCount; i++ )
                            {
                                char gonderilecekMesaj[ 1025 ];
                                snprintf( gonderilecekMesaj, sizeof( gonderilecekMesaj ), "%s\n", mesajBuffer );
                                Net_Gonder( xSubscriberSockets[ i ], gonderilecekMesaj, (int) strlen( gonderilecekMesaj ));
                            }
                            xSemaphoreGive( xSubscriberListMutex );

                            printf( "[ClientHandler] Veri %d subscriber'a iletildi.\n", xSubscriberCount );
                        }
                        else
                        {
                            printf( "[ClientHandler] Mesaj eksik/hatali alanlar icerdigi icin "
                                    "subscriber'lara iletilmedi.\n" );
                        }

                        cJSON_Delete( parsedJson );
                    }
                }

                int islenenUzunluk = (int)( newlinePos - mesajBuffer ) + 1;
                int kalanUzunluk = mesajBufferUzunluk - islenenUzunluk;
                memmove( mesajBuffer, newlinePos + 1, kalanUzunluk );
                mesajBufferUzunluk = kalanUzunluk;
                mesajBuffer[ mesajBufferUzunluk ] = '\0';
            }
        }
        else if( bytesReceived == NET_SONUC_BAGLANTI_KAPANDI )
        {
            printf( "[ClientHandler] Client baglantiyi kapatti.\n" );
            break;
        }
        else if( bytesReceived == NET_SONUC_HATA )
        {
            printf( "[ClientHandler] HATA: veri alinamadi - baglanti sonlandiriliyor.\n" );
            break;
        }
        /* NET_SONUC_VERI_YOK durumunda hicbir sey yapmiyoruz - normal, devam. */

        if( !bIsAuthenticated &&
            ( xTaskGetTickCount() - xBaglantiBaslangic ) > pdMS_TO_TICKS( 5000 ) )
        {
            printf( "[ClientHandler] UYARI: 5 saniye icinde authentication gelmedi, baglanti kapatiliyor.\n" );
            break;
        }

        vTaskDelay( pdMS_TO_TICKS( 100 ) );
    }
    xAktifClientSayisi--;
    if( xAktifClientSayisi == 0 )
    {
        /* Son client de ayrildi - idle sayaci SIMDI, bu andan itibaren
        * baslasin. */
        xSonBaglantiZamani = xTaskGetTickCount();
    }
    /* YENI: Eger bu bir subscriber idiyse, subscriber listesinden de
    * CIKAR - aksi halde liste sadece buyur, hicbir zaman kucalmaz, bu da
    * MAX_CLIENTS sinirina cabuk ulasilmasina yol acar (ozellikle
    * test_komut.py gibi kisa omurlu subscriber baglantilari icin). */
    if( bIsSubscriber )
    {
        xSemaphoreTake( xSubscriberListMutex, portMAX_DELAY );
        for( int i = 0; i < xSubscriberCount; i++ )
        {
            if( xSubscriberSockets[ i ] == clientSocket )
            {
                /* Bulunan elemani, listenin SONUNDAKI elemanla degistir -
                * boylece array'de bosluk kalmiyor, sadece toplam sayi
                * bir azaliyor. Sira onemli degil, sadece "kimin gecerli
                * oldugu" onemli. */
                xSubscriberSockets[ i ] = xSubscriberSockets[ xSubscriberCount - 1 ];
                xSubscriberCount--;
                printf( "[ClientHandler] Subscriber listeden cikarildi "
                        "(kalan subscriber sayisi: %d).\n", xSubscriberCount );
                break;
            }
        }
        xSemaphoreGive( xSubscriberListMutex );
    }



    Net_Kapat( clientSocket );
    printf( "[ClientHandler] Task sonlandiriliyor.\n" );
    vTaskDelete( NULL );
}


/* =======================================================================
 * vUdpCommandTask()
 *
 * MQTT/TCP/JSON altyapisindan TAMAMEN BAGIMSIZ, hafif bir komut kanali.
 * UDP kullanir (baglanti kurmaya gerek yok), duz metin komutlar kabul
 * eder (JSON yok, framing yok, authentication yok, Queue yok). Her
 * rolde (broker/publisher/subscriber) calisir, dogrudan FreeRTOS
 * API'lerini cagirip anlik cevap verir.
 *
 * Ana TCP portunun +1000'i uzerinde dinler (orn. broker 8080 ise,
 * UDP komut kanali 9080'de).
 * ===================================================================== */
static void vUdpCommandTask( void *pvParameters )
{
    ( void ) pvParameters;

    /* Her ROL, kendi BENZERSIZ UDP portunu alsin - boylece ayni makinede
     * broker/publisher/subscriber ayni anda calisirken portlari
     * CAKISMASIN. Rol numarasina gore ek bir ofset ekliyoruz. */
    int xUdpPort = xPortNumarasi + 1000 + ( (int) xMyRole * 10 );

    /* Soket olusturma, bind ve non-blocking ayari - TUMU
     * Net_UdpDinlemeBaslat() icinde, platformdan bagimsiz sekilde
     * yapiliyor. */
    NetSocket_t udpSocket = Net_UdpDinlemeBaslat( xUdpPort );

    if( udpSocket == NET_INVALID_SOCKET )
    {
        printf( "[UdpCmd] HATA: UDP dinleme baslatilamadi.\n" );
        vTaskDelete( NULL );
    }

    printf( "[UdpCmd] MQTT'den BAGIMSIZ komut kanali - UDP port %d'de dinlemede...\n",
            xUdpPort );

    char recvBuf[ 64 ];

    for( ;; )
    {
        NetAdres_t xGonderenAdres;

        int alinanBayt = Net_UdpAl( udpSocket, recvBuf, sizeof( recvBuf ) - 1, &xGonderenAdres );

        if( alinanBayt > 0 )
        {
            recvBuf[ alinanBayt ] = '\0';

            /* Basit metin komutlari - JSON YOK, framing YOK, Queue YOK -
             * MQTT altyapisindan tamamen bagimsiz, dogrudan cevap. */
            char cevap[ 128 ];

            if( strncmp( recvBuf, "PING", 4 ) == 0 )
            {
                snprintf( cevap, sizeof( cevap ), "PONG" );
            }
            else if( strncmp( recvBuf, "HEAP", 4 ) == 0 )
            {
                snprintf( cevap, sizeof( cevap ), "HEAP:%u",
                          (unsigned int) xPortGetFreeHeapSize() );
            }
            else if( strncmp( recvBuf, "STATUS", 6 ) == 0 )
            {
                snprintf( cevap, sizeof( cevap ), "ROLE:%d,TASKS:%u",
                          (int) xMyRole, (unsigned int) uxTaskGetNumberOfTasks() );
            }
            else
            {
                snprintf( cevap, sizeof( cevap ), "BILINMEYEN_KOMUT" );
            }

            Net_UdpGonder( udpSocket, cevap, (int) strlen( cevap ), &xGonderenAdres );

            printf( "[UdpCmd] Komut alindi: '%s' -> Cevap: '%s'\n", recvBuf, cevap );
        }

        vTaskDelay( pdMS_TO_TICKS( 100 ) );
    }
}



/* =======================================================================
 * vStatusBroadcastCallback()
 *
 * Bu fonksiyon AYRI BIR TASK DEGIL - Timer Service Task tarafindan,
 * belirlenen periyotta (2 saniyede bir) otomatik cagrilir. Health Task'in
 * kendi stack'ini kullanmiyor, ayri bir stack tahsisi de gerekmiyor.
 * ===================================================================== */
static void vStatusBroadcastCallback( TimerHandle_t xTimer )
{
    ( void ) xTimer;

    if( xSubscriberListMutex == NULL )
    {
        return;
    }

    /* ONEMLI: Timer callback'leri ASLA uzun sure ya da sinirsiz
     * (portMAX_DELAY ile) bloklanmamali - cunku TUM timer'lar TEK BIR
     * Timer Service Task uzerinde calisir. Bu callback bloklanirsa,
     * sistemdeki DIGER TUM timer'lar da gecikir. Bu yuzden mutex'i
     * SINIRLI bir sure (100ms) bekliyoruz, alamazsak bu turu atliyoruz. */
    if( xSemaphoreTake( xSubscriberListMutex, pdMS_TO_TICKS( 100 ) ) != pdTRUE )
    {
        printf( "[StatusTimer] UYARI: Mutex alinamadi, bu tur atlaniyor.\n" );
        return;
    }

    cJSON *statusRoot = cJSON_CreateObject();
    cJSON_AddStringToObject( statusRoot, "topic", "system/status" );

    char sayiStr[ 16 ];
    snprintf( sayiStr, sizeof( sayiStr ), "%d", xSubscriberCount );
    cJSON_AddStringToObject( statusRoot, "payload", sayiStr );

    /* YENI: NTP ile senkronize edilmis gercek zaman - tutarlilik icin
    * diger tum mesaj tiplerinde (sensor/sicaklik, system/health) oldugu
    * gibi burada da ekleniyor. */
    cJSON_AddNumberToObject( statusRoot, "zaman", (double) prvSuankiUnixZaman() );

    char *statusJson = cJSON_PrintUnformatted( statusRoot );
    char gonderilecek[ 300 ];
    snprintf( gonderilecek, sizeof( gonderilecek ), "%s\n", statusJson );

    for( int i = 0; i < xSubscriberCount; i++ )
    {
        Net_Gonder( xSubscriberSockets[ i ], gonderilecek, (int) strlen( gonderilecek ));
    }

    cJSON_free( statusJson );
    cJSON_Delete( statusRoot );

    xSemaphoreGive( xSubscriberListMutex );

    printf( "[StatusTimer] Durum yayinlandi (subscriber sayisi: %d)\n", xSubscriberCount );
}

/* =======================================================================
 * vIdleTimeoutCallback()
 *
 * Periyodik olarak (5 saniyede bir) kontrol eder: en son bir client
 * baglandigindan bu yana ne kadar sure gecti? Belirlenen esigi
 * (IDLE_TIMEOUT_MS) asarsa, broker'i DUZGUN sekilde kapatir.
 * ===================================================================== */


static void vMqttPublisherTask(void *pvParameters)
{
    (void)pvParameters;
    int mesajSayaci = 0;
    for (;;) {
        PublishData_t veri = {0};
        snprintf(veri.topic, sizeof(veri.topic), "sensor/sicaklik");
        snprintf(veri.payload, sizeof(veri.payload), "%.1f", 20.0f + (esp_random() % 100) / 10.0f);
        snprintf(veri.sehir, sizeof(veri.sehir), "Ankara");
        snprintf(veri.tarih, sizeof(veri.tarih), "simulasyon");
        snprintf(veri.durum, sizeof(veri.durum), "rastgele_veri");
        veri.mesaj_no = mesajSayaci++;
        xQueueSend(xPublishQueue, &veri, portMAX_DELAY);
        vTaskDelay(pdMS_TO_TICKS(3000));
    }
}

static void vMqttSubscriberTask( void *pvParameters )
{
    ( void ) pvParameters;

    for( ;; )
    {
        printf( "[MqttSub] Gelen veri bekleniyor...\n" );
        vTaskDelay( pdMS_TO_TICKS( 2000 ) );
    }
    
}



static void vEspPublisherNetwork(void)
{
    for (;;) {
        NetSocket_t sock = Net_Baglan(cBrokerIP, xPortNumarasi);
        if (sock == NET_INVALID_SOCKET) {
            printf("[Network] Broker %s:%d baglantisi yok; 2 saniye sonra tekrar.\n", cBrokerIP, xPortNumarasi);
            vTaskDelay(pdMS_TO_TICKS(2000));
            continue;
        }
        char auth[128];
        int len = snprintf(auth, sizeof(auth), "AUTH:%s|ROLE:PUBLISHER\n", SHARED_AUTH_TOKEN);
        bool connected = len > 0 && len < sizeof(auth) && Net_Gonder(sock, auth, len) == len;
        if (connected) printf("[Network] Broker'a baglanildi; PUBLISHER kimligi gonderildi.\n");
        while (connected) {
            PublishData_t data;
            if (xQueueReceive(xPublishQueue, &data, pdMS_TO_TICKS(1000)) != pdPASS) continue;
            cJSON *obj = cJSON_CreateObject();
            if (!obj) continue;
            bool ok = cJSON_AddStringToObject(obj, "topic", data.topic) &&
                      cJSON_AddStringToObject(obj, "payload", data.payload) &&
                      cJSON_AddNumberToObject(obj, "mesaj_no", data.mesaj_no) &&
                      cJSON_AddStringToObject(obj, "sehir", data.sehir) &&
                      cJSON_AddStringToObject(obj, "tarih", data.tarih) &&
                      cJSON_AddStringToObject(obj, "durum", data.durum) &&
                      cJSON_AddNumberToObject(obj, "zaman", (double)prvSuankiUnixZaman());
            char *json = ok ? cJSON_PrintUnformatted(obj) : NULL;
            if (json) {
                char line[256];
                len = snprintf(line, sizeof(line), "%s\n", json);
                if (len > 0 && len < sizeof(line)) {
                    connected = Net_Gonder(sock, line, len) == len;
                    if (connected) printf("[Network] JSON gonderildi: %s\n", json);
                }
                cJSON_free(json);
            }
            cJSON_Delete(obj);
        }
        Net_Kapat(sock);
        printf("[Network] Gonderim basarisiz; yeniden baglaniliyor.\n");
        vTaskDelay(pdMS_TO_TICKS(2000));
    }
}
