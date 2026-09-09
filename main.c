#include "cJSON.h"
#include <winsock2.h>
#include <ws2tcpip.h>  /* getaddrinfo() icin */
#include <winhttp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "FreeRTOS.h"
#include "task.h"
#include "queue.h"
#include "semphr.h"
#include <stdbool.h>
#include "timers.h"


#include <time.h>        /* time_t icin */


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

static SicaklikKaydi_t xSicaklikVerileri[ MAX_SICAKLIK_KAYIT ];
static int xSicaklikKayitSayisi = 0;


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

#define STACK_SIZE_HEALTH          ( configMINIMAL_STACK_SIZE * 4 )
#define STACK_SIZE_INTERNAL_COMM   ( configMINIMAL_STACK_SIZE * 4 )
#define STACK_SIZE_NETWORK         ( configMINIMAL_STACK_SIZE * 6 )
#define STACK_SIZE_MQTT            ( configMINIMAL_STACK_SIZE * 4 )
#define STACK_SIZE_CLIENT_HANDLER  ( configMINIMAL_STACK_SIZE * 8 )   /* HTTPS istegi + 4KB yerel buffer icin buyutuldu */
#define MAX_CLIENTS                 5
#define SHARED_AUTH_TOKEN           "gizli_sifre123"
#define DEFAULT_PORT         8080
#define DEFAULT_BROKER_IP    "127.0.0.1"
#define STACK_SIZE_UDP_COMMAND     ( configMINIMAL_STACK_SIZE * 4 )
#define IDLE_TIMEOUT_MS   30000   /* 30 saniye hic baglanti gelmezse kapan */

#define NTP_SERVER              "pool.ntp.org"
#define NTP_PORT                123
#define NTP_SYNC_INTERVAL_MS    ( 5 * 60 * 1000 )   /* 5 dakikada bir yeniden senkronize et */
#define NTP_UNIX_EPOCH_FARKI    2208988800UL         /* 1900-1970 arasi saniye farki */

#define STACK_SIZE_ANLIK_HAVA      ( configMINIMAL_STACK_SIZE * 8 )   /* WinHTTP icin biraz daha fazla stack */
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
static SOCKET xSubscriberSockets[ MAX_CLIENTS ];
static int    xSubscriberCount = 0;
static SemaphoreHandle_t xSubscriberListMutex = NULL;


static TaskHandle_t xHealthTaskHandle          = NULL;
static TaskHandle_t xInternalCommTaskHandle    = NULL;
static TaskHandle_t xNetworkTaskHandle         = NULL;
static TaskHandle_t xMqttPublisherTaskHandle   = NULL;
static TaskHandle_t xMqttSubscriberTaskHandle  = NULL;
/* Broker durum yayini icin software timer. */
static TimerHandle_t xStatusTimer = NULL;
static TimerHandle_t xIdleTimeoutTimer = NULL;
static TickType_t xSonBaglantiZamani = 0;
static volatile int xAktifClientSayisi = 0;
static bool bBilerekKapatiliyor = false;
/* Komut satirindan override edilebilen ag ayarlari. Varsayilan
 * degerlerle baslar, prvParseNetworkArgsFromArgs() cagrildiginda
 * kullanici argüman verdiyse guncellenir. */
static int  xPortNumarasi = DEFAULT_PORT;
static char cBrokerIP[ 64 ] = DEFAULT_BROKER_IP;

/* NTP'den alinan zaman ile yerel tick sayaci arasindaki fark (saniye).
 * Bu ofset, periyodik olarak NTP ile yeniden senkronize edilir; aradaki
 * surede ise projenin kendi "real-time clock"u gibi calisir - her an
 * icin agdan tekrar sormaya gerek kalmadan hesaplanabilir. */
static volatile time_t xUnixZamanOfseti = 0;
static volatile bool bNtpSenkronize = false;
/* Su anki secili sehir - hem config dosyasindan hem runtime komuttan
 * (subscriber'dan gelen cmd/sehir_sorgu ile) degistirilebiliyor. Iki
 * farkli task/context'ten erisildigi icin mutex ile koruyoruz. */
static char cSuankiSehir[ 64 ] = "Ankara";
static SemaphoreHandle_t xSehirMutex = NULL;


static void vHealthTask( void *pvParameters );
static void vInternalCommTask( void *pvParameters );
static void vNetworkTask( void *pvParameters );
static void vMqttPublisherTask( void *pvParameters );
static void vMqttSubscriberTask( void *pvParameters );
static void vClientHandlerTask( void *pvParameters );

static SystemRole_t prvParseRoleFromArgs( int argc, char *argv[] );
static void prvParseNetworkArgsFromArgs( int argc, char *argv[] );  
static void prvPrintUsage( const char *pcProgramName );  
static void prvCreateTasksForRole( SystemRole_t xRole );
static void vStatusBroadcastCallback( TimerHandle_t xTimer );
static void prvSicaklikVerisiYukle( const char *pcDosyaYolu );
static void vUdpCommandTask( void *pvParameters );
static void vIdleTimeoutCallback( TimerHandle_t xTimer );

static bool prvNtpSorgula( time_t *pxSonucUnixZaman );
static void vNtpSyncTask( void *pvParameters );
static time_t prvSuankiUnixZaman( void );
static bool prvHttpsGet( const wchar_t *pcHost, const wchar_t *pcYol,
                          char *pcCevapBuffer, size_t xBufferBoyutu );
static void prvAsciiToWide( const char *pcKaynak, wchar_t *pcHedef, size_t xHedefBoyutu );
static void prvSehirConfigYukle( const char *pcDosyaYolu );
static void vAnlikHavaTask( void *pvParameters );
static void prvAnlikHavaKaydet( const char *pcSehirAdi, const AnlikHavaSonucu_t *pxSonuc, time_t zaman );


int main( int argc, char *argv[] )
{

      /* stdout'u UNBUFFERED (tamponsuz) moda al - boylece her printf()
     * cagrisi HEMEN gonderilir, buffer dolmasini beklemez. Bu ozellikle
     * cikti bir PIPE'a yonlendirildiginde (ornegin Python'un
     * subprocess.Popen ile bu programi baslatip ciktisini okumasi gibi)
     * kritik - cunku pipe'a yazarken C runtime varsayilan olarak
     * TAM TAMPONLAMA kullanir, bu da ciktinin "gec, toplu halde"
     * gelmesine sebep olur. */
    setvbuf( stdout, NULL, _IONBF, 0 );
    
     /* 0) ADIM: Winsock kutuphanesini baslat.
     * Bu, Windows'a ozel bir zorunluluk - Linux/STM32'de bu adim
     * gerekmeyecek (o yuzden ileride bu kodu soyutlama katmanina
     * tasiyacagiz). WSAStartup basarisiz olursa, hicbir soket
     * fonksiyonu calismaz. */
    WSADATA wsaData;
    int wsaResult = WSAStartup( MAKEWORD( 2, 2 ), &wsaData );
    if( wsaResult != 0 )
    {
        printf( "HATA: WSAStartup basarisiz oldu, kod: %d\n", wsaResult );
        return EXIT_FAILURE;
    }
    printf( "[main] Winsock baslatildi (versiyon: %d.%d)\n",
            LOBYTE( wsaData.wVersion ), HIBYTE( wsaData.wVersion ) );

    
    
    /* 1) ADIM: Rolu belirle - HENUZ FreeRTOS scheduler baslamadi,
     *    normal C kodu olarak calisiyoruz. */
    xMyRole = prvParseRoleFromArgs( argc, argv );
    prvParseNetworkArgsFromArgs( argc, argv );   /* <-- YENİ SATIR */
    prvSicaklikVerisiYukle( "ankara_sicaklik_verileri.csv" );
    prvSehirConfigYukle( "sehir_config.json" );

    if( xMyRole == ROLE_UNDEFINED )
    {
        prvPrintUsage( argv[ 0 ] );
        return EXIT_FAILURE;
    }

    printf( "=================================================\n" );
    printf( " Network Simulation baslatiliyor\n" );
    printf( " Rol: %s\n",
            ( xMyRole == ROLE_BROKER )     ? "BROKER"     :
            ( xMyRole == ROLE_PUBLISHER )  ? "PUBLISHER"  :
                                              "SUBSCRIBER" );
    printf( "=================================================\n\n" );
    

    /* Subscriber listesini koruyacak mutex'i olustur - scheduler
    * baslamadan once, herkesten once hazir olmali. */
    xSubscriberListMutex = xSemaphoreCreateMutex();
    if( xSubscriberListMutex == NULL )
    {
        printf( "HATA: Subscriber mutex'i olusturulamadi!\n" );
        return EXIT_FAILURE;
    }
    /* Suanki secili sehri koruyacak mutex - hem periyodik yayin task'i
    * hem runtime komut (subscriber'dan gelen) hem de config yukleme
    * ayni degiskene erisebiliyor. */
    xSehirMutex = xSemaphoreCreateMutex();
    if( xSehirMutex == NULL )
    {
        printf( "HATA: Sehir mutex'i olusturulamadi!\n" );
        return EXIT_FAILURE;
    }



    /* Network -> Internal Comm arasi veri tasimak icin queue.
    * 10 eleman kapasiteli - ayni anda en fazla 10 mesaj biriktirebilir. */
    xInternalCommQueue = xQueueCreate( 10, sizeof( SensorData_t ) );
    if( xInternalCommQueue == NULL )
    {
        printf( "HATA: Internal Comm queue'su olusturulamadi!\n" );
        return EXIT_FAILURE;
    }
    /* MQTT Publisher -> Network Task arasi veri tasimak icin. */
    xPublishQueue = xQueueCreate( 10, sizeof( PublishData_t ) );
    if( xPublishQueue == NULL )
    {
        printf( "HATA: Publish queue'su olusturulamadi!\n" );
        return EXIT_FAILURE;
    }
    /* Sadece BROKER rolunde durum yayini timer'ini olustur - digger
    * rollerin buna ihtiyaci yok. */
    if( xMyRole == ROLE_BROKER )
    {
    xStatusTimer = xTimerCreate(
        "StatusTimer",              /* timer ismi (debug icin) */
        pdMS_TO_TICKS( 2000 ),      /* periyot: 2 saniyede bir */
        pdTRUE,                      /* pdTRUE = otomatik tekrar (periyodik) */
        NULL,                         /* timer ID - kullanmiyoruz */
        vStatusBroadcastCallback      /* callback fonksiyonu */
    );

    if( xStatusTimer == NULL )
    {
        printf( "HATA: Status timer olusturulamadi!\n" );
        return EXIT_FAILURE;
    }

    if( xTimerStart( xStatusTimer, 0 ) != pdPASS )
    {
        printf( "HATA: Status timer baslatilamadi!\n" );
        return EXIT_FAILURE;
    }
     /* --- YENI: Idle-timeout timer'i --- */
    xIdleTimeoutTimer = xTimerCreate(
        "IdleTimeout",
        pdMS_TO_TICKS( 5000 ),      /* her 5 saniyede bir kontrol et */
        pdTRUE,
        NULL,
        vIdleTimeoutCallback
    );

    if( xIdleTimeoutTimer == NULL )
    {
        printf( "HATA: Idle timeout timer olusturulamadi!\n" );
        return EXIT_FAILURE;
    }

    if( xTimerStart( xIdleTimeoutTimer, 0 ) != pdPASS )
    {
        printf( "HATA: Idle timeout timer baslatilamadi!\n" );
        return EXIT_FAILURE;
    }
}

    /* 2) ADIM: Role uygun task'lari olustur. */
    prvCreateTasksForRole( xMyRole );

    /* Scheduler baslamadan ONCE, bir kez BLOKLAYICI NTP sorgusu yaparak
    * ilk senkronizasyonu garanti altina aliyoruz. Boylece hicbir task
    * (ozellikle publisher) calismaya baslamadan once, xUnixZamanOfseti
    * zaten dogru deger ile dolu oluyor - "zaman: 0" gibi anlamsiz ilk
    * mesajlarin onune geciliyor. */
    {
        time_t ilkSenkronZamani;

        printf( "[NTP] Baslangic senkronizasyonu yapiliyor...\n" );

        if( prvNtpSorgula( &ilkSenkronZamani ) )
        {
            xUnixZamanOfseti = ilkSenkronZamani;   /* tick henuz 0'a yakin, offset ~= sunucu zamani */
            bNtpSenkronize = true;
            printf( "[NTP] Baslangic senkronizasyonu basarili.\n" );
        }
        else
        {
            printf( "[NTP] UYARI: Baslangic senkronizasyonu basarisiz - "
                    "vNtpSyncTask periyodik olarak tekrar deneyecek.\n" );
        }
    }

    /* 3) ADIM: Scheduler'i baslat - bu satirdan sonra kontrol
     *    bir daha asla buraya donmez. */
    vTaskStartScheduler();
    

   /* Buraya iki sebepten ulasilabilir:
    * 1) Gercek bir hata - scheduler hic baslayamadi (yetersiz heap)
    * 2) BILEREK - vIdleTimeoutCallback icinde vTaskEndScheduler()
    *    cagirdik, bu PLANLI bir kapanis. */
    if( bBilerekKapatiliyor )
    {
        printf( "\n[main] Broker, idle-timeout nedeniyle kendini duzgun "
                "sekilde kapatti.\n" );
        WSACleanup();
        return EXIT_SUCCESS;
    }
else
{
    printf( "HATA: Scheduler baslatilamadi (yetersiz heap olabilir)\n" );
    WSACleanup();
    return EXIT_FAILURE;
}
}

/* Basit ASCII -> wide-char (UTF-16) donusumu. WinHTTP fonksiyonlari
 * wide-char string bekliyor; bizim host/yol string'lerimiz her zaman
 * saf ASCII oldugu icin (Turkce karakter icermiyor), tek tek
 * genisletmek yeterli - tam bir Unicode donusum kutuphanesine
 * (MultiByteToWideChar) ihtiyacimiz yok. */
static void prvAsciiToWide( const char *pcKaynak, wchar_t *pcHedef, size_t xHedefBoyutu )
{
    size_t i = 0;
    for( ; i < xHedefBoyutu - 1 && pcKaynak[ i ] != '\0'; i++ )
    {
        pcHedef[ i ] = (wchar_t) pcKaynak[ i ];
    }
    pcHedef[ i ] = L'\0';
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

    char cevapBuffer[ 4096 ];
    char yolBuffer[ 256 ];
    wchar_t yolWide[ 256 ];

    /* --- ASAMA 1: GEOCODING (sehir adi -> koordinat) --- */
    snprintf( yolBuffer, sizeof( yolBuffer ),
              "/v1/search?name=%s&count=1&language=tr&format=json",
              pcSehirAdi );
    prvAsciiToWide( yolBuffer, yolWide, sizeof( yolWide ) / sizeof( wchar_t ) );

    if( !prvHttpsGet( L"geocoding-api.open-meteo.com", yolWide,
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
    prvAsciiToWide( yolBuffer, yolWide, sizeof( yolWide ) / sizeof( wchar_t ) );

    if( !prvHttpsGet( L"api.open-meteo.com", yolWide,
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
static void prvSehirConfigYukle( const char *pcDosyaYolu )
{
    FILE *fp = fopen( pcDosyaYolu, "r" );

    if( fp == NULL )
    {
        printf( "[main] UYARI: '%s' bulunamadi, varsayilan sehir "
                "('%s') kullanilacak.\n", pcDosyaYolu, cSuankiSehir );
        return;
    }

    char icerik[ 512 ];
    size_t okunanBayt = fread( icerik, 1, sizeof( icerik ) - 1, fp );
    icerik[ okunanBayt ] = '\0';
    fclose( fp );

    cJSON *configJson = cJSON_Parse( icerik );

    if( configJson == NULL )
    {
        printf( "[main] UYARI: '%s' gecersiz JSON, varsayilan sehir "
                "kullanilacak.\n", pcDosyaYolu );
        return;
    }

    cJSON *sehirItem = cJSON_GetObjectItem( configJson, "sehir" );

    if( sehirItem != NULL && cJSON_IsString( sehirItem ) )
    {
        strncpy( cSuankiSehir, sehirItem->valuestring, sizeof( cSuankiSehir ) - 1 );
        cSuankiSehir[ sizeof( cSuankiSehir ) - 1 ] = '\0';
        printf( "[main] Config'den sehir yuklendi: %s\n", cSuankiSehir );
    }
    else
    {
        printf( "[main] UYARI: config'de 'sehir' alani bulunamadi/gecersiz.\n" );
    }

    cJSON_Delete( configJson );
}
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
                send( xSubscriberSockets[ i ], gonderilecekHava, (int) strlen( gonderilecekHava ), 0 );
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
static void prvAnlikHavaKaydet( const char *pcSehirAdi, const AnlikHavaSonucu_t *pxSonuc, time_t zaman )
{
    /* Dosya daha once var miydi kontrol et - yoksa basligi (header) yaz. */
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


/* =======================================================================
 * prvHttpsGet()
 *
 * WinHTTP kullanarak bir HTTPS GET istegi atar, cevabin BODY kismini
 * pcCevapBuffer'a yazar. TLS/sertifika dogrulama gibi tum sifreleme
 * islerini WinHTTP bizim yerimize hallediyor - biz sadece "bu host'a,
 * bu yola git, cevabi getir" diyoruz.
 * ===================================================================== */
static bool prvHttpsGet( const wchar_t *pcHost, const wchar_t *pcYol,
                          char *pcCevapBuffer, size_t xBufferBoyutu )
{
    bool basarili = false;
    HINTERNET hSession = NULL, hConnect = NULL, hRequest = NULL;

    hSession = WinHttpOpen( L"FreeRTOS-NetworkSim/1.0",
                             WINHTTP_ACCESS_TYPE_DEFAULT_PROXY,
                             WINHTTP_NO_PROXY_NAME,
                             WINHTTP_NO_PROXY_BYPASS, 0 );

    if( hSession == NULL )
    {
        printf( "[HTTP] HATA: WinHttpOpen basarisiz, kod: %lu\n", GetLastError() );
        return false;
    }

    hConnect = WinHttpConnect( hSession, pcHost, INTERNET_DEFAULT_HTTPS_PORT, 0 );

    if( hConnect == NULL )
    {
        printf( "[HTTP] HATA: WinHttpConnect basarisiz, kod: %lu\n", GetLastError() );
        WinHttpCloseHandle( hSession );
        return false;
    }

    hRequest = WinHttpOpenRequest( hConnect, L"GET", pcYol,
                                    NULL, WINHTTP_NO_REFERER,
                                    WINHTTP_DEFAULT_ACCEPT_TYPES,
                                    WINHTTP_FLAG_SECURE );

    if( hRequest == NULL )
    {
        printf( "[HTTP] HATA: WinHttpOpenRequest basarisiz, kod: %lu\n", GetLastError() );
        WinHttpCloseHandle( hConnect );
        WinHttpCloseHandle( hSession );
        return false;
    }

    BOOL gonderildi = WinHttpSendRequest( hRequest,
                                           WINHTTP_NO_ADDITIONAL_HEADERS, 0,
                                           WINHTTP_NO_REQUEST_DATA, 0, 0, 0 );

    if( gonderildi && WinHttpReceiveResponse( hRequest, NULL ) )
    {
        size_t toplamOkunan = 0;
        DWORD mevcutVeri = 0;

        do
        {
            mevcutVeri = 0;

            if( !WinHttpQueryDataAvailable( hRequest, &mevcutVeri ) || mevcutVeri == 0 )
            {
                break;
            }

            if( toplamOkunan + mevcutVeri >= xBufferBoyutu )
            {
                mevcutVeri = (DWORD) ( xBufferBoyutu - toplamOkunan - 1 );
            }

            DWORD okunanBayt = 0;

            if( !WinHttpReadData( hRequest, pcCevapBuffer + toplamOkunan, mevcutVeri, &okunanBayt ) )
            {
                break;
            }

            toplamOkunan += okunanBayt;

        } while( mevcutVeri > 0 && toplamOkunan < xBufferBoyutu - 1 );

        pcCevapBuffer[ toplamOkunan ] = '\0';
        basarili = ( toplamOkunan > 0 );
    }
    else
    {
        printf( "[HTTP] HATA: istek/cevap basarisiz, kod: %lu\n", GetLastError() );
    }

    WinHttpCloseHandle( hRequest );
    WinHttpCloseHandle( hConnect );
    WinHttpCloseHandle( hSession );

    return basarili;
}



/* =======================================================================
 * prvNtpSorgula()
 *
 * TEK BIR NTP sorgusu yapar: DNS ile sunucuyu bulur, UDP ile 48 byte'lik
 * istek paketi gonderir, cevabi okuyup Unix zamanina cevirir.
 * ===================================================================== */
static bool prvNtpSorgula( time_t *pxSonucUnixZaman )
{
    struct addrinfo hints;
    struct addrinfo *sonuc = NULL;

    memset( &hints, 0, sizeof( hints ) );
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    char portStr[ 6 ];
    snprintf( portStr, sizeof( portStr ), "%d", NTP_PORT );

    /* --- DNS COZUMLEME --- */
    int dnsSonuc = getaddrinfo( NTP_SERVER, portStr, &hints, &sonuc );

    if( dnsSonuc != 0 || sonuc == NULL )
    {
        printf( "[NTP] HATA: DNS cozumleme basarisiz (%s), kod: %d\n",
                NTP_SERVER, dnsSonuc );
        return false;
    }

    printf( "[NTP] DNS cozumlendi: %s\n", NTP_SERVER );

    SOCKET ntpSocket = socket( AF_INET, SOCK_DGRAM, IPPROTO_UDP );

    if( ntpSocket == INVALID_SOCKET )
    {
        printf( "[NTP] HATA: soket olusturulamadi.\n" );
        freeaddrinfo( sonuc );
        return false;
    }

    /* recv() sonsuza kadar beklemesin diye 3 saniyelik zaman asimi. */
    DWORD timeout = 3000;
    setsockopt( ntpSocket, SOL_SOCKET, SO_RCVTIMEO, (const char *) &timeout, sizeof( timeout ) );

    NtpPaketi_t paket;
    memset( &paket, 0, sizeof( paket ) );
    paket.li_vn_mode = 0x1B;   /* LI=0, VN=3 (NTPv3), Mode=3 (client istegi) */

    int gonderilen = sendto( ntpSocket, (char *) &paket, sizeof( paket ), 0,
                              sonuc->ai_addr, (int) sonuc->ai_addrlen );
    freeaddrinfo( sonuc );

    if( gonderilen == SOCKET_ERROR )
    {
        printf( "[NTP] HATA: sendto basarisiz, kod: %d\n", WSAGetLastError() );
        closesocket( ntpSocket );
        return false;
    }

    int alinan = recv( ntpSocket, (char *) &paket, sizeof( paket ), 0 );
    closesocket( ntpSocket );

    if( alinan != (int) sizeof( paket ) )
    {
        printf( "[NTP] HATA: gecersiz cevap (beklenen %d byte, alinan %d byte).\n",
                (int) sizeof( paket ), alinan );
        return false;
    }

    /* txTm_s: sunucunun cevabi GONDERDIGI andaki, 1900'den beri gecen
     * saniye. Network byte order'dan (buyuk-endian) makinemizin byte
     * order'ina ceviriyoruz (ntohl), sonra 1970 referansina kaydiriyoruz. */
    uint32_t txTm_s = ntohl( paket.txTm_s );
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
static void vNtpSyncTask( void *pvParameters )
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

/* =======================================================================
 * prvSuankiUnixZaman()
 *
 * "Su an kac" sorusunun cevabi - aga hic gitmeden, kaydedilen offset ve
 * o anki tick sayacindan hesaplar. NTP hic senkronize olmadiysa (henuz
 * ilk sorgu yapilmadiysa), offset 0'dir, donen deger sadece "tick
 * sayacinin saniyeye cevrilmis hali" olur (anlamli bir gercek zaman
 * DEGILDIR, bilgi amaclidir).
 * ===================================================================== */
static time_t prvSuankiUnixZaman( void )
{
    TickType_t suankiTick = xTaskGetTickCount();
    time_t tickSaniye = (time_t) ( ( (uint64_t) suankiTick * portTICK_PERIOD_MS ) / 1000 );

    return xUnixZamanOfseti + tickSaniye;
}


static void prvSicaklikVerisiYukle( const char *pcDosyaYolu )
{
    FILE *fp = fopen( pcDosyaYolu, "r" );

    if( fp == NULL )
    {
        printf( "[main] UYARI: '%s' bulunamadi, sahte (rastgele) veri "
                "kullanilacak.\n", pcDosyaYolu );
        return;
    }

    char satir[ 128 ];
    fgets( satir, sizeof( satir ), fp );  /* baslik satirini atla */

    while( fgets( satir, sizeof( satir ), fp ) != NULL &&
           xSicaklikKayitSayisi < MAX_SICAKLIK_KAYIT )
    {
        if( sscanf( satir, "%23[^,],%15[^,],%f,%39[^\r\n]",
            xSicaklikVerileri[ xSicaklikKayitSayisi ].sehir,
            xSicaklikVerileri[ xSicaklikKayitSayisi ].tarih,
            &xSicaklikVerileri[ xSicaklikKayitSayisi ].sicaklik,
            xSicaklikVerileri[ xSicaklikKayitSayisi ].durum ) == 4 )
        {
            xSicaklikKayitSayisi++;
        }
    }

    fclose( fp );
    printf( "[main] Gercek sicaklik veri seti yuklendi: %d kayit (%s).\n",
            xSicaklikKayitSayisi, pcDosyaYolu );
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
static void prvParseNetworkArgsFromArgs( int argc, char *argv[] )
{
    if( argc >= 3 )
    {
        int girilenPort = atoi( argv[ 2 ] );

        if( girilenPort > 0 && girilenPort <= 65535 )
        {
            xPortNumarasi = girilenPort;
        }
        else
        {
            printf( "[main] UYARI: Gecersiz port '%s', varsayilan %d kullanilacak.\n",
                    argv[ 2 ], DEFAULT_PORT );
        }
    }

    if( argc >= 4 )
    {
        strncpy( cBrokerIP, argv[ 3 ], sizeof( cBrokerIP ) - 1 );
        cBrokerIP[ sizeof( cBrokerIP ) - 1 ] = '\0';
    }

    printf( "[main] Ag ayarlari -> Port: %d, Broker IP: %s\n",
            xPortNumarasi, cBrokerIP );
}
static SystemRole_t prvParseRoleFromArgs( int argc, char *argv[] )
{
    if( argc < 2 )
    {
        return ROLE_UNDEFINED;
    }

    if( strcmp( argv[ 1 ], "broker" ) == 0 )
    {
        return ROLE_BROKER;
    }
    else if( strcmp( argv[ 1 ], "publisher" ) == 0 )
    {
        return ROLE_PUBLISHER;
    }
    else if( strcmp( argv[ 1 ], "subscriber" ) == 0 )
    {
        return ROLE_SUBSCRIBER;
    }

    return ROLE_UNDEFINED;
}


/* =======================================================================
 * prvPrintUsage()
 * ===================================================================== */
static void prvPrintUsage( const char *pcProgramName )
{
    printf( "Kullanim: %s <rol>\n", pcProgramName );
    printf( "  rol: broker | publisher | subscriber\n\n" );
}

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
                            (void *) xRole,
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
        int dolulukYuzdesi = (int)( 100 - ( bosHeap * 100 / configTOTAL_HEAP_SIZE ) );
        /* Zaman damgasini BURADA, tum printf'lerden ONCE yakaliyoruz -
        * boylece olcum, sadece "ag + islem" gecikmesini yansitir,
        * konsol yazma suresini DEGIL. */
        

        printf( "\n[Health] ===== SISTEM SAGLIK RAPORU =====\n" );
        printf( "[Health] Heap: %u byte bos / %u toplam (doluluk: %%%d)\n",
                (unsigned int) bosHeap,
                (unsigned int) configTOTAL_HEAP_SIZE,
                dolulukYuzdesi );
        printf( "[Health] Heap en dusuk seviye: %u byte (leak gostergesi: surekli dusuyorsa sizinti var)\n",
                (unsigned int) minBosHeap );

        /* --- 2) TASK ANALIZI: durum + stack high water mark --- */
        UBaseType_t uxTaskSayisi = uxTaskGetNumberOfTasks();
        TaskStatus_t *pxDurumlar = pvPortMalloc( uxTaskSayisi * sizeof( TaskStatus_t ) );

        if( pxDurumlar != NULL )
        {
            UBaseType_t uxAlinan = uxTaskGetSystemState( pxDurumlar, uxTaskSayisi, NULL );

            printf( "[Health] %-18s %-10s %s\n", "TASK", "DURUM", "STACK BOS (word)" );
            for( UBaseType_t i = 0; i < uxAlinan; i++ )
            {
                printf( "[Health] %-18s %-10s %u\n",
                        pxDurumlar[ i ].pcTaskName,
                        prvTaskDurumuStr( pxDurumlar[ i ].eCurrentState ),
                        (unsigned int) pxDurumlar[ i ].usStackHighWaterMark );

                /* Stack tasmasina yaklasan task'lari OZEL OLARAK uyar. */
                if( pxDurumlar[ i ].usStackHighWaterMark < 50 )
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
                unsigned long long xOlcumZamani = (unsigned long long) GetTickCount64();
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
                    send( xSubscriberSockets[ i ], gonderilecek, (int) strlen( gonderilecek ), 0 );
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

        vTaskDelayUntil( &xLastWakeTime, pdMS_TO_TICKS( 5000 ) );
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

    if( xRole == ROLE_BROKER )
    {
        printf( "[Network] BROKER modu: baglanti dinlemeye hazirlaniliyor...\n" );

        /* 1) Dinleme soketi olustur. */
        SOCKET listenSocket = socket( AF_INET, SOCK_STREAM, 0 );
        if( listenSocket == INVALID_SOCKET )
        {
            printf( "[Network] HATA: socket() basarisiz, kod: %d\n", WSAGetLastError() );
            vTaskDelete( NULL );
        }

        /* 2) Adres/port bilgisini doldur ve bind et. */
        struct sockaddr_in serverAddr;
        memset( &serverAddr, 0, sizeof( serverAddr ) );
        serverAddr.sin_family      = AF_INET;
        serverAddr.sin_addr.s_addr = INADDR_ANY;
        serverAddr.sin_port        = htons( (uint16_t) xPortNumarasi );

        if( bind( listenSocket, (struct sockaddr *) &serverAddr, sizeof( serverAddr ) ) == SOCKET_ERROR )
        {
            printf( "[Network] HATA: bind() basarisiz, kod: %d\n", WSAGetLastError() );
            closesocket( listenSocket );
            vTaskDelete( NULL );
        }

        /* 3) Dinlemeye basla. */
        if( listen( listenSocket, 5 ) == SOCKET_ERROR )
        {
            printf( "[Network] HATA: listen() basarisiz, kod: %d\n", WSAGetLastError() );
            closesocket( listenSocket );
            vTaskDelete( NULL );
        }

        printf( "[Network] Broker port %d'de dinlemede...\n", xPortNumarasi );

        /* Soketi NON-BLOCKING moda al. */
        u_long ulMode = 1;
        ioctlsocket( listenSocket, FIONBIO, &ulMode );

        SOCKET clientSocket = INVALID_SOCKET;

        /* BROKER'a ozel sonsuz dongu - accept() burada, if bloğunun İÇİNDE */
        for( ;; )
        {
            clientSocket = accept( listenSocket, NULL, NULL );

            if( clientSocket != INVALID_SOCKET )
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
                    closesocket( clientSocket );
                }

                clientSocket = INVALID_SOCKET;
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

        /* 1) Soket olustur - broker tarafiyla ayni mantik */
        SOCKET clientSocket = socket( AF_INET, SOCK_STREAM, 0 );
        if( clientSocket == INVALID_SOCKET )
        {
            printf( "[Network] HATA: socket() basarisiz, kod: %d\n", WSAGetLastError() );
            vTaskDelete( NULL );
        }

        /* 2) Baglanilacak adresi belirt - broker'in adresi/portu */
        struct sockaddr_in brokerAddr;
        memset( &brokerAddr, 0, sizeof( brokerAddr ) );
        brokerAddr.sin_family = AF_INET;
        brokerAddr.sin_port   = htons( (uint16_t) xPortNumarasi );
        /* "127.0.0.1" (localhost) string'ini binary IP adresine cevir */
        inet_pton( AF_INET, cBrokerIP, &brokerAddr.sin_addr );

        /* 3) BAGLANMAYI DENE - bu asamada BILEREK blocking birakiyoruz,
         * cunku "baglanana kadar bekle" burada mantikli bir davranis.
         * Broker henuz ayakta degilse, bu cagri BASARISIZ olur (bekleyip
         * sonsuza kadar durmaz) - bu yuzden bir retry donguesu kuruyoruz. */
        int connectResult = SOCKET_ERROR;

        while( connectResult == SOCKET_ERROR )
        {
            connectResult = connect( clientSocket,
                                      (struct sockaddr *) &brokerAddr,
                                      sizeof( brokerAddr ) );

            if( connectResult == SOCKET_ERROR )
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

            send( clientSocket, kimlikMesaji, (int) strlen( kimlikMesaji ), 0 );
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

                    send( clientSocket, gonderilecekVeri, (int) strlen( gonderilecekVeri ), 0 );
                    printf( "[Network] JSON mesaj gonderildi: %s\n", jsonString );

                    cJSON_free( jsonString );
                    cJSON_Delete( root );
                }
            }
        }
        else
        {
            /* SUBSCRIBER: gelen veriyi dinle */
            u_long ulMode = 1;
            ioctlsocket( clientSocket, FIONBIO, &ulMode );

            char recvBuffer[ 256 ];
            char mesajBuffer[ 1024 ] = { 0 };  /* framing icin biriktirme buffer'i */
            int mesajBufferUzunluk = 0;

            for( ;; )
            {
                int bytesReceived = recv( clientSocket, recvBuffer, sizeof( recvBuffer ) - 1, 0 );

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
                                        unsigned long long uzakZaman = (unsigned long long) tsItem->valuedouble;
                                        unsigned long long yerelZaman = (unsigned long long) GetTickCount64();
                                        long long gecikmeMs = (long long) ( yerelZaman - uzakZaman );

                                        printf( "[HealthPeer] Uzak instance'in health verisi %lld ms'de ulasti.\n",
                                                gecikmeMs );
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
                else if( bytesReceived == 0 )
                {
                    printf( "[Network] Broker baglantiyi kapatti.\n" );
                    break;
                }
                else
                {
                    int hataKodu = WSAGetLastError();
                    if( hataKodu != WSAEWOULDBLOCK )
                    {
                        printf( "[Network] HATA: recv() basarisiz, kod: %d\n", hataKodu );
                        break;
                    }
                }

                vTaskDelay( pdMS_TO_TICKS( 100 ) );
            }
        }
    }
}

static void vClientHandlerTask( void *pvParameters )
{
    SOCKET clientSocket = (SOCKET)(uintptr_t) pvParameters;
    bool bIsSubscriber = false;
    bool bIsAuthenticated = false;

    printf( "[ClientHandler] Yeni client task'i basladi (socket: %d)\n",
            (int) clientSocket );
            xAktifClientSayisi++;

    u_long ulMode = 1;
    ioctlsocket( clientSocket, FIONBIO, &ulMode );

    char recvBuffer[ 256 ];
    char mesajBuffer[ 1024 ] = { 0 };
    int mesajBufferUzunluk = 0;

    /* Authentication icin makul bir zaman asimi - sonsuza kadar
     * bekleme, 5 saniyede gelmezse baglantiyi kapat. */
    TickType_t xBaglantiBaslangic = xTaskGetTickCount();

    for( ;; )
    {
        int bytesReceived = recv( clientSocket, recvBuffer, sizeof( recvBuffer ) - 1, 0 );

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
                        closesocket( clientSocket );
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
                        closesocket( clientSocket );
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
                                    send( xSubscriberSockets[ i ], gonderilecekHava, (int) strlen( gonderilecekHava ), 0 );
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
                                char gonderilecekMesaj[ 300 ];
                                snprintf( gonderilecekMesaj, sizeof( gonderilecekMesaj ), "%s\n", mesajBuffer );
                                send( xSubscriberSockets[ i ], gonderilecekMesaj, (int) strlen( gonderilecekMesaj ), 0 );
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
        else if( bytesReceived == 0 )
        {
            printf( "[ClientHandler] Client baglantiyi kapatti.\n" );
            break;
        }
        else
        {
            int hataKodu = WSAGetLastError();
            if( hataKodu != WSAEWOULDBLOCK )
            {
                printf( "[ClientHandler] HATA: recv() basarisiz, kod: %d - "
                        "baglanti sonlandiriliyor.\n", hataKodu );
                break;
            }
        }

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


    closesocket( clientSocket );
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

    SOCKET udpSocket = socket( AF_INET, SOCK_DGRAM, IPPROTO_UDP );

    if( udpSocket == INVALID_SOCKET )
    {
        printf( "[UdpCmd] HATA: UDP soket olusturulamadi, kod: %d\n", WSAGetLastError() );
        vTaskDelete( NULL );
    }

    struct sockaddr_in udpAddr;
    memset( &udpAddr, 0, sizeof( udpAddr ) );
    udpAddr.sin_family      = AF_INET;
    udpAddr.sin_addr.s_addr = INADDR_ANY;
    /* Her ROL, kendi BENZERSIZ UDP portunu alsin - boylece ayni makinede
    * broker/publisher/subscriber ayni anda calisirken portlari
    * CAKISMASIN. Rol numarasina gore ek bir ofset ekliyoruz. */
    udpAddr.sin_port = htons( (uint16_t) ( xPortNumarasi + 1000 + ( (int) xMyRole * 10 ) ) );

    if( bind( udpSocket, (struct sockaddr *) &udpAddr, sizeof( udpAddr ) ) == SOCKET_ERROR )
    {
        printf( "[UdpCmd] HATA: UDP bind basarisiz, kod: %d\n", WSAGetLastError() );
        closesocket( udpSocket );
        vTaskDelete( NULL );
    }

    u_long ulMode = 1;
    ioctlsocket( udpSocket, FIONBIO, &ulMode );

    printf( "[UdpCmd] MQTT'den BAGIMSIZ komut kanali - UDP port %d'de dinlemede...\n",
        xPortNumarasi + 1000 + ( (int) xMyRole * 10 ) );

    char recvBuf[ 64 ];

    for( ;; )
    {
        struct sockaddr_in gonderenAdres;
        int adresBoyu = sizeof( gonderenAdres );

        int alinanBayt = recvfrom( udpSocket, recvBuf, sizeof( recvBuf ) - 1, 0,
                                    (struct sockaddr *) &gonderenAdres, &adresBoyu );

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

            sendto( udpSocket, cevap, (int) strlen( cevap ), 0,
                    (struct sockaddr *) &gonderenAdres, adresBoyu );

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
        send( xSubscriberSockets[ i ], gonderilecek, (int) strlen( gonderilecek ), 0 );
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
static void vIdleTimeoutCallback( TimerHandle_t xTimer )
{
    ( void ) xTimer;
     /* En az bir client hala bagliysa, idle sayilmaz - hemen cik. */
    if( xAktifClientSayisi > 0 )
    {
        return;
    }

    TickType_t suankiZaman = xTaskGetTickCount();
    TickType_t gecenSure   = suankiZaman - xSonBaglantiZamani;

    if( gecenSure > pdMS_TO_TICKS( IDLE_TIMEOUT_MS ) )
    {
        printf( "\n[IdleTimeout] %lu saniyedir hic aktif client yok - "
                "broker kendini kapatiyor...\n",
                (unsigned long) ( gecenSure * portTICK_PERIOD_MS / 1000 ) );

        /* NOT: vTaskEndScheduler() bu Windows Simulator portunda TAM
         * calismiyor - Timer Service task'ini silip zamanlamayi
         * durduruyor ama diger task'lar (Health gibi) kendi Windows
         * thread'lerinde calismaya devam ediyor (her FreeRTOS task'i
         * bu portta GERCEK bir Windows thread'i oldugu icin). Bu
         * yuzden TUM PROCESS'i dogrudan sonlandiriyoruz - bu, gercek
         * bir cihazin kapanmasiyla islevsel olarak ayni sonucu verir. */
        WSACleanup();
        exit( EXIT_SUCCESS );
    }
}

static void vMqttPublisherTask( void *pvParameters )
{
    ( void ) pvParameters;
    int mesajSayaci = 0;

    for( ;; )
    {
        PublishData_t veri;

        /* --- Sahte veri yerine, GERCEK veri setinden SIRAYLA oku --- */
        float gercekDeger;
        char gercekSehir[ 24 ] = "bilinmiyor";
        char gercekTarih[ 16 ] = "bilinmiyor";
        char gercekDurum[ 40 ] = "bilinmiyor";

        if( xSicaklikKayitSayisi > 0 )
        {
            static int xVeriIndeksi = 0;

            gercekDeger = xSicaklikVerileri[ xVeriIndeksi ].sicaklik;
            strncpy( gercekSehir, xSicaklikVerileri[ xVeriIndeksi ].sehir, sizeof( gercekSehir ) - 1 );
            strncpy( gercekTarih, xSicaklikVerileri[ xVeriIndeksi ].tarih, sizeof( gercekTarih ) - 1 );
            strncpy( gercekDurum, xSicaklikVerileri[ xVeriIndeksi ].durum, sizeof( gercekDurum ) - 1 );

            printf( "[MqttPub] Gercek veri kullaniliyor: %s, %s tarihli kayit\n", gercekSehir, gercekTarih );

            xVeriIndeksi = ( xVeriIndeksi + 1 ) % xSicaklikKayitSayisi;
        }
        else
        {
            gercekDeger = 20.0f + ( rand() % 100 ) / 10.0f;
            strncpy( gercekDurum, "bilinmiyor", sizeof( gercekDurum ) - 1 );
        }

        snprintf( veri.payload, sizeof( veri.payload ), "%.1f", gercekDeger );
        strncpy( veri.sehir, gercekSehir, sizeof( veri.sehir ) - 1 );
        strncpy( veri.tarih, gercekTarih, sizeof( veri.tarih ) - 1 );
        strncpy( veri.durum, gercekDurum, sizeof( veri.durum ) - 1 );

        strncpy( veri.topic, "sensor/sicaklik", sizeof( veri.topic ) - 1 );
        veri.topic[ sizeof( veri.topic ) - 1 ] = '\0';

        veri.mesaj_no = mesajSayaci;

        printf( "[MqttPub] Veri uretildi: %s (%s) = %s, durum: %s (no: %d)\n",
        veri.topic, veri.sehir, veri.payload, veri.durum, mesajSayaci );

        /* Ureteni Network Task'a TESLIM ET - JSON'a cevirme ve
         * gonderme islerine hic karismiyoruz, o Network Task'in isi. */
        if( xQueueSend( xPublishQueue, &veri, portMAX_DELAY ) != pdPASS )
        {
            printf( "[MqttPub] UYARI: Veri Network task'ina iletilemedi (queue dolu).\n" );
        }

        mesajSayaci++;
        vTaskDelay( pdMS_TO_TICKS( 3000 ) );
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


void vApplicationMallocFailedHook( void )
{
    printf( "Malloc failed!\r\n" );
    for( ;; );
}
/*-----------------------------------------------------------*/

void vApplicationIdleHook( void )
{
    /* Bos birakildi */
}
/*-----------------------------------------------------------*/

void vApplicationStackOverflowHook( TaskHandle_t pxTask, char * pcTaskName )
{
    ( void ) pxTask;
    ( void ) pcTaskName;
    printf( "Stack overflow!\r\n" );
    for( ;; );
}
/*-----------------------------------------------------------*/

void vApplicationTickHook( void )
{
    /* Bos birakildi */
}
void vAssertCalled( unsigned long ulLine, const char * const pcFileName )
{
    printf( "ASSERT! Line %ld, file %s\r\n", ulLine, pcFileName );
    for( ;; );
}
void vConfigureTimerForRunTimeStats( void )
{
    /* Bos birakildi - runtime stats kullanilmiyor */
}