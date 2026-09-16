#include "cJSON.h"
#include "net_port.h"
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
#include "ntp.h"    
#include "subscribers.h"
#include "weather.h"
#include "health.h"
#include "roles.h"
#define MAX_SICAKLIK_KAYIT 1100   



typedef struct
{
    char sehir[ 24 ];
    char tarih[ 16 ];
    float sicaklik;
    char durum[ 40 ];
} SicaklikKaydi_t;

static SicaklikKaydi_t xSicaklikVerileri[ MAX_SICAKLIK_KAYIT ];
static int xSicaklikKayitSayisi = 0;
SystemRole_t xMyRole = ROLE_UNDEFINED;

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
#define SHARED_AUTH_TOKEN           "gizli_sifre123"
#define DEFAULT_PORT         8080
#define DEFAULT_BROKER_IP    "127.0.0.1"
#define STACK_SIZE_UDP_COMMAND     ( configMINIMAL_STACK_SIZE * 4 )
#define IDLE_TIMEOUT_MS   30000   /* 30 saniye hic baglanti gelmezse kapan */

#define NTP_PORT                123
#define NTP_SYNC_INTERVAL_MS    ( 5 * 60 * 1000 )   /* 5 dakikada bir yeniden senkronize et */

#define STACK_SIZE_ANLIK_HAVA      ( configMINIMAL_STACK_SIZE * 8 )   /* WinHTTP icin biraz daha fazla stack */

/* ---------------------------------------------------------------------
 * TASK HANDLE'LARI VE PROTOTIPLERI
 * ------------------------------------------------------------------- */

/* ---------------------------------------------------------------------
 * BROKER ICIN SUBSCRIBER LISTESI
 * Bagli subscriber'larin soketlerini burada tutuyoruz. Birden fazla
 * ClientHandlerTask (her biri farkli bir client icin calisan) bu listeye
 * AYNI ANDA erisebilir - bu yuzden bir MUTEX ile korumak zorundayiz.
 * ------------------------------------------------------------------- */
SemaphoreHandle_t xSubscriberListMutex = NULL;
NetSocket_t xSubscriberSockets[ MAX_CLIENTS ];
int xSubscriberCount = 0;


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


/* Su anki secili sehir - hem config dosyasindan hem runtime komuttan
 * (subscriber'dan gelen cmd/sehir_sorgu ile) degistirilebiliyor. Iki
 * farkli task/context'ten erisildigi icin mutex ile koruyoruz. */
char cSuankiSehir[ 64 ] = "Ankara";
SemaphoreHandle_t xSehirMutex = NULL;
TaskHandle_t xAnlikHavaTaskHandle = NULL;


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


static void prvSehirConfigYukle( const char *pcDosyaYolu );
static void prvAuthenticationIsle( NetSocket_t clientSocket, const char *pcMesajBuffer,
                                    bool *pbIsAuthenticated, bool *pbIsSubscriber );
static void prvSehirSorguKomutunuIsle( const char *pcSehir );
static void prvNormalVeriYayinla( const char *pcMesajBuffer );
static void prvGelenMesajiIsle( bool bIsSubscriber, char *pcMesajBuffer );
static void prvClientHandlerTemizle( NetSocket_t clientSocket, bool bIsSubscriber );
static void prvBrokerDinlemeDongusu( void );
static void prvClientKimlikGonder( NetSocket_t clientSocket, SystemRole_t xRole );
static void prvPublisherAkisi( NetSocket_t clientSocket );
static void prvSubscriberAkisi( NetSocket_t clientSocket );


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
    
    /* Ag katmanini baslat - platformdan bagimsiz. Windows'ta WSAStartup
    * cagirir, ESP32'de muhtemelen hicbir sey yapmayacak, ama uygulama
    * kodu bu farki GORMEZ. */
    if( !Net_Baslat() )
    {
        return EXIT_FAILURE;
    }
    
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
    Ntp_BaslangicSenkronizasyonuYap();

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
        Net_Temizle();
        return EXIT_SUCCESS;
    }
    else
    {
        printf( "HATA: Scheduler baslatilamadi (yetersiz heap olabilir)\n" );
        Net_Temizle();
        return EXIT_FAILURE;
    }
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
            xResult = xTaskCreate( vAnlikHavaTask,
                        "AnlikHava",
                        STACK_SIZE_ANLIK_HAVA,
                        NULL,
                        PRIORITY_ANLIK_HAVA,
                        &xAnlikHavaTaskHandle );
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
        prvBrokerDinlemeDongusu();
        return;   /* prvBrokerDinlemeDongusu zaten sonsuz dongu, buraya hic gelinmez */
    }

    NetSocket_t clientSocket = NET_INVALID_SOCKET;

    printf( "[Network] CLIENT modu: broker'a baglanmaya hazirlaniliyor...\n" );

    while( clientSocket == NET_INVALID_SOCKET )
    {
        clientSocket = Net_Baglan( cBrokerIP, xPortNumarasi );

        if( clientSocket == NET_INVALID_SOCKET )
        {
            printf( "[Network] Broker'a baglanilamadi, 2 saniye sonra tekrar denenecek...\n" );
            vTaskDelay( pdMS_TO_TICKS( 2000 ) );
        }
    }

    prvClientKimlikGonder( clientSocket, xRole );

    if( xRole == ROLE_PUBLISHER )
    {
        prvPublisherAkisi( clientSocket );
    }
    else
    {
        prvSubscriberAkisi( clientSocket );
    }
}

static void prvBrokerDinlemeDongusu( void )
{
    printf( "[Network] BROKER modu: baglanti dinlemeye hazirlaniliyor...\n" );

    NetSocket_t listenSocket = Net_DinlemeBaslat( xPortNumarasi );

    if( listenSocket == NET_INVALID_SOCKET )
    {
        printf( "[Network] HATA: dinleme baslatilamadi.\n" );
        vTaskDelete( NULL );
    }

    printf( "[Network] Broker port %d'de dinlemede...\n", xPortNumarasi );

    NetSocket_t clientSocket = NET_INVALID_SOCKET;

    for( ;; )
    {
        clientSocket = Net_BaglantiKabulEt( listenSocket );

        if( clientSocket != NET_INVALID_SOCKET )
        {
            xSonBaglantiZamani = xTaskGetTickCount();
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

            clientSocket = NET_INVALID_SOCKET;
        }

        vTaskDelay( pdMS_TO_TICKS( 100 ) );
    }
}

static void prvClientKimlikGonder( NetSocket_t clientSocket, SystemRole_t xRole )
{
    printf( "[Network] Broker'a basariyla baglanildi!\n" );

    char kimlikMesaji[ 128 ];
    const char *rolString = ( xRole == ROLE_PUBLISHER ) ? "PUBLISHER" : "SUBSCRIBER";
    snprintf( kimlikMesaji, sizeof( kimlikMesaji ), "AUTH:%s|ROLE:%s\n", SHARED_AUTH_TOKEN, rolString );

    Net_Gonder( clientSocket, kimlikMesaji, (int) strlen( kimlikMesaji ) );
    printf( "[Network] Kimlik bildirildi: %s\n", kimlikMesaji );
}

static void prvPublisherAkisi( NetSocket_t clientSocket )
{
    PublishData_t gelenVeri;

    for( ;; )
    {
        if( xQueueReceive( xPublishQueue, &gelenVeri, portMAX_DELAY ) == pdPASS )
        {
            cJSON *root = cJSON_CreateObject();
            cJSON_AddStringToObject( root, "topic", gelenVeri.topic );
            cJSON_AddStringToObject( root, "payload", gelenVeri.payload );
            cJSON_AddNumberToObject( root, "mesaj_no", gelenVeri.mesaj_no );
            cJSON_AddStringToObject( root, "sehir", gelenVeri.sehir );
            cJSON_AddStringToObject( root, "tarih", gelenVeri.tarih );
            cJSON_AddStringToObject( root, "durum", gelenVeri.durum );
            cJSON_AddNumberToObject( root, "zaman", (double) Ntp_SuankiZaman() );

            char *jsonString = cJSON_PrintUnformatted( root );

            char gonderilecekVeri[ 256 ];
            snprintf( gonderilecekVeri, sizeof( gonderilecekVeri ), "%s\n", jsonString );

            Net_Gonder( clientSocket, gonderilecekVeri, (int) strlen( gonderilecekVeri ) );
            printf( "[Network] JSON mesaj gonderildi: %s\n", jsonString );

            cJSON_free( jsonString );
            cJSON_Delete( root );
        }
    }
}

static void prvSubscriberAkisi( NetSocket_t clientSocket )
{
    Net_NonBlockingYap( clientSocket );

    char recvBuffer[ 256 ];
    char mesajBuffer[ 1024 ] = { 0 };
    int mesajBufferUzunluk = 0;

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

                        if( strcmp( topicItem->valuestring, "system/health" ) == 0 &&
                            cJSON_IsObject( payloadItem ) )
                        {
                            cJSON *tsItem = cJSON_GetObjectItem( payloadItem, "ts" );

                            if( tsItem != NULL && cJSON_IsNumber( tsItem ) )
                            {
                                unsigned long long uzakZaman = (unsigned long long) tsItem->valuedouble;
                                unsigned long long yerelZaman = (unsigned long long) Net_SistemZamaniMs();
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

                if( !bIsAuthenticated )
                {
                    prvAuthenticationIsle( clientSocket, mesajBuffer,
                                            &bIsAuthenticated, &bIsSubscriber );
                }
                else
                {
                    prvGelenMesajiIsle( bIsSubscriber, mesajBuffer );
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

        if( !bIsAuthenticated &&
            ( xTaskGetTickCount() - xBaglantiBaslangic ) > pdMS_TO_TICKS( 5000 ) )
        {
            printf( "[ClientHandler] UYARI: 5 saniye icinde authentication gelmedi, baglanti kapatiliyor.\n" );
            break;
        }

        vTaskDelay( pdMS_TO_TICKS( 100 ) );
    }

    prvClientHandlerTemizle( clientSocket, bIsSubscriber );
    vTaskDelete( NULL );
}
static void prvAuthenticationIsle( NetSocket_t clientSocket, const char *pcMesajBuffer,
                                    bool *pbIsAuthenticated, bool *pbIsSubscriber )
{
    char bufferKopyasi[ 256 ];
    strncpy( bufferKopyasi, pcMesajBuffer, sizeof( bufferKopyasi ) - 1 );
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
    *pbIsAuthenticated = true;

    if( roleKismi != NULL && strcmp( roleKismi, "ROLE:SUBSCRIBER" ) == 0 )
    {
        *pbIsSubscriber = true;
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

static void prvSehirSorguKomutunuIsle( const char *pcSehir )
{
    if( xSemaphoreTake( xSehirMutex, pdMS_TO_TICKS( 100 ) ) == pdTRUE )
    {
        strncpy( cSuankiSehir, pcSehir, sizeof( cSuankiSehir ) - 1 );
        cSuankiSehir[ sizeof( cSuankiSehir ) - 1 ] = '\0';
        xSemaphoreGive( xSehirMutex );
    }

    AnlikHavaSonucu_t sonuc = Weather_SehirSorgula( cSuankiSehir );
    if( sonuc.basarili )
    {
        Weather_Yayinla( cSuankiSehir, &sonuc );

        printf( "[ClientHandler] Anlik hava yayinlandi: %s = %.1f C\n",
                cSuankiSehir, sonuc.sicaklik );

        Weather_Kaydet( cSuankiSehir, &sonuc );

        if( xAnlikHavaTaskHandle != NULL )
        {
            xTaskNotifyGive( xAnlikHavaTaskHandle );
        }
    }
    else
    {
        printf( "[ClientHandler] HATA: '%s' icin anlik hava alinamadi.\n", cSuankiSehir );
    }
}

static void prvNormalVeriYayinla( const char *pcMesajBuffer )
{
    xSemaphoreTake( xSubscriberListMutex, portMAX_DELAY );
    for( int i = 0; i < xSubscriberCount; i++ )
    {
        char gonderilecekMesaj[ 300 ];
        snprintf( gonderilecekMesaj, sizeof( gonderilecekMesaj ), "%s\n", pcMesajBuffer );
        Net_Gonder( xSubscriberSockets[ i ], gonderilecekMesaj, (int) strlen( gonderilecekMesaj ) );
    }
    xSemaphoreGive( xSubscriberListMutex );

    printf( "[ClientHandler] Veri %d subscriber'a iletildi.\n", xSubscriberCount );
}

static void prvGelenMesajiIsle( bool bIsSubscriber, char *pcMesajBuffer )
{
    int mesajUzunlugu = (int) strlen( pcMesajBuffer );

    if( mesajUzunlugu == 0 )
    {
        printf( "[ClientHandler] UYARI: Bos mesaj alindi, yok sayiliyor.\n" );
        return;
    }

    if( mesajUzunlugu > 200 )
    {
        printf( "[ClientHandler] UYARI: Anormal uzunlukta mesaj (%d byte), "
                "reddediliyor.\n", mesajUzunlugu );
        return;
    }

    cJSON *parsedJson = cJSON_Parse( pcMesajBuffer );

    if( parsedJson == NULL )
    {
        printf( "[ClientHandler] UYARI: Gecersiz JSON alindi, reddediliyor. "
                "Gelen: %s\n", pcMesajBuffer );
        return;
    }

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
        printf( "[ClientHandler] Sehir sorgu komutu alindi: %s\n",
                payloadItem->valuestring );
        prvSehirSorguKomutunuIsle( payloadItem->valuestring );
    }
    else if( bIsSubscriber )
    {
        printf( "[ClientHandler] YETKI IHLALI: Subscriber veri gondermeye "
                "calisti, veri reddediliyor. Gelen: %s\n", pcMesajBuffer );
    }
    else if( bGecerliMesaj )
    {
        printf( "[ClientHandler] Gecerli JSON alindi -> topic: %s, payload: %s\n",
                topicItem->valuestring, payloadItem->valuestring );
        prvNormalVeriYayinla( pcMesajBuffer );
    }
    else
    {
        printf( "[ClientHandler] Mesaj eksik/hatali alanlar icerdigi icin "
                "subscriber'lara iletilmedi.\n" );
    }

    cJSON_Delete( parsedJson );
}

static void prvClientHandlerTemizle( NetSocket_t clientSocket, bool bIsSubscriber )
{
    xAktifClientSayisi--;
    if( xAktifClientSayisi == 0 )
    {
        xSonBaglantiZamani = xTaskGetTickCount();
    }

    if( bIsSubscriber )
    {
        xSemaphoreTake( xSubscriberListMutex, portMAX_DELAY );
        for( int i = 0; i < xSubscriberCount; i++ )
        {
            if( xSubscriberSockets[ i ] == clientSocket )
            {
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
    cJSON_AddNumberToObject( statusRoot, "zaman", (double) Ntp_SuankiZaman() );

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
        Net_Temizle();
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