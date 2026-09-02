#include "cJSON.h"
#include <winsock2.h>
#include <ws2tcpip.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "FreeRTOS.h"
#include "task.h"
#include "queue.h"
#include "semphr.h"
#include <stdbool.h>
#include "timers.h"


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
    char payload[ 64 ];
} SensorData_t;

/* MQTT Publisher Task'tan Network Task'a GIDECEK veriyi tasiyan yapi.
 * SensorData_t'den ayri tutuyoruz cunku farkli bir yonde, farkli bir
 * amac icin kullaniliyor - mesaj_no alani da sadece bu yonde var. */
typedef struct
{
    char topic[ 64 ];
    char payload[ 16 ];
    int  mesaj_no;
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

#define STACK_SIZE_HEALTH          ( configMINIMAL_STACK_SIZE * 4 )
#define STACK_SIZE_INTERNAL_COMM   ( configMINIMAL_STACK_SIZE * 4 )
#define STACK_SIZE_NETWORK         ( configMINIMAL_STACK_SIZE * 6 )
#define STACK_SIZE_MQTT            ( configMINIMAL_STACK_SIZE * 4 )
#define STACK_SIZE_CLIENT_HANDLER  ( configMINIMAL_STACK_SIZE * 4 )
#define MAX_CLIENTS                 5
#define SHARED_AUTH_TOKEN           "gizli_sifre123"


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

static void vHealthTask( void *pvParameters );
static void vInternalCommTask( void *pvParameters );
static void vNetworkTask( void *pvParameters );
static void vMqttPublisherTask( void *pvParameters );
static void vMqttSubscriberTask( void *pvParameters );
static void vClientHandlerTask( void *pvParameters );

static SystemRole_t prvParseRoleFromArgs( int argc, char *argv[] );
static void prvPrintUsage( const char *pcProgramName );
static void prvCreateTasksForRole( SystemRole_t xRole );
static void vStatusBroadcastCallback( TimerHandle_t xTimer );

int main( int argc, char *argv[] )
{
    
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
}

    /* 2) ADIM: Role uygun task'lari olustur. */
    prvCreateTasksForRole( xMyRole );

    /* 3) ADIM: Scheduler'i baslat - bu satirdan sonra kontrol
     *    bir daha asla buraya donmez. */
    vTaskStartScheduler();

    /* Buraya normalde HICBIR ZAMAN ulasilmaz. */
    printf( "HATA: Scheduler baslatilamadi (yetersiz heap olabilir)\n" );
    WSACleanup();
    return EXIT_FAILURE;
}
/*-----------------------------------------------------------*/
/* =======================================================================
 * prvParseRoleFromArgs()
 * Komut satiri argumanini okuyup enum'a cevirir.
 * ===================================================================== */
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

    /* --- Role ozel task'lar --- */

    switch( xRole )
    {
        case ROLE_BROKER:
        printf( "[main] Broker rolu - ek is mantigi task'i yok.\n" );
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

static void vHealthTask( void *pvParameters )
{
    ( void ) pvParameters;
    TickType_t xLastWakeTime = xTaskGetTickCount();

    for( ;; )
    {
        printf( "[Health] Sistem kontrolu yapiliyor... "
                "(bos heap: %u byte)\n",
                ( unsigned int ) xPortGetFreeHeapSize() );


        vTaskDelayUntil( &xLastWakeTime, pdMS_TO_TICKS( 1000 ) );
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
        serverAddr.sin_port        = htons( 8080 );

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

        printf( "[Network] Broker port 8080'de dinlemede...\n" );

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
        brokerAddr.sin_port   = htons( 8080 );

        /* "127.0.0.1" (localhost) string'ini binary IP adresine cevir */
        inet_pton( AF_INET, "127.0.0.1", &brokerAddr.sin_addr );

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
            snprintf( kimlikMesaji, sizeof( kimlikMesaji ), "AUTH:%s|ROLE:%s", SHARED_AUTH_TOKEN, rolString );

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

                            if( topicItem != NULL && cJSON_IsString( topicItem ) &&
                            payloadItem != NULL && cJSON_IsString( payloadItem ) )
                            {
                                /* Dogrulanan veriyi ISLEMEK yerine, Internal Comm task'ina
                                * QUEUE uzerinden gonderiyoruz - artik bu task sadece
                                * "network'ten veri al ve ilet" gorevini yapiyor. */
                                SensorData_t veri;
                                strncpy( veri.topic, topicItem->valuestring, sizeof( veri.topic ) - 1 );
                                veri.topic[ sizeof( veri.topic ) - 1 ] = '\0';
                                strncpy( veri.payload, payloadItem->valuestring, sizeof( veri.payload ) - 1 );
                                veri.payload[ sizeof( veri.payload ) - 1 ] = '\0';

                                /* portMAX_DELAY: queue doluysa, yer acilana kadar bekle.
                                * (Simdilik dolma ihtimali cok dusuk, 10 elemanlik kapasite var.) */
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

    printf( "[ClientHandler] Yeni client task'i basladi (socket: %d)\n",
            (int) clientSocket );

    u_long ulMode = 1;
    ioctlsocket( clientSocket, FIONBIO, &ulMode );

    char recvBuffer[ 256 ];
    char mesajBuffer[ 1024 ] = { 0 };  /* biriken veriyi tutan kalici buffer */
    int mesajBufferUzunluk = 0;

    /* --- ILK ASAMA: Kimlik mesajini bekle ---
     * Client, baglandiktan hemen sonra "ROLE:PUBLISHER" ya da
     * "ROLE:SUBSCRIBER" gonderecek. Non-blocking oldugu icin
     * hemen gelmeyebilir, bu yuzden kisa bir bekleme donguse kuruyoruz. */
    int kimlikBekleSayaci = 0;
    while( kimlikBekleSayaci < 50 )  /* en fazla 50 * 100ms = 5 saniye bekle */
    {
        int bytesReceived = recv( clientSocket, recvBuffer, sizeof( recvBuffer ) - 1, 0 );

        if( bytesReceived > 0 )
        {
            recvBuffer[ bytesReceived ] = '\0';

            /* Mesaji "AUTH:token|ROLE:rol" formatinda ayristiriyoruz.
            * strtok, verilen ayraca (burada "|") gore string'i parcalara boler. */
            char bufferKopyasi[ 256 ];
            strncpy( bufferKopyasi, recvBuffer, sizeof( bufferKopyasi ) - 1 );
            bufferKopyasi[ sizeof( bufferKopyasi ) - 1 ] = '\0';

            char *authKismi = strtok( bufferKopyasi, "|" );  /* "AUTH:gizli_sifre123" */
            char *roleKismi = strtok( NULL, "|" );             /* "ROLE:PUBLISHER" */

            bool bAuthBasarili = false;

            if( authKismi != NULL && strncmp( authKismi, "AUTH:", 5 ) == 0 )
            {
                const char *gelenToken = authKismi + 5;  /* "AUTH:" kismini atla */

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

            /* Simdi rol kismini isliyoruz */
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

            break;
        }

        kimlikBekleSayaci++;
        vTaskDelay( pdMS_TO_TICKS( 100 ) );
    }

    /* --- IKINCI ASAMA: Normal veri dinleme donguse --- */
        for( ;; )
    {
        int bytesReceived = recv( clientSocket, recvBuffer, sizeof( recvBuffer ) - 1, 0 );

        if( bytesReceived > 0 )
        {
            recvBuffer[ bytesReceived ] = '\0';

            /* Gelen veriyi kalici buffer'a EKLE (biriktir) */
            if( mesajBufferUzunluk + bytesReceived < (int) sizeof( mesajBuffer ) - 1 )
            {
                memcpy( mesajBuffer + mesajBufferUzunluk, recvBuffer, bytesReceived );
                mesajBufferUzunluk += bytesReceived;
                mesajBuffer[ mesajBufferUzunluk ] = '\0';
            }

            /* Buffer icinde tam mesaj(lar) var mi diye kontrol et -
            * '\n' karakterini ara. Birden fazla mesaj birikmis olabilir,
            * bu yuzden WHILE ile hepsini sirayla isliyoruz. */
            char *newlinePos;
            while( ( newlinePos = strchr( mesajBuffer, '\n' ) ) != NULL )
            {
                /* '\n' karakterinin oldugu yeri '\0' yaparak, tek bir
                * tam mesaji izole ediyoruz. */
                *newlinePos = '\0';

                /* --- TEMEL VERI DOGRULAMASI --- */
                int mesajUzunlugu = (int) strlen( mesajBuffer );

                if( mesajUzunlugu == 0 )
                {
                    /* Bos mesaj - islenecek bir sey yok, atla. */
                    printf( "[ClientHandler] UYARI: Bos mesaj alindi, yok sayiliyor.\n" );
                }
                else if( mesajUzunlugu > 200 )
                {
                    /* Beklenenden cok uzun bir mesaj - supheli, reddet.
                    * (Normal test mesajlarimiz ~40-50 karakter civarinda.) */
                    printf( "[ClientHandler] UYARI: Anormal uzunlukta mesaj (%d byte), "
                            "reddediliyor.\n", mesajUzunlugu );
                }
                else
                {
                    /* --- JSON DOGRULAMASI --- */
                    cJSON *parsedJson = cJSON_Parse( mesajBuffer );

                    if( parsedJson == NULL )
                    {
                        /* JSON parse basarisiz - bozuk/gecersiz veri. */
                        printf( "[ClientHandler] UYARI: Gecersiz JSON alindi, reddediliyor. "
                                "Gelen: %s\n", mesajBuffer );
                    }
                    else
                    {
                        /* JSON gecerli - simdi gerekli alanlarin var olup olmadigini
                        * ve dogru tipte olup olmadigini kontrol ediyoruz. */
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

                        if( bGecerliMesaj )
                        {
                            printf( "[ClientHandler] Gecerli JSON alindi -> topic: %s, payload: %s\n",
                                    topicItem->valuestring, payloadItem->valuestring );

                            /* Dogrulanan veriyi subscriber'lara ilet. */
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

                        /* --- ONEMLI: parsedJson icin ayrilan bellegi TEMIZLE --- */
                        cJSON_Delete( parsedJson );
                    }
                }
                /* Islenen mesaji buffer'dan CIKAR - kalan kismi (varsa
                * bir sonraki mesajin basi) buffer'in basina kaydir. */
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
            /* bytesReceived < 0 - bir hata kodu var, AMA bu her zaman
            * gercek bir hata anlamina gelmez. Non-blocking modda,
            * "su an veri yok" durumu da boyle rapor edilir. */
            int hataKodu = WSAGetLastError();

            if( hataKodu != WSAEWOULDBLOCK )
            {
                /* WSAEWOULDBLOCK DISINDA bir kod geldi - bu GERCEK bir hata,
                * ornegin karsi taraf aniden koptu (WSAECONNRESET) gibi. */
                printf( "[ClientHandler] HATA: recv() basarisiz, kod: %d - "
                        "baglanti sonlandiriliyor.\n", hataKodu );
                break;
            }
            /* hataKodu == WSAEWOULDBLOCK ise: normal durum, veri yok,
            * dongu devam etsin. */
        }



        vTaskDelay( pdMS_TO_TICKS( 100 ) );
    }

    closesocket( clientSocket );
    printf( "[ClientHandler] Task sonlandiriliyor.\n" );
    vTaskDelete( NULL );
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


static void vMqttPublisherTask( void *pvParameters )
{
    ( void ) pvParameters;
    int mesajSayaci = 0;

    for( ;; )
    {
        /* --- Sahte sensor verisi uret (ARTIK BURADA, Network Task'ta DEGIL) --- */
        PublishData_t veri;

        float sahteDeger = 20.0f + ( rand() % 100 ) / 10.0f;
        snprintf( veri.payload, sizeof( veri.payload ), "%.1f", sahteDeger );

        strncpy( veri.topic, "sensor/sicaklik", sizeof( veri.topic ) - 1 );
        veri.topic[ sizeof( veri.topic ) - 1 ] = '\0';

        veri.mesaj_no = mesajSayaci;

        printf( "[MqttPub] Veri uretildi: %s = %s (no: %d)\n",
                veri.topic, veri.payload, mesajSayaci );

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