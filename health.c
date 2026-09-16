#include "health.h"
#include "ntp.h"
#include "net_port.h"
#include "subscribers.h"
#include "cJSON/cJSON.h"

#include <stdio.h>
#include <string.h>

#include "FreeRTOS.h"
#include "task.h"
#include "semphr.h"
#include "roles.h"


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

void vHealthTask( void *pvParameters )
{
    ( void ) pvParameters;
    TickType_t xLastWakeTime = xTaskGetTickCount();

    for( ;; )
    {
        size_t bosHeap    = xPortGetFreeHeapSize();
        size_t minBosHeap = xPortGetMinimumEverFreeHeapSize();
        int dolulukYuzdesi = (int)( 100 - ( bosHeap * 100 / configTOTAL_HEAP_SIZE ) );

        printf( "\n[Health] ===== SISTEM SAGLIK RAPORU =====\n" );
        printf( "[Health] Heap: %u byte bos / %u toplam (doluluk: %%%d)\n",
                (unsigned int) bosHeap,
                (unsigned int) configTOTAL_HEAP_SIZE,
                dolulukYuzdesi );
        printf( "[Health] Heap en dusuk seviye: %u byte (leak gostergesi: surekli dusuyorsa sizinti var)\n",
                (unsigned int) minBosHeap );

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

                if( pxDurumlar[ i ].usStackHighWaterMark < 50 )
                {
                    printf( "[Health] !!! UYARI: '%s' task'inin stack'i tasma sinirina yaklasiyor!\n",
                            pxDurumlar[ i ].pcTaskName );
                }
            }

            vPortFree( pxDurumlar );
        }
        printf( "[Health] =================================\n\n" );

        if( xMyRole == ROLE_BROKER && xSubscriberListMutex != NULL )
        {
            if( xSemaphoreTake( xSubscriberListMutex, pdMS_TO_TICKS( 100 ) ) == pdTRUE )
            {
                unsigned long long xOlcumZamani = (unsigned long long) Net_SistemZamaniMs();
                cJSON *healthRoot = cJSON_CreateObject();
                cJSON_AddStringToObject( healthRoot, "topic", "system/health" );

                cJSON *healthPayload = cJSON_CreateObject();
                cJSON_AddNumberToObject( healthPayload, "heap", (double) bosHeap );
                cJSON_AddNumberToObject( healthPayload, "min_heap", (double) minBosHeap );
                cJSON_AddNumberToObject( healthPayload, "doluluk", dolulukYuzdesi );
                cJSON_AddNumberToObject( healthPayload, "task_sayisi", (double) uxTaskSayisi );
                cJSON_AddNumberToObject( healthPayload, "ts", (double) xOlcumZamani );
                cJSON_AddNumberToObject( healthPayload, "zaman", (double) Ntp_SuankiZaman() );

                cJSON_AddItemToObject( healthRoot, "payload", healthPayload );

                char *healthJson = cJSON_PrintUnformatted( healthRoot );
                char gonderilecek[ 256 ];
                snprintf( gonderilecek, sizeof( gonderilecek ), "%s\n", healthJson );

                for( int i = 0; i < xSubscriberCount; i++ )
                {
                    Net_Gonder( xSubscriberSockets[ i ], gonderilecek, (int) strlen( gonderilecek ) );
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