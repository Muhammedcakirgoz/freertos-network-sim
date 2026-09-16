#ifndef SUBSCRIBERS_H
#define SUBSCRIBERS_H

#include "net_port.h"
#include "FreeRTOS.h"
#include "semphr.h"

#define MAX_CLIENTS 5

/* Bu degiskenler main.c'de TANIMLANIYOR (define edilecek), diger
 * modulller (weather.c gibi) burada sadece "boyle bir sey VAR"
 * diyerek (extern) paylasiyor. */
extern SemaphoreHandle_t xSubscriberListMutex;
extern NetSocket_t xSubscriberSockets[ MAX_CLIENTS ];
extern int xSubscriberCount;

#endif /* SUBSCRIBERS_H */