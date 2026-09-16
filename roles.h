#ifndef ROLES_H
#define ROLES_H

/* =======================================================================
 * roles.h
 *
 * Sistemin rol tanimi (Broker/Publisher/Subscriber). main.c'de
 * TANIMLANIYOR (xMyRole degiskeni), diger modulller (health.c gibi)
 * bu ortak header'dan hem tipi hem degiskeni PAYLASIYOR.
 * ===================================================================== */

typedef enum
{
    ROLE_UNDEFINED = 0,
    ROLE_BROKER,
    ROLE_PUBLISHER,
    ROLE_SUBSCRIBER
} SystemRole_t;

extern SystemRole_t xMyRole;

#endif /* ROLES_H */