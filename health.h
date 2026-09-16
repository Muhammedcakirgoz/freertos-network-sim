#ifndef HEALTH_H
#define HEALTH_H

/* =======================================================================
 * health.h
 *
 * Sistem sagligi raporlama modulunun PUBLIC arayuzu.
 * ===================================================================== */

/* Periyodik saglik raporlama gorevi - xTaskCreate ile baslatilir. */
void vHealthTask( void *pvParameters );

#endif /* HEALTH_H */