#include "net_port.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "lwip/sockets.h"
#include "lwip/netdb.h"
#include "esp_timer.h"
#include "esp_http_client.h"
#include "esp_crt_bundle.h"

_Static_assert(sizeof(struct sockaddr_in) <= sizeof(((NetAdres_t *)0)->ucVeri), "NetAdres_t too small");

bool Net_Baslat(void) { return true; }
void Net_Temizle(void) { }
uint64_t Net_SistemZamaniMs(void) { return (uint64_t)esp_timer_get_time() / 1000; }
void Net_Kapat(NetSocket_t s) { if (s >= 0) close((int)s); }

static int result(int n)
{
    if (n >= 0) return n;
    return errno == EAGAIN || errno == EWOULDBLOCK || errno == ETIMEDOUT || errno == EINTR
        ? NET_SONUC_VERI_YOK : NET_SONUC_HATA;
}

bool Net_NonBlockingYap(NetSocket_t s)
{
    int flags = fcntl((int)s, F_GETFL, 0);
    return flags >= 0 && fcntl((int)s, F_SETFL, flags | O_NONBLOCK) == 0;
}

bool Net_ZamanAsimiAyarla(NetSocket_t s, int ms)
{
    if (ms < 0) return false;
    struct timeval tv = {.tv_sec = ms / 1000, .tv_usec = (ms % 1000) * 1000};
    return setsockopt((int)s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == 0;
}

static NetSocket_t bind_socket(int port, int type)
{
    if (port < 1 || port > 65535) return NET_INVALID_SOCKET;
    int s = socket(AF_INET, type, 0);
    if (s < 0) return NET_INVALID_SOCKET;
    struct sockaddr_in addr = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port),
                               .sin_addr.s_addr = htonl(INADDR_ANY)};
    if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) < 0 || !Net_NonBlockingYap(s)) {
        close(s);
        return NET_INVALID_SOCKET;
    }
    return s;
}

NetSocket_t Net_DinlemeBaslat(int port)
{
    NetSocket_t s = bind_socket(port, SOCK_STREAM);
    if (s >= 0 && listen((int)s, 5) < 0) { Net_Kapat(s); return NET_INVALID_SOCKET; }
    return s;
}
NetSocket_t Net_BaglantiKabulEt(NetSocket_t s) { return accept((int)s, NULL, NULL); }
NetSocket_t Net_UdpDinlemeBaslat(int port) { return bind_socket(port, SOCK_DGRAM); }
NetSocket_t Net_UdpSocketOlustur(void)
{
    int s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (s < 0) {
        printf("[NetPort] HATA: UDP socket olusturulamadi, errno: %d (%s)\n", errno, strerror(errno));
    }
    return s;
}

NetSocket_t Net_Baglan(const char *ip, int port)
{
    if (!ip || port < 1 || port > 65535) return NET_INVALID_SOCKET;
    struct sockaddr_in addr = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) return NET_INVALID_SOCKET;
    int s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s < 0) return NET_INVALID_SOCKET;
    if (!Net_NonBlockingYap(s)) { close(s); return NET_INVALID_SOCKET; }
    int rc = connect(s, (struct sockaddr *)&addr, sizeof(addr));
    if (rc < 0) {
        if (errno != EINPROGRESS) { close(s); return NET_INVALID_SOCKET; }
        fd_set writefds;
        FD_ZERO(&writefds);
        FD_SET(s, &writefds);
        struct timeval timeout = {.tv_sec = 5};
        int error = 0;
        socklen_t size = sizeof(error);
        if (select(s + 1, NULL, &writefds, NULL, &timeout) <= 0 ||
            getsockopt(s, SOL_SOCKET, SO_ERROR, &error, &size) < 0 || error != 0) {
            close(s); return NET_INVALID_SOCKET;
        }
    }
    /* Match the original interface: connected sockets start blocking. */
    int flags = fcntl(s, F_GETFL, 0);
    if (flags < 0 || fcntl(s, F_SETFL, flags & ~O_NONBLOCK) < 0) {
        close(s); return NET_INVALID_SOCKET;
    }
    return s;
}

int Net_Gonder(NetSocket_t sock, const char *data, int len)
{
    if (!data || len < 0 || sock < 0) return NET_SONUC_HATA;
    int total = 0;
    uint64_t deadline = Net_SistemZamaniMs() + 3000;
    while (total < len) {
        int n = send((int)sock, data + total, len - total, MSG_DONTWAIT);
        if (n > 0) { total += n; continue; }
        if (n < 0 && errno == EINTR && Net_SistemZamaniMs() < deadline) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            uint64_t now = Net_SistemZamaniMs();
            if (now >= deadline) break;
            uint64_t left = deadline - now;
            struct timeval tv = {.tv_sec = left / 1000, .tv_usec = (left % 1000) * 1000};
            fd_set wfds;
            FD_ZERO(&wfds);
            FD_SET((int)sock, &wfds);
            if (select((int)sock + 1, NULL, &wfds, NULL, &tv) > 0) continue;
        }
        break;
    }
    if (total == len) return total;
    /* A partial JSON line must never be followed by a new line on this stream. */
    shutdown((int)sock, SHUT_RDWR);
    return NET_SONUC_HATA;
}

int Net_Al(NetSocket_t s, char *buffer, int size)
{
    if (!buffer || size <= 0) return NET_SONUC_HATA;
    return result(recv((int)s, buffer, size, 0));
}

int Net_UdpAl(NetSocket_t s, char *buffer, int size, NetAdres_t *sender)
{
    if (!buffer || size <= 0) return NET_SONUC_HATA;
    struct sockaddr_in addr = {0};
    socklen_t length = sizeof(addr);
    int n = recvfrom((int)s, buffer, size, 0, (struct sockaddr *)&addr, &length);
    if (n >= 0 && sender) {
        memset(sender, 0, sizeof(*sender));
        memcpy(sender->ucVeri, &addr, length);
        sender->xUzunluk = (int)length;
    }
    return result(n);
}

int Net_UdpGonder(NetSocket_t s, const char *data, int len, const NetAdres_t *target)
{
    if (!target || !data || len < 0 || target->xUzunluk != sizeof(struct sockaddr_in)) {
        printf("[NetPort] HATA: Net_UdpGonder - gecersiz parametre (xUzunluk=%d, beklenen=%d)\n",
               target ? target->xUzunluk : -1, (int)sizeof(struct sockaddr_in));
        return NET_SONUC_HATA;
    }
    struct sockaddr_in addr;
    memcpy(&addr, target->ucVeri, sizeof(addr));
    int n = sendto((int)s, data, len, 0, (struct sockaddr *)&addr, sizeof(addr));
    if (n < 0) {
        printf("[NetPort] HATA: sendto basarisiz, errno: %d (%s)\n", errno, strerror(errno));
    }
    return n < 0 ? NET_SONUC_HATA : n;
}

bool Net_AdresCozumle(const char *host, int port, NetAdres_t *out)
{
    if (!host || !out || port < 1 || port > 65535) return false;
    memset(out, 0, sizeof(*out));

    struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_DGRAM};
    struct addrinfo *addr = NULL;
    char service[8];
    snprintf(service, sizeof(service), "%d", port);

    if (getaddrinfo(host, service, &hints, &addr) != 0 || !addr) return false;

    /* lwIP bazen ai_family=AF_INET istememize RAGMEN sonuc listesinde
     * IPv6 girdileri de dondurebiliyor. Listede GERCEKTEN IPv4 olan
     * (ai_family==AF_INET, boyutu sockaddr_in ile uyusan) ilk sonucu
     * bulana kadar geziyoruz - ilk sonucu KORKORU kullanmiyoruz. */
    bool ok = false;
    for (struct addrinfo *p = addr; p != NULL; p = p->ai_next) {
        if (p->ai_family == AF_INET && p->ai_addrlen == sizeof(struct sockaddr_in)
            && p->ai_addrlen <= sizeof(out->ucVeri)) {
            memcpy(out->ucVeri, p->ai_addr, p->ai_addrlen);
            out->xUzunluk = (int)p->ai_addrlen;
            ok = true;
            break;
        }
    }

    if (!ok) {
        printf("[NetPort] HATA: '%s' icin gecerli bir IPv4 adresi bulunamadi.\n", host);
    }

    freeaddrinfo(addr);
    return ok;
}

bool Net_HttpsGet(const char *host, const char *path, char *body, size_t capacity)
{
    if (!body || capacity < 2 || capacity > INT_MAX || !host || !path) return false;
    body[0] = '\0';
    /* Certificate dates require a real system clock, not just an app offset. */
    if (time(NULL) < 1700000000) {
        printf("[HTTPS] NTP senkronizasyonu bekleniyor.\n");
        return false;
    }
    char url[512];
    int size = snprintf(url, sizeof(url), "https://%s%s", host, path);
    if (size < 0 || size >= sizeof(url)) return false;
    esp_http_client_config_t cfg = {.url = url, .timeout_ms = 10000,
                                    .crt_bundle_attach = esp_crt_bundle_attach};
    esp_http_client_handle_t client = esp_http_client_init(&cfg);
    if (!client) return false;
    bool ok = false;
    if (esp_http_client_open(client, 0) == ESP_OK && esp_http_client_fetch_headers(client) >= 0) {
        int status = esp_http_client_get_status_code(client);
        if (status >= 200 && status < 300) {
            size_t used = 0;
            while (used < capacity - 1) {
                int n = esp_http_client_read(client, body + used, (int)(capacity - 1 - used));
                if (n <= 0) break;
                used += n;
            }
            body[used] = '\0';
            ok = used > 0 && esp_http_client_is_complete_data_received(client);
        }
    }
    esp_http_client_cleanup(client);
    if (!ok) body[0] = '\0';
    return ok;
}
