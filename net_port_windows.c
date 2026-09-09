/* =======================================================================
 * net_port_windows.c
 *
 * net_port.h arayuzunun WINDOWS (Winsock) implementasyonu.
 *
 * Bu dosya, projedeki TEK Windows'a ozel ag kodudur. Uygulama kodu
 * (main.c) bu dosyanin ICINE hic bakmaz - sadece net_port.h'deki
 * Net_* fonksiyonlarini cagirir.
 *
 * ESP32'ye tasinirken, bu dosyanin YERINE net_port_esp32.c yazilacak
 * (lwIP kullanarak) - main.c'ye TEK SATIR bile dokunulmayacak.
 * ===================================================================== */

#include "net_port.h"

#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdio.h>
#include <string.h>


/* --- YASAM DONGUSU --------------------------------------------------- */

bool Net_Baslat( void )
{
    WSADATA wsaData;
    int wsaResult = WSAStartup( MAKEWORD( 2, 2 ), &wsaData );

    if( wsaResult != 0 )
    {
        printf( "[NetPort] HATA: WSAStartup basarisiz, kod: %d\n", wsaResult );
        return false;
    }

    printf( "[NetPort] Winsock baslatildi (versiyon: %d.%d)\n",
            LOBYTE( wsaData.wVersion ), HIBYTE( wsaData.wVersion ) );
    return true;
}

void Net_Temizle( void )
{
    WSACleanup();
}


/* --- TCP: SUNUCU (BROKER) TARAFI ------------------------------------- */

NetSocket_t Net_DinlemeBaslat( int xPort )
{
    SOCKET listenSocket = socket( AF_INET, SOCK_STREAM, 0 );

    if( listenSocket == INVALID_SOCKET )
    {
        printf( "[NetPort] HATA: socket() basarisiz, kod: %d\n", WSAGetLastError() );
        return NET_INVALID_SOCKET;
    }

    struct sockaddr_in serverAddr;
    memset( &serverAddr, 0, sizeof( serverAddr ) );
    serverAddr.sin_family      = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port        = htons( (uint16_t) xPort );

    if( bind( listenSocket, (struct sockaddr *) &serverAddr, sizeof( serverAddr ) ) == SOCKET_ERROR )
    {
        printf( "[NetPort] HATA: bind() basarisiz, kod: %d\n", WSAGetLastError() );
        closesocket( listenSocket );
        return NET_INVALID_SOCKET;
    }

    if( listen( listenSocket, 5 ) == SOCKET_ERROR )
    {
        printf( "[NetPort] HATA: listen() basarisiz, kod: %d\n", WSAGetLastError() );
        closesocket( listenSocket );
        return NET_INVALID_SOCKET;
    }

    /* Dinleme soketini otomatik olarak non-blocking yapiyoruz - boylece
     * Net_BaglantiKabulEt() bloklanmaz. */
    u_long ulMode = 1;
    ioctlsocket( listenSocket, FIONBIO, &ulMode );

    return (NetSocket_t) listenSocket;
}

NetSocket_t Net_BaglantiKabulEt( NetSocket_t xDinleyenSoket )
{
    SOCKET clientSocket = accept( (SOCKET) xDinleyenSoket, NULL, NULL );

    if( clientSocket == INVALID_SOCKET )
    {
        return NET_INVALID_SOCKET;   /* bekleyen baglanti yok - NORMAL durum */
    }

    return (NetSocket_t) clientSocket;
}


/* --- TCP: ISTEMCI TARAFI --------------------------------------------- */

NetSocket_t Net_Baglan( const char *pcIp, int xPort )
{
    SOCKET clientSocket = socket( AF_INET, SOCK_STREAM, 0 );

    if( clientSocket == INVALID_SOCKET )
    {
        printf( "[NetPort] HATA: socket() basarisiz, kod: %d\n", WSAGetLastError() );
        return NET_INVALID_SOCKET;
    }

    struct sockaddr_in hedefAddr;
    memset( &hedefAddr, 0, sizeof( hedefAddr ) );
    hedefAddr.sin_family = AF_INET;
    hedefAddr.sin_port   = htons( (uint16_t) xPort );
    inet_pton( AF_INET, pcIp, &hedefAddr.sin_addr );

    if( connect( clientSocket, (struct sockaddr *) &hedefAddr, sizeof( hedefAddr ) ) == SOCKET_ERROR )
    {
        closesocket( clientSocket );
        return NET_INVALID_SOCKET;   /* cagiran taraf tekrar deneyecek */
    }

    return (NetSocket_t) clientSocket;
}


/* --- TCP: ORTAK VERI ISLEMLERI --------------------------------------- */

int Net_Gonder( NetSocket_t xSoket, const char *pcVeri, int xUzunluk )
{
    int sonuc = send( (SOCKET) xSoket, pcVeri, xUzunluk, 0 );

    if( sonuc == SOCKET_ERROR )
    {
        return NET_SONUC_HATA;
    }

    return sonuc;
}

int Net_Al( NetSocket_t xSoket, char *pcBuffer, int xBoyut )
{
    int sonuc = recv( (SOCKET) xSoket, pcBuffer, xBoyut, 0 );

    if( sonuc > 0 )
    {
        return sonuc;
    }

    if( sonuc == 0 )
    {
        return NET_SONUC_BAGLANTI_KAPANDI;
    }

    /* sonuc < 0: hata kodunu kontrol et. WSAEWOULDBLOCK, non-blocking
     * modda "su an veri yok" demektir - GERCEK bir hata DEGILDIR.
     * Bu platform-ozel ayrimi BURADA yapip, uygulama koduna standart
     * bir deger donduruyoruz. */
    int hataKodu = WSAGetLastError();

    if( hataKodu == WSAEWOULDBLOCK )
    {
        return NET_SONUC_VERI_YOK;
    }

    return NET_SONUC_HATA;
}

bool Net_NonBlockingYap( NetSocket_t xSoket )
{
    u_long ulMode = 1;
    return ( ioctlsocket( (SOCKET) xSoket, FIONBIO, &ulMode ) != SOCKET_ERROR );
}

void Net_Kapat( NetSocket_t xSoket )
{
    closesocket( (SOCKET) xSoket );
}


/* --- UDP ------------------------------------------------------------- */

NetSocket_t Net_UdpSocketOlustur( void )
{
    SOCKET udpSocket = socket( AF_INET, SOCK_DGRAM, IPPROTO_UDP );

    if( udpSocket == INVALID_SOCKET )
    {
        printf( "[NetPort] HATA: UDP socket() basarisiz, kod: %d\n", WSAGetLastError() );
        return NET_INVALID_SOCKET;
    }

    return (NetSocket_t) udpSocket;
}

NetSocket_t Net_UdpDinlemeBaslat( int xPort )
{
    NetSocket_t xSoket = Net_UdpSocketOlustur();

    if( xSoket == NET_INVALID_SOCKET )
    {
        return NET_INVALID_SOCKET;
    }

    struct sockaddr_in udpAddr;
    memset( &udpAddr, 0, sizeof( udpAddr ) );
    udpAddr.sin_family      = AF_INET;
    udpAddr.sin_addr.s_addr = INADDR_ANY;
    udpAddr.sin_port        = htons( (uint16_t) xPort );

    if( bind( (SOCKET) xSoket, (struct sockaddr *) &udpAddr, sizeof( udpAddr ) ) == SOCKET_ERROR )
    {
        printf( "[NetPort] HATA: UDP bind() basarisiz, kod: %d\n", WSAGetLastError() );
        Net_Kapat( xSoket );
        return NET_INVALID_SOCKET;
    }

    Net_NonBlockingYap( xSoket );

    return xSoket;
}

int Net_UdpAl( NetSocket_t xSoket, char *pcBuffer, int xBoyut, NetAdres_t *pxGonderen )
{
    struct sockaddr_in gonderenAddr;
    int adresBoyu = sizeof( gonderenAddr );

    int sonuc = recvfrom( (SOCKET) xSoket, pcBuffer, xBoyut, 0,
                           (struct sockaddr *) &gonderenAddr, &adresBoyu );

    if( sonuc > 0 )
    {
        if( pxGonderen != NULL )
        {
            /* Gonderenin adresini, platformdan bagimsiz opak yapiya
             * kopyaliyoruz - uygulama kodu icerigine hic bakmayacak,
             * sadece cevap gonderirken geri verecek. */
            memcpy( pxGonderen->ucVeri, &gonderenAddr, sizeof( gonderenAddr ) );
            pxGonderen->xUzunluk = adresBoyu;
        }
        return sonuc;
    }

    if( sonuc == 0 )
    {
        return NET_SONUC_BAGLANTI_KAPANDI;
    }

    int hataKodu = WSAGetLastError();

    if( hataKodu == WSAEWOULDBLOCK || hataKodu == WSAETIMEDOUT )
    {
        return NET_SONUC_VERI_YOK;
    }

    return NET_SONUC_HATA;
}

int Net_UdpGonder( NetSocket_t xSoket, const char *pcVeri, int xUzunluk, const NetAdres_t *pxHedef )
{
    int sonuc = sendto( (SOCKET) xSoket, pcVeri, xUzunluk, 0,
                         (const struct sockaddr *) pxHedef->ucVeri, pxHedef->xUzunluk );

    if( sonuc == SOCKET_ERROR )
    {
        return NET_SONUC_HATA;
    }

    return sonuc;
}

bool Net_ZamanAsimiAyarla( NetSocket_t xSoket, int xMilisaniye )
{
    DWORD timeout = (DWORD) xMilisaniye;
    return ( setsockopt( (SOCKET) xSoket, SOL_SOCKET, SO_RCVTIMEO,
                          (const char *) &timeout, sizeof( timeout ) ) != SOCKET_ERROR );
}


/* --- DNS ------------------------------------------------------------- */

bool Net_AdresCozumle( const char *pcHostAdi, int xPort, NetAdres_t *pxSonuc )
{
    struct addrinfo hints;
    struct addrinfo *cozumlenen = NULL;

    memset( &hints, 0, sizeof( hints ) );
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    char portStr[ 8 ];
    snprintf( portStr, sizeof( portStr ), "%d", xPort );

    int dnsSonuc = getaddrinfo( pcHostAdi, portStr, &hints, &cozumlenen );

    if( dnsSonuc != 0 || cozumlenen == NULL )
    {
        printf( "[NetPort] HATA: DNS cozumleme basarisiz (%s), kod: %d\n",
                pcHostAdi, dnsSonuc );
        return false;
    }

    if( cozumlenen->ai_addrlen > sizeof( pxSonuc->ucVeri ) )
    {
        printf( "[NetPort] HATA: adres yapisi beklenenden buyuk.\n" );
        freeaddrinfo( cozumlenen );
        return false;
    }

    memcpy( pxSonuc->ucVeri, cozumlenen->ai_addr, cozumlenen->ai_addrlen );
    pxSonuc->xUzunluk = (int) cozumlenen->ai_addrlen;

    freeaddrinfo( cozumlenen );

    return true;
}


/* --- ZAMAN ----------------------------------------------------------- */

uint64_t Net_SistemZamaniMs( void )
{
    return (uint64_t) GetTickCount64();
}
