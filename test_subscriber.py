"""
test_subscriber.py

Broker'a SUBSCRIBER olarak baglanir. Hem gelen mesajlari dinler hem
de sehir sorgu komutu gonderebilirsin - AMA bunu iki ayri THREAD
yerine, TEK BIR dongude, sirayla yapiyoruz. Boylece terminale
YAZAN sadece TEK BIR yer oluyor - onceki versiyondaki "iki thread
ayni anda yaziyor, cikti karisiyor" sorunu KOKTEN ortadan kalkiyor.

Kullanim:
    python test_subscriber.py [broker_ip] [port]

Bir sehir sorgulamak icin, ismini yazip Enter'a bas. Hicbir sey
yazmadan sadece Enter'a basarsan, gelen mesajlari izlemeye devam
edersin. Cikmak icin 'q' yaz.

NOT: Bu script Windows'a ozeldir (msvcrt kutuphanesi kullanir).
"""

import socket
import sys
import json
import time
import msvcrt

AUTH_TOKEN = "gizli_sifre123"


def mesaji_yazdir(satir):
    try:
        mesaj = json.loads(satir)
        topic = mesaj.get("topic")
        payload = mesaj.get("payload")
        ekstra = {k: v for k, v in mesaj.items() if k not in ("topic", "payload")}
        print(f"[GELEN] topic={topic} payload={payload} ekstra={ekstra}")
    except json.JSONDecodeError:
        print(f"[GELEN HAM VERI] {satir}")


def main():
    broker_ip = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8080

    print(f"Broker'a baglaniliyor: {broker_ip}:{port} ...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((broker_ip, port))
    sock.setblocking(False)  # non-blocking - ana donguyu bloklamasin

    kimlik = f"AUTH:{AUTH_TOKEN}|ROLE:SUBSCRIBER\n"
    sock.sendall(kimlik.encode("utf-8"))
    print("SUBSCRIBER olarak authenticate olundu.")
    print("Sehir yazip Enter'a bas (sadece Enter = izlemeye devam, 'q' = cikis)\n")

    yazi_buffer = ""
    okuma_buffer = ""

    print("Sehir: ", end="", flush=True)

    while True:
        # --- 1) Soketten gelen veriyi kontrol et (non-blocking) ---
        try:
            veri = sock.recv(4096)
            if not veri:
                print("\n[BAGLANTI KAPANDI]")
                break
            okuma_buffer += veri.decode("utf-8", errors="replace")

            while "\n" in okuma_buffer:
                satir, okuma_buffer = okuma_buffer.split("\n", 1)
                satir = satir.strip()
                if satir:
                    # Mevcut yazi satirini temizleyip mesaji yazdir,
                    # sonra prompt'u (kullanicinin o ana kadar yazdigi
                    # ile birlikte) yeniden ciz.
                    print("\r" + " " * 100 + "\r", end="")
                    mesaji_yazdir(satir)
                    print(f"Sehir: {yazi_buffer}", end="", flush=True)
        except BlockingIOError:
            pass  # su an okunacak veri yok, normal

        # --- 2) Klavyeden karakter geldi mi kontrol et (non-blocking) ---
        if msvcrt.kbhit():
            ch = msvcrt.getwch()

            if ch == "\r":  # Enter'a basildi
                print()  # imleci yeni satira gecir
                sehir = yazi_buffer.strip()
                yazi_buffer = ""

                if sehir.lower() == "q":
                    break
                elif sehir:
                    komut = json.dumps({"topic": "cmd/sehir_sorgu", "payload": sehir})
                    sock.sendall((komut + "\n").encode("utf-8"))
                    print(f"[GONDERILDI] cmd/sehir_sorgu -> {sehir}")

                print("Sehir: ", end="", flush=True)

            elif ch == "\x08":  # Backspace
                if yazi_buffer:
                    yazi_buffer = yazi_buffer[:-1]
                    print("\b \b", end="", flush=True)

            elif ch.isprintable():
                yazi_buffer += ch
                print(ch, end="", flush=True)

        time.sleep(0.02)  # CPU'yu bosuna yakmamak icin kucuk bir bekleme

    sock.close()
    print("Baglanti kapatildi.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nKapatiliyor...")
