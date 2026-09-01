"""
monitor.py

FreeRTOS Network Simulation - Canli Izleme Araci

Bu program, broker'a NORMAL BIR SUBSCRIBER GIBI baglanir, gelen JSON
verilerini okur ve bir Tkinter penceresinde gorsellestirir:
  - Bagli subscriber sayisi (broker'in yayinladigi "system/status" verisi)
  - Sensor verisinin (topic: "sensor/sicaklik") canli grafigi

Bu arac, FreeRTOS/C projesinden TAMAMEN BAGIMSIZDIR - sadece TCP
soket uzerinden broker'i dinleyen ayri bir istemcidir. Bu sayede
FreeRTOS tarafindaki kod hic degismeden, izleme katmani ayri
bir teknolojiyle (Python) gelistirilebilmektedir.

Calistirmadan once:
    pip install matplotlib

Kullanim:
    python monitor.py
"""

import socket
import json
import threading
import time
import traceback
import tkinter as tk
from tkinter import ttk
from collections import deque

# --- Broker baglanti ayarlari (main.c ile AYNI olmali) ---
BROKER_HOST = "127.0.0.1"
BROKER_PORT = 8080
AUTH_TOKEN = "gizli_sifre123"   # main.c'deki SHARED_AUTH_TOKEN ile ayni olmali

# Grafikte gosterilecek maksimum veri noktasi sayisi
MAX_POINTS = 30


class BrokerMonitor:
    def __init__(self, root):
        self.root = root
        self.root.title("FreeRTOS Network Simulation - Monitor")
        self.root.geometry("700x500")

        # --- Ust panel: bagli client sayisi ---
        top_frame = ttk.Frame(root, padding=10)
        top_frame.pack(fill="x")

        ttk.Label(top_frame, text="Bagli Subscriber Sayisi:",
                  font=("Segoe UI", 11)).pack(side="left")
        self.client_count_var = tk.StringVar(value="—")
        ttk.Label(top_frame, textvariable=self.client_count_var,
                  font=("Segoe UI", 14, "bold")).pack(side="left", padx=10)

        self.connection_status_var = tk.StringVar(value="Baglaniyor...")
        ttk.Label(top_frame, textvariable=self.connection_status_var,
                  foreground="gray").pack(side="right")

        # --- Grafik alani (saf Tkinter Canvas ile - ek kutuphane gerekmez) ---
        self.values = deque(maxlen=MAX_POINTS)

        chart_frame = ttk.LabelFrame(root, text="Sensor Verisi (sensor/sicaklik)", padding=5)
        chart_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.canvas = tk.Canvas(chart_frame, bg="white", height=250)
        self.canvas.pack(fill="both", expand=True)

        # --- Log alani (son gelen mesajlar) ---
        log_frame = ttk.LabelFrame(root, text="Son Mesajlar", padding=5)
        log_frame.pack(fill="x", padx=10, pady=(0, 10))
        self.log_text = tk.Text(log_frame, height=6, font=("Consolas", 9))
        self.log_text.pack(fill="x")

        # Baglantiyi ayri bir thread'de baslat - Tkinter'in ana
        # dongusunu (mainloop) bloklamamak icin.
        self.running = True
        self.thread = threading.Thread(target=self._network_loop, daemon=True)
        self.thread.start()

        # Pencere kapatilinca thread'i de durdur
        root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _log(self, mesaj):
        """Log alanina yeni bir satir ekler (thread-safe: Tkinter
        cagrilarini ana thread'e yonlendiriyoruz)."""
        def _append():
            self.log_text.insert("end", mesaj + "\n")
            self.log_text.see("end")
        self.root.after(0, _append)

    def _update_client_count(self, sayi):
        self.root.after(0, lambda: self.client_count_var.set(str(sayi)))

    def _update_chart(self, deger):
        def _apply():
            self.values.append(deger)
            self._draw_chart()
        self.root.after(0, _apply)

    def _draw_chart(self):
        """Canvas uzerine, self.values icindeki degerleri kullanarak
        basit bir cizgi grafik ciziyoruz - matplotlib gibi harici bir
        kutuphaneye ihtiyac duymadan."""
        self.canvas.delete("all")

        if len(self.values) < 2:
            return

        width = self.canvas.winfo_width() or 600
        height = self.canvas.winfo_height() or 250
        padding = 30

        min_val = min(self.values)
        max_val = max(self.values)
        if max_val == min_val:
            max_val += 1  # sifira bolme hatasini onlemek icin

        def to_x(i):
            return padding + i * (width - 2 * padding) / (len(self.values) - 1)

        def to_y(v):
            oran = (v - min_val) / (max_val - min_val)
            return height - padding - oran * (height - 2 * padding)

        # Eksenler
        self.canvas.create_line(padding, height - padding, width - padding, height - padding, fill="gray")
        self.canvas.create_line(padding, padding, padding, height - padding, fill="gray")

        # Min/Max etiketleri
        self.canvas.create_text(padding, padding - 10, text=f"{max_val:.1f}", anchor="w", fill="gray")
        self.canvas.create_text(padding, height - padding + 10, text=f"{min_val:.1f}", anchor="w", fill="gray")

        # Veri cizgisi
        points = []
        for i, v in enumerate(self.values):
            x, y = to_x(i), to_y(v)
            points.append((x, y))

        for i in range(len(points) - 1):
            x1, y1 = points[i]
            x2, y2 = points[i + 1]
            self.canvas.create_line(x1, y1, x2, y2, fill="#2563eb", width=2)

        for x, y in points:
            self.canvas.create_oval(x - 3, y - 3, x + 3, y + 3, fill="#2563eb", outline="")

        # Son deger buyuk yazi ile
        if self.values:
            self.canvas.create_text(
                width - padding, padding,
                text=f"Son: {self.values[-1]:.1f}",
                anchor="e", font=("Segoe UI", 11, "bold"), fill="#1e3a8a"
            )

    def _set_status(self, text, color="gray"):
        def _apply():
            self.connection_status_var.set(text)
        self.root.after(0, _apply)

    def _network_loop(self):
        """Broker'a baglanir, kimligini bildirir, gelen JSON
        mesajlarini surekli okuyup arayuzu gunceller."""
        while self.running:
            sock = None
            try:
                self._set_status("Baglaniyor...")
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                sock.connect((BROKER_HOST, BROKER_PORT))

                # main.c'deki formatla AYNI: "AUTH:token|ROLE:SUBSCRIBER"
                kimlik_mesaji = f"AUTH:{AUTH_TOKEN}|ROLE:SUBSCRIBER"
                sock.sendall(kimlik_mesaji.encode("utf-8"))

                self._set_status("Bagli", "green")
                self._log(f"Broker'a baglanildi ({BROKER_HOST}:{BROKER_PORT})")

                sock.settimeout(1.0)
                buffer = ""

                while self.running:
                    try:
                        data = sock.recv(1024)
                    except socket.timeout:
                        continue

                    if not data:
                        raise ConnectionError("Broker baglantiyi kapatti")

                    buffer += data.decode("utf-8", errors="ignore")

                    # main.c ile ayni framing mantigi: '\n' ile ayristir
                    while "\n" in buffer:
                        satir, buffer = buffer.split("\n", 1)
                        satir = satir.strip()
                        if not satir:
                            continue
                        self._process_message(satir)

            except Exception as e:
                # DEBUG: tam hata izini (traceback) terminale yazdiriyoruz -
                # boylece hatanin GERCEK sebebini gorebiliriz.
                hata_detayi = traceback.format_exc()
                self._set_status(f"Baglanti hatasi: {e}", "red")
                self._log(f"HATA: {e} - 3 saniye sonra tekrar denenecek")
                print("----- HATA DETAYI -----")
                print(hata_detayi)
                print("------------------------")
            finally:
                if sock is not None:
                    try:
                        sock.close()
                    except Exception:
                        pass

            if self.running:
                time.sleep(3)

    def _process_message(self, satir):
        """Gelen tek bir JSON mesajini ayristirip arayuzu gunceller."""
        try:
            veri = json.loads(satir)
        except json.JSONDecodeError:
            self._log(f"UYARI: Gecersiz JSON: {satir}")
            return

        topic = veri.get("topic")
        payload = veri.get("payload")

        if topic is None or payload is None:
            self._log(f"UYARI: Eksik alan iceren mesaj: {satir}")
            return

        self._log(f"[{topic}] {payload}")

        if topic == "system/status":
            self._update_client_count(payload)
        elif topic == "sensor/sicaklik":
            try:
                self._update_chart(float(payload))
            except ValueError:
                pass

    def _on_close(self):
        self.running = False
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = BrokerMonitor(root)
    root.mainloop()
