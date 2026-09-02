"""
monitor.py

FreeRTOS Network Simulation - Kontrol Paneli ve Canli Izleme Araci

Bu program iki sekmeden olusur:

1) KONTROL PANELI: Broker, Publisher ve Subscriber process'lerini
   (freertos_demo.exe <rol>) BU PENCEREDEN baslatip durdurabilir,
   her birinin kendi konsol ciktisini AYRI bir panelde canli olarak
   gorebilirsin - 3 ayri terminal acmana gerek kalmaz.

2) CANLI IZLEME: Bu arac, broker'a NORMAL BIR SUBSCRIBER GIBI baglanir,
   gelen JSON verilerini okuyup bagli subscriber sayisini ve sensor
   verisinin canli grafigini gosterir.

Bu arac, FreeRTOS/C projesinden TAMAMEN BAGIMSIZDIR - sadece TCP
soket (izleme icin) ve subprocess (baslatma icin) kullanan ayri bir
istemcidir. FreeRTOS tarafindaki kod hic degismeden, bu katman
tamamen farkli bir teknolojiyle (Python) gelistirilmistir.

Kullanim:
    python monitor.py

ONEMLI: Asagidaki EXE_PATH degiskenini, kendi bilgisayarindaki
freertos_demo.exe dosyasinin TAM YOLUNA gore guncelle.
"""

import os
import socket
import json
import subprocess
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

# --- freertos_demo.exe dosyasinin TAM YOLU ---
# KENDI BILGISAYARINA GORE BU SATIRI GUNCELLE!
EXE_PATH = r"C:\Users\muhammed\Downloads\FreeRTOS-main\FreeRTOS-main\FreeRTOS\Demo\WIN32-MSVC\build\freertos_demo.exe"

# Grafikte gosterilecek maksimum veri noktasi sayisi
MAX_POINTS = 30


class ProcessPanel:
    """
    Tek bir rolu (broker/publisher/subscriber) temsil eden kontrol
    paneli. Baslat/Durdur butonu, durum gostergesi (renkli nokta) ve
    o process'e ait CANLI konsol ciktisini gosteren bir log alani
    icerir.

    Her panel, kendi freertos_demo.exe <rol> process'ini subprocess
    olarak baslatir ve stdout'unu AYRI BIR THREAD'DE okuyarak
    Tkinter arayuzune aktarir - boylece process'in ciktisini okumak
    GUI'yi (Tkinter'in ana dongusunu) hic bloklamaz.
    """

    RENK = {
        "broker": "#2563eb",
        "publisher": "#16a34a",
        "subscriber": "#9333ea",
    }

    def __init__(self, parent, title, role_arg, row, col):
        self.role_arg = role_arg
        self.process = None
        self.running = False

        renk = self.RENK.get(role_arg, "#444")

        frame = ttk.LabelFrame(parent, text=title, padding=8)
        frame.grid(row=row, column=col, sticky="nsew", padx=6, pady=6)
        parent.grid_columnconfigure(col, weight=1)
        parent.grid_rowconfigure(row, weight=1)

        control_frame = ttk.Frame(frame)
        control_frame.pack(fill="x")

        self.status_canvas = tk.Canvas(control_frame, width=14, height=14, highlightthickness=0)
        self.status_dot = self.status_canvas.create_oval(2, 2, 12, 12, fill="gray")
        self.status_canvas.pack(side="left", padx=(0, 6))

        self.status_label_var = tk.StringVar(value="Durduruldu")
        ttk.Label(control_frame, textvariable=self.status_label_var,
                  foreground="gray").pack(side="left")

        self.toggle_btn = ttk.Button(control_frame, text="Baslat", command=self.toggle)
        self.toggle_btn.pack(side="right")

        self.log_text = tk.Text(frame, height=14, width=42, font=("Consolas", 8),
                                 bg="#0b0f19", fg=renk, insertbackground=renk)
        self.log_text.pack(fill="both", expand=True, pady=(8, 0))

        scrollbar = ttk.Scrollbar(frame, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)

    # -------------------------------------------------------------
    def toggle(self):
        if self.running:
            self.stop()
        else:
            self.start()

    def start(self):
        if not os.path.exists(EXE_PATH):
            self._append_log(
                f"HATA: '{EXE_PATH}' bulunamadi.\n"
                f"monitor.py dosyasinin ustundeki EXE_PATH degiskenini "
                f"kendi bilgisayarina gore guncelle.\n"
            )
            return

        try:
            # CREATE_NO_WINDOW: Windows'ta ayri bir konsol penceresi
            # ACILMASINI ENGELLER - ciktiyi biz kendi panelimizde
            # gosterecegiz, ayri bir siyah pencereye gerek yok.
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

            self.process = subprocess.Popen(
                [EXE_PATH, self.role_arg],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                creationflags=creationflags,
            )
        except Exception as e:
            self._append_log(f"HATA: Baslatilamadi: {e}\n")
            return

        self.running = True
        self.log_text.delete("1.0", "end")
        self.status_canvas.itemconfig(self.status_dot, fill="green")
        self.status_label_var.set("Calisiyor")
        self.toggle_btn.config(text="Durdur")

        threading.Thread(target=self._read_output, daemon=True).start()

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            try:
                self.process.terminate()
            except Exception:
                pass
        self.running = False
        self._set_stopped_ui()

    def _read_output(self):
        """Process'in stdout'unu SATIR SATIR okuyup panele yansitir.
        Process kendiliginden kapanirsa (ornegin hata verirse), bunu
        da tespit edip arayuzu gunceller."""
        try:
            for line in self.process.stdout:
                self._append_log(line)
        except Exception:
            pass

        self.running = False
        self._set_stopped_ui()
        self._append_log("\n--- process sonlandi ---\n")

    def _append_log(self, text):
        def _apply():
            self.log_text.insert("end", text)
            self.log_text.see("end")
        try:
            self.log_text.after(0, _apply)
        except Exception:
            pass

    def _set_stopped_ui(self):
        def _apply():
            self.status_canvas.itemconfig(self.status_dot, fill="gray")
            self.status_label_var.set("Durduruldu")
            self.toggle_btn.config(text="Baslat")
        try:
            self.log_text.after(0, _apply)
        except Exception:
            pass


class BrokerMonitor:
    def __init__(self, root):
        self.root = root
        self.root.title("FreeRTOS Network Simulation - Kontrol Paneli")
        self.root.geometry("980x680")

        notebook = ttk.Notebook(root)
        notebook.pack(fill="both", expand=True)

        # --- SEKME 1: Kontrol Paneli ---
        control_tab = ttk.Frame(notebook)
        notebook.add(control_tab, text="Kontrol Paneli")

        self.panels = [
            ProcessPanel(control_tab, "BROKER", "broker", row=0, col=0),
            ProcessPanel(control_tab, "PUBLISHER", "publisher", row=0, col=1),
            ProcessPanel(control_tab, "SUBSCRIBER", "subscriber", row=0, col=2),
        ]

        # --- SEKME 2: Canli Izleme (broker'a subscriber gibi baglanan izleyici) ---
        monitor_tab = ttk.Frame(notebook)
        notebook.add(monitor_tab, text="Canli Izleme")
        self._build_monitor_tab(monitor_tab)

        self.running = True
        self.thread = threading.Thread(target=self._network_loop, daemon=True)
        self.thread.start()

        root.protocol("WM_DELETE_WINDOW", self._on_close)

    # -------------------------------------------------------------
    def _build_monitor_tab(self, root):
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

        self.values = deque(maxlen=MAX_POINTS)

        chart_frame = ttk.LabelFrame(root, text="Sensor Verisi (sensor/sicaklik)", padding=5)
        chart_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.canvas = tk.Canvas(chart_frame, bg="white", height=250)
        self.canvas.pack(fill="both", expand=True)

        log_frame = ttk.LabelFrame(root, text="Son Mesajlar", padding=5)
        log_frame.pack(fill="x", padx=10, pady=(0, 10))
        self.log_text = tk.Text(log_frame, height=6, font=("Consolas", 9))
        self.log_text.pack(fill="x")

    def _log(self, mesaj):
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
        self.canvas.delete("all")

        if len(self.values) < 2:
            return

        width = self.canvas.winfo_width() or 600
        height = self.canvas.winfo_height() or 250
        padding = 30

        min_val = min(self.values)
        max_val = max(self.values)
        if max_val == min_val:
            max_val += 1

        def to_x(i):
            return padding + i * (width - 2 * padding) / (len(self.values) - 1)

        def to_y(v):
            oran = (v - min_val) / (max_val - min_val)
            return height - padding - oran * (height - 2 * padding)

        self.canvas.create_line(padding, height - padding, width - padding, height - padding, fill="gray")
        self.canvas.create_line(padding, padding, padding, height - padding, fill="gray")

        self.canvas.create_text(padding, padding - 10, text=f"{max_val:.1f}", anchor="w", fill="gray")
        self.canvas.create_text(padding, height - padding + 10, text=f"{min_val:.1f}", anchor="w", fill="gray")

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
        while self.running:
            sock = None
            try:
                self._set_status("Baglaniyor...")
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                sock.connect((BROKER_HOST, BROKER_PORT))

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

                    while "\n" in buffer:
                        satir, buffer = buffer.split("\n", 1)
                        satir = satir.strip()
                        if not satir:
                            continue
                        self._process_message(satir)

            except Exception as e:
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
        # Pencere kapatilirken, kontrol panelinden baslatilmis TUM
        # process'leri de duzgunce durduruyoruz - arkada "yetim"
        # (orphan) process kalmasin diye.
        for panel in self.panels:
            panel.stop()
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = BrokerMonitor(root)
    root.mainloop()