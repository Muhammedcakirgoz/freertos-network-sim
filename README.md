# FreeRTOS Multi-Role Network Simulation

Donanımdan bağımsız, çok görevli (multi-task) bir ağ simülasyonu projesi. FreeRTOS Windows Simulator üzerinde çalışan tek bir kod tabanı, komut satırı argümanına göre **Broker**, **Publisher** veya **Subscriber** rolünde ayağa kalkarak, MQTT'nin publish/subscribe mimarisini TCP/IP soket programlama ile simüle eder.

## Proje Amacı

Bu proje, gömülü sistemlerdeki gerçek zamanlı işletim sistemi (RTOS) mantığını, görev önceliklendirmesini ve ağ haberleşmesini, fiziksel donanıma ihtiyaç duymadan bilgisayar üzerinde (localhost simülasyonu) test edilebilir hale getirmeyi hedefler. Mimari, ileride gerçek bir mikrodenetleyiciye (ESP32) taşınabilecek şekilde tasarlanmıştır — network katmanı soyutlanarak, iş mantığının donanım değişiminden etkilenmemesi sağlanmıştır.

## Mimari

```
                    ┌─────────────┐
                    │   BROKER    │
                    │  (port 8080)│
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │                         │
      ┌───────▼───────┐        ┌────────▼────────┐
      │   PUBLISHER    │        │   SUBSCRIBER     │
      └────────────────┘        └──────────────────┘
```

Aynı çalıştırılabilir dosya (`.exe`), komut satırı argümanına göre üç farklı rolde çalışabilir:

| Rol | Görev |
|---|---|
| `broker` | Bağlantıları kabul eder, publisher'dan gelen veriyi subscriber'lara yönlendirir |
| `publisher` | Broker'a bağlanır, periyodik olarak veri yayınlar |
| `subscriber` | Broker'a bağlanır, yayınlanan veriyi dinler |

### FreeRTOS Task Yapısı

| Task | Öncelik | Her Rolde mi? |
|---|---|---|
| Health | En yüksek | Evet |
| Internal Comm | Yüksek | Evet |
| Network | Orta | Evet |
| Client Handler | Orta (sadece Broker) | Sadece Broker, her bağlanan client için dinamik olarak oluşturulur |
| MQTT Publisher | Düşük | Sadece Publisher |
| MQTT Subscriber | Düşük | Sadece Subscriber |

## Gereksinimler

- Windows işletim sistemi
- [MSYS2](https://www.msys2.org/) (UCRT64 ortamı) — GCC derleyici, Make
- [CMake](https://cmake.org/) (3.15+)
- [FreeRTOS Kernel](https://github.com/FreeRTOS/FreeRTOS-Kernel) kaynak kodu (aşağıda kurulum talimatı var)

## Kurulum

### 1. Bu Depoyu Klonlayın

```bash
git clone https://github.com/<kullanici-adiniz>/<repo-adi>.git
cd <repo-adi>
```

### 2. FreeRTOS Kernel Kaynak Kodunu İndirin

Bu proje, FreeRTOS kernel kaynak kodunu içermez (üçüncü parti kütüphane olduğu için repoya dahil edilmemiştir). Aşağıdaki adımlarla indirin:

```bash
git clone https://github.com/FreeRTOS/FreeRTOS-Kernel.git ../FreeRTOS-Kernel-Source
```

Ardından `CMakeLists.txt` içindeki `KERNEL_DIR` değişkenini, indirdiğiniz klasörün yolunu gösterecek şekilde güncelleyin (gerekirse).

### 3. Derleyin

```bash
mkdir build
cd build
cmake .. -G "MinGW Makefiles"
make
```

## Kullanım

Derleme sonrası oluşan `freertos_demo.exe` dosyasını, farklı terminallerde farklı rollerle çalıştırın:

**Terminal 1 — Broker'ı başlatın (önce bu çalışmalı):**
```bash
./freertos_demo.exe broker
```

**Terminal 2 — Subscriber'ı başlatın:**
```bash
./freertos_demo.exe subscriber
```

**Terminal 3 — Publisher'ı başlatın:**
```bash
./freertos_demo.exe publisher
```

Publisher'ın gönderdiği verinin, broker üzerinden subscriber'a ulaştığını, subscriber terminalinde göreceksiniz.

## Şu Ana Kadar Tamamlananlar

- [x] Rol bazlı (broker/publisher/subscriber) modüler task mimarisi
- [x] Non-blocking TCP soket haberleşmesi (accept/connect)
- [x] Her bağlanan client için dinamik FreeRTOS task oluşturma
- [x] Mutex ile korunan subscriber listesi
- [x] Publisher → Broker → Subscriber uçtan uca veri akışı
- [x] Authentication: token bazlı bağlantı doğrulama
- [x] Authorization: rol bazlı yetki kontrolü (subscriber'ın publish edememesi)
- [x] TCP framing çözümü (mesaj sınırlarının `\n` ayracı ile belirlenmesi)
- [x] Temel veri doğrulama (recv hata kodu ayrımı, boş/aşırı uzun mesaj reddi)
- [x] JSON mesaj formatına geçiş (cJSON kütüphanesi ile)
- [x] JSON format/schema doğrulaması (parse hatası, eksik/yanlış tipte alan kontrolü)

## Yol Haritası (Devam Eden Çalışma)

- [ ] Internal Comm task'ı ile Network task'ı arasında Queue kullanımı
- [ ] Priority Inversion testleri (mutex + öncelik önceliklendirmesi senaryoları)
- [ ] ESP32 platformuna taşınabilirlik (network katmanının soyutlanması)
- [ ] cJSON parse/doğrulama mantığının ortak bir yardımcı fonksiyona çıkarılması (kod tekrarını azaltmak için)

## Proje Yapısı

```
.
├── main.c              # Ana uygulama kodu (task tanımları, network mantığı)
├── CMakeLists.txt       # Derleme yapılandırması
├── FreeRTOSConfig.h     # FreeRTOS kernel yapılandırma ayarları
├── cJSON/
│   ├── cJSON.c          # JSON kütüphanesi (üçüncü parti)
│   └── cJSON.h
└── README.md
```

## Notlar

Bu proje, bir staj programı kapsamında, gömülü sistemlerdeki RTOS ve ağ haberleşmesi kavramlarını öğrenmek amacıyla geliştirilmektedir.

## Mesaj Formatı

Sistem, veri taşımak için JSON formatını kullanır:

```json
{"topic":"sensor/sicaklik","payload":"24.1","mesaj_no":0}
```

Mesajlar TCP üzerinden gönderilirken, mesaj sınırlarını belirlemek amacıyla her mesajın sonuna bir satır sonu karakteri (`\n`) eklenir. Broker ve subscriber, gelen JSON verisini `cJSON_Parse()` ile ayrıştırıp gerekli alanların (`topic`, `payload`) varlığını ve tipini doğrular; geçersiz veya eksik veriler işlenmeden reddedilir.