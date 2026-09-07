# FreeRTOS Multi-Role Network Simulation

Donanımdan bağımsız, çok görevli (multi-task) bir ağ simülasyonu projesi. FreeRTOS Windows Simulator üzerinde çalışan tek bir kod tabanı, komut satırı argümanına göre **Broker**, **Publisher** veya **Subscriber** rolünde ayağa kalkarak, MQTT'nin publish/subscribe mimarisini TCP/IP soket programlama ile simüle eder.

## Proje Amacı

Bu proje, gömülü sistemlerdeki gerçek zamanlı işletim sistemi (RTOS) mantığını, görev önceliklendirmesini ve ağ haberleşmesini, fiziksel donanıma ihtiyaç duymadan bilgisayar üzerinde (localhost simülasyonu) test edilebilir hale getirmeyi hedefler. Mimari, ileride gerçek bir mikrodenetleyiciye (ESP32) taşınabilecek şekilde tasarlanmıştır.

## Mimari

```
                    ┌─────────────┐
                    │   BROKER    │
                    │ (port: CLI) │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │                         │
      ┌───────▼───────┐        ┌────────▼────────┐
      │   PUBLISHER    │        │   SUBSCRIBER     │
      └────────────────┘        └──────────────────┘
```

| Rol | Görev |
|---|---|
| `broker` | Bağlantıları kabul eder, publisher'dan gelen veriyi subscriber'lara yönlendirir, sistem durumu/sağlığını yayınlar |
| `publisher` | Broker'a bağlanır, gerçek bir veri setinden periyodik olarak veri yayınlar |
| `subscriber` | Broker'a bağlanır, yayınlanan veriyi dinler |

### FreeRTOS Task Yapısı ve Öncelikleri

| Task | Öncelik | Her Rolde mi? |
|---|---|---|
| Health | En yüksek (5) | Evet — sistem sağlığını izler, `system/health` yayınlar |
| Internal Comm | Yüksek (4) | Evet — Network Task'tan Queue ile veri alır |
| Network | Orta (3) | Evet — TCP soket işlemleri |
| Client Handler | Orta (3), dinamik | Sadece Broker, her bağlanan client için ayrı task |
| MQTT Publisher / Subscriber | Düşük (2) | Sadece ilgili rolde |

Senkronizasyon: **Queue** (task'lar arası veri aktarımı), **Mutex** (paylaşılan subscriber listesi koruması, priority inheritance ile test edilmiştir), **Software Timer** (periyodik durum yayını, ayrı task/stack gerektirmez).

## Gereksinimler

- Windows işletim sistemi
- [MSYS2](https://www.msys2.org/) (UCRT64 ortamı) — GCC derleyici, Make
- [CMake](https://cmake.org/) (3.15+)
- [FreeRTOS Kernel](https://github.com/FreeRTOS/FreeRTOS-Kernel) kaynak kodu
- Python 3.x + Tkinter (opsiyonel kontrol paneli için — Tkinter Python ile birlikte gelir, ek kurulum gerekmez)

## Kurulum ve Derleme

```bash
git clone https://github.com/<kullanici-adiniz>/<repo-adi>.git
cd <repo-adi>
git clone https://github.com/FreeRTOS/FreeRTOS-Kernel.git ../FreeRTOS-Kernel-Source
mkdir build && cd build
cmake .. -G "MinGW Makefiles"
mingw32-make
```

**Önemli:** `ankara_sicaklik_verileri.csv` dosyasının, `.exe`'nin çalıştığı `build` klasöründe de bulunması gerekir (publisher gerçek veri setini bu dosyadan okur).

## Kullanım

```
freertos_demo.exe <rol> [port] [broker_ip]
```

`port` ve `broker_ip` opsiyoneldir; belirtilmezse sırasıyla `8080` ve `127.0.0.1` kullanılır. Bu, aynı `.exe`'nin **farklı portlarda birden fazla instance** olarak çalıştırılabilmesini sağlar.

**Terminal 1 — Broker:**
```bash
./freertos_demo.exe broker 8080
```

**Terminal 2 — Subscriber:**
```bash
./freertos_demo.exe subscriber 8080
```

**Terminal 3 — Publisher:**
```bash
./freertos_demo.exe publisher 8080
```

### Python Kontrol Paneli (Önerilir)

```bash
python monitor.py
```

Broker/Publisher/Subscriber süreçlerini **tek bir pencereden** başlatıp durdurmayı, her sürecin canlı logunu ayrı panellerde izlemeyi ve bağımsız bir "Canlı İzleme" sekmesinde sensör verisi grafiğini gerçek zamanlı görmeyi sağlar. `monitor.py`'nin en üstündeki `EXE_PATH` değişkenini kendi `freertos_demo.exe` konumunuza göre güncelleyin.

## Mesaj Formatı

Sistem, veri taşımak için JSON formatını kullanır. Her mesajın sonuna, mesaj sınırlarını belirlemek amacıyla bir satır sonu karakteri (`\n`) eklenir — authentication mesajı da dahil olmak üzere **tüm** haberleşme bu çerçeveleme (framing) mekanizmasından geçer.

**Sensör verisi (`sensor/sicaklik`):**
```json
{"topic":"sensor/sicaklik","payload":"13.0","mesaj_no":0,"sehir":"Istanbul","tarih":"2024-11-15","durum":"Orta kuvvetli yagmurlu"}
```

**Sistem durumu (`system/status`, 2 saniyede bir):**
```json
{"topic":"system/status","payload":"1"}
```

**Sistem sağlığı (`system/health`, 5 saniyede bir):**
```json
{"topic":"system/health","payload":{"heap":485040,"min_heap":484592,"doluluk":4,"task_sayisi":6,"ts":1204982}}
```

`payload`, diğer topic'lerin aksine (`sensor/sicaklik`, `system/status`) düz string değil, **gerçek nested bir JSON nesnesidir** — heap, min_heap, doluluk, task_sayisi ve zaman damgası (`ts`) ayrı ayrı, doğru tipleriyle taşınır.

Broker ve subscriber, gelen JSON verisini `cJSON_Parse()` ile ayrıştırıp `topic`/`payload` alanlarının varlığını ve tipini doğrular; geçersiz veya eksik veriler işlenmeden reddedilir.

## MQTT'den Bağımsız İletişim Kanalı

Sistemin tek bir protokole bağımlı olmadığını göstermek amacıyla, **her rolde** çalışan ayrı bir görev (`vUdpCommandTask`), TCP/JSON/authentication altyapısından tamamen bağımsız olarak, **UDP** üzerinden **düz metin komutlar** kabul eder. Ana TCP portunun `+1000`'i üzerinde dinler (örn. broker `8080` çalışıyorsa, UDP kanalı `9080`'de).

| Komut | Cevap | Açıklama |
|---|---|---|
| `PING` | `PONG` | Canlılık kontrolü |
| `HEAP` | `HEAP:<byte>` | Anlık boş heap miktarı |
| `STATUS` | `ROLE:<n>,TASKS:<n>` | Rol ve görev sayısı |

**Test (PowerShell):**
```powershell
$udp = New-Object System.Net.Sockets.UdpClient
$udp.Connect("127.0.0.1", 9080)
$bytes = [System.Text.Encoding]::ASCII.GetBytes("PING")
$udp.Send($bytes, $bytes.Length)
$remoteEP = New-Object System.Net.IPEndPoint([System.Net.IPAddress]::Any, 0)
$response = $udp.Receive([ref]$remoteEP)
[System.Text.Encoding]::ASCII.GetString($response)
$udp.Close()
```

## Idle-Timeout ile Otomatik Kapanma

Broker, belirlenen bir süre boyunca (varsayılan 30 saniye, `IDLE_TIMEOUT_MS` ile ayarlanabilir) hiç yeni bağlantı kabul etmezse, kendini düzgün bir şekilde kapatır. Bu özellik geliştirilirken, FreeRTOS'un `vTaskEndScheduler()` fonksiyonunun Windows Simulator portunda **tam olarak çalışmadığı** (bazı görevlerin, port her FreeRTOS task'ını gerçek bir Windows thread'i olarak çalıştırdığı için, scheduler durdurulsa dahi çalışmaya devam ettiği) tespit edilmiş; bunun yerine `exit()` ile doğrudan process sonlandırma kullanılmıştır.

## Gerçek Veri Seti

Publisher, rastgele (`rand()`) sahte veri üretmek yerine, **gerçek, kaynağı belirtilmiş** bir hava durumu veri setinden (`ankara_sicaklik_verileri.csv`, **1022 kayıt, 68 şehir**) sırayla okuma yapar. Her yayınlanan mesaj, gerçek bir şehir, tarih, sıcaklık ve hava durumu açıklaması taşır — bu sayede sistem, Türkiye çapında dağıtık bir sensör ağını simüle eder.

**Kaynak:** Kaggle — "Türkiye Hava Durumu Verisi (Kasım 2024)" (68 il, 15-28 Kasım 2024 arası günlük gözlemler).

**Notlar:**
- Hava durumu açıklamalarındaki Türkçe karakterler (ş, ç, ğ, ı, ü, ö), C'de sabit boyutlu buffer'larda UTF-8 çok baytlı karakterlerin bozulma riskini önlemek amacıyla ASCII'ye çevrilmiştir (örn. `"Bölgesel"` → `"Bolgesel"`).
- Veri seti döngüsel olarak okunur; 1022 kaydın tamamını bir kez dolaşmak (3 saniyelik yayın periyodunda) yaklaşık 51 dakika sürer.
- Dosya bulunamazsa, sistem otomatik olarak eski (rastgele) veri üretim yöntemine döner — bu sayede program veri seti olmadan da çalışmaya devam eder.
- Python Kontrol Paneli üzerinden başlatılan süreçlerin de bu dosyayı doğru bulabilmesi için, `subprocess.Popen` çağrısında çalışma dizini (`cwd`) `.exe` dosyasının bulunduğu klasöre sabitlenmiştir.

## Güvenlik

- **Authentication:** Token bazlı bağlantı doğrulama (`AUTH:token|ROLE:rol` formatı, framing mekanizmasının bir parçası)
- **Authorization:** Subscriber rolündeki bir client'ın veri göndermesi (publish etmesi) engellenir, "yetki ihlali" olarak loglanır

## Tamamlanan Özellikler

- [x] Rol bazlı (broker/publisher/subscriber) modüler task mimarisi
- [x] Non-blocking TCP soket haberleşmesi, çoklu client desteği (her biri ayrı task)
- [x] TCP framing çözümü (mesaj sınırlarının `\n` ile belirlenmesi, authentication dahil)
- [x] Authentication ve Authorization
- [x] cJSON ile tam JSON mesajlaşma, format/schema doğrulaması
- [x] Queue mimarisi (hem publish hem subscribe yönünde)
- [x] Mutex ile korunan paylaşılan veri, **priority inheritance kanıtlanmıştır** (mutex vs semaphore karşılaştırmalı test — mutex ile gecikme sınırlı kalırken, semaphore ile sınırsız beklemeye/kilitlenmeye yol açmaktadır)
- [x] Software Timer entegrasyonu
- [x] Gelişmiş sistem sağlık yönetimi (heap analizi, stack high water mark, görev durumu izleme), `system/health` ile ağa yayınlanır
- [x] Çoklu instance desteği (port/broker IP komut satırı argümanı)
- [x] Instance'lar arası health veri gecikmesi ölçümü (~100-125 ms, aynı makinede)
- [x] Python/Tkinter görsel kontrol paneli (süreç yönetimi + canlı izleme)
- [x] Gerçek, kaynaklı bir veri setinden (1022 kayıt, 68 şehir) sensör verisi üretimi
- [x] MQTT/TCP/JSON altyapısından bağımsız UDP komut kanalı (PING/HEAP/STATUS)
- [x] Broker'ın idle-timeout ile kendini düzgün şekilde kapatabilmesi

## Bilinen Kısıtlamalar

FreeRTOS Windows Simulator portu, gerçekçi tek-çekirdekli zamanlama sağlamak amacıyla her görev iş parçacığını `SetThreadAffinityMask()` ile CPU'nun 0. çekirdeğine sabitlemekte ve işlem önceliğini `REALTIME_PRIORITY_CLASS` olarak ayarlamaktadır. Bu, **aynı fiziksel makinede birden fazla instance'ın eş zamanlı çalıştırılmasını** sınırlamaktadır (instance'lar aynı çekirdek için rekabet eder). Bu davranış, gerçek donanımda (her cihazın kendi bağımsız işlemcisine sahip olduğu bir senaryoda, örn. ESP32) yaşanmayacaktır; çoklu instance testleri farklı fiziksel makinelerde/VM'lerde sorunsuz çalışır.

Ayrıca, portun her FreeRTOS task'ını **gerçek bir Windows thread'i** olarak çalıştırması nedeniyle, `vTaskEndScheduler()` çağrısı sistemi tam olarak durduramamaktadır (Timer Service task'ı silinse de, diğer görevler kendi Windows thread'lerinde çalışmaya devam edebilir). Bu nedenle idle-timeout özelliği, `exit()` ile doğrudan process sonlandırma kullanmaktadır.

## Yol Haritası

- [ ] ESP32'ye taşınabilirlik (network katmanının soyutlanması)
- [ ] Gerçek zamanlı bir hava durumu API'sinden canlı veri çekme

## Proje Yapısı

```
.
├── main.c                          # Ana uygulama kodu
├── CMakeLists.txt                   # Derleme yapılandırması
├── FreeRTOSConfig.h                 # FreeRTOS kernel yapılandırma ayarları
├── monitor.py                       # Python/Tkinter kontrol paneli
├── ankara_sicaklik_verileri.csv     # Gerçek sensör veri seti
├── cJSON/
│   ├── cJSON.c
│   └── cJSON.h
└── README.md
```

## Notlar

Bu proje, bir staj programı kapsamında, gömülü sistemlerdeki RTOS ve ağ haberleşmesi kavramlarını (task senkronizasyonu, priority inversion, sistem sağlık yönetimi, ağ protokolü tasarımı) uygulamalı olarak öğrenmek amacıyla geliştirilmektedir.