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
- **WinHTTP** (Windows'la birlikte gelir, ek kurulum gerekmez) — Open-Meteo API entegrasyonu için
- **İnternet bağlantısı** — NTP senkronizasyonu ve Open-Meteo API sorguları için gereklidir (bağlantı yoksa sistem, sırasıyla eski rastgele veri üretimine ve varsayılan davranışlara döner, çökmez)

## Kurulum ve Derleme

```bash
git clone https://github.com/<kullanici-adiniz>/<repo-adi>.git
cd <repo-adi>
git clone https://github.com/FreeRTOS/FreeRTOS-Kernel.git ../FreeRTOS-Kernel-Source
mkdir build && cd build
cmake .. -G "MinGW Makefiles"
mingw32-make
```

**Önemli:** Aşağıdaki dosyaların, `.exe`'nin çalıştığı `build` klasöründe de bulunması gerekir:
- `ankara_sicaklik_verileri.csv` — publisher'ın okuduğu gerçek sensör veri seti
- `sehir_config.json` — anlık hava durumu sorgusu için varsayılan şehir

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
{"topic":"sensor/sicaklik","payload":"13.0","mesaj_no":0,"sehir":"Istanbul","tarih":"2024-11-15","durum":"Orta kuvvetli yagmurlu","zaman":1788853962}
```

`zaman`, NTP ile senkronize edilmiş gerçek Unix zaman damgasıdır (aşağıdaki "NTP ile Gerçek Zaman Senkronizasyonu" bölümüne bakın) — mesajın **gerçekte hangi anda üretildiğini** gösterir.

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

Sistemin tek bir protokole bağımlı olmadığını göstermek amacıyla, **her rolde** çalışan ayrı bir görev (`vUdpCommandTask`), TCP/JSON/authentication altyapısından tamamen bağımsız olarak, **UDP** üzerinden **düz metin komutlar** kabul eder. Her rol, kendi **benzersiz** UDP portunda dinler — ana TCP portunun `+1000`'i, artı role göre bir ofset (`rol_numarası × 10`) — böylece aynı makinede broker/publisher/subscriber aynı anda çalışırken portlar çakışmaz (örn. broker `8080` çalışıyorsa, broker'ın UDP kanalı `9080`'de, publisher'ınki `9090`'da olur).

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

## NTP ile Gerçek Zaman Senkronizasyonu

Sistem, her rolde çalışan bağımsız bir görev (`vNtpSyncTask`) aracılığıyla, gerçek dünya zamanını `pool.ntp.org` NTP sunucusundan alıp yerel olarak işletir. Bu, FreeRTOS'un kendi tick sayacının (`xTaskGetTickCount()`), her makinede farklı bir referans noktasından (o makinenin açılış anından) başlaması nedeniyle, tek başına **gerçek zaman anlamına gelmemesi** sorununu çözer — daha önce cross-instance health gecikme ölçümünde bu kısıtlama (`GetTickCount64()`'ün yalnızca aynı fiziksel makinede anlamlı olması) not edilmişti.

**Çalışma prensibi:**
1. **DNS çözümleme:** `getaddrinfo()` ile `pool.ntp.org` domaini IP adresine çevrilir.
2. **UDP ile NTP sorgusu:** Standart 48 byte'lık NTP paket formatı (RFC 5905) kullanılarak sunucuya istek gönderilir, cevaptaki `txTm_s` (transmit timestamp) alanı okunur.
3. **1900 → 1970 dönüşümü:** NTP zamanı 1900'den itibaren sayıldığı için, Unix zaman damgasına (1970 referanslı) çevrilirken sabit bir fark (2.208.988.800 saniye) çıkarılır.
4. **Yerel "real-time clock":** Alınan zaman ile o anki tick sayacı arasındaki fark (ofset) saklanır; bu sayede her yeni zaman ihtiyacında ağa tekrar gidilmeden, tick sayacından anlık gerçek zaman hesaplanabilir.
5. **Periyodik yeniden senkronizasyon:** Olası saat kaymasını (drift) düzeltmek amacıyla, senkronizasyon her 5 dakikada bir (`NTP_SYNC_INTERVAL_MS`) otomatik olarak tekrarlanır.

Elde edilen Unix zaman damgası, publisher'ın yayınladığı her JSON mesajına (`sensor/sicaklik`, `system/status`, `system/health`) `zaman` alanı olarak eklenir — bu sayede her mesajın **gerçekte hangi anda üretildiği**, dış dünyayla (diğer sistemler, log analiz araçları vb.) tutarlı bir referansla izlenebilir.

## Open-Meteo API Entegrasyonu — Anlık Şehir Sıcaklığı

Kaggle veri setinin sağladığı tarihsel/kaydedilmiş verilere ek olarak, sistem **gerçek zamanlı, anlık** hava durumu verisi için [Open-Meteo](https://open-meteo.com/) API'sine (ücretsiz, API anahtarı gerektirmeyen bir hava durumu servisi) bağlanabilmektedir. Bu, NTP entegrasyonuna benzer şekilde, dış bir "serverless" servise HTTPS üzerinden bağlanma pratiğidir.

### Çalışma Prensibi — İki Aşamalı Sorgu

1. **Geocoding (şehir adı → koordinat):** `geocoding-api.open-meteo.com` adresine, şehir ismiyle bir istek atılır, cevaptan enlem/boylam alınır.
2. **Forecast (koordinat → anlık sıcaklık):** `api.open-meteo.com` adresine, elde edilen koordinatlarla bir istek atılır, cevaptaki `current_weather.temperature` alanı okunur.

İkisi de **WinHTTP** (Windows'un yerleşik HTTPS istemci kütüphanesi) üzerinden, TLS şifrelemesi dahil tüm detaylar kütüphane tarafından yönetilerek gerçekleştirilir. ESP32'ye taşınırken, karşılığı `esp_http_client` kütüphanesi olacaktır.

### Şehir Seçimi — İki Bağımsız Mekanizma

**1) Başlangıç config dosyası (`sehir_config.json`):**
```json
{
    "sehir": "Ankara"
}
```
Program başlarken bu dosyadan okunan şehir, **varsayılan** olarak kullanılır; periyodik yayın (60 saniyede bir, `ANLIK_HAVA_SORGU_ARALIGI_MS`) bu şehir için çalışır.

**2) Runtime komutu (`cmd/sehir_sorgu`):** Herhangi bir subscriber, çalışma zamanında şu formatta bir mesaj göndererek şehri **anında** değiştirebilir/sorgulayabilir:
```json
{"topic":"cmd/sehir_sorgu","payload":"Cankaya"}
```
Bu, normal veri yayınlama yasağının (subscriber'lar `sensor/sicaklik` gibi topic'lere veri gönderemez) **bilinçli bir istisnasıdır** — sistemin authorization mantığı, "veri" ile "kontrol komutu" mesajlarını ayırt edecek şekilde genişletilmiştir. Broker, komutu aldığında hemen yeni bir sorgu yapar ve sonucu **tüm subscriber'lara** `sensor/anlik_sicaklik` topic'iyle yayınlar:
```json
{"topic":"sensor/anlik_sicaklik","payload":"23.9","sehir":"Cankaya","zaman":1788873234}
```

### Eşzamanlılık Koruması

Periyodik yayın görevi (`vAnlikHavaTask`) ile runtime komutları (`vClientHandlerTask` üzerinden), **aynı anda, farklı görevlerden** dış API'yi çağırabilmektedir. Test sırasında, bu ortamda (FreeRTOS Windows Simulator) eşzamanlı WinHTTP çağrılarının güvenilir sonuç vermediği tespit edilmiş; çözüm olarak dış API erişimi bir mutex (`xHttpMutex`) ile korunarak, aynı anda yalnızca bir görevin HTTPS isteği atabilmesi garanti altına alınmıştır.

### Test Aracı

`test_subscriber.py`, mentörün "kendi yazdığı bir subscriber broker'a bağlanıp şehir sorgulayabilsin" senaryosunu simüle eden, bağımsız bir Python betiğidir. Tek bir döngüde hem soket hem klavye girişini (non-blocking) işleyerek, terminal çıktısının karışmasını (çoklu thread kullanan ilk versiyonda yaşanan bir sorun) önler.

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
- [x] NTP ile gerçek zaman senkronizasyonu (DNS çözümleme, UDP NTP paket alışverişi, JSON mesajlarına Unix timestamp eklenmesi)
- [x] Open-Meteo API entegrasyonu (WinHTTP/HTTPS, geocoding + anlık hava durumu, config dosyası + runtime komutuyla çift yönlü şehir seçimi)

## Bilinen Kısıtlamalar

FreeRTOS Windows Simulator portu, gerçekçi tek-çekirdekli zamanlama sağlamak amacıyla her görev iş parçacığını `SetThreadAffinityMask()` ile CPU'nun 0. çekirdeğine sabitlemekte ve işlem önceliğini `REALTIME_PRIORITY_CLASS` olarak ayarlamaktadır. Bu, **aynı fiziksel makinede birden fazla instance'ın eş zamanlı çalıştırılmasını** sınırlamaktadır (instance'lar aynı çekirdek için rekabet eder). Bu davranış, gerçek donanımda (her cihazın kendi bağımsız işlemcisine sahip olduğu bir senaryoda, örn. ESP32) yaşanmayacaktır; çoklu instance testleri farklı fiziksel makinelerde/VM'lerde sorunsuz çalışır.

Ayrıca, portun her FreeRTOS task'ını **gerçek bir Windows thread'i** olarak çalıştırması nedeniyle, `vTaskEndScheduler()` çağrısı sistemi tam olarak durduramamaktadır (Timer Service task'ı silinse de, diğer görevler kendi Windows thread'lerinde çalışmaya devam edebilir). Bu nedenle idle-timeout özelliği, `exit()` ile doğrudan process sonlandırma kullanmaktadır.

## Yol Haritası

- [ ] ESP32'ye taşınabilirlik (network katmanının soyutlanması)

## Proje Yapısı

```
.
├── main.c                          # Ana uygulama kodu
├── CMakeLists.txt                   # Derleme yapılandırması
├── FreeRTOSConfig.h                 # FreeRTOS kernel yapılandırma ayarları
├── monitor.py                       # Python/Tkinter kontrol paneli
├── ankara_sicaklik_verileri.csv     # Gerçek sensör veri seti (68 şehir, 1022 kayıt)
├── sehir_config.json                # Anlık hava durumu için varsayılan şehir
├── test_subscriber.py               # Runtime şehir sorgu komutu test aracı
├── cJSON/
│   ├── cJSON.c
│   └── cJSON.h
└── README.md
```

## Notlar

Bu proje, bir staj programı kapsamında, gömülü sistemlerdeki RTOS ve ağ haberleşmesi kavramlarını (task senkronizasyonu, priority inversion, sistem sağlık yönetimi, ağ protokolü tasarımı) uygulamalı olarak öğrenmek amacıyla geliştirilmektedir.