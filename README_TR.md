# ESP32 ağ simülasyonu — ilk taşıma

Bu proje, gönderdiğin Windows kaynaklarından ESP-IDF 6.1 için hazırlanmıştır.
İlk hedef **PC broker + ESP32 publisher** bağlantısıdır. Windows projesindeki
dosyalarını değiştirme. Bu klasör bağımsız bir ESP-IDF projesidir.

## Kullanım

1. Bu klasörü bilgisayarında kısa bir yola kopyala/ZIP'ten çıkar; örneğin
   `C:\esp_projects\esp32_network_sim`. Klasörün doğrudan içinde kök
   `CMakeLists.txt` ve `main` bulunmalı.
2. VS Code'da **File → Open Folder** ile bu klasörü aç.
3. ESP-IDF ortamı sorulursa kurulu `C:\esp_new\v6.1\esp-idf` ortamını seç.
4. `main/app_config.h` dosyasında `WIFI_SSID`, `WIFI_SIFRE` ve
   `ESP32_BROKER_IP` değerlerini kendi bilgisayarında doldur. Şifreyi paylaşma.
   ESP32 ve bilgisayar aynı yerel ağda olmalı. Klasik ESP32 için 2.4 GHz Wi-Fi kullan.
5. PowerShell'de `ipconfig` çalıştır. Bağlı Wi-Fi/Ethernet adaptörünün IPv4
   adresini broker IP'si olarak yaz. `127.0.0.1` kullanma. `192.168.1.100`
   yalnızca örnek değerdir. Rol `ROLE_PUBLISHER`, TCP portu `8080` kalsın.
6. **ESP-IDF: Build Your Project** ile derle. Mevcut bir projeye dosya aktarmak
   yerine bu klasörü açmak, eski `hello_world_main.c` veya `sdkconfig` ile
   çakışmayı önler. Hedef bu pakette klasik `esp32` olarak doğrulanmaktadır;
   kartın S3/C3 gibi farklıysa yüklemeden önce hedefi değiştirip yeniden derle.
7. PC'de mevcut derlenmiş broker'ın bulunduğu klasörde çalıştır:

   ```powershell
   .\freertos_demo.exe broker 8080
   ```

8. Kartı bağla, **ESP-IDF: Select Port** ile COM portunu seç; ardından
   **ESP-IDF: Flash Your Project**, **ESP-IDF: Monitor Your Device**.

ESP-IDF terminalinden eşdeğer komutlar:

```text
idf.py build
idf.py -p COM_NUMARASI flash monitor
```

`COM_NUMARASI` yerine kartının gerçek portunu yaz. Monitörden çıkış: `Ctrl+]`.

## Beklenen doğrulama

Kartta sırayla aşağıdakilere benzer mesajlar beklenir:

```text
[WiFi] IP alindi: ...
[main] Rol: PUBLISHER | Broker: ...:8080
[Network] Broker'a baglanildi; PUBLISHER kimligi gonderildi.
[Network] JSON gonderildi: ...
```

PC broker'da `Authentication basarili`, `Bu client bir PUBLISHER` ve
`Gecerli JSON alindi` görülmesi uçtan uca doğrulamadır. Kartın "kimlik
gönderildi" mesajı tek başına broker'ın kimliği kabul ettiğini kanıtlamaz.

Wi-Fi IP alıyor ama broker bağlantısı kurulamıyorsa PC IP'sini, broker'ın
çalıştığını ve Windows Güvenlik Duvarı'nda özel ağ için broker erişimini
kontrol et. Mevcut PC broker 30 saniye istemci gelmeyince kapanabiliyor;
kart hazırken broker'ı yeniden başlatman gerekebilir.

## Yapılan uyarlamalar

- ESP-IDF `app_main` giriş noktası ve `freertos/...` başlıkları kullanılır;
  scheduler tekrar başlatılmaz, Windows'a ait FreeRTOSConfig/hook dosyaları taşınmaz.
- `net_port.h` arayüzü korunur; TCP/UDP/DNS/zaman lwIP ve ESP-IDF ile uygulanır.
- Publisher gönderim hatasında soketi kapatıp tekrar bağlanır; kısmi TCP
  yazmaları tamamlanır veya akış sonlandırılır. Mesaj teslim garantisi/ACK yoktur.
- Wi-Fi için IP olayı beklenir. Bağlantı koparsa yeniden bağlanma denenir.
- Stack boyutları byte olarak belirlenir; heap toplamı ESP-IDF API'sinden alınır.
- NTP sistem saatini ayarlar. Saat henüz yoksa HTTPS ertelenir. HTTPS,
  CA paketiyle sertifikayı doğrular; eksik veya buffer'a sığmayan yanıt reddedilir.
- `espressif/cjson` bağımlılığı manifest ile eklenir. ESP-IDF 6.x'te eski
  `REQUIRES json` satırı kullanılmaz. Kaynak:
  https://github.com/espressif/esp-idf/blob/master/docs/en/migration-guides/release-6.x/6.0/protocols.rst
- CSV/config dosyası okuma bu aşamada kaldırılmıştır. Publisher 3 saniyede bir
  rastgele sıcaklık üretir; gerçek sensör ölçümü değildir. Hava verisi dosyaya
  yazılmak yerine seri portta gösterilir. ESP32 broker için otomatik kapanma kaldırılmıştır.
- PC ve ESP32'nin açılıştan beri geçen sayaçları aynı başlangıca sahip değildir;
  bunların farkından gecikme hesaplanmaz.

## Kapsam ve kalan işler

Öncelikli deneme publisher rolüdür. Broker/subscriber kodu kaynakta korunmuştur;
bu rollerin ESP32 üzerindeki çoklu istemci, bellek baskısı, zamanlayıcı ve bağlantı
kopma senaryoları ayrıca test edilmelidir. Mevcut simülasyonun TCP üstünde
satırlarla ayrılmış JSON protokolü korunur; standart MQTT protokolü değildir.

ESP-IDF 6.1.0 ve klasik ESP32 hedefiyle `idf.py build` başarıyla tamamlandı.
Firmware boyutu 558944 byte; 1 MiB uygulama bölümünde %47 boş yer kaldı.
Derleme örnek ağ ayarlarıyla yapıldı; kendi ayarlarını girdikten sonra yeniden derle.
Fiziksel kartta yükleme ve uçtan uca ağ testi burada yapılmadı. Kart bilgileri
ve yerel ağ ayarları girildikten sonra ilk testin loglarıyla devam edilmelidir.
Dosya sistemi/CSV, gerçek sensör ve ölçülebilir gecikme testleri sonraki adımlardır.

`app_config.h` yerel ayar dosyasıdır ve `.gitignore` içine alınmıştır.
Git'ten yeni kopya alırken `app_config.example.h` dosyasını `app_config.h`
adıyla kopyala ve doldur. Dosya daha önce takip
ediliyorsa `.gitignore` onu otomatik olarak takipten çıkarmaz.
