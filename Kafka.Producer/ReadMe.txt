Exactly Once'in ilk katmanı, üretici (producer) tarafında başlar.

Problem: Bir producer mesajı broker'a gönderir. Broker mesajı başarıyla alır ve kaydeder, ancak producer'a göndereceği onay (ACK) mesajı ağdaki bir sorun nedeniyle producer'a ulaşmaz. Producer, mesajın ulaşıp ulaşmadığından emin olamaz ve (eğer retries > 0 olarak ayarlanmışsa) mesajı tekrar gönderir. Bu durum, broker'da aynı mesajın iki kez kaydedilmesine (duplicate) neden olur.

Çözüm: Producer konfigürasyonuna enable.idempotence = true ayarını eklemek.

Nasıl Çalışır?

Bu ayar etkinleştirildiğinde, Kafka producer'a benzersiz bir Producer ID (PID) atar.

Producer, gönderdiği her mesaj grubuna (batch) artan bir Sıra Numarası (Sequence Number) ekler.

Broker, her partisyon için (PID, Sıra Numarası) çiftini takip eder.

Eğer broker, zaten işlediği bir sıra numarasına sahip bir mesaj alırsa (yukarıdaki hata senaryosunda olduğu gibi), bu mesajı "yinelenen" (duplicate) olarak kabul eder, log'a tekrar yazmaz ancak producer'a "başarılı" (ACK) onayı gönderir.

Önemli Not: enable.idempotence = true ayarını yaptığınızda, producer ayarları otomatik olarak acks = all (tüm replikaların onayı beklenir) ve retries = Integer.MAX_VALUE (süresiz yeniden deneme) olarak ayarlanır. Bu, dayanıklılığı artırır.

Kısıtlama: Idempotent producer, yalnızca tek bir producer oturumu ve tek bir partisyon içinde tekrarı önler. Birden fazla partisyona atomik olarak yazmak için bir sonraki adıma ihtiyacımız var.