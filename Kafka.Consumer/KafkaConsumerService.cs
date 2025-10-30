#region

using Confluent.Kafka;

#endregion

namespace Kafka.Consumer;

internal class KafkaConsumerService
{
    /// <summary>
    ///     AT MOST ONCE (En Fazla Bir Kez) - Consumer Tarafı
    ///     Consumer mesajı okur okumaz offset'i commit eder, işleme başlamadan.
    ///     Mesaj işlenirken hata olursa kaybolur ama duplicate olmaz.
    ///     En düşük güvenilirlik, en yüksek performans.
    ///     Kullanım Alanı: Log toplama, monitoring metrikleri gibi veri kaybının tolere edilebildiği durumlar
    /// </summary>
    internal void ConsumeAtMostOnce()
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9094",
            GroupId = "at-most-once-consumer-group",

            // AutoOffsetReset: Consumer grubu için offset yoksa nereden başlasın
            // Earliest: En baştan okumaya başla
            AutoOffsetReset = AutoOffsetReset.Earliest,

            // EnableAutoCommit=true: Offset otomatik commit edilir
            // Mesaj okunur okunmaz commit edilir (işlenmeden önce)
            EnableAutoCommit = true,

            // AutoCommitIntervalMs: Offset'lerin ne sıklıkla commit edileceği
            // 5 saniyede bir otomatik commit
            AutoCommitIntervalMs = 5000,

            // EnableAutoOffsetStore=true: Mesaj poll edilince offset otomatik store edilir
            // Mesaj henüz işlenmemiş olsa bile offset ilerlemiş sayılır
            EnableAutoOffsetStore = true
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

        // Topic'e subscribe ol
        consumer.Subscribe("orders-topic-at-most-once");

        Console.WriteLine("AT MOST ONCE Consumer başladı...");

        try
        {
            while (true)
            {
                // 1. Mesajı consume et (poll)
                // Bu anda otomatik olarak offset store edilir (EnableAutoOffsetStore=true)
                var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));

                if (consumeResult == null) continue;

                // 2. Mesaj alındı ama henüz işlenmedi
                // AutoCommit aktif olduğu için 5 saniye içinde offset commit edilecek
                // İşleme başlamadan önce commit olursa, işlem sırasında hata olsa bile mesaj kaybolur
                Console.WriteLine(
                    $"Mesaj alındı - Offset: {consumeResult.Offset}, Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}");

                // 3. Mesajı işle (business logic)
                // Eğer burada exception fırlarsa mesaj kaybolur
                // Çünkü offset zaten commit edildi veya edilmek üzere
                ProcessMessage(consumeResult.Message);

                // 4. Manuel commit'e gerek yok, otomatik commit zaten yapılıyor
                // Bu yüzden işlem başarısız olsa bile mesaj bir daha işlenmez (AT MOST ONCE)

                Console.WriteLine("Mesaj işlendi (ancak hata olsaydı kaybolurdu)");
            }
        }
        catch (ConsumeException ex)
        {
            Console.WriteLine($"Consume hatası: {ex.Error.Reason}");
        }
        finally
        {
            consumer.Close();
        }
    }

    /// <summary>
    ///     AT LEAST ONCE (En Az Bir Kez) - Consumer Tarafı
    ///     Consumer mesajı işledikten SONRA offset'i commit eder.
    ///     Mesaj en az bir kez işlenir garanti edilir ama duplicate işlenebilir.
    ///     Orta seviye güvenilirlik ve performans.
    ///     Kullanım Alanı: E-ticaret, bildirimler gibi veri kaybının kabul edilemez olduğu
    ///     ama idempotent işlemlerin yapılabildiği senaryolar (consumer duplicate'ları handle edebilir)
    /// </summary>
    internal void ConsumeAtLeastOnce()
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9094",
            GroupId = "at-least-once-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,

            // EnableAutoCommit=false: Otomatik commit KAPALI
            // Offset'leri manuel olarak kontrol ederiz
            // Mesaj işlendikten SONRA commit ederiz
            EnableAutoCommit = false,

            // EnableAutoOffsetStore=false: Otomatik offset store KAPALI
            // StoreOffset() ile manuel olarak store edeceğiz
            EnableAutoOffsetStore = false,

            // IsolationLevel=ReadCommitted: Sadece commit edilmiş mesajları oku
            // Producer'ın transaction'larını bekle (exactly once için önemli)
            IsolationLevel = IsolationLevel.ReadCommitted
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

        consumer.Subscribe("orders-topic-at-least-once");

        Console.WriteLine("AT LEAST ONCE Consumer başladı...");

        try
        {
            while (true)
            {
                // 1. Mesajı consume et
                // Otomatik offset store kapalı olduğu için henüz offset ilerlemiyor
                var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));

                if (consumeResult == null) continue;

                Console.WriteLine(
                    $"Mesaj alındı - Offset: {consumeResult.Offset}, Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}");

                try
                {
                    // 2. Mesajı işle (business logic)
                    // Eğer burada hata olursa offset commit edilmez
                    // Consumer yeniden başladığında aynı mesaj tekrar işlenir (AT LEAST ONCE)
                    ProcessMessage(consumeResult.Message);

                    Console.WriteLine("Mesaj başarıyla işlendi");

                    // 3. İşlem başarılı olduktan SONRA offset'i store et
                    // Henüz Kafka'ya commit edilmedi, sadece local olarak store edildi
                    consumer.StoreOffset(consumeResult);

                    // 4. Offset'i Kafka'ya commit et
                    // Artık bu mesaj işlenmiş olarak kaydedildi
                    // Consumer crash olup yeniden başlasa bile bu mesaj bir daha işlenmez
                    consumer.Commit(consumeResult);

                    Console.WriteLine($"Offset commit edildi: {consumeResult.Offset}");

                    // NOT: Eğer StoreOffset/Commit sonrası ama bir sonraki mesaj okunmadan önce
                    // consumer crash olursa, network hatası olursa veya commit acknowledgment kaybolursa
                    // mesaj bir daha işlenebilir (DUPLICATE) - bu yüzden AT LEAST ONCE
                }
                catch (Exception ex)
                {
                    // 5. Hata durumunda offset commit edilmez
                    // Consumer yeniden başladığında aynı mesajı tekrar işlemeye çalışır
                    Console.WriteLine($"Mesaj işlenirken hata: {ex.Message}");
                    Console.WriteLine("Offset commit edilmedi, mesaj tekrar işlenecek");

                    // Opsiyonel: Retry stratejisi uygula
                    // Sonsuz döngüde takılmamak için dead letter queue kullanılabilir
                }
            }
        }
        catch (ConsumeException ex)
        {
            Console.WriteLine($"Consume hatası: {ex.Error.Reason}");
        }
        finally
        {
            consumer.Close();
        }
    }

    /// <summary>
    ///     EXACTLY ONCE (Tam Bir Kez) - Consumer Tarafı
    ///     Consumer mesajı işleyip offset'i tek bir atomik transaction içinde commit eder.
    ///     Mesaj tam olarak bir kez işlenir - ne kaybolur ne duplicate olur.
    ///     En yüksek güvenilirlik ama en düşük performans.
    ///     Kullanım Alanı: Finansal işlemler, para transferleri, stok güncellemeleri gibi
    ///     kritik business operasyonların yapıldığı yerler
    ///     NOT: Tam exactly once için consumer'ın da transactional olması ve
    ///     işlemi (örn: veritabanı) transaction içinde yapması gerekir
    /// </summary>
    internal void ConsumeExactlyOnce()
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9094",
            GroupId = "exactly-once-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,

            // EnableAutoCommit=false: Manuel commit zorunlu
            EnableAutoCommit = false,

            // EnableAutoOffsetStore=false: Manuel offset store zorunlu
            EnableAutoOffsetStore = false,

            // IsolationLevel=ReadCommitted: ZORUNLU exactly once için
            // Sadece producer'ın commit ettiği mesajları oku
            // Uncommitted (transaction içinde henüz commit edilmemiş) mesajları okuma
            IsolationLevel = IsolationLevel.ReadCommitted,

            // EnablePartitionEof=true: Partition sonuna geldiğinde bildir
            EnablePartitionEof = true
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

        consumer.Subscribe("orders-topic-exactly-once");

        Console.WriteLine("EXACTLY ONCE Consumer başladı...");

        try
        {
            while (true)
            {
                // 1. Mesajı consume et
                // IsolationLevel=ReadCommitted olduğu için sadece committed mesajlar gelir
                var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));

                if (consumeResult == null) continue;

                // Partition EOF kontrolü
                if (consumeResult.IsPartitionEOF)
                {
                    Console.WriteLine($"Partition {consumeResult.Partition} sonuna ulaşıldı");
                    continue;
                }

                Console.WriteLine(
                    $"Mesaj alındı - Offset: {consumeResult.Offset}, Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}");

                // 2. Exactly Once için Transaction başlat
                // Bu transaction hem mesaj işlemeyi hem offset commit'i kapsayacak
                // Örnek: Database transaction
                try
                {
                    // Transaction başlat (örneğin SQL transaction)
                    // using var dbTransaction = dbConnection.BeginTransaction();

                    // 3. Mesajı işle ve database'e yaz (transaction içinde)
                    // Eğer burada hata olursa hem database transaction hem offset commit rollback olur
                    ProcessMessageWithTransaction(consumeResult.Message);

                    // 4. Offset'i consumer'da store et
                    // Henüz commit edilmedi
                    consumer.StoreOffset(consumeResult);

                    // 5. Offset'i Kafka'ya commit et
                    // Bu da transaction'ın bir parçası gibi düşünülebilir
                    // Eğer database commit başarılı ama offset commit başarısız olursa
                    // veya tam tersi olursa exactly once bozulur
                    consumer.Commit(consumeResult);

                    // 6. Database transaction'ını commit et
                    // dbTransaction.Commit();

                    // ÖNEMLI: Gerçek exactly once için bu 2 commit (DB + Kafka) atomik olmalı
                    // Bunun için Kafka Streams veya özel transactional outbox pattern kullanılır
                    // Basit senaryolarda: İdempotent işlemler yaparak duplicate'ları tolere et
                    // veya database'de unique constraint ile duplicate'ları engelle

                    Console.WriteLine("Mesaj exactly once garantisi ile işlendi ve commit edildi");
                }
                catch (Exception ex)
                {
                    // 7. Hata durumunda hiçbir şey commit etme
                    // Database rollback yap
                    // Offset commit yapma
                    // Consumer yeniden başlayınca aynı mesajı tekrar işleyecek
                    Console.WriteLine($"Transaction başarısız, rollback yapılıyor: {ex.Message}");

                    // dbTransaction.Rollback();

                    // Offset commit edilmediği için mesaj tekrar işlenecek
                    // Ama database transaction rollback olduğu için duplicate data olmayacak
                }
            }
        }
        catch (ConsumeException ex)
        {
            Console.WriteLine($"Consume hatası: {ex.Error.Reason}");
        }
        finally
        {
            consumer.Close();
        }
    }

    /// <summary>
    ///     Mesajı işleyen basit metod (simülasyon)
    /// </summary>
    private void ProcessMessage(Message<string, string> message)
    {
        // Simülasyon: Mesaj işleme
        Console.WriteLine($"İşleniyor: {message.Value}");
        Thread.Sleep(100); // İşlem simülasyonu

        // Bazen hata fırlat (test için)
        // if (Random.Shared.Next(0, 10) > 7)
        //     throw new Exception("Rastgele işlem hatası!");
    }

    /// <summary>
    ///     Mesajı transaction içinde işleyen metod (exactly once için)
    /// </summary>
    private void ProcessMessageWithTransaction(Message<string, string> message)
    {
        // Simülasyon: Database'e transaction ile yaz
        Console.WriteLine($"Transaction içinde işleniyor: {message.Value}");
        Thread.Sleep(100);

        // Burada gerçek senaryoda:
        // - Database'e INSERT/UPDATE yapılır
        // - Unique constraint ile duplicate engellenir
        // - veya idempotency key kullanılır (message.Key)

        // Örnek: 
        // INSERT INTO orders (order_id, data) VALUES (@orderId, @data)
        // ON CONFLICT (order_id) DO NOTHING; -- Idempotent INSERT
    }
}