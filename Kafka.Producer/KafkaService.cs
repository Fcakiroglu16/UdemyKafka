#region

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Kafka.Producer.Events;

#endregion

namespace Kafka.Producer;

internal class KafkaService
{
    internal async Task CreateTopicAsync(string topicName)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = "localhost:9094"
        }).Build();

        try
        {
            await adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topicName, NumPartitions = 6, ReplicationFactor = 1
                }
            });

            Console.WriteLine($"Topic({topicName}) oluştu.");
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
    }


    internal async Task SendMessage()
    {
        // 1. Schema Registry istemcisini yapılandır
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "http://localhost:8081"
        };
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

        // 2. Producer'ı yapılandır
        var producerConfig = new ProducerConfig { BootstrapServers = "localhost:9094" };

        // 3. Producer'ı AvroSerializer ile oluştur
        // Anahtar (Key) string, Değer (Value) ise OrderCreatedEvent nesnesi olacak
        using var producer = new ProducerBuilder<string, OrderCreatedEvent>(producerConfig)
            .SetValueSerializer(new AvroSerializer<OrderCreatedEvent>(schemaRegistry))
            .Build();

        // 4. Mesajı C# nesnesi olarak gönder

        for (var i = 1; i <= 10; i++)
        {
            var eventMessage = new OrderCreatedEvent
            {
                OrderId = $"ORD-{i:D3}",
                CustomerId = "abc", //$"CUST-{i + 100}",
                Amount = i * 2 * 100
            };

            await producer.ProduceAsync("orders-topic", new Message<string, OrderCreatedEvent>
            {
                Key = eventMessage.OrderId,
                Value = eventMessage
            });

            Console.WriteLine($"Message {i}/10 sent: {eventMessage.OrderId}");
        }
    }

    /// <summary>
    ///     AT MOST ONCE (En Fazla Bir Kez) - Fire and Forget
    ///     Mesaj en fazla bir kez iletilir, kaybolabilir ama asla duplicate olmaz.
    ///     Producer mesajı gönderir ve acknowledgment beklemez.
    ///     En düşük güvenilirlik, en yüksek performans.
    ///     Kullanım Alanı: Log toplama, metric collection gibi veri kaybının kabul edilebilir olduğu senaryolar
    /// </summary>
    internal void SendMessageAtMostOnce()
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9094",

            // Acks=0: Producer, broker'dan hiçbir acknowledgment beklemez
            // Mesaj network buffer'a yazılır ve hemen başarılı kabul edilir
            // En hızlı yöntem ama mesaj kaybolabilir
            Acks = Acks.None,

            // EnableIdempotence=false: Idempotent producer kapalı
            // Retry mekanizması devre dışı (çünkü retry duplicate yaratabilir)
            EnableIdempotence = false,

            // MessageSendMaxRetries=0: Hata durumunda retry yapılmaz
            // Mesaj gönderilemezse direkt kaybolur
            MessageSendMaxRetries = 0
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        try
        {
            // Fire-and-forget: await kullanmadan gönder (opsiyonel)
            // veya await kullanıp exception'ı ignore et
            var message = new Message<string, string>
            {
                Key = "order-001",
                Value = "At Most Once - Bu mesaj kaybolabilir ama duplicate olmaz"
            };

            // DeliveryResult'ı beklemeden mesaj gönder
            _ = producer.ProduceAsync("orders-topic-at-most-once", message);

            Console.WriteLine("Mesaj gönderildi (acknowledgment beklenmedi)");
        }
        catch (Exception ex)
        {
            // Hata durumunda bile retry yapılmaz, mesaj kaybolur
            Console.WriteLine($"Hata: {ex.Message} - Mesaj kaybedildi");
        }
    }

    /// <summary>
    ///     AT LEAST ONCE (En Az Bir Kez)
    ///     Mesaj en az bir kez iletilir garanti edilir ama duplicate olabilir.
    ///     Producer acknowledgment bekler ve hata durumunda retry yapar.
    ///     Orta seviye güvenilirlik ve performans.
    ///     Kullanım Alanı: E-ticaret siparişleri, ödeme bildirimleri gibi veri kaybının kabul edilemez olduğu
    ///     ama duplicate'ların consumer tarafında handle edilebildiği senaryolar
    /// </summary>
    internal async Task SendMessageAtLeastOnce()
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9094",

            // Acks=Leader: Sadece leader broker'dan acknowledgment bekle
            // Leader partition'a yazıldıktan sonra başarılı kabul edilir
            // Replica'lara yazılması beklenmez (daha hızlı ama replica'lar sync olmadan leader crash ederse veri kaybı olabilir)
            Acks = Acks.Leader,

            // EnableIdempotence=false: Idempotent producer kapalı
            // Bu durumda retry duplicate mesajlara neden olabilir
            EnableIdempotence = false,

            // MessageSendMaxRetries: Hata durumunda maksimum retry sayısı
            // Network hatası veya timeout durumunda mesaj tekrar gönderilir
            // Bu duplicate mesajlara yol açabilir (acknowledgment kaybolursa ama mesaj yazılmışsa)
            MessageSendMaxRetries = 3,

            // RequestTimeoutMs: Broker'dan acknowledgment bekleme süresi
            RequestTimeoutMs = 30000,

            // RetryBackoffMs: Retry'lar arasındaki bekleme süresi
            RetryBackoffMs = 100
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        try
        {
            var message = new Message<string, string>
            {
                Key = "order-002",
                Value = "At Least Once - Bu mesaj en az bir kez iletilir, duplicate olabilir"
            };

            // ProduceAsync acknowledgment bekler
            // Hata durumunda otomatik retry yapar (MaxRetries kadar)
            var deliveryResult = await producer.ProduceAsync("orders-topic-at-least-once", message);

            Console.WriteLine(
                $"Mesaj başarıyla gönderildi - Offset: {deliveryResult.Offset}, Partition: {deliveryResult.Partition}");
        }
        catch (ProduceException<string, string> ex)
        {
            // Tüm retry'lar başarısız olduysa exception fırlatılır
            // Uygulama seviyesinde tekrar deneme yapılabilir (bu da duplicate yaratabilir)
            Console.WriteLine($"Mesaj gönderilemedi: {ex.Error.Reason}");

            // Manuel retry - bu da duplicate yaratabilir
            // throw; // veya yeniden göndermeyi dene
        }
    }

    /// <summary>
    ///     EXACTLY ONCE (Tam Bir Kez)
    ///     Mesaj tam olarak bir kez iletilir garanti edilir - ne kaybolur ne duplicate olur.
    ///     Idempotent producer ve transactions kullanılır.
    ///     En yüksek güvenilirlik ama en düşük performans.
    ///     Kullanım Alanı: Finansal işlemler, kritik business logic, stok yönetimi gibi
    ///     ne veri kaybının ne de duplicate'ların kabul edilemez olduğu senaryolar
    /// </summary>
    internal async Task SendMessageExactlyOnce()
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9094",

            // Acks=All: Tüm in-sync replica'lardan acknowledgment bekle
            // En güvenli ama en yavaş yöntem
            // Leader ve tüm ISR (In-Sync Replicas) mesajı aldıktan sonra başarılı kabul edilir
            Acks = Acks.All,

            // EnableIdempotence=true: Idempotent producer aktif
            // Producer her mesaja benzersiz bir sequence number atar
            // Broker duplicate mesajları otomatik olarak tespit edip reddeder
            // Bu sayede retry duplicate yaratmaz
            EnableIdempotence = true,

            // TransactionalId: Transaction desteği için gerekli
            // Aynı TransactionalId ile birden fazla producer oluşturulamaz
            // Bu ID ile producer yeniden başlatılırsa, önceki transaction'lar recover edilir
            TransactionalId = "order-producer-txn-1",

            // MaxInFlight=5: Idempotent producer için max in-flight request sayısı
            // EnableIdempotence=true olduğunda otomatik olarak <=5 olmalı
            MaxInFlight = 5,

            // MessageSendMaxRetries: Hata durumunda sınırsız retry
            // Idempotent olduğu için duplicate riski yok
            MessageSendMaxRetries = int.MaxValue,

            // RequestTimeoutMs: Acknowledgment bekleme süresi
            RequestTimeoutMs = 30000
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        try
        {
            // Transaction başlat
            // Tüm mesajlar transaction içinde atomik olarak commit edilir
            // Ya hepsi başarılı olur ya hiçbiri (all-or-nothing)
            producer.InitTransactions(TimeSpan.FromSeconds(30));
            producer.BeginTransaction();

            try
            {
                // Birden fazla mesajı atomik olarak gönder
                for (var i = 1; i <= 3; i++)
                {
                    var message = new Message<string, string>
                    {
                        Key = $"order-{i:D3}",
                        Value = $"Exactly Once - Transaction içinde mesaj {i}"
                    };

                    // Transaction içinde mesaj gönder
                    // Henüz consumer'lar tarafından görülemez (uncommitted)
                    var result = await producer.ProduceAsync("orders-topic-exactly-once", message);

                    Console.WriteLine($"Mesaj {i} transaction'a eklendi - Offset: {result.Offset}");
                }

                // Transaction'ı commit et
                // Şimdi tüm mesajlar atomik olarak görünür hale gelir
                // Idempotent producer sayesinde duplicate garantisi var
                producer.CommitTransaction();

                Console.WriteLine(
                    "Transaction başarıyla commit edildi - Tüm mesajlar exactly once garantisi ile iletildi");
            }
            catch (Exception ex)
            {
                // Hata durumunda transaction'ı geri al
                // Hiçbir mesaj consumer tarafından görünmez
                producer.AbortTransaction();
                Console.WriteLine($"Transaction iptal edildi: {ex.Message}");
                throw;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Exactly Once gönderilemedi: {ex.Message}");
        }
    }

    /// <summary>
    ///     EXACTLY ONCE - Schema Registry ile Avro kullanarak
    ///     Gerçek dünya örneği: OrderCreatedEvent ile exactly once semantiği
    /// </summary>
    internal async Task SendMessageExactlyOnceWithAvro()
    {
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "http://localhost:8081"
        };
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9094",
            Acks = Acks.All,
            EnableIdempotence = true,
            TransactionalId = "order-avro-producer-txn-1",
            MaxInFlight = 5
        };

        using var producer = new ProducerBuilder<string, OrderCreatedEvent>(producerConfig)
            .SetValueSerializer(new AvroSerializer<OrderCreatedEvent>(schemaRegistry))
            .Build();

        try
        {
            producer.InitTransactions(TimeSpan.FromSeconds(30));
            producer.BeginTransaction();

            try
            {
                for (var i = 1; i <= 5; i++)
                {
                    var eventMessage = new OrderCreatedEvent
                    {
                        OrderId = $"ORD-EXACT-{i:D3}",
                        CustomerId = $"CUST-{i + 100}",
                        Amount = i * 100
                    };

                    await producer.ProduceAsync("orders-topic-exactly-once", new Message<string, OrderCreatedEvent>
                    {
                        Key = eventMessage.OrderId,
                        Value = eventMessage
                    });

                    Console.WriteLine($"Order {eventMessage.OrderId} transaction'a eklendi");
                }

                producer.CommitTransaction();
                Console.WriteLine("Tüm orderlar exactly once garantisi ile kaydedildi");
            }
            catch (Exception ex)
            {
                producer.AbortTransaction();
                Console.WriteLine($"Order transaction iptal edildi: {ex.Message}");
                throw;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Exactly Once Avro gönderilemedi: {ex.Message}");
        }
    }
}