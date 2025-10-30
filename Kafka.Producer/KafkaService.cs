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

    internal async Task CreateTopicsWithDifferentPoliciesAsync()
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = "localhost:9094"
        }).Build();

        try
        {
            // ============================================================
            // 1. TIME-BASED RETENTION POLICY TOPIC
            // ============================================================
            // Mesajlar belirli bir süre sonra otomatik olarak silinir.
            // Use case: Log verisi, geçici event'ler, audit kayıtları
            var timeBasedTopic = new TopicSpecification
            {
                Name = "orders-time-based",
                NumPartitions = 6,
                ReplicationFactor = 1,
                Configs = new Dictionary<string, string>
                {
                    // cleanup.policy: Mesajların nasıl temizleneceğini belirler
                    // "delete" = Eski mesajlar zaman veya boyut limitine göre silinir
                    { "cleanup.policy", "delete" },

                    // retention.ms: Mesajların topic'te ne kadar süre saklanacağı (milisaniye)
                    // DEV: 604800000 ms = 7 gün (test için uygun)
                    // PROD: 2592000000 ms = 30 gün (standart), 7776000000 ms = 90 gün (uzun süreli)
                    { "retention.ms", "604800000" }, // 7 days

                    // retention.bytes: Partition başına maksimum veri boyutu
                    // -1 = Sınırsız (sadece zaman bazlı temizleme)
                    // PROD: "10737418240" = 10 GB per partition (disk kontrolü için)
                    { "retention.bytes", "-1" }
                }
            };

            // ============================================================
            // 2. SIZE-BASED RETENTION POLICY TOPIC
            // ============================================================
            // Partition boyutu belirli bir limiti aştığında en eski mesajlar silinir.
            // Use case: Disk alanı kritik olan sistemler, yüksek throughput senaryoları
            var sizeBasedTopic = new TopicSpecification
            {
                Name = "orders-size-based",
                NumPartitions = 6,
                ReplicationFactor = 1,
                Configs = new Dictionary<string, string>
                {
                    { "cleanup.policy", "delete" },

                    // retention.bytes: Her partition için maksimum boyut (byte)
                    // DEV: 1073741824 = 1 GB (test için küçük)
                    // PROD: "10737418240" = 10 GB, "53687091200" = 50 GB (yüksek trafik için)
                    { "retention.bytes", "1073741824" }, // 1 GB per partition

                    // retention.ms: Boyut limiti aşılmadığı sürece mesajlar ne kadar saklanır?
                    // -1 = Sınırsız (sadece boyut bazlı temizleme)
                    // PROD: "604800000" = 7 gün (hem boyut hem zaman kontrolü için)
                    { "retention.ms", "-1" }

                    // segment.bytes: Yeni segment dosyası ne zaman oluşturulur?
                    // Default: 1 GB
                    // PROD: "536870912" = 512 MB (daha küçük segment'ler, daha hızlı compaction)
                    // { "segment.bytes", "1073741824" }
                }
            };

            // ============================================================
            // 3. COMPACT RETENTION POLICY TOPIC
            // ============================================================
            // Her key için sadece en son değer saklanır (changelog pattern).
            // Use case: Database CDC, entity state management, cache invalidation
            var compactTopic = new TopicSpecification
            {
                Name = "orders-compact",
                NumPartitions = 6,
                ReplicationFactor = 1,
                Configs = new Dictionary<string, string>
                {
                    // cleanup.policy: "compact" = Her key için sadece son değer tutulur
                    // "compact,delete" = Hem compaction hem de zaman bazlı silme yapılır
                    { "cleanup.policy", "compact" },

                    // min.cleanable.dirty.ratio: Compaction ne zaman tetiklenir?
                    // Log'daki "kirli" (duplicate key'li) kayıt oranı bu değere ulaşınca compaction başlar
                    // DEV: 0.5 = %50 (sık compaction, test için ideal)
                    // PROD: 0.7 = %70 (daha az CPU kullanımı, dengeli)
                    //       0.9 = %90 (az compaction, daha fazla disk kullanımı)
                    { "min.cleanable.dirty.ratio", "0.5" },

                    // segment.ms: Active segment ne kadar süre sonra "closed" olur? (milisaniye)
                    // Sadece closed segment'ler compact edilebilir!
                    // DEV: 100 ms (test için çok hızlı compaction)
                    // PROD: 86400000 ms = 24 saat (standart)
                    //       604800000 ms = 7 gün (düşük trafikli sistemler)
                    { "segment.ms", "100" }

                    // PRODUCTION İÇİN EK ÖNERİLEN KONFIGÜRASYONLAR:

                    // delete.retention.ms: Tombstone mesajlar (null value) ne kadar saklanır?
                    // Tombstone = key silindiğini belirten marker
                    // PROD: 86400000 ms = 24 saat (consumer'ların silme işlemini görmesi için)
                    // { "delete.retention.ms", "86400000" },

                    // min.compaction.lag.ms: Mesaj ne kadar süre compact edilmeden kalır?
                    // Son mesajların yanlışlıkla compaction'da kaybolmaması için
                    // PROD: 60000 ms = 1 dakika (güvenlik için)
                    // { "min.compaction.lag.ms", "60000" },

                    // max.compaction.lag.ms: Mesaj maksimum ne kadar süre compact edilmeden kalabilir?
                    // PROD: 86400000 ms = 24 saat (düzenli compaction garantisi)
                    // { "max.compaction.lag.ms", "86400000" },

                    // segment.bytes: Segment boyutu (compaction performansını etkiler)
                    // PROD: 104857600 = 100 MB (küçük segment'ler, daha hızlı compaction)
                    // { "segment.bytes", "104857600" }
                }
            };

            // ============================================================
            // PRODUCTION BEST PRACTICE ÖRNEĞİ (COMPACT TOPIC)
            // ============================================================
            var compactTopicProduction = new TopicSpecification
            {
                Name = "orders-compact-prod",
                NumPartitions = 12, // PROD: Daha fazla partition = daha iyi paralellik
                ReplicationFactor = 3, // PROD: Yüksek availability için minimum 3
                Configs = new Dictionary<string, string>
                {
                    { "cleanup.policy", "compact,delete" }, // Hem compaction hem retention
                    { "min.cleanable.dirty.ratio", "0.7" },
                    { "segment.ms", "86400000" }, // 24 saat
                    { "delete.retention.ms", "86400000" }, // 24 saat
                    { "min.compaction.lag.ms", "60000" }, // 1 dakika
                    { "max.compaction.lag.ms", "604800000" }, // 7 gün
                    { "segment.bytes", "104857600" }, // 100 MB
                    { "retention.ms", "2592000000" }, // 30 gün (compaction sonrası zaman bazlı silme)
                    { "compression.type", "snappy" }, // Disk ve network optimizasyonu
                    { "min.insync.replicas", "2" } // Güvenilir yazma için minimum 2 replica sync
                }
            };

            await adminClient.CreateTopicsAsync(new[]
            {
                timeBasedTopic,
                sizeBasedTopic,
                compactTopic
                // compactTopicProduction // Production için yorumu kaldır
            });

            Console.WriteLine("Topic'ler başarıyla oluşturuldu:");
            Console.WriteLine("- orders-time-based (Time-based retention: 7 gün)");
            Console.WriteLine("- orders-size-based (Size-based retention: 1 GB per partition)");
            Console.WriteLine("- orders-compact (Compact policy - Test konfigürasyonu)");
        }
        catch (Exception e)
        {
            Console.WriteLine($"Hata: {e.Message}");
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
}