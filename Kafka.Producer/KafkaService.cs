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
                CustomerId = $"CUST-{i + 100}",
                Amount = 199.99 + (i * 10)
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