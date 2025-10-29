#region

using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Kafka.Producer.Events;

#endregion

namespace Kafka.Consumer;

internal class KafkaService
{
    internal async Task Consume()
    {
        var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://localhost:8081" };
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9094",
            GroupId = "order-processor",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var consumer = new ConsumerBuilder<string, OrderCreatedEvent>(consumerConfig)
            .SetValueDeserializer(new AvroDeserializer<OrderCreatedEvent>(schemaRegistry).AsSyncOverAsync())
            .Build();

        consumer.Subscribe("orders-topic");


        var cts = new CancellationTokenSource();
        while (!cts.IsCancellationRequested)
        {
            var consumeResult = consumer.Consume(cts.Token);

            // Veri otomatik olarak OrderCreatedEvent nesnesine dönüştü!
            var eventMessage = consumeResult.Message.Value;

            Console.WriteLine($"Gelen Sipariş: {eventMessage.OrderId}, Müşteri: {eventMessage.CustomerId}");
        }
    }
}