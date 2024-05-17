using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Shared.Events;

namespace Order.API.Services
{
    public class Bus(IConfiguration configuration, ILogger<Bus> logger) : IBus
    {
        private readonly ProducerConfig _config = new()
        {
            BootstrapServers = configuration.GetSection("BusSettings").GetSection("Kafka")["BootstrapServers"],
            Acks = Acks.All,
            MessageTimeoutMs = 6000,
            AllowAutoCreateTopics = true
        };

        public async Task<bool> Publish<T1, T2>(T1 key, T2 value, string topicOrQueueName)
        {
            using var producer = new ProducerBuilder<T1, T2>(_config).SetKeySerializer(new CustomKeySerializer<T1>())
                .SetValueSerializer(new CustomValueSerializer<T2>()).Build();

            var message = new Message<T1, T2>()
            {
                Key = key,
                Value = value
            };

            var result = await producer.ProduceAsync(topicOrQueueName, message);
        
            return result.Status == PersistenceStatus.Persisted;
        }


        public async Task CreateTopicOrQueue(List<string> topicOrQueueNameList)

        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = configuration.GetSection("BusSettings").GetSection("Kafka")["BootstrapServers"]
            }).Build();

            try
            {
                foreach (var topicOrQueueName in topicOrQueueNameList)
                {
                    await adminClient.CreateTopicsAsync(new[]
                    {
                        new TopicSpecification()
                        {
                            Name = topicOrQueueName, NumPartitions = 6, ReplicationFactor = 1
                        }
                    });


                    logger.LogInformation($"Topic({topicOrQueueName}) oluştu.");
                }
            }
            catch (Exception e)
            {
                logger.LogWarning(e.Message);
            }
        }
    }
}