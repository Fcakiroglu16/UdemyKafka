using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kafka.Producer
{
    internal class KafkaService
    {
        private const string TopicName = "mytopic";


        internal async Task CreateTopicAsync()
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:9094"
            }).Build();

            try
            {
                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification() { Name = TopicName, NumPartitions = 3, ReplicationFactor = 1 }
                });

                Console.WriteLine($"Topic({TopicName}) oluştu.");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }
}