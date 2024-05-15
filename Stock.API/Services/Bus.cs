using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Shared.Events;

namespace Stock.API.Services
{
    public class Bus(IConfiguration configuration) : IBus
    {
        public ConsumerConfig GetConsumerConfig(string groupId)
        {
            return new ConsumerConfig()
            {
                BootstrapServers = configuration.GetSection("BusSettings").GetSection("Kafka")["BootstrapServers"],
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };
        }
    }
}