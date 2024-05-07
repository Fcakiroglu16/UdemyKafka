using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer.Events;

namespace Kafka.Producer
{
    internal class KafkaService
    {
        internal async Task CreateTopicAsync(string topicName)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:9094"
            }).Build();

            try
            {
                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification() { Name = topicName, NumPartitions = 3, ReplicationFactor = 1 }
                });

                Console.WriteLine($"Topic({topicName}) oluştu.");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }


        internal async Task SendSimpleMessageWithNullKey(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<Null, string>(config).Build();


            foreach (var item in Enumerable.Range(1, 10))
            {
                var message = new Message<Null, string>()
                {
                    Value = $"Message(use case -1) - {item}"
                };

                var result = await producer.ProduceAsync(topicName, message);


                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("-----------------------------------");
                await Task.Delay(200);
            }
        }


        internal async Task SendSimpleMessageWithIntKey(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<int, string>(config).Build();


            foreach (var item in Enumerable.Range(1, 100))
            {
                var message = new Message<int, string>()
                {
                    Value = $"Message(use case -1) - {item}",
                    Key = item
                };

                var result = await producer.ProduceAsync(topicName, message);


                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("-----------------------------------");
                await Task.Delay(10);
            }
        }

        internal async Task SendComplexMessageWithIntKey(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
                .Build();


            foreach (var item in Enumerable.Range(1, 100))
            {
                var orderCreatedEvent = new OrderCreatedEvent()
                    { OrderCode = Guid.NewGuid().ToString(), TotalPrice = item * 100, UserId = item };


                var message = new Message<int, OrderCreatedEvent>()
                {
                    Value = orderCreatedEvent,
                    Key = item,
                };

                var result = await producer.ProduceAsync(topicName, message);


                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("-----------------------------------");
                await Task.Delay(10);
            }
        }
    }
}