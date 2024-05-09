using System.Text;
using Confluent.Kafka;
using Kafka.Consumer.Events;

namespace Kafka.Consumer
{
    internal class KafkaService
    {
        internal async Task ConsumeSimpleMessageWithNullKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"gelen mesaj : {consumeResult.Message.Value}");
                }

                await Task.Delay(500);
            }
        }


        internal async Task ConsumeSimpleMessageWithIntKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<int, string>(config).Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine(
                        $"gelen mesaj :Key={consumeResult.Message.Key} Value={consumeResult.Message.Value}");
                }

                await Task.Delay(500);
            }
        }

        internal async Task ConsumeComplexMessageWithIntKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>()).Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    var orderCreatedEvent = consumeResult.Message.Value;

                    Console.WriteLine(
                        $"gelen mesaj : {orderCreatedEvent.UserId} - {orderCreatedEvent.OrderCode} - {orderCreatedEvent.TotalPrice}");
                }

                await Task.Delay(10);
            }
        }


        internal async Task ConsumeComplexMessageWithIntKeyAndHeader(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>()).Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    var correlationId =
                        Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("correlation_id"));

                    var version = Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("version"));


                    // 2. yol
                    //var correlationId2 = consumeResult.Message.Headers[0].GetValueBytes();

                    //var version2 = consumeResult.Message.Headers[1].GetValueBytes();


                    Console.WriteLine($"headers: correlation_id:{correlationId},version:{version}");


                    var orderCreatedEvent = consumeResult.Message.Value;

                    Console.WriteLine(
                        $"gelen mesaj : {orderCreatedEvent.UserId} - {orderCreatedEvent.OrderCode} - {orderCreatedEvent.TotalPrice}");
                }

                await Task.Delay(10);
            }
        }


        internal async Task ConsumeComplexMessageWithComplexKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<MessageKey, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
                .SetKeyDeserializer(new CustomKeyDeserializer<MessageKey>())
                .Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    var messageKey = consumeResult.Message.Key;

                    Console.WriteLine(
                        $"gelen mesaj(key)=> key1 :{messageKey.Key1}, key2:{messageKey.Key2}");


                    var orderCreatedEvent = consumeResult.Message.Value;

                    Console.WriteLine(
                        $"gelen mesaj(value) => {orderCreatedEvent.UserId} - {orderCreatedEvent.OrderCode} - {orderCreatedEvent.TotalPrice}");
                }

                await Task.Delay(10);
            }
        }


        internal async Task ConsumeMessageWithTimestamp(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<MessageKey, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
                .SetKeyDeserializer(new CustomKeyDeserializer<MessageKey>())
                .Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"Message Timestamp : {consumeResult.Message.Timestamp.UtcDateTime}");
                }

                await Task.Delay(10);
            }
        }


        internal async Task ConsumeMessageFromSpecificPartition(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            //consumer.Subscribe(topicName);
            consumer.Assign(new TopicPartition(topicName, new Partition(2)));
            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"Message Timestamp : {consumeResult.Message.Value}");
                }

                await Task.Delay(10);
            }
        }


        internal async Task ConsumeMessageFromSpecificPartitionOffset(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "group-4",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            //consumer.Subscribe(topicName);
            consumer.Assign(new TopicPartitionOffset(topicName, 2, 0));
            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"Message Timestamp : {consumeResult.Message.Value}");
                }

                await Task.Delay(10);
            }
        }


        internal async Task ConsumeMessageWithAct(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "group-4",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    try
                    {
                        Console.WriteLine($"Message Timestamp : {consumeResult.Message.Value}");

                        consumer.Commit(consumeResult);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        throw;
                    }
                }

                await Task.Delay(10);
            }
        }


        internal async Task ConsumeMessageFromCluster(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:7000,localhost:7001,localhost:7002",
                GroupId = "group-x",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    try
                    {
                        Console.WriteLine($"Message Timestamp : {consumeResult.Message.Value}");

                        consumer.Commit(consumeResult);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        throw;
                    }
                }

                await Task.Delay(10);
            }
        }
    }
}