using System.Globalization;
using System.Text;
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
                BootstrapServers = "localhost:7000"
            }).Build();

            try
            {
                var config = new Dictionary<string, string>()
                {
                    { "min.insync.replicas", "2" }
                };


                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification()
                    {
                        Name = topicName, NumPartitions = 6, ReplicationFactor = 3, Configs = config
                    }
                });
                Console.WriteLine($"Topic({topicName}) oluştu.");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        internal async Task CreateTopicWithRetentionAsync(string topicName)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:9094"
            }).Build();

            try
            {
                // topic link : https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html

                TimeSpan day30Span = TimeSpan.FromDays(30);


                var configs = new Dictionary<string, string>()
                {
                    //{"retention.bytes","10000"} // topic partition byte cinsinden 10kb
                    //{ "retention.ms", "-1" } // ömür boyu kafka da kalır
                    { "retention.ms", day30Span.TotalMicroseconds.ToString() } // 30 gün
                };


                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification()
                    {
                        Name = topicName, NumPartitions = 6, ReplicationFactor = 1, Configs = configs
                    }
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


        internal async Task SendComplexMessageWithIntKeyAndHeader(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
                .Build();


            foreach (var item in Enumerable.Range(1, 3))
            {
                var orderCreatedEvent = new OrderCreatedEvent()
                    { OrderCode = Guid.NewGuid().ToString(), TotalPrice = item * 100, UserId = item };


                var header = new Headers
                {
                    { "correlation_id", Encoding.UTF8.GetBytes("123") },
                    { "version", Encoding.UTF8.GetBytes("v1") }
                };

                var message = new Message<int, OrderCreatedEvent>()
                {
                    Value = orderCreatedEvent,
                    Key = item,
                    Headers = header
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


        internal async Task SendComplexMessageWithComplexKey(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<MessageKey, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
                .SetKeySerializer(new CustomKeySerializer<MessageKey>())
                .Build();


            foreach (var item in Enumerable.Range(1, 3))
            {
                var orderCreatedEvent = new OrderCreatedEvent()
                    { OrderCode = Guid.NewGuid().ToString(), TotalPrice = item * 100, UserId = item };


                var message = new Message<MessageKey, OrderCreatedEvent>()
                {
                    Value = orderCreatedEvent,
                    Key = new MessageKey("key1 value", "key2 value"),
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


        internal async Task SendMessageWithTimestamp(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<MessageKey, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
                .SetKeySerializer(new CustomKeySerializer<MessageKey>())
                .Build();


            foreach (var item in Enumerable.Range(1, 3))
            {
                var orderCreatedEvent = new OrderCreatedEvent()
                    { OrderCode = Guid.NewGuid().ToString(), TotalPrice = item * 100, UserId = item };


                var message = new Message<MessageKey, OrderCreatedEvent>()
                {
                    Value = orderCreatedEvent,
                    Key = new MessageKey("key1 value", "key2 value"),
                    //Timestamp = new Timestamp(new DateTime(2012,02,02))
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


        internal async Task SendMessageToSpecificPartition(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };

            using var producer = new ProducerBuilder<Null, string>(config).Build();


            foreach (var item in Enumerable.Range(1, 10))
            {
                var message = new Message<Null, string>()
                {
                    Value = $"Mesaj {item}"
                };


                var topicPartition = new TopicPartition(topicName, new Partition(2));

                var result = await producer.ProduceAsync(topicPartition, message);


                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("-----------------------------------");
                await Task.Delay(10);
            }
        }


        internal async Task SendMessageWithAck(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094", Acks = Acks.All };

            using var producer = new ProducerBuilder<Null, string>(config).Build();


            foreach (var item in Enumerable.Range(1, 10))
            {
                var message = new Message<Null, string>()
                {
                    Value = $"Mesaj {item}"
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


        internal async Task SendMessageWithRetry(string topicName)
        {
            var config = new ProducerConfig()
                { BootstrapServers = "localhost:7000", Acks = Acks.All };


            // MessageTimeoutMs=3000;
            //MessageSendMaxRetries = 2 
            //RetryBackoffMs = 1000

            using var producer = new ProducerBuilder<Null, string>(config).Build();

            var message = new Message<Null, string>()
            {
                Value = $"Mesaj {DateTime.Now}"
            };


            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }

            //while (true)
            //{
            //    var message = new Message<Null, string>()
            //    {
            //        Value = $"Mesaj {DateTime.Now}"
            //    };

            //    // you can write exit with args

            //    if (Console.ReadLine() == "send")
            //    {
            //        var topicPartition = new TopicPartition("topicName", new Partition(1));

            //        try
            //        {
            //            var result = await producer.ProduceAsync(topicPartition, message);

            //            foreach (var propertyInfo in result.GetType().GetProperties())
            //            {
            //                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            //            }
            //        }
            //        catch (Exception e)
            //        {
            //            Console.WriteLine($"hata var :{e.Message}");
            //        }


            //        Console.WriteLine("-----------------------------------");
            //    }
            //    else
            //    {
            //        break;
            //    }
            //}
        }
    }
}