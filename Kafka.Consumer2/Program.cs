// See https://aka.ms/new-console-template for more information

using Kafka.Consumer2;

Console.WriteLine("Kafka Consumer 2");
var topicName = "use-case-1.1-topic";
var kafkaService = new KafkaService();

await kafkaService.ConsumeSimpleMessageWithNullKey(topicName);

Console.ReadLine();