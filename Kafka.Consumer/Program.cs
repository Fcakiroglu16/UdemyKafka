// See https://aka.ms/new-console-template for more information

using Kafka.Consumer;

Console.WriteLine("Kafka Consumer 1");
var topicName = "use-case-3-topic";
var kafkaService = new KafkaService();

await kafkaService.ConsumeComplexMessageWithIntKeyAndHeader(topicName);

Console.ReadLine();