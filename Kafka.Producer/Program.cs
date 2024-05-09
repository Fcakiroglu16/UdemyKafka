// See https://aka.ms/new-console-template for more information

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka Producer");


var kafkaService = new KafkaService();
var topicName = "d-topic";
//await kafkaService.CreateTopicAsync(topicName);
await kafkaService.SendMessageWithRetry(topicName);
//Console.ReadLine();

Console.WriteLine("Mesajlar gönderilmiştir.");