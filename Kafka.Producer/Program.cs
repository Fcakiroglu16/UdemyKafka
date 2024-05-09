// See https://aka.ms/new-console-template for more information

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka Producer");


var kafkaService = new KafkaService();
var topicName = "ack-topic";
await kafkaService.CreateTopicAsync(topicName);
await kafkaService.SendMessageWithAck(topicName);


Console.WriteLine("Mesajlar gönderilmiştir.");