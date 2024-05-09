// See https://aka.ms/new-console-template for more information

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka Producer");


var kafkaService = new KafkaService();
var topicName = "retry-topic";
//await kafkaService.CreateTopicRetryWithClusterAsync(topicName);
await kafkaService.SendMessageWithRetryToCluster(topicName);


Console.ReadLine();