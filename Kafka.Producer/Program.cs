// See https://aka.ms/new-console-template for more information

#region

using Kafka.Producer;

#endregion

Console.WriteLine("Kafka Producer");


var kafkaService = new KafkaService();
//kafkaService.SendMessageAtMostOnce();
//await kafkaService.SendMessageAtLeastOnce();

await kafkaService.SendMessageExactlyOnce();
//Console.ReadLine();