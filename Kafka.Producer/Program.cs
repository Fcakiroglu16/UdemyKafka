// See https://aka.ms/new-console-template for more information

#region

using Kafka.Producer;

#endregion

Console.WriteLine("Kafka Producer");


var kafkaService = new KafkaService();
await kafkaService.SendMessage();
//await kafkaService.SendMessageV2Deleted();
//await kafkaService.SendMessageV3();
//await kafkaService.SendMessageV4();
Console.ReadLine();