// See https://aka.ms/new-console-template for more information

#region

using Kafka.Consumer;

#endregion

Console.WriteLine("Kafka Consumer 1");
var kafkaService = new KafkaService();

await kafkaService.Consume();

Console.ReadLine();