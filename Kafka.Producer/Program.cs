// See https://aka.ms/new-console-template for more information

#region

using Kafka.Producer;

#endregion

Console.WriteLine("Kafka Producer");


var kafkaService = new KafkaService();
await kafkaService.CreateTopicsWithDifferentPoliciesAsync();
Console.ReadLine();