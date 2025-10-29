# Avro Tools'u global tool olarak yükleyin
dotnet tool install --global Apache.Avro.Tools --version 1.12.1

# Kafka.Producer klasörüne gidin
cd Kafka.Producer

# Avro schema'dan C# kodu üretin
avrogen -s Schemas\OrderCreatedEvent.avsc .