# Avro Tools'u global tool olarak yükleyin
dotnet tool install --global Apache.Avro.Tools --version 1.12.1

# Kafka.Producer klasörüne gidin
cd Kafka.Producer

# Avro schema'dan C# kodu üretin
avrogen -s Schemas\OrderCreatedEvent.avsc .




Backward
producer tarafında, yeni alanlar ekleyebilirsin, ancak mevcut alanları kaldıramazsın veya türlerini değiştiremezsin.
consumer=> producer tarafında var olan bir alan kaldırılırsa, producer bu datayı kafkaya gönderebiliyor, ama consumer okuyamıyor.