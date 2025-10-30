# Avro Tools'u global tool olarak yükleyin
dotnet tool install --global Apache.Avro.Tools --version 1.12.1

# Kafka.Producer klasörüne gidin
cd Kafka.Producer

# Avro schema'dan C# kodu üretin
avrogen -s Schemas\OrderCreatedEvent.avsc .




Backward
producer tarafında, yeni alanlar ekleyebilirsin, ancak mevcut alanları kaldıramazsın veya türlerini değiştiremezsin.

Forward
producter tarafında yeni bir alan ekliyorum ama nullable değil,backward hata veriyor, forward yaptığımda hata vermiyor











Producer açısındaın uyumluluk


Producer : var olan alanlarıdeğiştirmek, hem backward,hemde forward uyumluluk modlarında hata verir

Backward
Produecer : Senaryo 1: Alan Silmek (Başarılı ✅), Consumer :  hiçbir uyumluluk modunda v2 mesajlarını okuyamaz
Producer :  Senaryo 2 : nullable olmayan alan eklemek (başarısız)
producer :  Senaryo 3 : nullable alan eklemek (başarılı ✅), consumer : tüm uyumluluk modlarında v2 mesajlarını okuyabilir

Forward
Producer :  Senaryo 2 : nullable olmayan alan eklemek (başarılı),consumer : okuyabiliyor
  


