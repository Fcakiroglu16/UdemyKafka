# Avro Tools'u global tool olarak yükleyin
dotnet tool install --global Apache.Avro.Tools --version 1.12.1

# Kafka.Producer klasörüne gidin
cd Kafka.Producer

# Avro schema'dan C# kodu üretin
avrogen -s Schemas\OrderCreatedEvent.avsc .


//Stream Example

CREATE STREAM order_created_stream (
    OrderId VARCHAR,
    CustomerId VARCHAR,
    Amount DOUBLE
) WITH (
    KAFKA_TOPIC='order-created-events',
    VALUE_FORMAT='AVRO'
);


SET 'auto.offset.reset' = 'earliest';

CREATE STREAM high_value_orders AS
SELECT *
FROM order_created_stream
WHERE Amount > 1000
EMIT CHANGES;




DROP STREAM HIGH_VALUE_ORDERS;


//Table Example

CREATE TABLE customer_total_orders AS
SELECT
    CustomerId,
    COUNT(*) AS OrderCount,
    SUM(Amount) AS TotalAmount
FROM order_created_stream
GROUP BY CustomerId
EMIT CHANGES;

  


