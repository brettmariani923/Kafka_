using Common.Models;
using Confluent.Kafka;
using System.Text.Json;

namespace Common;

public class KafkaProducerService
{
    //sends messages to Kafka topics
    private readonly IProducer<string, string> _producer; //key and value are strings

    public KafkaProducerService(ProducerConfig config) =>
        _producer = new ProducerBuilder<string, string>(config).Build(); //sets up connection to kafka, constructor, takes a producerconfig object, uses it to builded _producer

    public async Task ProducePurchaseAsync(PurchaseEvent e) //takes a purchase event object as parameter, serializes into json (so it can be sent as text), calls _producer.ProduceAsync to send the message to the purchases topic, using the user id as the key and the serialized json as the value
    {
        var json = JsonSerializer.Serialize(e);
        await _producer.ProduceAsync(
            KafkaTopics.Purchases,
            new Message<string, string> { Key = e.UserId, Value = json });
    }

    public void Flush() => _producer.Flush(TimeSpan.FromSeconds(5)); //makes sure any buffered messages are delivered before shutting down
}
