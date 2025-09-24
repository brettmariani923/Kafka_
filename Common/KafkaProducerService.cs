using Common.Models;
using Confluent.Kafka;
using System.Text.Json;

namespace Common;

public class KafkaProducerService 
{
    private readonly IProducer<string, string> _producer; //key and value strings, IProducer<TKey, TValue> is interface for sending messages 

    public KafkaProducerService(ProducerConfig config) =>
        _producer = new ProducerBuilder<string, string>(config).Build(); //sets up connection to kafka using constructor, takes a producerconfig object, uses it to builded _producer

    public async Task ProducePurchaseAsync(PurchaseEvent e) 
    {
        var json = JsonSerializer.Serialize(e); //converts purchase event object to json string
        //calls _producer.ProduceAsync to send the message to the purchases topic, using the user id as the key and the serialized json as the value
        await _producer.ProduceAsync(
            KafkaTopics.Purchases,
            new Message<string, string> { Key = e.UserId, Value = json });
    }

    public void Flush() => _producer.Flush(TimeSpan.FromSeconds(5)); //makes sure any buffered messages are delivered before shutting down
}