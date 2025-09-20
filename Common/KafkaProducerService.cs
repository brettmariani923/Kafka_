using Common.Models;
using Confluent.Kafka;
using System.Text.Json;

namespace Common;

public class KafkaProducerService
{
    private readonly IProducer<string, string> _producer;

    public KafkaProducerService(ProducerConfig config) =>
        _producer = new ProducerBuilder<string, string>(config).Build();

    public async Task ProducePurchaseAsync(PurchaseEvent e)
    {
        var json = JsonSerializer.Serialize(e);
        await _producer.ProduceAsync(
            KafkaTopics.Purchases,
            new Message<string, string> { Key = e.UserId, Value = json });
    }

    public void Flush() => _producer.Flush(TimeSpan.FromSeconds(5));
}
