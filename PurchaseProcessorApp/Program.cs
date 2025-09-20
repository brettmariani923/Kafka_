using Common;
using Common.Kafka;
using Confluent.Kafka;

class Program
{
    static async Task Main(string[] args)
    {
        var consumerConfig = KafkaConfig.BuildConsumer(groupId: "purchase-processor");
        var producerConfig = KafkaConfig.BuildProducer();

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        consumer.Subscribe(KafkaTopics.Purchases);
        Console.WriteLine($"🔍 Listening to topic: {KafkaTopics.Purchases}");

        var validator = new Validator();

        try
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    var cr = consumer.Consume(cts.Token);
                    Console.WriteLine($"📥 Received: {cr.Message.Value}");

                    if (!validator.TryValidate(cr.Message.Value, out var evt))
                    {
                        Console.WriteLine($"❌ Invalid: {cr.Message.Value}");
                        continue;
                    }

                    await producer.ProduceAsync(
                        KafkaTopics.Analytics,
                        new Message<string, string> { Key = evt!.UserId, Value = cr.Message.Value });

                    Console.WriteLine($"✅ Forwarded to {KafkaTopics.Analytics}: {evt.UserId} bought {evt.Item}");
                }
                catch (ConsumeException ex)
                {
                    Console.WriteLine($"💥 Kafka error: {ex.Error.Reason}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("🛑 Graceful shutdown.");
        }
        finally
        {
            consumer.Close();
        }
    }
}
