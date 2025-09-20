using Common;
using Common.Kafka;
using Common.Models;
using Confluent.Kafka;
using System.Text.Json;

class Program
{
    static void Main(string[] args)
    {
        var config = KafkaConfig.BuildConsumer(groupId: "analytics-consumer");

        var userPurchaseCount = new Dictionary<string, int>();
        var itemCount = new Dictionary<string, int>();

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(KafkaTopics.Analytics);

        Console.CancelKeyPress += (_, e) => { e.Cancel = true; consumer.Close(); };

        Console.WriteLine($"📊 Listening to topic: {KafkaTopics.Analytics}");

        while (true)
        {
            try
            {
                var cr = consumer.Consume(CancellationToken.None);
                var evt = JsonSerializer.Deserialize<PurchaseEvent>(cr.Message.Value);
                if (evt is null)
                {
                    Console.WriteLine("⚠️ Skipped invalid JSON.");
                    continue;
                }

                userPurchaseCount[evt.UserId] = userPurchaseCount.GetValueOrDefault(evt.UserId) + 1;
                itemCount[evt.Item] = itemCount.GetValueOrDefault(evt.Item) + 1;

                Console.WriteLine($"🧾 {evt.UserId} bought {evt.Item}");

                if ((userPurchaseCount[evt.UserId] + itemCount[evt.Item]) % 5 == 0)
                {
                    Console.WriteLine("\n📈 Current Analytics Snapshot:");
                    Console.WriteLine("👥 Purchases per user:");
                    foreach (var kv in userPurchaseCount)
                        Console.WriteLine($"  - {kv.Key}: {kv.Value}");

                    Console.WriteLine("📦 Items purchased:");
                    foreach (var kv in itemCount)
                        Console.WriteLine($"  - {kv.Key}: {kv.Value}");
                    Console.WriteLine();
                }
            }
            catch (ConsumeException ex)
            {
                Console.WriteLine($"💥 Kafka error: {ex.Error.Reason}");
            }
            catch (JsonException)
            {
                Console.WriteLine("⚠️ Failed to parse JSON.");
            }
        }
    }
}
