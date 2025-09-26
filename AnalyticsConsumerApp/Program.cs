using Common;
using Common.Kafka;
using Common.Models;
using Confluent.Kafka;
using System.Text.Json;

class Program
{
    static void Main(string[] args)
    {
        var config = KafkaConfig.BuildConsumer(groupId: "analytics-consumer-dev1"); //configs from env vars

        var userPurchaseCount = new Dictionary<string, int>();
        var itemCount = new Dictionary<string, int>();

        using var consumer = new ConsumerBuilder<string, string>(config).Build(); //build consumer

        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        consumer.Subscribe(KafkaTopics.Analytics);
        Console.WriteLine($"📊 Listening to topic: {KafkaTopics.Analytics}"); // subscribe to analytics topic

        try
        {
            while (!cts.IsCancellationRequested)
            {
                var cr = consumer.Consume(cts.Token); //waiting until a message is available or token is cancelled

                var evt = JsonSerializer.Deserialize<PurchaseEvent>(cr.Message.Value); // turn json string into strongly typed object

                userPurchaseCount[evt.UserId] = userPurchaseCount.GetValueOrDefault(evt.UserId) + 1; //update counts
                itemCount[evt.Item] = itemCount.GetValueOrDefault(evt.Item) + 1;

                Console.WriteLine($"🧾 {evt.UserId} bought {evt.Item}");

                if ((userPurchaseCount[evt.UserId] + itemCount[evt.Item]) % 2 == 0)
                {
                    Console.WriteLine("\n📈 Current Analytics Snapshot:");
                    Console.WriteLine("👥 Purchases per user:");
                    foreach (var kv in userPurchaseCount) Console.WriteLine($"  - {kv.Key}: {kv.Value}");
                    Console.WriteLine("📦 Items purchased:");
                    foreach (var kv in itemCount) Console.WriteLine($"  - {kv.Key}: {kv.Value}");
                    Console.WriteLine();
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
