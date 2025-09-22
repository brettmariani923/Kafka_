using Common;
using Common.Kafka;
using Common.Models;
using Confluent.Kafka;
using System.Text.Json;

class Program
{
    static void Main(string[] args)
    {
        //analytics consumer,final stage of the pipeline
        var config = KafkaConfig.BuildConsumer(groupId: "analytics-consumer"); //builds consumer config object w/ settings

        var userPurchaseCount = new Dictionary<string, int>(); //in-memory dictionaries to track purchases per user and item counts
        var itemCount = new Dictionary<string, int>();

        using var consumer = new ConsumerBuilder<string, string>(config).Build(); //creates consumer using the config object (key/ string value)
        consumer.Subscribe(KafkaTopics.Analytics); //subscribe to analytics topic

        Console.CancelKeyPress += (_, e) => { e.Cancel = true; consumer.Close(); }; //Ctrl+C shutdown

        Console.WriteLine($"📊 Listening to topic: {KafkaTopics.Analytics}");

        while (true) //main loop to consume messages, 
        {
            try
            {
                var cr = consumer.Consume(CancellationToken.None); //keeps reading messages, blocking call that waits for new message
                var evt = JsonSerializer.Deserialize<PurchaseEvent>(cr.Message.Value); //deserializes json payload into PurchaseEvent object
                if (evt is null) //skips invalid json messages
                {
                    Console.WriteLine("⚠️ Skipped invalid JSON."); 
                    continue;
                }

                userPurchaseCount[evt.UserId] = userPurchaseCount.GetValueOrDefault(evt.UserId) + 1; //updates in-memory counts
                itemCount[evt.Item] = itemCount.GetValueOrDefault(evt.Item) + 1;

                Console.WriteLine($"🧾 {evt.UserId} bought {evt.Item}"); //logs each purchase event

                if ((userPurchaseCount[evt.UserId] + itemCount[evt.Item]) % 5 == 0) //every 5th event, prints current analytics snapshot
                {
                    Console.WriteLine("\n📈 Current Analytics Snapshot:");
                    Console.WriteLine("👥 Purchases per user:"); //how many purchases made by each user
                    foreach (var kv in userPurchaseCount)
                        Console.WriteLine($"  - {kv.Key}: {kv.Value}");

                    Console.WriteLine("📦 Items purchased:"); //how many times each item has been purchased
                    foreach (var kv in itemCount)
                        Console.WriteLine($"  - {kv.Key}: {kv.Value}");
                    Console.WriteLine();
                }
            }
            catch (ConsumeException ex) //error handling 
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
