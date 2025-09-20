using Common;
using Common.Kafka;
using Common.Models;
using Confluent.Kafka;

class Program
{
    static async Task Main(string[] args)
    {
        var producerConfig = KafkaConfig.BuildProducer();
        var producer = new KafkaProducerService(producerConfig);

        var random = new Random();
        string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka" };
        string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };

        for (int i = 0; i < 10; i++)
        {
            var user = users[random.Next(users.Length)];
            var item = items[random.Next(items.Length)];

            var purchase = new PurchaseEvent
            {
                UserId = user,
                Item = item,
                Timestamp = DateTime.UtcNow
            };

            await producer.ProducePurchaseAsync(purchase);
        }

        producer.Flush();
        Console.WriteLine("✅ Producer done.");
    }
}
