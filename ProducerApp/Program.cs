using Common;
using Common.Kafka;
using Common.Models;

class Program
{
    static async Task Main(string[] args)
    {
        //data generation to simulate purchase events to Kafka topic "purchases"
        var producerConfig = KafkaConfig.BuildProducer(); //calls helper to create producer config object w/ settings
        var producer = new KafkaProducerService(producerConfig); //creates instance of kafka producer service using the config object

        var random = new Random();
        string[] users = { "sUchia", "bMariani", "hKakashi", "rLee", "nUzumaki" };
        string[] items = { "scrolls", "kunai", "shuriken", "food pills", "ramen" };

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
