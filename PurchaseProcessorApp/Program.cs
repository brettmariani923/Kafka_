using Common;
using Common.Kafka;
using Confluent.Kafka;

class Program
{
    static async Task Main(string[] args)
    {
        var consumerConfig = KafkaConfig.BuildConsumer(groupId: "purchase-processor"); //create consumer config object w/ settings
        var producerConfig = KafkaConfig.BuildProducer(); //producer config object

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build(); //creates consumer using the config object 
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build(); //producer

        var cts = new CancellationTokenSource(); 
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); }; // Ctrl+C sets cancellation token

        consumer.Subscribe(KafkaTopics.Purchases); //subscribe to purchases
        Console.WriteLine($"🔍 Listening to topic: {KafkaTopics.Purchases}");

        var validator = new Validator(); //check json and required fields

        try
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    var cr = consumer.Consume(cts.Token); //consume message, blocking call that waits for new message or cancellation
                    Console.WriteLine($"📥 Received: {cr.Message.Value}");

                    if (!validator.TryValidate(cr.Message.Value, out var evt)) //validates the message, if invalid log error and skip to next message, evt is the strongly typed result if validation succeeds
                    {
                        Console.WriteLine($"❌ Invalid: {cr.Message.Value}");
                        continue;
                    }

                    await producer.ProduceAsync( //Forwards event to Analytics Topic
                        KafkaTopics.Analytics,
                        new Message<string, string> { Key = evt!.UserId, Value = cr.Message.Value });

                    Console.WriteLine($"✅ Forwarded to {KafkaTopics.Analytics}: {evt.UserId} bought {evt.Item}");
                }
                catch (ConsumeException ex) //error handling for consume errors
                {
                    Console.WriteLine($"💥 Kafka error: {ex.Error.Reason}");
                }
            }
        }
        catch (OperationCanceledException) //graceful shutdown on cancellation
        {
            Console.WriteLine("🛑 Graceful shutdown.");
        }
        finally
        {
            consumer.Close();
        }
    }
}
