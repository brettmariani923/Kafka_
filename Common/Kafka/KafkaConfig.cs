using Confluent.Kafka;

namespace Common.Kafka;

public static class KafkaConfig
{
    //helper to setup required env vars and read them, throws if missing
    public static (string Bootstrap, string User, string Pass) ReadRequired()
    {
        string? bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP");
        string? user = Environment.GetEnvironmentVariable("KAFKA_USERNAME");
        string? pass = Environment.GetEnvironmentVariable("KAFKA_PASSWORD");

        if (string.IsNullOrWhiteSpace(bootstrap))
            throw new InvalidOperationException("Missing env var KAFKA_BOOTSTRAP");
        if (string.IsNullOrWhiteSpace(user))
            throw new InvalidOperationException("Missing env var KAFKA_USERNAME");
        if (string.IsNullOrWhiteSpace(pass))
            throw new InvalidOperationException("Missing env var KAFKA_PASSWORD");

        return (bootstrap, user, pass);
    }

    //helper to build producer and consumer config objects with common settings
    public static ProducerConfig BuildProducer()
    {
        var (bootstrap, user, pass) = ReadRequired();
        return new ProducerConfig
        {
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl, //encrypted connection
            SaslMechanism = SaslMechanism.Plain, //username/password auth
            SaslUsername = user,
            SaslPassword = pass,
            Acks = Acks.All //ack is how many brokers must confirm they got the message before considering it sent
        };
    }

    public static ConsumerConfig BuildConsumer(string groupId, AutoOffsetReset offset = AutoOffsetReset.Earliest) //default to read from start if no offset stored
    {
        var (bootstrap, user, pass) = ReadRequired();
        return new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = user,
            SaslPassword = pass,
            GroupId = groupId, //consumers in same group share work of reading from partitions
            AutoOffsetReset = offset, //where to start if no offset is stored. offset is position in the log
            EnableAutoCommit = true, //Tells consumer to periodically commit offsets in background
            SessionTimeoutMs = 45_000, //consumer must send within this time
            SocketKeepaliveEnable = true //keep TCP connection alive
        };
    }
} 