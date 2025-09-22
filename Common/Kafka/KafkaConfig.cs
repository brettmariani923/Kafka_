using Confluent.Kafka;

namespace Common.Kafka;

public static class KafkaConfig
{
    //Helper to read required env vars and throw if missing
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

    public static ProducerConfig BuildProducer()
    {
        var (bootstrap, user, pass) = ReadRequired();
        return new ProducerConfig
        {
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = user,
            SaslPassword = pass,
            Acks = Acks.All
        };
    }

    public static ConsumerConfig BuildConsumer(string groupId, AutoOffsetReset offset = AutoOffsetReset.Earliest)
    {
        var (bootstrap, user, pass) = ReadRequired();
        return new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = user,
            SaslPassword = pass,
            GroupId = groupId,
            AutoOffsetReset = offset,
            EnableAutoCommit = true,
            SessionTimeoutMs = 45_000,
            SocketKeepaliveEnable = true
        };
    }
}
