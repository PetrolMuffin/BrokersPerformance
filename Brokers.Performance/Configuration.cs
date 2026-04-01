namespace Brokers.Performance;

public static class Configuration
{
    public static string RabbitMqHost => "localhost";
    public const int RabbitMqPort = 5672;

    public static string KafkaBootstrapServers => "localhost:9092";

    public static string NatsUrl => "nats://localhost:4222";
}
