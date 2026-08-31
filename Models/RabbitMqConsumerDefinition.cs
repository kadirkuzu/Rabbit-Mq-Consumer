namespace RabbitMqConsumerSample.Models;

public sealed class RabbitMqConsumerDefinition
{
    public required string Exchange { get; init; }
    public required string ExchangeType { get; init; }

    public required string Queue { get; init; }
    public required string RoutingKey { get; init; }

    public string? DeadLetterExchange { get; init; }
    public ushort PrefetchCount { get; init; } = 10;
}
