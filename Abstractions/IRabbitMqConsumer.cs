using RabbitMqConsumerSample.Models;

namespace RabbitMqConsumerSample.Abstractions;

public interface IRabbitMqConsumer<TMessage>
{
    RabbitMqConsumerDefinition Definition { get; }

    Task HandleAsync(
        TMessage message,
        CancellationToken cancellationToken);
}
