using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMqConsumerSample.Abstractions;
using RabbitMqConsumerSample.Models;

namespace RabbitMqConsumerSample.Examples;

public sealed class PushNotificationConsumer(
    IServiceScopeFactory scopeFactory)
    : IRabbitMqConsumer<PushNotificationMessage>
{
    public RabbitMqConsumerDefinition Definition { get; } = new()
    {
        Exchange = "notifications",
        ExchangeType = ExchangeType.Topic,
        Queue = "push.notifications",
        RoutingKey = "push.send",
        DeadLetterExchange = "push.notifications.dlx",
        PrefetchCount = 10
    };

    public async Task HandleAsync(
        PushNotificationMessage message,
        CancellationToken cancellationToken)
    {
        // The hosted consumer is long-lived, so scoped dependencies
        // are resolved inside a new scope for every message.
        await using var scope = scopeFactory.CreateAsyncScope();

        var service = scope.ServiceProvider
            .GetRequiredService<IPushNotificationService>();

        await service.SendAsync(message, cancellationToken);
    }
}
