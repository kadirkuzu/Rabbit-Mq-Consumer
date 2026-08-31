# Generic RabbitMQ Consumer for .NET

A small, strongly typed RabbitMQ consumer infrastructure built on .NET `BackgroundService`.

The generic consumer owns the messaging concerns while concrete consumers focus only on their queue definition and business operation.

```text
Publisher -> Topic Exchange -> Queue -> RabbitMqConsumer<TMessage>
                                          |
                                          v
                              IRabbitMqConsumer<TMessage>
                                          |
                                          v
                                    Business Service
```

## Included features

- Durable exchange and queue declaration
- Routing-key binding
- Strongly typed JSON deserialization
- Manual ACK/NACK
- Configurable prefetch/QoS
- Dead-letter exchange and queue
- Graceful hosted-service shutdown
- Per-message dependency injection scope

## Consumer contract

Each concrete consumer provides its topology and message handler:

```csharp
public interface IRabbitMqConsumer<TMessage>
{
    RabbitMqConsumerDefinition Definition { get; }

    Task HandleAsync(
        TMessage message,
        CancellationToken cancellationToken);
}
```

See [`PushNotificationConsumer`](Examples/PushNotificationConsumer.cs) for a concrete example.

## Registration

Register the typed handler and its generic hosted consumer:

```csharp
services.AddSingleton<
    IRabbitMqConsumer<PushNotificationMessage>,
    PushNotificationConsumer>();

services.AddHostedService<
    RabbitMqConsumer<PushNotificationMessage>>();
```

`IRabbitMqConnection` should be registered as a singleton so the application reuses a long-lived RabbitMQ connection.

## Delivery semantics

The sample uses manual acknowledgements and therefore follows an **at-least-once** processing model. Production consumers should be idempotent. If publishing must be atomic with a database transaction, use the Transactional Outbox pattern together with publisher confirms.
