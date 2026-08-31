namespace RabbitMqConsumerSample.Models;

public sealed record PushNotificationMessage(
    Guid UserId,
    string Title,
    string Body);
