using RabbitMqConsumerSample.Models;

namespace RabbitMqConsumerSample.Examples;

public interface IPushNotificationService
{
    Task SendAsync(
        PushNotificationMessage message,
        CancellationToken cancellationToken);
}
