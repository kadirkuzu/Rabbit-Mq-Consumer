using RabbitMQ.Client;

namespace RabbitMqConsumerSample.Abstractions;

public interface IRabbitMqConnection : IDisposable
{
    IConnection Connection { get; }
}
