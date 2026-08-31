using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqConsumerSample.Abstractions;
using RabbitMqConsumerSample.Models;

namespace RabbitMqConsumerSample;

public sealed class RabbitMqConsumer<TMessage> : BackgroundService
{
    private readonly IRabbitMqConnection _connection;
    private readonly IRabbitMqConsumer<TMessage> _handler;
    private readonly ILogger<RabbitMqConsumer<TMessage>> _logger;

    private IModel? _channel;
    private string? _consumerTag;
    private CancellationToken _stoppingToken;

    public RabbitMqConsumer(
        IRabbitMqConnection connection,
        IRabbitMqConsumer<TMessage> handler,
        ILogger<RabbitMqConsumer<TMessage>> logger)
    {
        _connection = connection;
        _handler = handler;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        _stoppingToken = stoppingToken;

        var definition = _handler.Definition;
        _channel = _connection.Connection.CreateModel();

        // Declare the exchange that receives published messages.
        _channel.ExchangeDeclare(
            exchange: definition.Exchange,
            type: definition.ExchangeType,
            durable: true,
            autoDelete: false);

        IDictionary<string, object>? queueArguments = null;

        // Route rejected messages to a dedicated dead-letter queue.
        if (!string.IsNullOrWhiteSpace(definition.DeadLetterExchange))
        {
            _channel.ExchangeDeclare(
                exchange: definition.DeadLetterExchange,
                type: ExchangeType.Fanout,
                durable: true,
                autoDelete: false);

            var deadLetterQueue = $"{definition.Queue}.dlq";

            _channel.QueueDeclare(
                queue: deadLetterQueue,
                durable: true,
                exclusive: false,
                autoDelete: false);

            _channel.QueueBind(
                queue: deadLetterQueue,
                exchange: definition.DeadLetterExchange,
                routingKey: string.Empty);

            queueArguments = new Dictionary<string, object>
            {
                ["x-dead-letter-exchange"] = definition.DeadLetterExchange
            };
        }

        // Declare and bind the main queue.
        _channel.QueueDeclare(
            queue: definition.Queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArguments);

        _channel.QueueBind(
            queue: definition.Queue,
            exchange: definition.Exchange,
            routingKey: definition.RoutingKey);

        // Limit the number of unacknowledged messages per consumer.
        _channel.BasicQos(
            prefetchSize: 0,
            prefetchCount: definition.PrefetchCount,
            global: false);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.Received += OnMessageAsync;

        // Acknowledge messages only after the handler completes.
        _consumerTag = _channel.BasicConsume(
            queue: definition.Queue,
            autoAck: false,
            consumer: consumer);

        _logger.LogInformation(
            "RabbitMQ consumer started. Queue: {Queue}",
            definition.Queue);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            // Expected during graceful shutdown.
        }
    }

    private async Task OnMessageAsync(
        object sender,
        BasicDeliverEventArgs args)
    {
        try
        {
            var message = JsonSerializer.Deserialize<TMessage>(args.Body.Span)
                ?? throw new JsonException("Message could not be deserialized.");

            await _handler.HandleAsync(message, _stoppingToken);

            // The message was processed successfully.
            Acknowledge(args.DeliveryTag);
        }
        catch (OperationCanceledException)
            when (_stoppingToken.IsCancellationRequested)
        {
            // Requeue messages interrupted by application shutdown.
            Reject(args.DeliveryTag, requeue: true);
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "RabbitMQ message processing failed. Queue: {Queue}, DeliveryTag: {DeliveryTag}",
                _handler.Definition.Queue,
                args.DeliveryTag);

            // With DLX configured, requeue:false routes the message to the DLQ.
            Reject(args.DeliveryTag, requeue: false);
        }
    }

    private void Acknowledge(ulong deliveryTag)
    {
        if (_channel?.IsOpen == true)
            _channel.BasicAck(deliveryTag, multiple: false);
    }

    private void Reject(ulong deliveryTag, bool requeue)
    {
        if (_channel?.IsOpen == true)
            _channel.BasicNack(deliveryTag, multiple: false, requeue);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_channel?.IsOpen == true && !string.IsNullOrWhiteSpace(_consumerTag))
            _channel.BasicCancel(_consumerTag);

        await base.StopAsync(cancellationToken);
    }

    public override void Dispose()
    {
        try
        {
            _channel?.Close();
        }
        catch
        {
            // The channel may already be closed during shutdown.
        }

        _channel?.Dispose();
        base.Dispose();
    }
}
