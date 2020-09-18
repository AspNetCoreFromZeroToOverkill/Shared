using System;
using System.Threading;
using System.Threading.Tasks;
using CodingMilitia.PlayBall.Shared.EventBus.Kafka.Configuration;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CodingMilitia.PlayBall.Shared.EventBus.Kafka
{
public class KafkaEventConsumer<TKey, TEvent> : IEventConsumer<TEvent>
{
    private readonly string _topic;
    private readonly ILogger<KafkaEventConsumer<TKey, TEvent>> _logger;
    private readonly ConsumerConfig _consumerConfig;
    private readonly IDeserializer<TKey>? _keyDeserializer;
    private readonly IDeserializer<TEvent> _valueDeserializer;

    public KafkaEventConsumer(
        string topic,
        KafkaSettings settings,
        KafkaConsumerSettings consumerSettings,
        IDeserializer<TKey>? keyDeserializer,
        IDeserializer<TEvent> valueDeserializer,
        ILogger<KafkaEventConsumer<TKey, TEvent>> logger)
    {
        _topic = topic;
        _keyDeserializer = keyDeserializer;
        _valueDeserializer = valueDeserializer;
        _logger = logger;
        _consumerConfig = new ConsumerConfig
        {
            GroupId = consumerSettings.ConsumerGroup,
            BootstrapServers = string.Join(",", settings.BootstrapServers),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
    }

    public Task Subscribe(Action<TEvent> callback, CancellationToken ct)
    {
        var consumerLoopCompletionSource =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        // polling for messages is a blocking operation,
        // so spawning a new thread to keep doing it in the background
        var thread = new Thread(() =>
        {
            // creating a consumer on subscribe, so if we call it multiple times, it'll create one consumer per subscription
            using var consumer = CreateConsumer();

            _logger.LogDebug("Subscribing...");
            consumer.Subscribe(_topic);
            _logger.LogDebug("Subscribed");

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    _logger.LogDebug("Waiting for message...");

                    var result = consumer.Consume(ct);

                    _logger.LogDebug(
                        "Received event {eventId}, of type {eventType}!",
                        result.Message.Key,
                        result.Message.Value.GetType().Name);

                    callback(result.Message.Value);

                    // note: committing every time can have a negative impact on performance
                    // in the future, we should batch commits
                    consumer.Commit(result);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    _logger.LogInformation("Shutting down gracefully.");
                }
                catch (Exception ex)
                {
                    // TODO: implement error handling/retry logic
                    // like this, the failed message will eventually be "marked as processed"
                    // (commit to a newer offset) even though it failed
                    _logger.LogError(ex, "Error occurred when consuming event!");
                }
            }

            GracefullyCloseConsumer(consumer);
            
            consumerLoopCompletionSource.SetResult(true);
        })
        {
            IsBackground = true
        };

        thread.Start();

        return consumerLoopCompletionSource.Task;
    }

    // Kafka consumption isn't async, so... GetAwaiter().GetResult() it is ¯\_(ツ)_/¯
    public Task Subscribe(Func<TEvent, Task> callback, CancellationToken ct)
        => Subscribe(@event => callback(@event).GetAwaiter().GetResult(), ct);

    private IConsumer<TKey, TEvent> CreateConsumer()
    {
        var consumerBuilder = new ConsumerBuilder<TKey, TEvent>(_consumerConfig)
            .SetValueDeserializer(_valueDeserializer);

        if (_keyDeserializer != null)
        {
            consumerBuilder.SetKeyDeserializer(_keyDeserializer);
        }

        return consumerBuilder.Build();
    }

    private void GracefullyCloseConsumer(IConsumer<TKey, TEvent> consumer)
    {
        try
        {
            // unlike Dispose, Close on IConsumer gracefully disconnects this consumer from Kafka
            // allowing for a better rebalancing behavior, without the need to rely on timeouts
            consumer.Close();
        }
        catch (Exception ex)
        {
            // this method is called when terminating a consumer, so we don't want to blow up the application,
            // hence log and let it go
            _logger.LogWarning(ex, "Failed to gracefully disconnect Kafka consumer.");
        }
    }
}
}