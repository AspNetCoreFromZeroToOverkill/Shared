using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CodingMilitia.PlayBall.Shared.EventBus.Kafka.Configuration;
using Confluent.Kafka;

namespace CodingMilitia.PlayBall.Shared.EventBus.Kafka
{
    public class KafkaEventPublisher<TKey, TEvent> : IEventPublisher<TEvent>, IDisposable
    {
        private readonly string _topic;
        private readonly Func<TEvent, TKey> _keyProvider;
        private readonly IProducer<TKey, TEvent> _producer;

        public KafkaEventPublisher(
            string topic,
            KafkaSettings settings,
            ISerializer<TKey> keySerializer,
            ISerializer<TEvent> valueSerializer,
            Func<TEvent, TKey> keyProvider)
        {
            _topic = topic;
            _keyProvider = keyProvider;

            var config = new ProducerConfig
            {
                BootstrapServers = string.Join(",", settings.BootstrapServers),
                Partitioner = Partitioner.Consistent
            };

            var producerBuilder = new ProducerBuilder<TKey, TEvent>(config)
                .SetValueSerializer(valueSerializer);

            if (keySerializer != null)
            {
                producerBuilder.SetKeySerializer(keySerializer);
            }

            _producer = producerBuilder.Build();
        }

        public async Task PublishAsync(TEvent @event, CancellationToken ct)
        {
            await _producer.ProduceAsync(
                _topic,
                new Message<TKey, TEvent>
                {
                    Key = _keyProvider(@event),
                    Value = @event,
                    Timestamp = Timestamp.Default
                },
                ct);
        }

        public async Task PublishAsync(IEnumerable<TEvent> events, CancellationToken ct)
        {
            // TODO: check out if there's some batch optimized alternative

            foreach (var @event in events)
            {
                await _producer.ProduceAsync(
                    _topic,
                    new Message<TKey, TEvent>
                    {
                        Key = _keyProvider(@event),
                        Value = @event,
                        Timestamp = Timestamp.Default
                    }, ct);
            }
        }

        public void Dispose()
        {
            _producer?.Dispose();
        }
    }
}