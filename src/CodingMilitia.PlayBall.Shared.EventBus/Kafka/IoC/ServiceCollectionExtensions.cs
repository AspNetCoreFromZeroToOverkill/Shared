using System;
using CodingMilitia.PlayBall.Shared.EventBus.Kafka.Configuration;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

// ReSharper disable once CheckNamespace - ease discoverability
namespace CodingMilitia.PlayBall.Shared.EventBus.Kafka
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddKafkaTopicPublisher<TKey, TTopicEventBase>(
            this IServiceCollection services,
            string topic,
            KafkaSettings settings,
            ISerializer<TKey> keySerializer,
            ISerializer<TTopicEventBase> valueSerializer,
            Func<TTopicEventBase, TKey> keyProvider)
            => services.AddSingleton<IEventPublisher<TTopicEventBase>>(
                new KafkaEventPublisher<TKey, TTopicEventBase>(
                    topic,
                    settings,
                    keySerializer,
                    valueSerializer,
                    keyProvider
                ));

        public static IServiceCollection AddKafkaTopicConsumer<TKey, TTopicEventBase>(
            this IServiceCollection services,
            string topic,
            KafkaSettings settings,
            KafkaConsumerSettings consumerSettings,
            IDeserializer<TKey> keyDeserializer,
            IDeserializer<TTopicEventBase> valueDeserializer)
            => services.AddSingleton<IEventConsumer<TTopicEventBase>>(s =>
                new KafkaEventConsumer<TKey, TTopicEventBase>(
                    topic,
                    settings,
                    consumerSettings,
                    keyDeserializer,
                    valueDeserializer,
                    s.GetRequiredService<ILogger<KafkaEventConsumer<TKey, TTopicEventBase>>>()));
    }
}