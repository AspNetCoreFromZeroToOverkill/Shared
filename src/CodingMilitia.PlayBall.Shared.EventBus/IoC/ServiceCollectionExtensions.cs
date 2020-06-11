using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

// ReSharper disable once CheckNamespace - ease discoverability
namespace CodingMilitia.PlayBall.Shared.EventBus
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddTopicDistributor<TServiceEventRoot>(
            this IServiceCollection services,
            IReadOnlyCollection<Type> topicEventBaseTypes)
            => services.AddSingleton<IEventPublisher<TServiceEventRoot>>(
                s => new TopicDistributor<TServiceEventRoot>(s, topicEventBaseTypes));
    }
}