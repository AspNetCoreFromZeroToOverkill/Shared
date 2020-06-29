using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace CodingMilitia.PlayBall.Shared.EventBus
{
    // indirection to put between outbox publishers and the actual event publishers,
    // to simplify routing to topics based on the types of events 
    public class TopicDistributor<TServiceEventRoot> : IEventPublisher<TServiceEventRoot>
    {
        private readonly IServiceProvider _serviceProvider;

        private readonly ReadOnlyDictionary<Type, Type> _eventToBaseEventMap;

        private readonly ReadOnlyDictionary<Type, Func<TServiceEventRoot, CancellationToken, Task>>
            _singleEventPublisherMap;

        private readonly ReadOnlyDictionary<Type, Func<IEnumerable<TServiceEventRoot>, CancellationToken, Task>>
            _multipleEventPublisherMap;

        public TopicDistributor(
            IServiceProvider serviceProvider,
            IReadOnlyCollection<Type> baseEventTypes)
        {
            // should probably be replaced by an IEventPublisher provider interface, to avoid coupling to IServiceProvider
            _serviceProvider = serviceProvider;
            _eventToBaseEventMap = CreateEventToBaseEventMap(baseEventTypes);
            (_singleEventPublisherMap, _multipleEventPublisherMap) = CreateEventPublisherMaps(baseEventTypes);
        }

        public async Task PublishAsync(TServiceEventRoot @event, CancellationToken ct)
        {
            await _singleEventPublisherMap[_eventToBaseEventMap[@event.GetType()]](@event, ct);
        }

        public async Task PublishAsync(IEnumerable<TServiceEventRoot> events, CancellationToken ct)
        {
            await Task.WhenAll(events
                .GroupBy(e => _eventToBaseEventMap[e.GetType()])
                .Select(g => _multipleEventPublisherMap[g.Key](g.Select(e => e), ct))
                .ToList());
        }

        private async Task InnerPublishSingleAsync<TTopicEventBase>(TServiceEventRoot @event, CancellationToken ct)
            where TTopicEventBase : TServiceEventRoot
        {
            var publisher = _serviceProvider.GetRequiredService<IEventPublisher<TTopicEventBase>>();
            await publisher.PublishAsync((TTopicEventBase) @event, ct);
        }

        private async Task InnerPublishMultipleAsync<TTopicEventBase>(
            IEnumerable<TServiceEventRoot> events,
            CancellationToken ct)
            where TTopicEventBase : TServiceEventRoot
        {
            var publisher = _serviceProvider.GetRequiredService<IEventPublisher<TTopicEventBase>>();
            await publisher.PublishAsync(events.Cast<TTopicEventBase>(), ct);
        }

        private static ReadOnlyDictionary<Type, Type> CreateEventToBaseEventMap(
            IReadOnlyCollection<Type> baseEventTypes)
        {
            var eventToBaseEventMap = new Dictionary<Type, Type>();

            foreach (var baseEventType in baseEventTypes)
            {
                var derivedTypes =
                    baseEventType
                        .Assembly
                        .GetTypes()
                        .Where(type => type.IsClass && !type.IsAbstract && type.IsSubclassOf(baseEventType));

                foreach (var derivedType in derivedTypes)
                {
                    eventToBaseEventMap.Add(derivedType, baseEventType);
                }
            }

            return new ReadOnlyDictionary<Type, Type>(eventToBaseEventMap);
        }

        // over-engineering incoming!
        private (ReadOnlyDictionary<Type, Func<TServiceEventRoot, CancellationToken, Task>>,
            ReadOnlyDictionary<Type, Func<IEnumerable<TServiceEventRoot>, CancellationToken, Task>>)
            CreateEventPublisherMaps(IReadOnlyCollection<Type> baseEventTypes)
        {
            // map concrete InnerPublishSingleAsync and InnerPublishMultipleAsync to the types they should be used with

            var singleEventPublisherMap = new Dictionary<Type, Func<TServiceEventRoot, CancellationToken, Task>>();
            var multipleEventPublisherMap =
                new Dictionary<Type, Func<IEnumerable<TServiceEventRoot>, CancellationToken, Task>>();

            foreach (var baseEventType in baseEventTypes)
            {
                var thisExpression = Expression.Constant(this);
                var eventParameterExpression = Expression.Parameter(typeof(TServiceEventRoot));
                var eventsParameterExpression = Expression.Parameter(typeof(IEnumerable<TServiceEventRoot>));
                var ctParameterExpression = Expression.Parameter(typeof(CancellationToken));

                var publishSingleMethod = GetType()
                    .GetMethod(nameof(InnerPublishSingleAsync), BindingFlags.Instance | BindingFlags.NonPublic)
                    .MakeGenericMethod(baseEventType);

                var publishSingleLambda =
                    Expression.Lambda<Func<TServiceEventRoot, CancellationToken, Task>>(
                            Expression.Call(
                                thisExpression,
                                publishSingleMethod,
                                eventParameterExpression,
                                ctParameterExpression),
                            eventParameterExpression,
                            ctParameterExpression)
                        .Compile();

                singleEventPublisherMap.Add(baseEventType, publishSingleLambda);

                var publishMultipleMethod = GetType()
                    .GetMethod(nameof(InnerPublishMultipleAsync), BindingFlags.Instance | BindingFlags.NonPublic)
                    .MakeGenericMethod(baseEventType);

                var publishMultipleLambda =
                    Expression.Lambda<Func<IEnumerable<TServiceEventRoot>, CancellationToken, Task>>(
                            Expression.Call(
                                thisExpression,
                                publishMultipleMethod,
                                eventsParameterExpression,
                                ctParameterExpression),
                            eventsParameterExpression,
                            ctParameterExpression)
                        .Compile();

                multipleEventPublisherMap.Add(baseEventType, publishMultipleLambda);
            }

            return (
                new ReadOnlyDictionary<Type, Func<TServiceEventRoot, CancellationToken, Task>>(singleEventPublisherMap),
                new ReadOnlyDictionary<Type, Func<IEnumerable<TServiceEventRoot>, CancellationToken, Task>>(
                    multipleEventPublisherMap));
        }
    }
}