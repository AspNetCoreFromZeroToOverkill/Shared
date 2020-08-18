using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace CodingMilitia.PlayBall.Shared.EventBus
{
    public interface IEventPublisher<in TEvent>
    {
        Task PublishAsync(TEvent @event, CancellationToken ct);
        
        Task PublishAsync(IEnumerable<TEvent> events, CancellationToken ct);
    }
}