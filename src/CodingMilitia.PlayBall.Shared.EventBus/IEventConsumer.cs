using System;
using System.Threading;
using System.Threading.Tasks;

namespace CodingMilitia.PlayBall.Shared.EventBus
{
    public interface IEventConsumer<out TEvent>
    {
        Task Subscribe(Action<TEvent> callback, CancellationToken ct);
        
        Task Subscribe(Func<TEvent, Task> callback, CancellationToken ct);
    }
}