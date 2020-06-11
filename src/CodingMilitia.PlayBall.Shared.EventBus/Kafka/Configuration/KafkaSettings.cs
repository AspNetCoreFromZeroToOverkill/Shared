using System;

namespace CodingMilitia.PlayBall.Shared.EventBus.Kafka.Configuration
{
    public class KafkaSettings
    {
        public string[] BootstrapServers { get; set; } = Array.Empty<string>();
    }
}