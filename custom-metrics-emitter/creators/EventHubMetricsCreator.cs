using System;
using Microsoft.Extensions.Configuration;

namespace custom_metrics_emitter
{
    public class EventHubMetricsCreator
    {
        public Emitter Create(ILogger<Worker> logger, string region, string resourceId, string eh_connectionString,
            string eventhubName,
            string consumerGroup,
            string checkpoint_connectionString,
            string checkpoint_containerName)
        {
            return new EventHubEmitter(logger, region, resourceId, eh_connectionString, eventhubName,
                consumerGroup, checkpoint_connectionString, checkpoint_containerName);
        }
    }
}

