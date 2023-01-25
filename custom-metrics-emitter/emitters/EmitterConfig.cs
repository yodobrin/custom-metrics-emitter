﻿using System;
namespace custom_metrics_emitter
{
	public class EmitterConfig
	{        
        public string Region { get; set; } = default!;
        public string SubscriptionId { get; set; } = default!;
        public string ResourceGroup { get; set; } = default!;
        public string TenantId { get; set; } = default!;
        public string EventHubNamespace { get; set; } = default!;
        public string EventHubName { get; set; } = default!;
        public string ConsumerGroup { get; set; } = default!;
        public string CheckpointAccountName { get; set; } = default!;
        public string CheckpointContainerName { get; set; } = default!;
        public int CustomMetricInterval { get; set; } = default!;
    }
}

