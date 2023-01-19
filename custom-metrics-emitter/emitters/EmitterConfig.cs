using System;
namespace custom_metrics_emitter
{
	public class EmitterConfig
	{
		public string EventHubConnectionString { get; set; }
        public string Region { get; set; }
        public string ResourceId { get; set; }
        public string EventHubName { get; set; }
        public string ConsumerGroup { get; set; }
        public string CheckpointConnectionString { get; set; }
        public string CheckpointContainerName { get; set; }

    }
}

