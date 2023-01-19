using System;
using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using custom_metrics_emitter.emitters;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;

namespace custom_metrics_emitter
{
	public class EventHubEmitter : Emitter
	{
		private const string LAG_METRIC_NAME = "Lag";
		private const string EVENT_HUB_CUSTOM_METRIC_NAMESPACE = "Event Hub custom metrics";
        private const string OWNER_ID = "ownerid";
        private const string SEQUENCE_NUMBER = "sequenceNumber";
        private const string OFFSET_KEY = "offset";
        private const string SERVICE_BUS_HOST_NAME = ".servicebus.windows.net";

        private readonly EventHubClient _eventhubClient;
        private readonly BlobContainerClient _checkpointContainerClient;
		
        private readonly string _eventHubConnectionString;
        private readonly string _fullEventHubConnectionString;
        private readonly string _eventhubName;
        private readonly string _consumerGroup;
        private readonly string _fullyQualifiedNamespace;

        private readonly string _checkpointConnectionString;
		private readonly string _checkpointContainerName;

        private string Prefix() => $"{_fullyQualifiedNamespace.ToLowerInvariant()}/{_eventhubName.ToLowerInvariant()}/{_consumerGroup.ToLowerInvariant()}";
        private string OwnershipPrefix() => $"{Prefix()}/ownership/";
        private string CheckpointPrefix() => $"{Prefix()}/checkpoint/";
        private string CheckpointBlobName(string partitionId) => $"{CheckpointPrefix()}{partitionId}";
        private readonly ILogger<Worker> _logger;

        public EventHubEmitter(ILogger<Worker> logger, string region, string resourceId, string eh_connectionstring,
			string eventhubName,
			string consumerGroup,
			string checkpoint_connectionString,
			string checkpoint_containerName) : base(region, resourceId)
		{
            _logger = logger;
			_eventhubName = eventhubName;
			_consumerGroup = consumerGroup;
			_eventHubConnectionString = eh_connectionstring;
            _fullEventHubConnectionString = _eventHubConnectionString + ";EntityPath=" + _eventhubName;
            _eventhubClient = EventHubClient.CreateFromConnectionString(_fullEventHubConnectionString);

            int index = _eventHubConnectionString.IndexOf(SERVICE_BUS_HOST_NAME);
			int start = "Endpoint=sb://".Length;
			_fullyQualifiedNamespace = _eventHubConnectionString.Substring(start, index - start + SERVICE_BUS_HOST_NAME.Length);
			
            _checkpointConnectionString = checkpoint_connectionString;
            _checkpointContainerName = checkpoint_containerName;
			_checkpointContainerClient = new BlobContainerClient(_checkpointConnectionString, _checkpointContainerName);
        }

        public override async Task<HttpResponseMessage> SendAsync(AccessToken accessToken)
        {
            long lag = await CalcLagAsync();

			var data = new EmitterSchema()
			{
				time = DateTime.Now,
				data = new CustomMetricData()
				{
					baseData = new CustomMetricBaseData()
					{
						metric = LAG_METRIC_NAME,
						Namespace = EVENT_HUB_CUSTOM_METRIC_NAMESPACE,
						dimNames = new string[2] { "EventHubName", "ConsumerGroup" },
						series = new CustomMetricBaseDataSeriesItem[]
						{
							new CustomMetricBaseDataSeriesItem()
							{
								dimValues = new string[2] { _eventhubName, _consumerGroup },
								count = lag
							}
						}
					}
				}
			};

            if (lag > 0)
            {
                var res = await EmitterHelper.SendCustomMetric(Region, ResourceId, data, accessToken);
                return res;
            }
            return new HttpResponseMessage(System.Net.HttpStatusCode.PartialContent);
        }

		private async Task<long> CalcLagAsync()
		{
            long retVal = 0;
            var checkpointBlobsPrefix = CheckpointPrefix();

            var ehInfo = await _eventhubClient.GetRuntimeInformationAsync();
            foreach (string partitionId in ehInfo.PartitionIds)
            {
                retVal += await UnprocessedMessageInPartition(partitionId);
            }

            return retVal;
        }

        private async Task<long> UnprocessedMessageInPartition(string partitionId)
        {
            long retVal = 0;
            try
            {
                var partitionInfo = await _eventhubClient.GetPartitionRuntimeInformationAsync(partitionId);
                CancellationToken defaultToken = default;

                // if partitionInfo.LastEnqueuedOffset = -1, that means event hub partition is empty
                if ((partitionInfo != null) && (partitionInfo.LastEnqueuedOffset == "-1"))
                {
                    return retVal;
                }

                string checkpointName = CheckpointBlobName(partitionId);

                try
                {
                    BlobProperties properties = await _checkpointContainerClient
                       .GetBlobClient(checkpointName)
                       .GetPropertiesAsync(conditions: null, cancellationToken: defaultToken)
                       .ConfigureAwait(false);

                    string strSeqNum, strOffset;
                    if ((properties.Metadata.TryGetValue(SEQUENCE_NUMBER, out strSeqNum)) && (properties.Metadata.TryGetValue(OFFSET_KEY, out strOffset)))
                    {
                        long seqNum;
                        if (long.TryParse(strSeqNum, out seqNum))
                        {
                            _logger.LogInformation("Got metadata " + checkpointName + " seq=" + seqNum + " offset=" + strOffset);

                            // If checkpoint.Offset is empty that means no messages has been processed from an event hub partition
                            // And since partitionInfo.LastSequenceNumber = 0 for the very first message hence
                            // total unprocessed message will be partitionInfo.LastSequenceNumber + 1
                            if (string.IsNullOrEmpty(strOffset) == true)
                            {
                                retVal = partitionInfo.LastEnqueuedSequenceNumber + 1;
                                return retVal;
                            }

                            if (partitionInfo.LastEnqueuedSequenceNumber >= seqNum)
                            {
                                retVal = partitionInfo.LastEnqueuedSequenceNumber - seqNum;
                                return retVal;
                            }

                            // Partition is a circular buffer, so it is possible that
                            // partitionInfo.LastSequenceNumber < blob checkpoint's SequenceNumber
                            retVal = (long.MaxValue - partitionInfo.LastEnqueuedSequenceNumber) + seqNum;

                            if (retVal < 0)
                                retVal = 0;
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError("Checkpoint container not exist " + ex.ToString());                    
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
            }
            return retVal;
        }
    }
}

