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
            var totalLag = await CalcLagAsync();

            var emitterdata = new EmitterSchema()
            {
                time = DateTime.Now,
                data = new CustomMetricData()
                {
                    baseData = new CustomMetricBaseData()
                    {
                        metric = LAG_METRIC_NAME,
                        Namespace = EVENT_HUB_CUSTOM_METRIC_NAMESPACE,
                        dimNames = new string[3] { "EventHubName", "ConsumerGroup", "PartitionId" },
                        series = new CustomMetricBaseDataSeriesItem[totalLag.Count]
					}
				}
			};            
            for (int i = 0; i < totalLag.Count; i++)
            {
                emitterdata.data.baseData.series[i] = new CustomMetricBaseDataSeriesItem()
                {
                    dimValues = new string[3] { _eventhubName, _consumerGroup, i.ToString() },
                    count = i + 1,
                    sum = totalLag[i]
                };
            }

            var res = await EmitterHelper.SendCustomMetric(Region, ResourceId, emitterdata, accessToken);
            return res;                        
        }

		private async Task<SortedList<int, long>> CalcLagAsync()
		{
            SortedList<int, long> retVal = new SortedList<int, long>();
            var checkpointBlobsPrefix = CheckpointPrefix();

            var ehInfo = await _eventhubClient.GetRuntimeInformationAsync();
            foreach (string partitionId in ehInfo.PartitionIds)
            {
                long partitionLag = await CalcLagInPartition(partitionId);
                retVal.Add(int.Parse(partitionId), partitionLag);
            }

            return retVal;
        }

        private async Task<long> CalcLagInPartition(string partitionId)
        {
            long retVal = 0;
            try
            {
                var partitionInfo = await _eventhubClient.GetPartitionRuntimeInformationAsync(partitionId);
                CancellationToken defaultToken = default;

                // if partitionInfo.LastEnqueuedOffset = -1, that means event hub partition is empty
                if ((partitionInfo != null) && (partitionInfo.LastEnqueuedOffset == "-1"))
                {
                    _logger.LogInformation("CalcLagInPartition Empty");                    
                }
                else
                {
                    string checkpointName = CheckpointBlobName(partitionId);

                    try
                    {
                        _logger.LogInformation("CalcLagInPartition GetProperties:" + checkpointName);
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
                                _logger.LogInformation("CalcLagInPartition Start: " + checkpointName + " seq=" + seqNum + " offset=" + strOffset);

                                // If checkpoint.Offset is empty that means no messages has been processed from an event hub partition
                                // And since partitionInfo.LastSequenceNumber = 0 for the very first message hence
                                // total unprocessed message will be partitionInfo.LastSequenceNumber + 1
                                if (string.IsNullOrEmpty(strOffset) == true)
                                {
                                    retVal = partitionInfo.LastEnqueuedSequenceNumber + 1;
                                }
                                else
                                {
                                    if (partitionInfo.LastEnqueuedSequenceNumber >= seqNum)
                                    {
                                        retVal = partitionInfo.LastEnqueuedSequenceNumber - seqNum;
                                    }
                                    else
                                    {
                                        // Partition is a circular buffer, so it is possible that
                                        // partitionInfo.LastSequenceNumber < blob checkpoint's SequenceNumber
                                        retVal = (long.MaxValue - partitionInfo.LastEnqueuedSequenceNumber) + seqNum;

                                        if (retVal < 0)
                                            retVal = 0;
                                    }
                                }
                                _logger.LogInformation("CalcLagInPartition End: " + checkpointName + " seq=" + seqNum + " offset=" + strOffset + " lag=" + retVal);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("CalcLagInPartition: Checkpoint container not exist " + ex.ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("CalcLagInPartition: " + ex.ToString());
            }
            return retVal;
        }
    }
}

