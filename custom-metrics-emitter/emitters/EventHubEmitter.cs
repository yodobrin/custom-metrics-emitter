using System;
using System.Threading.Channels;
using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using custom_metrics_emitter.emitters;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;

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
        private const string BLOB_URI_TEMPLATE = "https://{0}.blob.core.windows.net/{1}";

        private readonly EventHubClient _eventhubClient;
        private readonly BlobContainerClient _checkpointContainerClient;
		
        private readonly string _checkpointAccountName;
		private readonly string _checkpointContainerName;

        private string Prefix() => $"{_config.EventHubNamespace.ToLowerInvariant()}/{_config.EventHubName.ToLowerInvariant()}/{_config.ConsumerGroup.ToLowerInvariant()}";
        private string OwnershipPrefix() => $"{Prefix()}/ownership/";
        private string CheckpointPrefix() => $"{Prefix()}/checkpoint/";
        private string CheckpointBlobName(string partitionId) => $"{CheckpointPrefix()}{partitionId}";
        

        public EventHubEmitter(ILogger<Worker> logger, ChainedTokenCredential token, EmitterConfig config) :
            base(logger, token, config)
		{
            TokenProvider tp = TokenProvider.CreateAzureActiveDirectoryTokenProvider(
               async (audience, authority, state) =>
               {
                   var token = await _token.GetTokenAsync(new Azure.Core.TokenRequestContext(new[] { $"{audience}/.default" }));
                   return token.Token;

                   #region Alternative with spn
                   //IConfidentialClientApplication app = ConfidentialClientApplicationBuilder.Create(APP CLIENT ID)
                   //           .WithAuthority(authority)
                   //           .WithClientSecret(APP CLIENT PASSWORD)
                   //           .Build();

                   //var authResult = await app.AcquireTokenForClient(new string[] { $"{audience}/.default" }).ExecuteAsync();
                   //return authResult.AccessToken;
                   #endregion
               },
               $"https://login.microsoftonline.com/" + _config.TenantId);
            
            _eventhubClient = EventHubClient.CreateWithTokenProvider(new Uri(string.Format("sb://{0}/", _config.EventHubNamespace)),
                _config.EventHubName, tp);            
            
            _checkpointAccountName = _config.CheckpointAccountName;
            _checkpointContainerName = _config.CheckpointContainerName;
            _checkpointContainerClient = new BlobContainerClient(new Uri(
                string.Format(BLOB_URI_TEMPLATE, _checkpointAccountName, _checkpointContainerName)),
                _token);
        }

        public override async Task<HttpResponseMessage> SendAsync(AccessToken accessToken)
        {
            var totalLag = await GetLagAsync();

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
                    dimValues = new string[3] { _config.EventHubName, _config.ConsumerGroup, i.ToString() },
                    count = i + 1,
                    sum = totalLag[i]
                };
            }

            var res = await EmitterHelper.SendCustomMetric(_config.Region, _config.ResourceId, emitterdata, accessToken);
            return res;                        
        }

		private async Task<SortedList<int, long>> GetLagAsync()
		{
            SortedList<int, long> retVal = new SortedList<int, long>();
            var checkpointBlobsPrefix = CheckpointPrefix();

            var ehInfo = await _eventhubClient.GetRuntimeInformationAsync();
            foreach (string partitionId in ehInfo.PartitionIds)
            {
                long partitionLag = await LagInPartition(partitionId);
                retVal.Add(int.Parse(partitionId), partitionLag);
            }

            return retVal;
        }

        private async Task<long> LagInPartition(string partitionId)
        {
            long retVal = 0;
            try
            {
                var partitionInfo = await _eventhubClient.GetPartitionRuntimeInformationAsync(partitionId);
                CancellationToken defaultToken = default;

                // if partitionInfo.LastEnqueuedOffset = -1, that means event hub partition is empty
                if ((partitionInfo != null) && (partitionInfo.LastEnqueuedOffset == "-1"))
                {
                    _logger.LogInformation("LagInPartition Empty");                    
                }
                else
                {
                    string checkpointName = CheckpointBlobName(partitionId);

                    try
                    {
                        _logger.LogInformation("LagInPartition GetProperties:" + checkpointName);

                        BlobProperties properties = await _checkpointContainerClient
                           .GetBlobClient(checkpointName)
                           .GetPropertiesAsync(conditions: null, cancellationToken: defaultToken)
                           .ConfigureAwait(false);

                        string strSeqNum, strOffset;
                        if ((properties.Metadata.TryGetValue(SEQUENCE_NUMBER, out strSeqNum!)) &&
                            (properties.Metadata.TryGetValue(OFFSET_KEY, out strOffset!)))
                        {
                            long seqNum;
                            if (long.TryParse(strSeqNum, out seqNum))
                            {
                                _logger.LogInformation("LagInPartition Start: " + checkpointName + " seq=" + seqNum + " offset=" + strOffset);

                                // If checkpoint.Offset is empty that means no messages has been processed from an event hub partition
                                // And since partitionInfo.LastSequenceNumber = 0 for the very first message hence
                                // total unprocessed message will be partitionInfo.LastSequenceNumber + 1
                                if (string.IsNullOrEmpty(strOffset) == true)
                                {
                                    retVal = partitionInfo!.LastEnqueuedSequenceNumber + 1;
                                }
                                else
                                {
                                    if (partitionInfo!.LastEnqueuedSequenceNumber >= seqNum)
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
                                _logger.LogInformation("LagInPartition End: " + checkpointName + " seq=" + seqNum + " offset=" + strOffset + " lag=" + retVal);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("LagInPartition: Checkpoint container not exist " + ex.ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("LagInPartition: " + ex.ToString());
            }
            return retVal;
        }
    }
}

