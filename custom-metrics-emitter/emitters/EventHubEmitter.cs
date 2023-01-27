namespace custom_metrics_emitter;

using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using custom_metrics_emitter.emitters;
using Microsoft.Azure.EventHubs;

internal record LagInformation(string PartitionId, long Lag);

public class EventHubEmitter
{
    private const string LAG_METRIC_NAME = "Lag";
    private const string EVENT_HUB_CUSTOM_METRIC_NAMESPACE = "Event Hub custom metrics";

    // Implementation details from the EventHub .NET SDK
    private const string SEQUENCE_NUMBER = "sequenceNumber";
    private const string OFFSET_KEY = "offset";
    private readonly string _prefix;
    private string CheckpointBlobName(string partitionId) => $"{_prefix}/checkpoint/{partitionId}";

    private const string SERVICE_BUS_HOST_SUFFIX = ".servicebus.windows.net";
    private const string STORAGE_HOST_SUFFIX = ".blob.core.windows.net";

    private readonly ILogger<Worker> _logger;
    private readonly EmitterConfig _cfg;
    private readonly string _eventhubresourceId;

    private readonly EmitterHelper _emitter;
    private readonly EventHubClient _eventhubClient = default!;
    private readonly BlobContainerClient _checkpointContainerClient = default!;

    public EventHubEmitter(ILogger<Worker> logger, EmitterConfig config, DefaultAzureCredential defaultCredential)
    {
        (_logger, _cfg)= (logger, config);

        _eventhubresourceId = $"/subscriptions/{_cfg.SubscriptionId}/resourceGroups/{_cfg.ResourceGroup}/providers/Microsoft.EventHub/namespaces/{_cfg.EventHubNamespace}";
        _prefix = $"{_cfg.EventHubNamespace.ToLowerInvariant()}{SERVICE_BUS_HOST_SUFFIX}/{_cfg.EventHubName.ToLowerInvariant()}/{_cfg.ConsumerGroup.ToLowerInvariant()}";

        _emitter = new EmitterHelper(_logger, defaultCredential);

        _eventhubClient = EventHubClient.CreateWithTokenProvider(
            endpointAddress: new Uri($"sb://{_cfg.EventHubNamespace}{SERVICE_BUS_HOST_SUFFIX}/"),
            entityPath: _cfg.EventHubName,
            tokenProvider: GetTokenProvider(defaultCredential));

        _checkpointContainerClient = new BlobContainerClient(
            blobContainerUri: new($"https://{_cfg.CheckpointAccountName}{STORAGE_HOST_SUFFIX}/{_cfg.CheckpointContainerName}"),
            credential: defaultCredential);
    }

    public async Task<HttpResponseMessage> ReadFromBlobStorageAndPublishToAzureMonitorAsync(CancellationToken cancellationToken = default)
    {
        var totalLag = await GetLagAsync(cancellationToken);

        var emitterdata = new EmitterSchema(
            time: DateTime.UtcNow,
            data: new CustomMetricData(
                baseData: new CustomMetricBaseData(
                    metric: LAG_METRIC_NAME,
                    Namespace: EVENT_HUB_CUSTOM_METRIC_NAMESPACE,
                    dimNames: new[] { "EventHubName", "ConsumerGroup", "PartitionId" },
                    series: totalLag.Select((lagInfo, idx) =>
                        new CustomMetricBaseDataSeriesItem(
                            dimValues: new[] { _cfg.EventHubName, _cfg.ConsumerGroup, lagInfo.PartitionId },
                            min: null, max: null,
                            count: idx + 1,
                            sum: lagInfo.Lag)))));

        return await _emitter.SendCustomMetric(
            region: _cfg.Region,
            resourceId: _eventhubresourceId,
            metricToSend: emitterdata,
            cancellationToken: cancellationToken);
    }

    private async Task<IEnumerable<LagInformation>> GetLagAsync(CancellationToken cancellationToken = default)
    {
        // var checkpointBlobsPrefix = CheckpointPrefix();

        EventHubRuntimeInformation ehInfo = await _eventhubClient.GetRuntimeInformationAsync();

        // Query all partitions in parallel
        var tasks = ehInfo.PartitionIds.Select(
            partitionId => (
                PartitionId: partitionId,
                Task: LagInPartition(partitionId, cancellationToken)
            ));
        await Task.WhenAll(tasks.Select(i => i.Task));

        return tasks
            .Select(x => new LagInformation(x.PartitionId, x.Task.Result))
            .OrderBy(x => x.PartitionId);
    }

    private async Task<long> LagInPartition(string partitionId, CancellationToken cancellationToken = default)
    {
        long retVal = 0;
        try
        {
            var partitionInfo = await _eventhubClient.GetPartitionRuntimeInformationAsync(partitionId);

            // if partitionInfo.LastEnqueuedOffset = -1, that means event hub partition is empty
            if ((partitionInfo != null) && (partitionInfo.LastEnqueuedOffset == "-1"))
            {
                _logger.LogInformation("LagInPartition Empty");
            }
            else
            {
                string checkpointName = CheckpointBlobName(partitionId);
                _logger.LogInformation("LagInPartition Checkpoint GetProperties: {name}", checkpointName);

                BlobProperties properties = await _checkpointContainerClient
                    .GetBlobClient(checkpointName)
                    .GetPropertiesAsync(cancellationToken: cancellationToken)
                    .ConfigureAwait(false);

                string strSeqNum, strOffset;
                if (properties.Metadata.TryGetValue(SEQUENCE_NUMBER, out strSeqNum!) &&
                    properties.Metadata.TryGetValue(OFFSET_KEY, out strOffset!))
                {
                    if (long.TryParse(strSeqNum, out long seqNum))
                    {
                        _logger.LogInformation("LagInPartition Start: {checkpoint name} seq={seqNum} offset={offset}", checkpointName, seqNum, strOffset);

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
                        _logger.LogInformation("LagInPartition End: {checkpoint name} seq={seqNum} offset={offset} lag={lag}", checkpointName, seqNum, strOffset, retVal);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError("LagInPartition Error: {error}", ex.ToString());
        }
        return retVal;
    }

    private TokenProvider GetTokenProvider(DefaultAzureCredential defaultCredential)
    {
        return TokenProvider.CreateAzureActiveDirectoryTokenProvider(
            authority: $"https://login.microsoftonline.com/{_cfg.TenantId}",
            authCallback: async (audience, authority, state) =>
            {
                var token = await defaultCredential.GetTokenAsync(new TokenRequestContext(new[] { $"{audience}/.default" }));

                return token.Token;

                #region Alternative with spn
                //IConfidentialClientApplication app = ConfidentialClientApplicationBuilder.Create(APP CLIENT ID).WithAuthority(authority).WithClientSecret(APP CLIENT PASSWORD).Build();
                //var authResult = await app.AcquireTokenForClient(new string[] { $"{audience}/.default" }).ExecuteAsync();
                //return authResult.AccessToken;
                #endregion
            });
    }
}