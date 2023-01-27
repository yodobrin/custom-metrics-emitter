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
    // private const string OWNER_ID = "ownerid";
    private const string SEQUENCE_NUMBER = "sequenceNumber";
    private const string OFFSET_KEY = "offset";
    private const string SERVICE_BUS_HOST_SUFFIX = ".servicebus.windows.net";
    private const string METRICS_SCOPE = "https://monitor.azure.com/.default";

    private EventHubClient _eventhubClient = default!;
    private BlobContainerClient _checkpointContainerClient = default!;
    private AccessToken _metricAccessToken = default!;

    private readonly ILogger<Worker> _logger;
    private readonly EmitterConfig _config;
    private readonly string _checkpointAccountName;
    private readonly string _checkpointContainerName;
    private readonly string _eventhubresourceId;

    private readonly string _prefix;
    // private string OwnershipPrefix() => $"{_prefix}/ownership/";
    private readonly string _checkpointPrefix;
    private string CheckpointBlobName(string partitionId) => $"{_checkpointPrefix}{partitionId}";

    public EventHubEmitter(ILogger<Worker> logger, EmitterConfig config)
    {
        (_logger, _config) = (logger, config);
        _eventhubresourceId = $"/subscriptions/{config.SubscriptionId}/resourceGroups/{config.ResourceGroup}/providers/Microsoft.EventHub/namespaces/{config.EventHubNamespace}";
        _checkpointAccountName = config.CheckpointAccountName;
        _checkpointContainerName = config.CheckpointContainerName;

        _prefix = $"{config.EventHubNamespace.ToLowerInvariant()}{SERVICE_BUS_HOST_SUFFIX}/{config.EventHubName.ToLowerInvariant()}/{config.ConsumerGroup.ToLowerInvariant()}";
        _checkpointPrefix = $"{_prefix}/checkpoint/";
    }

    public async Task RefreshTokens(DefaultAzureCredential defaultCredential, CancellationToken cancellationToken)
    {
        _eventhubClient = EventHubClient.CreateWithTokenProvider(
            endpointAddress: new Uri($"sb://{_config.EventHubNamespace}{SERVICE_BUS_HOST_SUFFIX}/"),
            entityPath: _config.EventHubName,
            tokenProvider: GetTokenProvider(defaultCredential));

        _checkpointContainerClient = new BlobContainerClient(
            blobContainerUri: new($"https://{_checkpointAccountName}.blob.core.windows.net/{_checkpointContainerName}"),
            credential: defaultCredential);

        _metricAccessToken = await GetTokenAsync(defaultCredential, cancellationToken);
    }

    public async Task<HttpResponseMessage> SendAsync(CancellationToken cancellationToken = default)
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
                            dimValues: new[] { _config.EventHubName, _config.ConsumerGroup, lagInfo.PartitionId.ToString() },
                            min: null, max: null,
                            count: idx + 1,
                            sum: lagInfo.Lag)))));

        return await EmitterHelper.SendCustomMetric(
            region: _config.Region,
            resourceId: _eventhubresourceId,
            metricToSend: emitterdata,
            accessToken: _metricAccessToken,
            logger: _logger, cancellationToken: cancellationToken);
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
          async (audience, authority, state) =>
          {
              var token = await defaultCredential.GetTokenAsync(new Azure.Core.TokenRequestContext(new[] { $"{audience}/.default" }));
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
          $"https://login.microsoftonline.com/{_config.TenantId}");
    }

    private async Task<AccessToken> GetTokenAsync(DefaultAzureCredential defaultCredential, CancellationToken cancellationToken = default)
    {
        var scope = new string[] { METRICS_SCOPE };
        return await defaultCredential.GetTokenAsync(new TokenRequestContext(scope), cancellationToken);
    }
}