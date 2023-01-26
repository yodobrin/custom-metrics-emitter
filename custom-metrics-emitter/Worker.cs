namespace custom_metrics_emitter;

using Azure.Identity;

public class Worker : BackgroundService
{
    private DateTime _lastRefreshToken = DateTime.MinValue;
    private readonly TimeSpan ONE_HOUR = TimeSpan.FromHours(1);
    private readonly int DEFAULT_INTERVAL = 10000;
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _config;
    private readonly DefaultAzureCredential _defaultCredential;
    // private static readonly HttpClient _httpClient = new HttpClient();

    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _config = configuration;

        //set managedidenity specific clientid
        if (!string.IsNullOrEmpty(_config["ManagedIdentityClientId"]))
            _defaultCredential = new DefaultAzureCredential(
                new DefaultAzureCredentialOptions { ManagedIdentityClientId = _config["ManagedIdentityClientId"] });
        else
            _defaultCredential = new DefaultAzureCredential();
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var emitterConfig = ReadConfiguration();
            EventHubEmitter ehEmitter = new(_logger, emitterConfig);

            while (!cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                //check if need to get new tokens
                if (DateTime.Now.Subtract(ONE_HOUR) > _lastRefreshToken)
                {
                    await ehEmitter.RefreshTokens(_defaultCredential, cancellationToken);
                    _lastRefreshToken = DateTime.Now;
                    _logger.LogInformation("Get refresh tokens at: {lastrefresh}", _lastRefreshToken.ToString());
                }
                var res = await ehEmitter.SendAsync(cancellationToken);
                if (((int)res.StatusCode) >= 400)
                {
                    _logger.LogError("Error sending custom event with status: {status}", res.StatusCode);
                }
                else
                {
                    _logger.LogInformation("Send Custom Metric end with status: {status}", res.StatusCode);
                }
                await Task.Delay(emitterConfig.CustomMetricInterval, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError("{error}", ex.ToString());
        }
    }

    private EmitterConfig ReadConfiguration()
    {
        EmitterConfig config = new()
        {
            EventHubNamespace = _config.GetValue<string>("EventHubNamespace") ?? string.Empty,
            EventHubName = _config.GetValue<string>("EventHubName") ?? string.Empty,
            ConsumerGroup = _config.GetValue<string>("ConsumerGroup") ?? string.Empty,
            CheckpointAccountName = _config.GetValue<string>("CheckpointAccountName") ?? string.Empty,
            CheckpointContainerName = _config.GetValue<string>("CheckpointContainerName") ?? string.Empty,
            Region = _config.GetValue<string>("Region") ?? string.Empty,
            TenantId = _config.GetValue<string>("TenantId") ?? string.Empty,
            SubscriptionId = _config.GetValue<string>("SubscriptionId") ?? string.Empty,
            ResourceGroup = _config.GetValue<string>("ResourceGroup") ?? string.Empty,
            CustomMetricInterval = DEFAULT_INTERVAL,
        };

        if ((string.IsNullOrEmpty(config.EventHubNamespace)) || (string.IsNullOrEmpty(config.EventHubName))
            || (string.IsNullOrEmpty(config.ConsumerGroup)) || (string.IsNullOrEmpty(config.CheckpointAccountName))
            || (string.IsNullOrEmpty(config.CheckpointContainerName)) || (string.IsNullOrEmpty(config.Region))
            || (string.IsNullOrEmpty(config.TenantId)) || (string.IsNullOrEmpty(config.SubscriptionId)) || (string.IsNullOrEmpty(config.ResourceGroup)))
        {
            throw new Exception("Configuration error, missing values");
        }

        if (_config.GetValue<string>("CustomMetricInterval") != null &&
            int.TryParse(_config.GetValue<string>("CustomMetricInterval"), out int interval))
        {
            config.CustomMetricInterval = interval;
        }

        return config;
    }
}