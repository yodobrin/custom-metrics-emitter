namespace custom_metrics_emitter;

using Azure.Identity;

public class Worker : BackgroundService
{
    private static readonly TimeSpan ONE_HOUR = TimeSpan.FromHours(1);
    private const int DEFAULT_INTERVAL = 10_000;

    private DateTime _lastRefreshToken = DateTime.MinValue;
    private readonly ILogger<Worker> _logger;
    private readonly EmitterConfig _emitterConfig;
    private readonly DefaultAzureCredential _defaultCredential;
    // private static readonly HttpClient _httpClient = new HttpClient();

    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _emitterConfig = ReadConfiguration(configuration);
        _defaultCredential = string.IsNullOrEmpty(_emitterConfig.ManagedIdentityClientId)
            ? new DefaultAzureCredential()
            : new DefaultAzureCredential(options: new (){ ManagedIdentityClientId = _emitterConfig.ManagedIdentityClientId });
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            EventHubEmitter ehEmitter = new(_logger, _emitterConfig);

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
                await Task.Delay(_emitterConfig.CustomMetricInterval, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError("{error}", ex.ToString());
        }
    }

    private static EmitterConfig ReadConfiguration(IConfiguration cfg)
    {
        EmitterConfig config = new()
        {
            EventHubNamespace = cfg.GetValue<string>("EventHubNamespace") ?? string.Empty,
            EventHubName = cfg.GetValue<string>("EventHubName") ?? string.Empty,
            ConsumerGroup = cfg.GetValue<string>("ConsumerGroup") ?? string.Empty,
            CheckpointAccountName = cfg.GetValue<string>("CheckpointAccountName") ?? string.Empty,
            CheckpointContainerName = cfg.GetValue<string>("CheckpointContainerName") ?? string.Empty,
            Region = cfg.GetValue<string>("Region") ?? string.Empty,
            TenantId = cfg.GetValue<string>("TenantId") ?? string.Empty,
            SubscriptionId = cfg.GetValue<string>("SubscriptionId") ?? string.Empty,
            ResourceGroup = cfg.GetValue<string>("ResourceGroup") ?? string.Empty,
            ManagedIdentityClientId = cfg.GetValue<string>("ManagedIdentityClientId") ?? string.Empty,
            CustomMetricInterval = DEFAULT_INTERVAL,
        };

        if ((string.IsNullOrEmpty(config.EventHubNamespace)) || (string.IsNullOrEmpty(config.EventHubName))
            || (string.IsNullOrEmpty(config.ConsumerGroup)) || (string.IsNullOrEmpty(config.CheckpointAccountName))
            || (string.IsNullOrEmpty(config.CheckpointContainerName)) || (string.IsNullOrEmpty(config.Region))
            || (string.IsNullOrEmpty(config.TenantId)) || (string.IsNullOrEmpty(config.SubscriptionId)) || (string.IsNullOrEmpty(config.ResourceGroup)))
        {
            throw new Exception("Configuration error, missing values");
        }

        if (cfg.GetValue<string>("CustomMetricInterval") != null &&
            int.TryParse(cfg.GetValue<string>("CustomMetricInterval"), out int interval))
        {
            config.CustomMetricInterval = interval;
        }

        return config;
    }
}