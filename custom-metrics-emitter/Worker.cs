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
        string optional(string name) => cfg.GetValue<string>(name) ?? string.Empty;

        string require(string name)
        {
            var val = optional(name);
            if (string.IsNullOrEmpty(val))
            {
                throw new ArgumentException($"Configuration error, missing key {name}", nameof(cfg));
            }
            return val;
        }

        int getIntOrDefault(string name, int defaulT) =>
            !string.IsNullOrEmpty(cfg.GetValue<string>(name)) && int.TryParse(cfg.GetValue<string>(name), out int value) ? value : defaulT;

        return new()
        {
            EventHubNamespace = require("EventHubNamespace"),
            EventHubName = require("EventHubName"),
            ConsumerGroup = require("ConsumerGroup"),
            CheckpointAccountName = require("CheckpointAccountName"),
            CheckpointContainerName = require("CheckpointContainerName"),
            Region = require("Region"),
            TenantId = require("TenantId"),
            SubscriptionId = require("SubscriptionId"),
            ResourceGroup = require("ResourceGroup"),
            ManagedIdentityClientId = optional("ManagedIdentityClientId"),
            CustomMetricInterval = getIntOrDefault("CustomMetricInterval", DEFAULT_INTERVAL),
        };
    }
}