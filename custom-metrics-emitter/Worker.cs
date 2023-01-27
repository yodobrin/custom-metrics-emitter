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

    public Worker(ILogger<Worker> logger, IConfiguration cfg)
    {
        _logger = logger;

        _emitterConfig = new(
            EventHubNamespace: cfg.require("EventHubNamespace"),
            EventHubName: cfg.require("EventHubName"),
            ConsumerGroup: cfg.require("ConsumerGroup"),
            CheckpointAccountName: cfg.require("CheckpointAccountName"),
            CheckpointContainerName: cfg.require("CheckpointContainerName"),
            Region: cfg.require("Region"),
            TenantId: cfg.require("TenantId"),
            SubscriptionId: cfg.require("SubscriptionId"),
            ResourceGroup: cfg.require("ResourceGroup"),
            ManagedIdentityClientId: cfg.optional("ManagedIdentityClientId"),
            CustomMetricInterval: cfg.getIntOrDefault("CustomMetricInterval", DEFAULT_INTERVAL));

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
}

internal static class IConfigurationExtensions
{
    internal static string optional(this IConfiguration cfg, string name) => cfg.GetValue<string>(name) ?? string.Empty;

    internal static string require(this IConfiguration cfg, string name)
    {
        var val = cfg.optional(name);
        if (string.IsNullOrEmpty(val))
        {
            throw new ArgumentException($"Configuration error, missing key {name}", nameof(cfg));
        }
        return val;
    }

    internal static int getIntOrDefault(this IConfiguration cfg, string name, int defaulT) =>
        !string.IsNullOrEmpty(cfg.GetValue<string>(name)) && int.TryParse(cfg.GetValue<string>(name), out int value) ? value : defaulT;
}