using System.Net.Http;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.Extensions.Options;
using Azure.Identity;
using Azure.Core;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Web;
using Microsoft.Extensions.Configuration;

namespace custom_metrics_emitter;

public class Worker : BackgroundService
{
    private DateTime _lastRefreshToken = DateTime.MinValue;
    private readonly TimeSpan ONE_HOUR = new TimeSpan(1, 0, 0);
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _config;
    private readonly DefaultAzureCredential _defaultCredential = new DefaultAzureCredential();
    private static HttpClient _httpClient = new HttpClient();
    
    private const string METRICS_SCOPE = "https://monitor.azure.com/.default";

    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _config = configuration;
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var emitterConfig = ReadConfiguration();
            EventHubEmitter ehEmitter =
                new EventHubEmitter(_logger, _defaultCredential, emitterConfig);

            AccessToken accessToken = default!;
            while (!stoppingToken.IsCancellationRequested)
            {                
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                //check if need to get new token
                if (DateTime.Now.Subtract(ONE_HOUR) > _lastRefreshToken)
                {
                    accessToken = await GetTokenAsync();
                    _lastRefreshToken = DateTime.Now;
                    _logger.LogInformation("Refresh token at: {lastrefresh}", _lastRefreshToken.ToString());
                }
                var res = await ehEmitter.SendAsync(accessToken);
                if (((int)res.StatusCode) >= 400)
                {
                    _logger.LogError("Error sending custom event with status: {status}", res.StatusCode);
                }
                else
                {
                    _logger.LogInformation("Send Custom Metric end with status: {status}", res.StatusCode);                   
                }
                await Task.Delay(10000, stoppingToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError("{error}",ex.ToString());
        }
    }

    private EmitterConfig ReadConfiguration()
    {        
        var config = new EmitterConfig()
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
        };

        if ((string.IsNullOrEmpty(config.EventHubNamespace)) || (string.IsNullOrEmpty(config.EventHubName))
            || (string.IsNullOrEmpty(config.ConsumerGroup)) || (string.IsNullOrEmpty(config.CheckpointAccountName))
            || (string.IsNullOrEmpty(config.CheckpointContainerName)) || (string.IsNullOrEmpty(config.Region))
            || (string.IsNullOrEmpty(config.TenantId)) || (string.IsNullOrEmpty(config.SubscriptionId)) || (string.IsNullOrEmpty(config.ResourceGroup)))
        {
            throw new Exception("Configuration error, missing values");
        }
        return config;
    }    

    private async Task<AccessToken> GetTokenAsync()
    {        
        var scope = new string[] { METRICS_SCOPE };
        return await _defaultCredential.GetTokenAsync(new TokenRequestContext(scope));        
    }
}

