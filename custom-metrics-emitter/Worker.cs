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
    private readonly int DEFAULT_INTERVAL = 10000;
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _config;
    private readonly DefaultAzureCredential _defaultCredential;           
    private static HttpClient _httpClient = new HttpClient();
    
    

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
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var emitterConfig = ReadConfiguration();
            EventHubEmitter ehEmitter =
                new EventHubEmitter(_logger, emitterConfig);
            
            while (!stoppingToken.IsCancellationRequested)
            {                
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                //check if need to get new tokens
                if (DateTime.Now.Subtract(ONE_HOUR) > _lastRefreshToken)
                {                    
                    await ehEmitter.RefreshTokens(_defaultCredential);
                    _lastRefreshToken = DateTime.Now;
                    _logger.LogInformation("Get refresh tokens at: {lastrefresh}", _lastRefreshToken.ToString());
                }
                var res = await ehEmitter.SendAsync();
                if (((int)res.StatusCode) >= 400)
                {
                    _logger.LogError("Error sending custom event with status: {status}", res.StatusCode);
                }
                else
                {
                    _logger.LogInformation("Send Custom Metric end with status: {status}", res.StatusCode);                   
                }
                await Task.Delay(emitterConfig.CustomMetricInterval, stoppingToken);
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
        if (_config.GetValue<string>("CustomMetricInterval") != null)
        {
            int interval = default!;
            if (int.TryParse(_config.GetValue<string>("CustomMetricInterval"), out interval))
            {
                config.CustomMetricInterval = interval;
            }
            else
            {
                config.CustomMetricInterval = DEFAULT_INTERVAL;
            }
        }
        else
        {
            config.CustomMetricInterval = DEFAULT_INTERVAL;
        }
        return config;
    }        
}

