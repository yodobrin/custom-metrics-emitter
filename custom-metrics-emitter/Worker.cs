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

namespace custom_metrics_emitter;

public class Worker : BackgroundService
{
    private DateTime _lastRefreshToken = DateTime.MinValue;
    private readonly TimeSpan ONE_HOUR = new TimeSpan(1, 0, 0);
    private readonly ILogger<Worker> _logger;
    private readonly EmitterConfig _config;
    private readonly TelemetryClient _telemetryClient;
    private readonly ChainedTokenCredential _credential;
    private static HttpClient _httpClient = new HttpClient();
    
    private const string METRICS_SCOPE = "https://monitor.azure.com/.default";

    public Worker(ILogger<Worker> logger, IOptions<EmitterConfig> options, TelemetryClient tc)
    {
        _logger = logger;
        _config = options.Value;
        _telemetryClient = tc;
        _credential = SetChainedTokenCredential();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {            
            EventHubEmitter ehEmitter =
                new EventHubEmitter(_logger, _credential, _config);


            AccessToken accessToken = default!;
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                using (_telemetryClient.StartOperation<RequestTelemetry>("operation"))
                {
                    //check if need to get new token
                    if (DateTime.Now.Subtract(ONE_HOUR) > _lastRefreshToken)
                    {
                        accessToken = await GetTokenAsync();
                        _lastRefreshToken = DateTime.Now;
                        _logger.LogInformation("Refresh token at:" + _lastRefreshToken.ToString());
                    }
                    var res = await ehEmitter.SendAsync(accessToken);
                    if (((int)res.StatusCode) >= 400)
                    {
                        _logger.LogError("Error sending custom event with status:" + res.StatusCode);
                    }
                    else
                    {
                        _logger.LogInformation("Send Custom Metric end with status:" + res.StatusCode);
                        _telemetryClient.TrackEvent("Send Custom Metric call event completed with status:" + res.StatusCode);
                    }
                }
                await Task.Delay(10000, stoppingToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex.ToString());
            _telemetryClient.TrackException(ex);
        }
        finally
        {
            _telemetryClient.Flush();
            Task.Delay(5000, stoppingToken).Wait(stoppingToken);
        }
    }

    private ChainedTokenCredential SetChainedTokenCredential()
    {
        ChainedTokenCredential retVal;

        var clientId = Environment.GetEnvironmentVariable("AZURE_CLIENT_ID");
        var clientSecret = Environment.GetEnvironmentVariable("AZURE_CLIENT_SECRET");
        var tenantId = Environment.GetEnvironmentVariable("AZURE_TENANT_ID");
        if ((string.IsNullOrEmpty(clientId) == false) && (string.IsNullOrEmpty(clientSecret) == false)
            && (string.IsNullOrEmpty(tenantId) == false))
        {
            retVal = new ChainedTokenCredential(
                new ClientSecretCredential(tenantId, clientId, clientSecret));
        }
        else
        {
            retVal = new ChainedTokenCredential(
                new AzureCliCredential(),
                new ManagedIdentityCredential()
            );
        }
        
        return retVal;      
    }

    private async Task<AccessToken> GetTokenAsync()
    {        
        var scope = new string[] { METRICS_SCOPE };
        
        return await _credential.GetTokenAsync(new TokenRequestContext(scope));        
    }
}

