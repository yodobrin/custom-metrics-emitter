namespace custom_metrics_emitter.emitters;

using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

public record AccessTokenAndExpiration(bool isExpired, string token);

public class EmitterHelper
{
    private static readonly HttpClient _httpClient = new();
    private readonly ILogger<Worker> _logger;
    private readonly TokenStore _TokenStore;

    public EmitterHelper(ILogger<Worker> logger, DefaultAzureCredential defaultAzureCredential)
    {
        _logger = logger;
        _TokenStore = new TokenStore(
            defaultAzureCredential);
    }  

    public async Task<HttpResponseMessage> SendCustomMetric(
        string? region, string? resourceId, EmitterSchema metricToSend,
        CancellationToken cancellationToken = default)
    {
        if ((region != null) && (resourceId != null))
        {
            var record = await _TokenStore.RefreshAzureMonitorCredentialOnDemandAsync(cancellationToken);
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", record.token);
            _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            string uri = $"https://{region}.monitoring.azure.com{resourceId}/metrics";
            string jsonString = JsonSerializer.Serialize(metricToSend, _jsonOptions);

            StringContent content = new(
                content: jsonString,
                encoding: Encoding.UTF8,
                mediaType: "application/json");

            _logger.LogInformation("SendCustomMetric:{uri} with payload:{payload}", uri, jsonString);

            return await _httpClient.PostAsync(uri, content, cancellationToken);
        }

        return new HttpResponseMessage(HttpStatusCode.LengthRequired);
    }

    public Task<AccessTokenAndExpiration> RefreshAzureEventHubCredentialOnDemandAsync(CancellationToken cancellationToken = default)
    {
        return _TokenStore.RefreshAzureEventHubCredentialOnDemandAsync(cancellationToken);
    }

    public Task<AccessTokenAndExpiration> RefreshCredentialOnDemandAsync(string audience,
        CancellationToken cancellationToken = default)
    {
        return _TokenStore.RefreshCredentialOnDemand(audience, cancellationToken);
    }

    private static JsonSerializerOptions _jsonOptions = CreateJsonOptions();

    internal static JsonSerializerOptions CreateJsonOptions()
    {
        JsonSerializerOptions options = new() { WriteIndented = false };
        options.Converters.Add(new SortableDateTimeConverter());
        return options;
    }

    private class SortableDateTimeConverter : JsonConverter<DateTime>
    {
        private const string format = "s"; //SortableDateTimePattern yyyy'-'MM'-'dd'T'HH':'mm':'ss

        public override void Write(Utf8JsonWriter writer, DateTime date, JsonSerializerOptions options)
        {
            writer.WriteStringValue(date.ToUniversalTime().ToString(format));
        }
        public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return DateTime.ParseExact(reader.GetString()!, format, provider: null);
        }
    }

    private class TokenStore
    {
        private static readonly string MONITOR_SCOPE = "https://monitor.azure.com/.default";
        private static readonly string EVENTHUBS_SCOPE = "https://eventhubs.azure.net/.default";

        private readonly DefaultAzureCredential _defaultAzureCredential;
        private ConcurrentDictionary<string, AccessToken?> _scopeAndTokens = new();


        public TokenStore(DefaultAzureCredential defaultAzureCredential)
        {
            (_defaultAzureCredential) = (defaultAzureCredential);
            RefreshAzureMonitorCredentialOnDemand();
            RefreshAzureEventHubCredentialOnDemand();
        }

        public Task<AccessTokenAndExpiration> RefreshAzureMonitorCredentialOnDemandAsync(CancellationToken cancellationToken = default)
        {
            return RefreshCredentialOnDemand(MONITOR_SCOPE, cancellationToken);
        }

        public Task<AccessTokenAndExpiration> RefreshAzureEventHubCredentialOnDemandAsync(CancellationToken cancellationToken = default)
        {
            return RefreshCredentialOnDemand(EVENTHUBS_SCOPE, cancellationToken);
        }

        public AccessTokenAndExpiration RefreshAzureMonitorCredentialOnDemand(CancellationToken cancellationToken = default)
        {
            return RefreshCredentialOnDemand(MONITOR_SCOPE, cancellationToken).Result;
        }

        public AccessTokenAndExpiration RefreshAzureEventHubCredentialOnDemand(CancellationToken cancellationToken = default)
        {
            return RefreshCredentialOnDemand(EVENTHUBS_SCOPE, cancellationToken).Result;
        }

        public async Task<AccessTokenAndExpiration> RefreshCredentialOnDemand(string scope, CancellationToken cancellationToken = default)
        {
            bool needsNewToken(TimeSpan safetyInterval)
            {
                AccessToken? token;
                if (_scopeAndTokens.TryGetValue(scope, out token))
                {
                    if (!token.HasValue) return true;
                    var timeUntilExpiry = token!.Value.ExpiresOn.Subtract(DateTimeOffset.UtcNow);
                    return timeUntilExpiry < safetyInterval;
                }
                return true;
            }

            var isExpired = needsNewToken(safetyInterval: TimeSpan.FromMinutes(5.0));

            if (isExpired)
            {
                _scopeAndTokens.TryAdd(scope, await _defaultAzureCredential.GetTokenAsync(
                    requestContext: new TokenRequestContext(new[] { scope }),
                    cancellationToken: cancellationToken));
            }

            return new AccessTokenAndExpiration(isExpired, _scopeAndTokens[scope]!.Value.Token);
        }
    }
}
