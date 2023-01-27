namespace custom_metrics_emitter.emitters;

using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

public class EmitterHelper
{
    private static readonly HttpClient _httpClient = new();
    private readonly ILogger<Worker> _logger;
    private readonly TokenUpdater _TokenUpdater;

    public EmitterHelper(ILogger<Worker> logger, DefaultAzureCredential defaultAzureCredential)
    {
        _logger = logger;
        _TokenUpdater = new TokenUpdater(
            defaultAzureCredential,
            scope: "https://monitor.azure.com/.default");
    }

    public async Task<HttpResponseMessage> SendCustomMetric(
        string? region, string? resourceId, EmitterSchema metricToSend,
        CancellationToken cancellationToken = default)
    {
        if ((region != null) && (resourceId != null))
        {
            var accessToken = await _TokenUpdater.RefreshAzureMonitorCredentialOnDemand(cancellationToken);
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
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

    private class TokenUpdater
    {
        private readonly DefaultAzureCredential _defaultAzureCredential;
        private readonly string _scope;

        public TokenUpdater(DefaultAzureCredential defaultAzureCredential, string scope)
        {
            (_defaultAzureCredential, _scope) = (defaultAzureCredential, scope);
        }

        private AccessToken? _metricAccessToken = null;

        public async Task<string> RefreshAzureMonitorCredentialOnDemand(CancellationToken cancellationToken = default)
        {
            bool needsNewToken()
            {
                if (!_metricAccessToken.HasValue) return true;
                var minutesUntilExpiry = DateTimeOffset.UtcNow.Subtract(_metricAccessToken.Value.ExpiresOn).TotalMinutes;
                return minutesUntilExpiry < 5.0;
            }

            if (needsNewToken())
            {
                _metricAccessToken = await _defaultAzureCredential.GetTokenAsync(
                    requestContext: new TokenRequestContext(new[] { _scope }),
                    cancellationToken: cancellationToken);
            }

            return _metricAccessToken!.Value.Token;
        }
    }
}