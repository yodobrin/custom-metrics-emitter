namespace custom_metrics_emitter.emitters;

using Azure.Core;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

public class EmitterHelper
{
    private static readonly HttpClient _httpClient = new();

    public static Task<HttpResponseMessage> SendCustomMetric(string? region, string? resourceId,
        EmitterSchema metricToSend, AccessToken accessToken, ILogger<Worker> logger, CancellationToken cancellationToken = default)
    {
        if ((region != null) && (resourceId != null))
        {
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken.Token);
            _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            string uri = $"https://{region}.monitoring.azure.com{resourceId}/metrics";
            string jsonString = JsonSerializer.Serialize(metricToSend, _jsonOptions);

            StringContent content = new(
                content: jsonString,
                encoding: Encoding.UTF8,
                mediaType: "application/json");

            logger.LogInformation("SendCustomMetric:{uri} with payload:{payload}", uri, jsonString);

            return _httpClient.PostAsync(uri, content, cancellationToken);
        }

        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.LengthRequired));
    }

    private static JsonSerializerOptions _jsonOptions = CreateJsonOptions();

    private static JsonSerializerOptions CreateJsonOptions()
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
            writer.WriteStringValue(date.ToString(format));
        }
        public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return DateTime.ParseExact(reader.GetString()!, format, provider: null);
        }
    }
}