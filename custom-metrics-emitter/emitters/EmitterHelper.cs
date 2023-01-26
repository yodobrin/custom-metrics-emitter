namespace custom_metrics_emitter.emitters;

using Azure.Core;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

public class EmitterHelper
{
    private static readonly HttpClient _httpClient = new();

    public static async Task<HttpResponseMessage> SendCustomMetric(string? region, string? resourceId,
        EmitterSchema metricToSend, AccessToken accessToken, ILogger<Worker> logger, CancellationToken cancellationToken = default)
    {
        if ((region != null) && (resourceId != null))
        {
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken.Token);
            _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            string uri = $"https://{region}.monitoring.azure.com{resourceId}/metrics";
            string jsonString = JsonSerializer.Serialize<EmitterSchema>(metricToSend);

            StringContent content = new(
                content: jsonString,
                encoding: Encoding.UTF8,
                mediaType: "application/json");

            logger.LogInformation("SendCustomMetric:{uri} with payload:{payload}", uri, jsonString);
            var response = await _httpClient.PostAsync(uri, content, cancellationToken);
            return response;
        }

        return new HttpResponseMessage(HttpStatusCode.LengthRequired);
    }
}