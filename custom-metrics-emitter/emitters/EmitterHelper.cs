using System;
using System.Text;
using System.Text.Json;
using System.Net.Http.Headers;
using Azure.Core;

namespace custom_metrics_emitter.emitters
{
	public class EmitterHelper
	{
        private static HttpClient _httpClient = new HttpClient();

        const string CustomMetricsUrl = "https://{0}.monitoring.azure.com{1}/metrics";

        public static async Task<HttpResponseMessage> SendCustomMetric(string? region, string? resourceId,
            EmitterSchema metricToSend, AccessToken accessToken, ILogger<Worker> logger)
        {
            if ((region != null) && (resourceId != null))
            {
                _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken.Token);
                _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));                
                string uri = string.Format(CustomMetricsUrl, region, resourceId);
                string jsonString = JsonSerializer.Serialize<EmitterSchema>(metricToSend);
                logger.LogInformation("SendCustomMetric:{uri} with payload:{payload}", uri, jsonString);
                StringContent content = new StringContent(jsonString, Encoding.UTF8, "application/json");
                var response = await _httpClient.PostAsync(uri, content);
                return response;
            }

            return new HttpResponseMessage(System.Net.HttpStatusCode.LengthRequired);
        }
    }    
}

