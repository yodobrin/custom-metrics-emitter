using System;
using System.Text.Json;
using Azure.Core;

namespace custom_metrics_emitter
{
	public abstract class Emitter
	{
		protected readonly string? Region;
		//format: /subscription/../resourceGroups/../providers/..
		protected readonly string? ResourceId;

        public Emitter(string? region, string? resourceId)
		{
			Region = region;
			ResourceId = resourceId;
		}

		public virtual async Task<HttpResponseMessage> SendAsync(AccessToken accessToken)
		{
			await Task.Delay(1);
			return new HttpResponseMessage(System.Net.HttpStatusCode.NotAcceptable);
		}
	}

		
}

