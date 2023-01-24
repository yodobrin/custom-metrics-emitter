using System;
using System.Text.Json;
using Azure.Core;
using Azure.Identity;

namespace custom_metrics_emitter
{
	public abstract class Emitter
	{
        protected readonly ILogger<Worker> _logger;        
		protected readonly EmitterConfig _config;

        public Emitter(ILogger<Worker> logger, EmitterConfig config)
		{
			_logger = logger;
			_config = config;
		}

		public virtual async Task<HttpResponseMessage> SendAsync(AccessToken accessToken)
		{
			await Task.Delay(1);
			return new HttpResponseMessage(System.Net.HttpStatusCode.NotAcceptable);
		}
	}

		
}

