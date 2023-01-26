namespace custom_metrics_emitter;

using System.Net;

public abstract class Emitter
{
    protected readonly ILogger<Worker> _logger;
    protected readonly EmitterConfig _config;

    public Emitter(ILogger<Worker> logger, EmitterConfig config)
    {
        _logger = logger;
        _config = config;
    }

    public virtual async Task<HttpResponseMessage> SendAsync(CancellationToken cancellationToken = default)
    {
        await Task.Delay(1, cancellationToken);
        return new HttpResponseMessage(HttpStatusCode.NotAcceptable);
    }
}