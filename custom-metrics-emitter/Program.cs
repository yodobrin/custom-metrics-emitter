using custom_metrics_emitter;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        IConfiguration configuration = hostContext.Configuration;
        services.Configure<EmitterConfig>(configuration.GetSection(nameof(EmitterConfig)));
        services.AddApplicationInsightsTelemetryWorkerService();
        services.AddHostedService<Worker>();
    })
    .Build();

await host.RunAsync();

