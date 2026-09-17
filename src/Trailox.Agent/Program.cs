using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Gateway;
using Trailox.Agent.Runner;

namespace Trailox.Agent;

/// <summary>
/// trailox-agent: reads your database's own audit tables inside your network and ships the
/// rows to Trailox over HTTPS, outbound only. See README.md.
///
/// Commands: (none) run · validate-config · healthcheck · version
/// Exit codes: 0 stopped · 1 healthcheck failed · 2 config invalid · 3 key rejected · 4 agent too old
/// </summary>
public static class Program
{
    public const int ExitConfigInvalid = 2;
    public const int ExitKeyRejected = 3;
    public const int ExitVersionTooOld = 4;

    public static readonly string Version =
        Assembly.GetExecutingAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0]
        ?? "0.0.0";

    public static string ConfigPath => Environment.GetEnvironmentVariable("TRAILOX_CONFIG") ?? "/etc/trailox/agent.yaml";
    public static string HealthPath => Environment.GetEnvironmentVariable("TRAILOX_HEALTH_FILE") ?? Path.Combine(Path.GetTempPath(), "trailox-agent.health");

    public static async Task<int> Main(string[] args)
    {
        var command = args.Length > 0 ? args[0].ToLowerInvariant() : "run";
        switch (command)
        {
            case "version":
                Console.WriteLine(Version);
                return 0;

            case "healthcheck":
                return new HealthFile(HealthPath).IsHealthy() ? 0 : 1;

            case "validate-config":
                try
                {
                    var (cfg, fp) = ConfigLoader.Load(ConfigPath, new EnvironmentSecretReader());
                    Console.WriteLine($"ok: {cfg.Endpoints.Count} endpoint(s), gateway {cfg.Gateway}, {fp}");
                    return 0;
                }
                catch (Exception ex) when (ex is ConfigException or IOException)
                {
                    Console.Error.WriteLine(ex.Message);
                    return ExitConfigInvalid;
                }

            case "run":
                return await RunAsync(args);

            default:
                Console.Error.WriteLine($"unknown command '{args[0]}'. Commands: run (default), validate-config, healthcheck, version");
                return 2;
        }
    }

    private static async Task<int> RunAsync(string[] args)
    {
        AgentConfig config;
        string fingerprint;
        try
        {
            (config, fingerprint) = ConfigLoader.Load(ConfigPath, new EnvironmentSecretReader());
        }
        catch (Exception ex) when (ex is ConfigException or IOException)
        {
            Console.Error.WriteLine(ex.Message);
            return ExitConfigInvalid;
        }

        var key = ReadAgentKey();
        if (key == null)
        {
            Console.Error.WriteLine("TRAILOX_AGENT_KEY (or TRAILOX_AGENT_KEY_FILE) is required: the key shown once when the agent was enrolled.");
            return ExitConfigInvalid;
        }

        var builder = Host.CreateApplicationBuilder(args);
        ConfigureLogging(builder.Logging);

        // One named HttpClient per engine, plus the gateway's. Timeouts are generous on purpose:
        // a window on a busy server can take minutes to stream.
        foreach (var engine in EngineRegistry.All)
        {
            builder.Services.AddHttpClient(engine.Name, c => c.Timeout = TimeSpan.FromMinutes(30));
        }
        builder.Services.AddHttpClient("gateway", c => c.Timeout = TimeSpan.FromMinutes(30));
        // Snowflake serves every result partition after the first gzip-compressed.
        builder.Services.AddHttpClient(new Engines.Snowflake.SnowflakeEngine().Name)
            .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler { AutomaticDecompression = System.Net.DecompressionMethods.All });
        // Databricks hands result pages out as presigned storage links that take NO Authorization
        // header; they are fetched through a client that never carries one.
        builder.Services.AddHttpClient(Engines.Databricks.DatabricksEngine.LinksClientName, c => c.Timeout = TimeSpan.FromMinutes(30));

        builder.Services.AddSingleton(config);
        builder.Services.AddSingleton(new ErrorRing());
        builder.Services.AddSingleton(new HealthFile(HealthPath));
        builder.Services.AddSingleton(sp =>
            new GatewayClient(sp.GetRequiredService<IHttpClientFactory>().CreateClient("gateway"), new Uri(config.Gateway), key, Version));
        builder.Services.AddSingleton<IReadOnlyList<EndpointSession>>(sp =>
        {
            var factory = sp.GetRequiredService<IHttpClientFactory>();
            var probeInterval = TimeSpan.FromMinutes(config.ProbeIntervalMinutes);
            return config.Endpoints
                .Select(e => new EndpointSession(e, EngineRegistry.Require(e.Engine).CreateSource(e, factory), probeInterval))
                .ToList();
        });
        builder.Services.AddSingleton(sp =>
            new TaskExecutor(sp.GetRequiredService<GatewayClient>(), sp.GetRequiredService<ErrorRing>(), sp.GetRequiredService<ILoggerFactory>().CreateLogger("tasks")));
        builder.Services.AddSingleton(sp => new AgentLoop(
            config, fingerprint, sp.GetRequiredService<IReadOnlyList<EndpointSession>>(), sp.GetRequiredService<GatewayClient>(),
            sp.GetRequiredService<TaskExecutor>(), sp.GetRequiredService<ErrorRing>(), sp.GetRequiredService<HealthFile>(),
            sp.GetRequiredService<IHostApplicationLifetime>(), sp.GetRequiredService<ILogger<AgentLoop>>()));
        builder.Services.AddHostedService(sp => sp.GetRequiredService<AgentLoop>());

        using var host = builder.Build();
        await host.RunAsync();
        return Environment.ExitCode;
    }

    private static void ConfigureLogging(ILoggingBuilder logging)
    {
        logging.ClearProviders();
        logging.AddSimpleConsole(o =>
        {
            o.SingleLine = true;
            o.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
            o.UseUtcTimestamp = true;
        });
        logging.SetMinimumLevel(Enum.TryParse<LogLevel>(Environment.GetEnvironmentVariable("TRAILOX_LOG_LEVEL"), true, out var level) ? level : LogLevel.Information);
        logging.AddFilter("System.Net.Http", LogLevel.Warning);
        logging.AddFilter("Microsoft", LogLevel.Warning);
    }

    private static string? ReadAgentKey()
    {
        var key = Environment.GetEnvironmentVariable("TRAILOX_AGENT_KEY");
        if (!string.IsNullOrWhiteSpace(key))
        {
            return key.Trim();
        }
        var file = Environment.GetEnvironmentVariable("TRAILOX_AGENT_KEY_FILE");
        if (!string.IsNullOrWhiteSpace(file) && File.Exists(file))
        {
            var fromFile = File.ReadAllText(file).Trim();
            return fromFile.Length > 0 ? fromFile : null;
        }
        return null;
    }
}
