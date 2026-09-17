using Microsoft.Extensions.Logging;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Runner;

/// <summary>
/// One configured endpoint at run time: its engine source and what the last probe found.
/// Engine-neutral; everything database-specific is behind <see cref="IEngineSource"/>.
/// </summary>
public sealed class EndpointSession
{
    private readonly TimeSpan _probeInterval;
    private DateTime _probedAtUtc = DateTime.MinValue;

    public EndpointConfig Config { get; }
    public IEngineSource Source { get; }
    public EndpointCaps Caps { get; private set; } = new();
    public string? ProbeError { get; private set; }

    public EndpointSession(EndpointConfig config, IEngineSource source, TimeSpan probeInterval)
    {
        Config = config;
        Source = source;
        _probeInterval = probeInterval;
    }

    public bool NeedsProbe => ProbeError != null || DateTime.UtcNow - _probedAtUtc > _probeInterval;

    /// <summary>
    /// Asks the server what it is and what it holds. Fails soft into <see cref="ProbeError"/>:
    /// the check-in still reports the endpoint, so the Agents page can show WHY it is silent.
    /// </summary>
    public async Task ProbeAsync(ILogger logger, CancellationToken ct)
    {
        try
        {
            Caps = await Source.ProbeAsync(ct);
            ProbeError = null;
            _probedAtUtc = DateTime.UtcNow;
            // Probed columns are a ClickHouse notion (optional query_log columns); other engines report none.
            var columns = Caps.Columns.Count > 0 ? $", {Caps.Columns.Count} probed columns present" : "";
            var text = Caps.TextReadable ? "" : ", statement text redacted at source";
            logger.LogInformation("endpoint {Alias} ({Engine}): version {Version}, session log {SessionLog}{Columns}{Text}",
                Config.Alias, Config.Engine, Caps.Version, Caps.HasSessionLog ? "present" : "absent", columns, text);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ProbeError = Trim(ex.Message);
            _probedAtUtc = DateTime.MinValue;
            logger.LogWarning("endpoint {Alias}: probe failed: {Message}", Config.Alias, ProbeError);
        }
    }

    public EndpointReport Report() => new()
    {
        Alias = Config.Alias,
        Engine = Config.Engine,
        Kind = Config.Kind,
        ClusterName = Config.ClusterName,
        MonitorUser = Config.Username,
        CollectSessionLog = Config.CollectSessionLog,
        StoreRawQueryText = Config.StoreRawQueryText,
        ExcludedDatabases = Config.ExcludedDatabases,
        Caps = Caps,
        ProbeError = ProbeError,
    };

    internal static string Trim(string message) => message.Length > 600 ? message[..600] : message;
}
