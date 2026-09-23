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

    /// <summary>
    /// Whether this source is turned on in Trailox, as of the last check-in. Null until the server has
    /// said: the first probe has to happen before then, because a probe is how a source registers.
    /// </summary>
    public bool? EnabledOnServer { get; private set; }

    /// <summary>
    /// A source turned off in Trailox is left alone: a probe runs statements, and on Databricks and
    /// Snowflake that wakes a warehouse on the customer's bill. It matters most when the probe is failing,
    /// because a failed probe is otherwise retried on every check-in.
    /// </summary>
    public bool NeedsProbe => EnabledOnServer != false && (ProbeError != null || DateTime.UtcNow - _probedAtUtc > _probeInterval);

    /// <summary>What a check-in said about this source. Says it in the log once, on the change.</summary>
    public void HeardFromServer(bool enabled, ILogger logger)
    {
        if (EnabledOnServer == enabled)
        {
            return;
        }
        if (!enabled)
        {
            logger.LogInformation("endpoint {Alias} is turned off in Trailox: it will not be probed, and no windows will be sent for it, until it is turned on again", Config.Alias);
        }
        else if (EnabledOnServer == false)
        {
            logger.LogInformation("endpoint {Alias} is turned on again in Trailox", Config.Alias);
        }
        EnabledOnServer = enabled;
    }

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
        PollMinutes = Config.PollMinutes,
        BackfillDays = Config.BackfillDays,
    };

    internal static string Trim(string message) => message.Length > 600 ? message[..600] : message;
}
