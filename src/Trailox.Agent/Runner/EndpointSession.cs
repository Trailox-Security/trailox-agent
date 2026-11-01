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
    /// <summary>How soon a refused source is probed again: a fix should show soon, and a probe at every check-in costs a session each.</summary>
    internal static readonly TimeSpan RefusedProbeInterval = TimeSpan.FromMinutes(5);

    private readonly TimeSpan _probeInterval;
    private readonly Func<DateTime> _utcNow;
    private DateTime _probedAtUtc = DateTime.MinValue;
    private bool _passwordRejected;
    private bool _refused;

    public EndpointConfig Config { get; }
    public IEngineSource Source { get; }
    public EndpointCaps Caps { get; private set; } = new();
    public string? ProbeError { get; private set; }

    public EndpointSession(EndpointConfig config, IEngineSource source, TimeSpan probeInterval, Func<DateTime>? utcNow = null)
    {
        Config = config;
        Source = source;
        _probeInterval = probeInterval;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
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
    /// <remarks>A rejected password stops the probes until the agent restarts (<see cref="SourceException.AuthenticationRejected"/>).</remarks>
    public bool NeedsProbe => !_passwordRejected && EnabledOnServer != false
        && ((ProbeError != null && !_refused) || _utcNow() - _probedAtUtc > (_refused ? RefusedProbeInterval : _probeInterval));

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
            _refused = false;
            _probedAtUtc = _utcNow();
            // Probed columns are a ClickHouse notion (optional query_log columns); other engines report none.
            var columns = Caps.Columns.Count > 0 ? $", {Caps.Columns.Count} probed columns present" : "";
            var text = Caps.TextReadable ? "" : ", statement text redacted at source";
            logger.LogInformation("endpoint {Alias} ({Engine}): version {Version}, session log {SessionLog}{Columns}{Text}",
                Config.Alias, Config.Engine, Caps.Version, Caps.HasSessionLog ? "present" : "absent", columns, text);
        }
        catch (SourceException ex) when (ex.AuthenticationRejected)
        {
            _passwordRejected = true;
            ProbeError = Trim(ex.Message + ". The agent stops signing in to this database until it restarts: "
                + (ex.AuthenticationFix ?? "fix the password") + ", then restart the agent");
            _probedAtUtc = DateTime.MinValue;
            logger.LogWarning("endpoint {Alias}: probe failed: {Message}", Config.Alias, ProbeError);
        }
        catch (SourceException ex) when (ex.RefusedCaps is { } caps)
        {
            Caps = caps;
            ProbeError = Trim(ex.Message);
            _refused = true;
            _probedAtUtc = _utcNow();
            logger.LogWarning("endpoint {Alias}: the probe refused this source: {Message}", Config.Alias, ProbeError);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ProbeError = Trim(ex.Message);
            _refused = false;
            _probedAtUtc = DateTime.MinValue;
            logger.LogWarning("endpoint {Alias}: probe failed: {Message}", Config.Alias, ProbeError);
        }
    }

    /// <remarks>
    /// <c>monitorUser</c> is the account that signs in to the database: the catalog user when there is one (1.9.0,
    /// MongoDB Atlas, where <see cref="EndpointConfig.Username"/> is an API service account that never does), so
    /// that user's own sign-ins and commands are shown as Trailox's own.
    /// </remarks>
    public EndpointReport Report() => new()
    {
        Alias = Config.Alias,
        Engine = Config.Engine,
        Kind = Config.Kind,
        ClusterName = Config.ClusterName,
        MonitorUser = string.IsNullOrWhiteSpace(Config.CatalogUsername) ? Config.Username : Config.CatalogUsername,
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
