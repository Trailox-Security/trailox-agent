using Trailox.Agent.Config;
using Trailox.Agent.Engines.Databricks;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Engines.Snowflake;

/// <summary>
/// One Snowflake account: probes what the role can see, and turns each task into one statement
/// whose partitions are re-emitted as NDJSON straight into the upload, keyed by the LOWER-CASED
/// column names docs/PROTOCOL.md lists. Values are forwarded as the API returns them (strings or null).
/// </summary>
public sealed class SfSource : IEngineSource
{
    // Lazy: an unreadable key must surface as THIS endpoint's probe error, not as an exception
    // while the runner builds its sessions, which would stop every other endpoint too.
    private readonly Lazy<SfStatements> _lazyStatements;
    private readonly EndpointConfig _endpoint;

    /// <summary>How often the oldest statement and sign-in are read again (see <see cref="ProbeAsync"/>).</summary>
    internal static readonly TimeSpan EarliestEvery = TimeSpan.FromDays(1);

    private readonly TimeProvider _clock;
    private readonly Earliest _earliestEvent = new();
    private readonly Earliest _earliestSession = new();

    public SfSource(HttpClient http, EndpointConfig endpoint) : this(http, endpoint, TimeProvider.System)
    {
    }

    internal SfSource(HttpClient http, EndpointConfig endpoint, TimeProvider clock)
    {
        _endpoint = endpoint;
        _clock = clock;
        var role = endpoint.Options.TryGetValue(SnowflakeEngine.RoleOption, out var r) ? r : SnowflakeEngine.DefaultRole;
        _lazyStatements = new Lazy<SfStatements>(() => new SfStatements(
            http, new SfAuth(endpoint.Host, endpoint.Username, endpoint.Password),
            SnowflakeEngine.ApiHost(endpoint.Host), endpoint.ClusterName, role));
    }

    private SfStatements Statements => _lazyStatements.Value;

    /// <summary>
    /// The version is HARD: it is the one statement that proves the key, the role and the
    /// warehouse all work, so its failure is the probe error the customer needs to see. Login
    /// history fails toward unreadable; statement text is never masked for these viewer roles.
    /// </summary>
    /// <remarks>
    /// ⚠ THE OLDEST ROWS ARE READ AT MOST ONCE A DAY (from 1.15.0). Each is a MIN over the whole of QUERY_HISTORY or
    ///   LOGIN_HISTORY, which on a large account is a long scan on the customer's warehouse, and it ran at every probe
    ///   to report how far back the account's history reaches - which moves a day a day. A read that fails waits for the
    ///   next day too, so a scan that keeps running into a timeout is not paid hourly; the last value read is kept.
    /// </remarks>
    public async Task<EndpointCaps> ProbeAsync(CancellationToken ct)
    {
        var version = await Statements.ScalarAsync(SfRawSelects.ProbeVersion, ct) ?? "unknown";
        var loginsReadable = _endpoint.CollectSessionLog && await ReadableAsync(SfRawSelects.ProbeLogins, ct);

        return new EndpointCaps
        {
            Version = version,
            Columns = new List<string>(),
            HasSessionLog = loginsReadable,
            TextReadable = true,
            EarliestEventMicros = await EarliestAsync(_earliestEvent, SfRawSelects.ProbeEarliestEvent, ct),
            EarliestSessionMicros = loginsReadable ? await EarliestAsync(_earliestSession, SfRawSelects.ProbeEarliestSession, ct) : 0,
        };
    }

    /// <summary>The oldest row a view holds, read again once <see cref="EarliestEvery"/> has passed since the last attempt.</summary>
    private async Task<long> EarliestAsync(Earliest earliest, string sql, CancellationToken ct)
    {
        var now = _clock.GetUtcNow();
        if (earliest.AttemptedAt is { } at && now - at < EarliestEvery)
        {
            return earliest.Micros;
        }
        earliest.AttemptedAt = now;
        var read = await SoftLongAsync(sql, ct);
        if (read > 0)
        {
            earliest.Micros = read;
        }
        return earliest.Micros;
    }

    /// <summary>The last oldest-row value read (0 until one is), and when it was last attempted.</summary>
    private sealed class Earliest
    {
        public long Micros { get; set; }
        public DateTimeOffset? AttemptedAt { get; set; }
    }

    public async Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct)
    {
        var sql = SfRawSelects.ForTask(_endpoint, task);
        if (sql == null)
        {
            return null;
        }
        // Settles before the upload starts, so a refusal is a source error, not a half-sent chunk.
        var result = await Statements.ExecuteAsync(sql, ct);
        var columns = result.Columns.Select(c => c.ToLowerInvariant()).ToList();
        var body = new NdjsonRowStream(columns, Statements.RowsAsync(result, ct), ct);
        return new RowStream(body, body);
    }

    private async Task<bool> ReadableAsync(string sql, CancellationToken ct)
    {
        try
        {
            await Statements.ScalarAsync(sql, ct);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    private async Task<long> SoftLongAsync(string sql, CancellationToken ct)
    {
        try
        {
            return long.TryParse(await Statements.ScalarAsync(sql, ct), out var n) ? n : 0;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return 0;
        }
    }
}
