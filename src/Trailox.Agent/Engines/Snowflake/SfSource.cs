using Trailox.Agent.Config;
using Trailox.Agent.Engines.Databricks;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Engines.Snowflake;

/// <summary>
/// One Snowflake account: probes what the role can see, and turns each task into one statement
/// whose partitions are re-emitted as NDJSON straight into the upload, keyed by the LOWER-CASED
/// column names the raw tables use. Values are forwarded as the API returns them (strings or null).
/// </summary>
public sealed class SfSource : IEngineSource
{
    // Lazy: an unreadable key must surface as THIS endpoint's probe error, not as an exception
    // while the runner builds its sessions, which would stop every other endpoint too.
    private readonly Lazy<SfStatements> _lazyStatements;
    private readonly EndpointConfig _endpoint;

    public SfSource(HttpClient http, EndpointConfig endpoint)
    {
        _endpoint = endpoint;
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
            EarliestEventMicros = await SoftLongAsync(SfRawSelects.ProbeEarliestEvent, ct),
            EarliestSessionMicros = loginsReadable ? await SoftLongAsync(SfRawSelects.ProbeEarliestSession, ct) : 0,
        };
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
