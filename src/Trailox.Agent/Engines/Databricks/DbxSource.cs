using Trailox.Agent.Config;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Engines.Databricks;

/// <summary>
/// One Databricks workspace: probes what the principal can see, and turns each task into one
/// statement whose pages are re-emitted as NDJSON straight into the upload - except the service
/// principal directory, which is a listing of the workspace's SCIM API (<see cref="DbxDirectory"/>).
/// </summary>
public sealed class DbxSource : IEngineSource
{
    private readonly DbxStatements _statements;
    private readonly DbxDirectory _directory;
    private readonly EndpointConfig _endpoint;

    public DbxSource(HttpClient api, HttpClient links, EndpointConfig endpoint)
    {
        _endpoint = endpoint;
        var auth = new DbxAuth(api, endpoint.Host, endpoint.Username, endpoint.Password);
        _statements = new DbxStatements(api, links, auth, endpoint.Host, endpoint.ClusterName);
        _directory = new DbxDirectory(api, auth, endpoint.Host);
    }

    /// <summary>
    /// Version (soft), whether statement text is readable (fails toward readable: only an
    /// all-redacted sample of at least one row is evidence), whether the audit log is readable
    /// (fails toward unreadable: a stream believed readable would fail every window), and the
    /// oldest rows so Trailox does not plan windows before them.
    /// </summary>
    public async Task<EndpointCaps> ProbeAsync(CancellationToken ct)
    {
        var version = await SoftScalarAsync(DbxRawSelects.ProbeVersion, ct) ?? "unknown";
        var textReadable = await ProbeTextReadableAsync(ct);
        var auditReadable = _endpoint.CollectSessionLog && await ProbeAuditReadableAsync(ct);

        return new EndpointCaps
        {
            Version = version,
            Columns = new List<string>(),
            HasSessionLog = auditReadable,
            TextReadable = textReadable,
            EarliestEventMicros = await SoftLongAsync(DbxRawSelects.ProbeEarliestEvent, ct),
            EarliestSessionMicros = auditReadable ? await SoftLongAsync(DbxRawSelects.ProbeEarliestSession, ct) : 0,
        };
    }

    public async Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct)
    {
        if (task.Stream == ProtocolInfo.Streams.CatalogServicePrincipals)
        {
            // Not a statement: the directory is a REST listing, and it is read whole first, so a
            // refusal surfaces as a source error before any upload starts.
            var principals = await _directory.ListAsync(ct);
            var listed = new NdjsonRowStream(DbxDirectory.Columns, AsAsync(principals), ct);
            return new RowStream(listed, listed);
        }

        var sql = DbxRawSelects.ForTask(_endpoint, task);
        if (sql == null)
        {
            return null;
        }
        // The statement settles here, so a refusal surfaces as a source error before any upload
        // starts; only the paging happens while the gateway reads.
        var result = await _statements.ExecuteAsync(sql, ct);
        var body = new NdjsonRowStream(result.Columns, _statements.RowsAsync(result, ct), ct);
        return new RowStream(body, body);
    }

    private async Task<bool> ProbeTextReadableAsync(CancellationToken ct)
    {
        try
        {
            var result = await _statements.ExecuteAsync(DbxRawSelects.ProbeTextRedaction, ct);
            await foreach (var row in _statements.RowsAsync(result, ct))
            {
                var sampled = AsLong(row, 0);
                var redacted = AsLong(row, 1);
                return !(sampled > 0 && redacted == sampled);
            }
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return true;
        }
    }

    private async Task<bool> ProbeAuditReadableAsync(CancellationToken ct)
    {
        try
        {
            await _statements.ScalarAsync(DbxRawSelects.ProbeAudit, ct);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    private async Task<string?> SoftScalarAsync(string sql, CancellationToken ct)
    {
        try
        {
            return await _statements.ScalarAsync(sql, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    private async Task<long> SoftLongAsync(string sql, CancellationToken ct) =>
        long.TryParse(await SoftScalarAsync(sql, ct), out var n) ? n : 0;

    private static async IAsyncEnumerable<System.Text.Json.JsonElement> AsAsync(IReadOnlyList<System.Text.Json.JsonElement> rows)
    {
        foreach (var row in rows)
        {
            yield return row;
        }
        await Task.CompletedTask;
    }

    private static long AsLong(System.Text.Json.JsonElement row, int index) =>
        row.ValueKind == System.Text.Json.JsonValueKind.Array && row.GetArrayLength() > index && long.TryParse(row[index].ToString(), out var n) ? n : 0;
}
