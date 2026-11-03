using Trailox.Agent.Config;
using Trailox.Agent.Protocol;
using Trailox.Agent.Runner;
using Streams = Trailox.Agent.Protocol.ProtocolInfo.Streams;

namespace Trailox.Agent.Engines.Oracle;

/// <summary>
/// One Oracle source: a PDB or a non-CDB. The audit poll reads the unified audit trail for one window at a time, and the
/// catalog streams read the dictionary once a day. The agent keeps no connection between tasks.
/// </summary>
public sealed class OraSource : IEngineSource
{
    private readonly EndpointConfig _endpoint;
    private readonly OraCatalog _catalog;
    private readonly Func<DateTime> _utcNow;
    private readonly OraGrowth _growth = new();

    /// <summary>Whether the view has the SOURCE column, once a probe or a poll has asked. It does not go away while the agent runs.</summary>
    private bool? _sourceColumn;

    /// <summary>Why the newest probe refused the source; null when it did not.</summary>
    private string? _refused;

    public OraSource(EndpointConfig endpoint, IOraDriver? driver = null, Func<DateTime>? utcNow = null, TimeSpan? callLimit = null)
    {
        _endpoint = endpoint;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _catalog = new OraCatalog(endpoint, driver ?? new OdpDriver(), _utcNow, callLimit);
    }

    /// <remarks>
    /// 🔴 A FAILED CHECK REFUSES THE SOURCE, WITH WHAT THE PROBE FOUND. It is thrown with the caps, so the checks still reach the
    /// source page, and Trailox issues no task. A task issued before the refusal arrived is refused here without connecting.
    /// </remarks>
    public async Task<EndpointCaps> ProbeAsync(CancellationToken ct)
    {
        var facts = new OraFacts { NowUtc = _utcNow(), Kind = _endpoint.Kind, Host = _endpoint.Host };
        await using (var session = await _catalog.OpenAsync(ct))
        {
            try
            {
                await OraProbe.ReadAsync(session, facts, ct);
            }
            catch (OraDriverException ex)
            {
                throw _catalog.Failure(ex, ct);
            }
        }
        if (!facts.Slow.Contains(OraProbe.ColumnsView))
        {
            _sourceColumn = facts.SourceColumn;
        }
        if (facts.Space is { } space)
        {
            _growth.Add(facts.NowUtc, OraProbeChecks.TrailTablespace(facts) ?? "", space.UsedBytes);
            facts.GrowthBytesPerDay = _growth.BytesPerDay;
        }
        var checks = OraProbeChecks.All(facts).ToList();
        var caps = new EndpointCaps
        {
            Version = facts.Version,
            Columns = new List<string>(),
            HasSessionLog = true,
            TextReadable = true,
            // The trail's oldest record is never read: it would cost the whole table. Trailox starts from its backfill.
            EarliestEventMicros = 0,
            EarliestSessionMicros = 0,
            Oracle = OraProbeChecks.Caps(facts, checks),
        };
        var failed = checks.Where(c => c.Status == "fail").Select(c => c.Message).ToList();
        _refused = failed.Count == 0 ? null : string.Join("; ", failed);
        if (_refused != null)
        {
            throw new SourceException(_refused) { RefusedCaps = caps };
        }
        return caps;
    }

    public async Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct)
    {
        Func<RowWriter, CancellationToken, Task>? produce = task.Stream switch
        {
            Streams.OraAudit => (rows, token) => PollAsync(task, rows, token),
            Streams.CatalogUsers => _catalog.UsersAsync,
            Streams.CatalogGrants => _catalog.GrantsAsync,
            Streams.CatalogTables => _catalog.TablesAsync,
            Streams.CatalogColumns => _catalog.ColumnsAsync,
            Streams.CatalogSettings => _catalog.SettingsAsync,
            _ => null,
        };
        if (produce == null)
        {
            return null;
        }
        if (_refused != null)
        {
            throw new SourceException(_refused);
        }
        return await StreamedRows.OpenAsync(produce, ct);
    }

    /// <summary>One window of the trail, into the upload as it is fetched.</summary>
    private async Task PollAsync(AgentTask task, RowWriter output, CancellationToken ct)
    {
        TaskHeartbeat.Beat();
        await using var session = await _catalog.OpenAsync(ct, task.ReadTime);
        try
        {
            _sourceColumn ??= (await OraCatalog.AllAsync(session, OraSql.SourceColumn, ct)).Count > 0;
            var binds = new[]
            {
                new OraBind("lo", FromMicros(task.StartMicros)),
                new OraBind("hi", FromMicros(task.EndMicros)),
                new OraBind("me", _endpoint.Username),
            };
            var sql = OraSql.Poll(_endpoint.StoreRawQueryText, _sourceColumn.Value);
            await using var rows = await session.QueryAsync(sql, binds, ct, OraSql.ReadLimit(task.ReadLimitSeconds));
            await new OraPoll(task.StartMicros, _endpoint.StoreRawQueryText, _sourceColumn.Value, output).ReadAsync(rows, ct);
        }
        catch (OraDriverException ex)
        {
            throw _catalog.Failure(ex, ct);
        }
    }

    private static DateTime FromMicros(long micros) => DateTime.UnixEpoch.AddTicks(micros * 10);
}
