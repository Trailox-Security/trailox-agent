using System.Globalization;
using System.Text.RegularExpressions;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Engines.Oracle;

/// <summary>
/// The growth of the trail's tablespace, from this agent's probes. Nothing else tells a day's size: a count reads the whole
/// partition. What it keeps lives and dies with the process, so a new process has no measure for its first hour.
/// </summary>
/// <remarks>
/// 🔴 WHAT WAS SEEN AND WHAT IS PROJECTED ARE KEPT APART. A busy hour times 24 is no day: a customer who does a day's work in
/// eight hours would be refused every morning. <see cref="GrownBytes"/> is growth that happened, and only that can refuse
/// a source; <see cref="BytesPerDay"/> is the projection, which can only warn. Space that falls is a purge, not negative
/// growth: the interval counts as none, and the growth around it still counts.
/// </remarks>
public sealed class OraGrowth
{
    private static readonly TimeSpan Shortest = TimeSpan.FromHours(1);
    private static readonly TimeSpan Longest = TimeSpan.FromHours(24);

    private readonly Queue<(DateTime At, long Used)> _readings = new();
    private string _tablespace = "";

    /// <summary>One probe's reading. Space in another tablespace is a moved trail: the readings before it say nothing.</summary>
    public void Add(DateTime at, string tablespace, long usedBytes)
    {
        if (tablespace != _tablespace)
        {
            _readings.Clear();
            _tablespace = tablespace;
        }
        _readings.Enqueue((at, usedBytes));
        while (_readings.Count > 2 && at - _readings.Peek().At > Longest)
        {
            _readings.Dequeue();
        }
    }

    private TimeSpan Span => _readings.Count < 2 ? TimeSpan.Zero : _readings.Last().At - _readings.Peek().At;

    /// <summary>What the trail grew by in the last 24 hours, purges aside; null until two readings are an hour apart.</summary>
    public long? GrownBytes
    {
        get
        {
            if (Span < Shortest)
            {
                return null;
            }
            var grown = 0L;
            var previous = _readings.Peek().Used;
            foreach (var (_, used) in _readings)
            {
                grown += Math.Max(0, used - previous);
                previous = used;
            }
            return grown;
        }
    }

    /// <summary>That growth projected to a day; null with no measure.</summary>
    public long? BytesPerDay => GrownBytes is { } grown ? (long)(grown / Span.TotalDays) : null;
}

/// <summary>Reads what <see cref="OraProbeChecks"/> decides on. One statement a fact; none reads the trail's rows.</summary>
internal static class OraProbe
{
    private const int FirstSupportedMajor = 19;

    private static readonly Regex HighValueTime = new(@"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", RegexOptions.Compiled);

    public static async Task ReadAsync(IOraSession session, OraFacts f, CancellationToken ct)
    {
        await ReadServerAsync(session, f, ct);
        f.PollReadable = await Try(session, f, "UNIFIED_AUDIT_TRAIL", OraSql.PollViewReadable, ct) != null;
        f.Unreadable.Remove("UNIFIED_AUDIT_TRAIL");
        f.SourceColumn = await Try(session, f, "ALL_TAB_COLUMNS", OraSql.SourceColumn, ct) is { Count: > 0 };
        await ReadTrailAsync(session, f, ct);
        await ReadPoliciesAsync(session, f, ct);
        await ReadOwnAsync(session, f, ct);
        await ReadOldSettingsAsync(session, f, ct);
    }

    private static async Task ReadServerAsync(IOraSession session, OraFacts f, CancellationToken ct)
    {
        var product = await OraCatalog.AllAsync(session, OraSql.Product, ct);
        if (product.Count == 0)
        {
            throw new SourceException("the server names no Oracle product in PRODUCT_COMPONENT_VERSION");
        }
        f.Product = Trim(product[0][0] as string, 128);
        f.Version = Trim(product[0][1] as string, 64);
        f.Major = int.TryParse(f.Version.Split('.')[0], NumberStyles.None, CultureInfo.InvariantCulture, out var major) ? major : 0;
        if (f.Major < FirstSupportedMajor)
        {
            throw new SourceException($"Oracle {f.Version} is not supported: the agent reads Oracle 19c and later");
        }
        var own = (await OraCatalog.AllAsync(session, OraSql.Session, ct))[0];
        f.OwnUser = own[0] as string ?? "";
        f.Container = own[1] as string ?? "";
        f.Program = Trim(own[2] as string, 128);
        f.AuditSessionId = own[3]?.ToString();
        f.RdsAdmin = (await OraCatalog.AllAsync(session, OraSql.RdsAdmin, ct)).Count > 0;
        foreach (var row in await Try(session, f, "V_$OPTION", OraSql.Options, ct) ?? new())
        {
            if (row[0] as string == "Unified Auditing")
            {
                f.UnifiedOption = row[1] as string;
            }
        }
        foreach (var row in await Try(session, f, "V_$PARAMETER", OraSql.Parameters, ct) ?? new())
        {
            f.Parameters[row[0] as string ?? ""] = row[1]?.ToString();
        }
    }

    private static async Task ReadTrailAsync(IOraSession session, OraFacts f, CancellationToken ct)
    {
        if (await Try(session, f, "DBA_TABLES", OraSql.TrailStatistics, ct) is { Count: > 0 } statistics)
        {
            f.TrailRows = statistics[0][0] as long?;
            f.TrailBlocks = statistics[0][1] as long?;
            f.Partitioned = statistics[0][2] as string == "YES";
        }
        if (await Try(session, f, "DBA_PART_TABLES", OraSql.TrailPartitioning, ct) is { Count: > 0 } partitioning)
        {
            f.Interval = partitioning[0][0] as string ?? "";
            f.DefaultTablespace = partitioning[0][1] as string;
        }
        if (await Try(session, f, "DBA_TAB_PARTITIONS", OraSql.TrailPartitions, ct) is { } partitions)
        {
            f.Partitions = partitions.Select(p => new OraPartition(p[0] as string ?? "", HighValue(p[1] as string), p[2] as string ?? "")).ToList();
        }
        if (await Try(session, f, "DBA_AUDIT_MGMT_CONFIG_PARAMS", OraSql.TrailConfiguredTablespace, ct) is { Count: > 0 } configured)
        {
            f.ConfiguredTablespace = configured[0][0] as string;
        }
        if (OraProbeChecks.TrailTablespace(f) is { } tablespace)
        {
            var bind = new OraBind("t", tablespace);
            var files = await Try(session, f, "DBA_DATA_FILES", OraSql.TablespaceFiles, ct, bind);
            var free = await Try(session, f, "DBA_FREE_SPACE", OraSql.TablespaceFree, ct, bind);
            if (files is { Count: > 0 } && free is { Count: > 0 })
            {
                f.Space = new OraSpace(Whole(files[0][0]), Whole(files[0][1]), Whole(free[0][0]));
            }
        }
        f.Spilled = await Try(session, f, "V_$UNIFIED_AUDIT_TRAIL", OraSql.SpillAny, ct) is { } spill ? spill.Count > 0 : null;
    }

    private static async Task ReadPoliciesAsync(IOraSession session, OraFacts f, CancellationToken ct)
    {
        if (await Try(session, f, "AUDIT_UNIFIED_ENABLED_POLICIES", OraSql.EnabledPolicies, ct) is { } enabled)
        {
            f.PoliciesRead = true;
            f.Policies = enabled.Select(p => new OraclePolicy
            {
                Name = p[0] as string ?? "",
                EnabledOption = p[1] as string ?? "",
                EntityName = p[2] as string ?? "",
                EntityType = p[3] as string ?? "",
                Success = p[4] as string ?? "",
                Failure = p[5] as string ?? "",
            }).ToList();
        }
        if (await Try(session, f, "AUDIT_UNIFIED_POLICIES", OraSql.PolicyObjects, ct, new OraBind("cap", (long)OraSql.MaxPolicyObjects)) is { } objects)
        {
            f.PolicyObjects = objects.Select(p => new OraclePolicyObject
            {
                Policy = p[0] as string ?? "",
                Schema = p[1] as string ?? "",
                Name = p[2] as string ?? "",
            }).ToList();
        }
    }

    private static async Task ReadOwnAsync(IOraSession session, OraFacts f, CancellationToken ct)
    {
        if (await Try(session, f, "USER_RESOURCE_LIMITS", OraSql.OwnResourceLimits, ct) is { } limits)
        {
            f.Limits = limits.ToDictionary(l => l[0] as string ?? "", l => l[1]?.ToString(), StringComparer.Ordinal);
        }
        if (await Try(session, f, "USER_USERS", OraSql.OwnAccount, ct) is { Count: > 0 } account)
        {
            f.PasswordExpiry = account[0][0] as DateTime?;
        }
        if (await Try(session, f, "DBA_AUDIT_MGMT_CLEANUP_JOBS", OraSql.CleanupJobs, ct) is { } jobs)
        {
            f.PurgeJob = jobs.Any(j => j[2] as string == "UNIFIED AUDIT TRAIL" && !string.Equals(j[1] as string, "DISABLED", StringComparison.OrdinalIgnoreCase));
        }
    }

    private static async Task ReadOldSettingsAsync(IOraSession session, OraFacts f, CancellationToken ct)
    {
        var statements = await Try(session, f, "DBA_STMT_AUDIT_OPTS", OraSql.OldStatementAudit, ct);
        var privileges = await Try(session, f, "DBA_PRIV_AUDIT_OPTS", OraSql.OldPrivilegeAudit, ct);
        f.OldAuditSettings = statements == null || privileges == null ? null : statements.Count + privileges.Count;
    }

    /// <summary>A partition's end, from <c>TIMESTAMP' 2026-10-29 00:00:00'</c>: the key is the UTC column, so it is UTC.</summary>
    internal static DateTime? HighValue(string? text)
    {
        var m = HighValueTime.Match(text ?? "");
        return m.Success && DateTime.TryParseExact(m.Value, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var end) ? end : null;
    }

    private static async Task<List<object?[]>?> Try(IOraSession session, OraFacts f, string name, string sql, CancellationToken ct, params OraBind[] binds)
    {
        try
        {
            return await OraCatalog.AllAsync(session, sql, ct, binds);
        }
        catch (OraDriverException ex) when (!OraErrors.SessionLost(ex.Number))
        {
            f.Unreadable.Add(name);
            return null;
        }
    }

    private static long Whole(object? value) => value switch
    {
        long n => n,
        double d => (long)d,
        _ => 0,
    };

    private static string Trim(string? text, int max) => text == null ? "" : text.Length <= max ? text : text[..max];
}
