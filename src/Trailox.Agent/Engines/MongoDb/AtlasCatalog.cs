using System.Buffers;
using System.Globalization;
using System.Text.Json;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Engines.MongoDb;

/// <summary>
/// What the Admin API says about the project, as rows: its events for a window (atlas_events), and four
/// daily snapshots (catalog_users, catalog_roles, catalog_access_list, and from 1.9.0 catalog_databases).
/// Project Read Only reads all five; none of them holds secret material (databaseUsers has no password field,
/// measured).
/// </summary>
public sealed class AtlasCatalog
{
    /// <summary>
    /// A day of hourly points, of which the newest non-null one is kept. Measured on an M10: a point
    /// about every hour (xx:24, then 15:35), and a window is exactly [now - period, now], so PT2H held one point,
    /// 54 minutes old, and would hold none after one or two missed collections. A day costs one request all the same.
    /// </summary>
    internal const string MeasurementPeriod = "P1D";

    // The metrics of one catalog_databases row, by column; a metric Atlas leaves out, or gives no value, is 0.
    private static readonly (string Column, string Metric)[] DatabaseMetrics =
    [
        ("objects", "DATABASE_OBJECT_COUNT"), ("data_size", "DATABASE_DATA_SIZE"), ("storage_size", "DATABASE_STORAGE_SIZE"),
        ("index_size", "DATABASE_INDEX_SIZE"), ("collections", "DATABASE_COLLECTION_COUNT"), ("views", "DATABASE_VIEW_COUNT"),
        ("indexes", "DATABASE_INDEX_COUNT"),
    ];

    private readonly AtlasApi _api;
    private readonly string _ownClientId;

    public AtlasCatalog(AtlasApi api, string ownClientId)
    {
        _api = api;
        _ownClientId = ownClientId;
    }

    /// <summary>
    /// The project's events with start &lt;= created &lt; end, except the agent's own: every log download is a
    /// MONGODB_LOGS_DOWNLOADED event with our client id as its username (about 860 a day for 3 nodes, measured).
    /// </summary>
    /// <remarks>
    /// 🔴 ONLY THOSE: the type MONGODB_LOGS_DOWNLOADED with our client id as the username. Any other event done with
    ///    the agent's service account is shipped, because a stolen or over-privileged secret is exactly what
    ///    Trailox must show. An event ABOUT the account done by a person (its access list changed, say) carries
    ///    our id as clientId and the person as username, and is kept too.
    /// </remarks>
    public async Task<ReadOnlyMemory<byte>> EventsAsync(long startMicros, long endMicros, CancellationToken ct)
    {
        var from = Iso(startMicros);
        var to = Iso(endMicros);
        var events = await _api.ListAsync($"/events?minDate={Uri.EscapeDataString(from)}&maxDate={Uri.EscapeDataString(to)}", "the project's events", ct);
        var rows = new ArrayBufferWriter<byte>(16 * 1024);
        var sink = new RowSink(rows);
        foreach (var e in events)
        {
            if (!DateTime.TryParse(Str(e, "created"), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var created))
            {
                continue;
            }
            var micros = (created - DateTime.UnixEpoch).Ticks / 10;
            if (micros < startMicros || micros >= endMicros || IsOwnDownload(e))
            {
                continue;
            }
            var w = sink.Writer;
            w.WriteStartObject();
            w.WriteString("event_id"u8, Str(e, "id"));
            RowSink.Time(w, "created"u8, micros);
            w.WriteString("event_type"u8, Str(e, "eventTypeName"));
            w.WriteString("username"u8, Str(e, "username"));
            w.WriteString("remote_address"u8, Str(e, "remoteAddress"));
            w.WriteString("target_username"u8, Str(e, "targetUsername"));
            w.WriteString("database"u8, Str(e, "database"));
            w.WriteString("cluster_name"u8, Str(e, "clusterName"));
            w.WriteString("raw"u8, WithoutLinks(e));
            w.WriteEndObject();
            sink.EndRow();
        }
        return rows.WrittenMemory;
    }

    public async Task<ReadOnlyMemory<byte>> UsersAsync(string snapshotDate, CancellationToken ct)
    {
        var users = await _api.ListAsync("/databaseUsers", "the project's database users", ct);
        var rows = new ArrayBufferWriter<byte>(16 * 1024);
        var sink = new RowSink(rows);
        foreach (var u in users)
        {
            var w = sink.Writer;
            w.WriteStartObject();
            // 🔴 snapshot_date is sent on every catalog row, as docs/PROTOCOL.md says.
            w.WriteString("snapshot_date"u8, snapshotDate);
            w.WriteString("username"u8, Str(u, "username"));
            w.WriteString("database_name"u8, Str(u, "databaseName"));
            w.WriteString("roles"u8, Json(u, "roles"));
            w.WriteString("scopes"u8, Json(u, "scopes"));
            w.WriteString("aws_iam_type"u8, Str(u, "awsIAMType"));
            w.WriteString("ldap_auth_type"u8, Str(u, "ldapAuthType"));
            w.WriteString("oidc_auth_type"u8, Str(u, "oidcAuthType"));
            w.WriteString("x509_type"u8, Str(u, "x509Type"));
            w.WriteString("delete_after_date"u8, Str(u, "deleteAfterDate"));
            w.WriteEndObject();
            sink.EndRow();
        }
        return rows.WrittenMemory;
    }

    public async Task<ReadOnlyMemory<byte>> RolesAsync(string snapshotDate, CancellationToken ct)
    {
        var roles = await _api.ListAsync("/customDBRoles/roles", "the project's custom roles", ct);
        var rows = new ArrayBufferWriter<byte>(4 * 1024);
        var sink = new RowSink(rows);
        foreach (var r in roles)
        {
            var w = sink.Writer;
            w.WriteStartObject();
            w.WriteString("snapshot_date"u8, snapshotDate);
            w.WriteString("role_name"u8, Str(r, "roleName"));
            w.WriteString("actions"u8, Json(r, "actions"));
            w.WriteString("inherited_roles"u8, Json(r, "inheritedRoles"));
            w.WriteEndObject();
            sink.EndRow();
        }
        return rows.WrittenMemory;
    }

    public async Task<ReadOnlyMemory<byte>> AccessListAsync(string snapshotDate, CancellationToken ct)
    {
        var entries = await _api.ListAsync("/accessList", "the project's IP access list", ct);
        var rows = new ArrayBufferWriter<byte>(4 * 1024);
        var sink = new RowSink(rows);
        foreach (var a in entries)
        {
            var w = sink.Writer;
            w.WriteStartObject();
            w.WriteString("snapshot_date"u8, snapshotDate);
            w.WriteString("cidr_block"u8, Str(a, "cidrBlock"));
            w.WriteString("ip_address"u8, Str(a, "ipAddress"));
            w.WriteString("aws_security_group"u8, Str(a, "awsSecurityGroup"));
            w.WriteString("comment"u8, Str(a, "comment"));
            w.WriteString("delete_after_date"u8, Str(a, "deleteAfterDate"));
            w.WriteEndObject();
            sink.EndRow();
        }
        return rows.WrittenMemory;
    }

    /// <summary>
    /// catalog_databases (1.9.0): one row per (primary, database), from each primary's database list and each
    /// database's measurements, without MongoDB's own databases. Counts and sizes from metadata: no new
    /// permission, and nothing connects to the cluster.
    /// </summary>
    /// <remarks>
    /// 🔴 THE PROCESS ID HOLDS A COLON (host:27017). It is escaped as a path segment; Atlas answered both forms the same.
    ///   An unknown one is 404 HOST_NOT_FOUND.
    /// ⚠ The databases list leaves admin out but has local and config, and a database it lists may have no point at
    ///   all yet (all its metrics 0): it is reported as it is.
    /// </remarks>
    public async Task<ReadOnlyMemory<byte>> DatabasesAsync(string snapshotDate, IReadOnlyList<string> primaries, CancellationToken ct)
    {
        var rows = new ArrayBufferWriter<byte>(4 * 1024);
        var sink = new RowSink(rows);
        foreach (var process in primaries)
        {
            var path = $"/processes/{Uri.EscapeDataString(process)}/databases";
            var names = (await _api.ListAsync(path, $"the databases of {process}", ct))
                .Select(d => Str(d, "databaseName"))
                .Where(n => n.Length > 0 && !MongoCatalog.SkippedDatabases.Contains(n))
                .Distinct(StringComparer.Ordinal)
                .Order(StringComparer.Ordinal);
            foreach (var database in names)
            {
                using var doc = await _api.GetJsonAsync($"{path}/{Uri.EscapeDataString(database)}/measurements?granularity=PT1H&period={MeasurementPeriod}",
                    $"the measurements of database {database} on {process}", ct);
                var newest = NewestValues(doc.RootElement);
                var w = sink.Writer;
                w.WriteStartObject();
                w.WriteString("snapshot_date"u8, snapshotDate);
                w.WriteString("process"u8, process);
                w.WriteString("database"u8, database);
                foreach (var (column, metric) in DatabaseMetrics)
                {
                    w.WriteNumber(column, newest.GetValueOrDefault(metric));
                }
                w.WriteEndObject();
                sink.EndRow();
                TaskHeartbeat.Beat();
            }
        }
        return rows.WrittenMemory;
    }

    /// <summary>Each measurement's newest point with a value, by name. A point without one (null) is skipped, never read as 0.</summary>
    internal static Dictionary<string, long> NewestValues(JsonElement root)
    {
        var values = new Dictionary<string, long>(StringComparer.Ordinal);
        if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("measurements", out var measurements) || measurements.ValueKind != JsonValueKind.Array)
        {
            return values;
        }
        foreach (var measurement in measurements.EnumerateArray())
        {
            if (!measurement.TryGetProperty("dataPoints", out var points) || points.ValueKind != JsonValueKind.Array)
            {
                continue;
            }
            DateTime? newestAt = null;
            foreach (var point in points.EnumerateArray())
            {
                if (point.ValueKind != JsonValueKind.Object || !point.TryGetProperty("value", out var value) || value.ValueKind != JsonValueKind.Number)
                {
                    continue;
                }
                // Atlas lists points oldest first; the time decides all the same, and one that does not parse counts as
                // later than the ones before it.
                var at = DateTime.TryParse(Str(point, "timestamp"), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var t)
                    ? t
                    : newestAt ?? DateTime.MinValue;
                if (newestAt == null || at >= newestAt)
                {
                    newestAt = at;
                    values[Str(measurement, "name")] = value.TryGetInt64(out var n) ? n : (long)Math.Round(value.GetDouble());
                }
            }
        }
        return values;
    }

    private bool IsOwnDownload(JsonElement e) =>
        string.Equals(Str(e, "eventTypeName"), "MONGODB_LOGS_DOWNLOADED", StringComparison.Ordinal)
        && string.Equals(Str(e, "username"), _ownClientId, StringComparison.Ordinal);

    private static string Iso(long micros) =>
        DateTime.UnixEpoch.AddTicks(micros * 10).ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

    private static string Str(JsonElement e, string name) =>
        e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() ?? "" : "";

    /// <summary>A member as JSON text ("[]" when absent): roles, scopes and actions are sent as JSON arrays.</summary>
    private static string Json(JsonElement e, string name) =>
        e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) ? v.GetRawText() : "[]";

    /// <summary>The event as JSON without its "links" (API URLs, noise in every row).</summary>
    private static string WithoutLinks(JsonElement e)
    {
        using var buffer = new MemoryStream();
        using (var w = new Utf8JsonWriter(buffer))
        {
            w.WriteStartObject();
            foreach (var p in e.EnumerateObject())
            {
                if (p.NameEquals("links"))
                {
                    continue;
                }
                p.WriteTo(w);
            }
            w.WriteEndObject();
        }
        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }
}
