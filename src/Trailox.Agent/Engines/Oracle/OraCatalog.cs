using System.Globalization;
using System.Text.Json;
using Trailox.Agent.Config;
using Trailox.Agent.Engines.MongoDb;
using Trailox.Agent.Protocol;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Engines.Oracle;

/// <summary>Oracle's error numbers the agent acts on.</summary>
internal static class OraErrors
{
    /// <summary>
    /// Whether the failure took the session with it. Any other failure of a statement belongs to that statement: a missing
    /// grant, a column this release does not have, a limit. An optional read says so and the next one goes on.
    /// </summary>
    public static bool SessionLost(int number) => number is 0 or 28 or 1012 or 1092 or 2396 or 3113 or 3114 or 3135 or (>= 12150 and <= 12999);

    /// <summary>
    /// A bound stopped the call: it took too long, or hit ORA-02393 or ORA-02395, the profile's per-call limits. The same
    /// read would stop the same way.
    /// </summary>
    public static bool StoppedByBound(OraDriverException ex) => ex.TimedOut || ex.Number is 2393 or 2395;

    /// <summary>ORA-28000: the one refusal that can end with no change on the agent's side.</summary>
    public const int AccountLocked = 28000;

    /// <summary>
    /// How long a locked account is left alone before one more try. 🔴 A LOCK IS NOT ALWAYS THE AGENT'S TO FIX: anyone who can
    /// reach the listener can lock a known user name with wrong passwords, and it ends with the profile's lock time or an
    /// administrator. A stop until a restart would leave the trail unread after that; one try an hour costs one record.
    /// </summary>
    public static readonly TimeSpan LockedRetry = TimeSpan.FromHours(1);

    /// <summary>
    /// What to do when the server refused the sign-in itself; null for any other failure.
    /// 🔴 EACH OF THESE IS ONE MORE FAILED SIGN-IN IN THE CUSTOMER'S TRAIL. At a retry a minute the default profile locks
    /// the user after ten wrong passwords, and a user with no CREATE SESSION writes about 1,900 records a day.
    /// </summary>
    public static string? SignInFix(int number) => number switch
    {
        1017 => "fix the password",
        1045 => "grant CREATE SESSION",
        AccountLocked => "unlock the account",
        28001 => "set a new password and give it to the agent",
        _ => null,
    };
}

/// <summary>
/// The SQL side of an Oracle source: connections, the rejected flag, and the daily catalog. One connection for each task,
/// and for each page of columns; each is closed when its statements are done.
/// </summary>
public sealed class OraCatalog
{
    private const int FlushRows = 1000;

    private readonly EndpointConfig _endpoint;
    private readonly IOraDriver _driver;
    private readonly Func<DateTime> _utcNow;
    private readonly TimeSpan _callLimit;
    private SourceException? _rejected;
    private SourceException? _locked;
    private DateTime _lockedUntilUtc;

    /// <param name="callLimit">The longest any one call on a session may run; <see cref="OraSql.CommandTimeoutSeconds"/> unless a test says.</param>
    public OraCatalog(EndpointConfig endpoint, IOraDriver driver, Func<DateTime>? utcNow = null, TimeSpan? callLimit = null)
    {
        _endpoint = endpoint;
        _driver = driver;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _callLimit = callLimit ?? TimeSpan.FromSeconds(OraSql.CommandTimeoutSeconds);
    }

    private OraTarget Target => new(_endpoint.Host, _endpoint.Port, _endpoint.Options["service_name"], _endpoint.Tls, _endpoint.Username, _endpoint.Password);

    /// <summary>A session with the agent's settings on it. A sign-in the server refuses is said so, and the runner then stops signing in.</summary>
    /// <remarks>
    /// 🔴 AFTER A REFUSED SIGN-IN, NO SIGN-IN AT ALL until the agent restarts: every open fails at once as the first did,
    ///    without connecting. The runner stops the probes; this stops the tasks issued meanwhile, each of which would write
    ///    one more failed sign-in into the customer's trail. Network errors keep the runner's retry.
    /// 🔴 A LOCKED ACCOUNT IS HELD THE SAME WAY, FOR <see cref="OraErrors.LockedRetry"/>. Then one open tries again: refused, it
    ///    holds for another hour; let in, the source goes on with no restart.
    /// </remarks>
    /// <param name="ct">Stops the sign-in.</param>
    /// <param name="time">Counts what the session's calls wait on the server, for a read that reports it.</param>
    public async Task<IOraSession> OpenAsync(CancellationToken ct, ReadTime? time = null)
    {
        if (_rejected != null)
        {
            throw _rejected;
        }
        if (_locked != null && _utcNow() < _lockedUntilUtc)
        {
            throw _locked;
        }
        IOraSession? session = null;
        try
        {
            session = new OraBoundedSession(await _driver.OpenAsync(Target, ct), _callLimit, time);
            _locked = null;
            foreach (var setting in OraSql.SessionSettings)
            {
                await session.ExecuteAsync(setting, ct);
            }
            return session;
        }
        catch (OraDriverException ex)
        {
            if (session != null)
            {
                await session.DisposeAsync();
            }
            throw OraErrors.SignInFix(ex.Number) is { } fix ? Refused(ex, fix) : Failure(ex, ct);
        }
        catch
        {
            if (session != null)
            {
                await session.DisposeAsync();
            }
            throw;
        }
    }

    /// <summary>A sign-in the server refused, remembered: for good, or for an hour when the account is locked.</summary>
    private SourceException Refused(OraDriverException ex, string fix)
    {
        var locked = ex.Number == OraErrors.AccountLocked;
        var refused = new SourceException(Text(ex))
        {
            AuthenticationRejected = true,
            AuthenticationFix = fix,
            SignInRetryAfter = locked ? OraErrors.LockedRetry : null,
        };
        if (locked)
        {
            _locked = refused;
            _lockedUntilUtc = _utcNow() + OraErrors.LockedRetry;
        }
        else
        {
            _rejected = refused;
        }
        return refused;
    }

    /// <summary>A failed statement as the task reports it. One stopped by a bound says so, and a cancellation stays one.</summary>
    internal SourceException Failure(OraDriverException ex, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return new SourceException(Text(ex)) { TooLargeAtSource = OraErrors.StoppedByBound(ex) };
    }

    private string Text(OraDriverException ex) => $"Oracle {_endpoint.Host}: {ex.Message}";

    /// <summary>The rows of a statement that returns few, as plain values.</summary>
    internal static async Task<List<object?[]>> AllAsync(IOraSession session, string sql, CancellationToken ct, params OraBind[] binds)
    {
        var found = new List<object?[]>();
        await using var rows = await session.QueryAsync(sql, binds, ct);
        while (await rows.ReadAsync(ct))
        {
            var row = new object?[rows.Columns.Count];
            for (var i = 0; i < row.Length; i++)
            {
                row[i] = rows.Value(i);
            }
            found.Add(row);
        }
        return found;
    }

    /// <summary><c>catalog_users</c>.</summary>
    public async Task UsersAsync(RowWriter output, CancellationToken ct)
    {
        var date = Today();
        await using var session = await OpenAsync(ct);
        await WriteAsync(session, OraSql.Users, output, date, ct);
    }

    /// <summary><c>catalog_grants</c>: role, system and object grants, each a statement of its own, with a <c>kind</c> column.</summary>
    public async Task GrantsAsync(RowWriter output, CancellationToken ct)
    {
        var date = Today();
        var cut = false;
        await using (var session = await OpenAsync(ct))
        {
            foreach (var sql in OraSql.Grants)
            {
                cut |= await WriteAsync(session, sql, output, date, ct, Cap(OraSql.MaxGrantsPerKind)) >= OraSql.MaxGrantsPerKind;
                TaskHeartbeat.Beat();
            }
        }
        if (cut)
        {
            Truncated(output.Sink, Protocol.ProtocolInfo.Streams.CatalogGrants, date);
        }
    }

    /// <summary><c>catalog_tables</c>: sizes from the optimizer's statistics.</summary>
    public async Task TablesAsync(RowWriter output, CancellationToken ct)
    {
        var date = Today();
        await using var session = await OpenAsync(ct);
        if (await WriteAsync(session, OraSql.Tables, output, date, ct, Cap(OraSql.MaxTables)) >= OraSql.MaxTables)
        {
            Truncated(output.Sink, Protocol.ProtocolInfo.Streams.CatalogTables, date);
        }
    }

    /// <summary>
    /// <c>catalog_columns</c>, in pages of <see cref="OraSql.ColumnPageTables"/> tables, a connection for each, so no statement
    /// or session holds the server long.
    /// </summary>
    public async Task ColumnsAsync(RowWriter output, CancellationToken ct)
    {
        var date = Today();
        List<object?[]> tables;
        await using (var session = await OpenAsync(ct))
        {
            tables = await ReadTableNamesAsync(session, ct);
        }
        long written = 0;
        foreach (var (first, last) in Pages(tables.Count))
        {
            if (written >= OraSql.MaxColumnsPerSweep)
            {
                break;
            }
            await using var session = await OpenAsync(ct);
            written += await WriteAsync(session, OraSql.Columns, output, date, ct,
                new OraBind("o1", tables[first][0]!), new OraBind("t1", tables[first][1]!),
                new OraBind("o2", tables[last][0]!), new OraBind("t2", tables[last][1]!));
            TaskHeartbeat.Beat();
        }
        if (written >= OraSql.MaxColumnsPerSweep || tables.Count >= OraSql.MaxTables)
        {
            Truncated(output.Sink, Protocol.ProtocolInfo.Streams.CatalogColumns, date);
        }
    }

    private async Task<List<object?[]>> ReadTableNamesAsync(IOraSession session, CancellationToken ct)
    {
        try
        {
            return await AllAsync(session, OraSql.TableNames, ct, Cap(OraSql.MaxTables));
        }
        catch (OraDriverException ex)
        {
            throw Failure(ex, ct);
        }
    }

    /// <summary>The first and last index of each page of tables.</summary>
    internal static IEnumerable<(int First, int Last)> Pages(int tables)
    {
        for (var i = 0; i < tables; i += OraSql.ColumnPageTables)
        {
            yield return (i, Math.Min(i + OraSql.ColumnPageTables, tables) - 1);
        }
    }

    /// <summary>
    /// <c>catalog_settings</c>: each part of the server's audit setup, read on its own, each row named after its part. A part
    /// that cannot be read is a row named <c>unreadable.</c> and the object, and the others are still read.
    /// </summary>
    public async Task SettingsAsync(RowWriter output, CancellationToken ct)
    {
        var date = Today();
        await using var session = await OpenAsync(ct);
        foreach (var (section, name, sql) in OraSql.Settings)
        {
            try
            {
                await using var rows = await session.QueryAsync(sql, Array.Empty<OraBind>(), ct);
                while (await rows.ReadAsync(ct))
                {
                    var (setting, detail) = (rows.Value(1)?.ToString() ?? "", rows.Value(2)?.ToString());
                    WriteSetting(output.Sink, $"{section}.{rows.Value(0)}", detail == null ? setting : $"{setting} {detail}", date);
                }
            }
            catch (OraDriverException ex) when (!OraErrors.SessionLost(ex.Number))
            {
                WriteSetting(output.Sink, $"unreadable.{name}", "ORA-" + ex.Number.ToString("D5", CultureInfo.InvariantCulture), date);
            }
            catch (OraDriverException ex)
            {
                throw Failure(ex, ct);
            }
            await output.FlushAsync(ct);
            TaskHeartbeat.Beat();
        }
    }

    private static void WriteSetting(RowSink sink, string name, string setting, string date)
    {
        var w = sink.Writer;
        w.WriteStartObject();
        w.WriteString("name"u8, name);
        w.WriteString("setting"u8, setting);
        w.WriteString("snapshot_date"u8, date);
        w.WriteEndObject();
        sink.EndRow();
    }

    /// <summary>
    /// One row that says the catalog was cut at its limits: a full row of the stream, every column sent, whose kind
    /// (<c>kind</c>, <c>partitioned</c> or <c>data_type</c>) is <c>truncated</c>. It is not a table, column or grant.
    /// </summary>
    internal static void Truncated(RowSink sink, string stream, string snapshotDate)
    {
        var w = sink.Writer;
        w.WriteStartObject();
        switch (stream)
        {
            case Protocol.ProtocolInfo.Streams.CatalogTables:
                w.WriteString("owner"u8, "");
                w.WriteString("table_name"u8, "");
                w.WriteNull("num_rows"u8);
                w.WriteNull("blocks"u8);
                w.WriteString("partitioned"u8, TruncatedKind);
                w.WriteNull("last_analyzed"u8);
                break;
            case Protocol.ProtocolInfo.Streams.CatalogColumns:
                w.WriteString("owner"u8, "");
                w.WriteString("table_name"u8, "");
                w.WriteString("column_name"u8, "");
                w.WriteString("data_type"u8, TruncatedKind);
                w.WriteNumber("column_id"u8, 0);
                break;
            default:
                w.WriteString("kind"u8, TruncatedKind);
                w.WriteString("grantee"u8, "");
                w.WriteString("privilege"u8, "");
                w.WriteString("owner"u8, "");
                w.WriteString("table_name"u8, "");
                w.WriteString("grantable"u8, "");
                break;
        }
        w.WriteString("snapshot_date"u8, snapshotDate);
        w.WriteEndObject();
        sink.EndRow();
    }

    public const string TruncatedKind = "truncated";

    private static OraBind Cap(int cap) => new("cap", (long)cap);

    /// <summary>Runs one statement and writes each row as JSON, with its <c>snapshot_date</c>. Returns the rows written.</summary>
    private async Task<long> WriteAsync(IOraSession session, string sql, RowWriter output, string date, CancellationToken ct, params OraBind[] binds)
    {
        try
        {
            await using var rows = await session.QueryAsync(sql, binds, ct);
            var sink = output.Sink;
            long written = 0;
            while (await rows.ReadAsync(ct))
            {
                var w = sink.Writer;
                w.WriteStartObject();
                for (var i = 0; i < rows.Columns.Count; i++)
                {
                    WriteValue(w, rows.Columns[i], rows.Value(i));
                }
                w.WriteString("snapshot_date"u8, date);
                w.WriteEndObject();
                sink.EndRow();
                if (++written % FlushRows == 0)
                {
                    await output.FlushAsync(ct);
                    TaskHeartbeat.Beat();
                }
            }
            await output.FlushAsync(ct);
            return written;
        }
        catch (OraDriverException ex)
        {
            throw Failure(ex, ct);
        }
    }

    internal static void WriteValue(Utf8JsonWriter w, string name, object? value)
    {
        switch (value)
        {
            case null:
                w.WriteNull(name);
                break;
            case string s:
                w.WriteString(name, s);
                break;
            case long n:
                w.WriteNumber(name, n);
                break;
            case double d when double.IsFinite(d):
                w.WriteNumber(name, d);
                break;
            case double:
                w.WriteNull(name);
                break;
            // Oracle's dates reach far past any a column holds: outside these years they are not dates anyone set.
            case DateTime t when t.Year is < 1900 or > 2299:
                w.WriteNull(name);
                break;
            case DateTime t:
                RowSink.Time(w, System.Text.Encoding.UTF8.GetBytes(name), (t - DateTime.UnixEpoch).Ticks / 10);
                break;
            default:
                w.WriteString(name, Convert.ToString(value, CultureInfo.InvariantCulture));
                break;
        }
    }

    private string Today() => _utcNow().ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
}
