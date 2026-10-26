using System.Data;
using System.Globalization;
using System.Text.Json;
using Npgsql;
using Trailox.Agent.Config;
using Trailox.Agent.Engines.MongoDb;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// The SQL side of a PostgreSQL source: connections, the <c>pg_stat_statements</c> snapshot, and the daily catalog.
/// One connection at a time; each is closed when its statements are done.
/// </summary>
public sealed class PgCatalog
{
    private readonly EndpointConfig _endpoint;
    private readonly string _database;
    private string? _rejected;

    public PgCatalog(EndpointConfig endpoint)
    {
        _endpoint = endpoint;
        _database = endpoint.Options.GetValueOrDefault("database", PostgresEngine.DefaultDatabase);
    }

    public string Database => _database;

    internal string ConnectionString(string host, string database) => new NpgsqlConnectionStringBuilder
    {
        Host = host,
        Port = _endpoint.Port,
        Database = database,
        Username = _endpoint.Username,
        Password = _endpoint.Password,
        SslMode = _endpoint.Tls ? SslMode.Require : SslMode.Disable,
        // SCRAM-SHA-256 only: a server, or anything answering in its place, that asks for the password in clear text or
        // as MD5, or lets the agent in without one, is refused before the password leaves the agent.
        RequireAuth = "ScramSHA256",
        // tls: true checks no certificate (an RDS one needs the RDS CA bundle). Channel binding ties the SCRAM sign-in
        // to the certificate the agent saw, so a machine in the path that shows its own cannot pass the sign-in on.
        ChannelBinding = _endpoint.Tls ? ChannelBinding.Require : ChannelBinding.Disable,
        // Npgsql 10 probes GSS encryption by default and, on a distroless image without a Kerberos library, fails the
        // open before the password is ever sent.
        GssEncryptionMode = GssEncryptionMode.Disable,
        ApplicationName = "trailox-agent",
        Pooling = false,
        Timeout = 30,
        CommandTimeout = 30,
        Options = PgSql.SessionOptions,
    }.ConnectionString;

    /// <summary>A connection to one server and database. A rejected password is said so, and the runner then stops signing in.</summary>
    /// <remarks>
    /// 🔴 AFTER A REJECTED PASSWORD, NO SIGN-IN AT ALL until the agent restarts: every open fails at once as the first
    ///    did, without connecting. The runner stops the probes; this stops the tasks issued meanwhile, each of which
    ///    would write one more failed sign-in into the server's log.
    /// </remarks>
    public async Task<NpgsqlConnection> OpenAsync(string host, string database, CancellationToken ct)
    {
        if (_rejected != null)
        {
            throw Rejected(_rejected);
        }
        var conn = new NpgsqlConnection(ConnectionString(host, database));
        try
        {
            await conn.OpenAsync(ct);
            return conn;
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InvalidPassword)
        {
            await conn.DisposeAsync();
            throw Reject($"PostgreSQL {host}: password authentication failed for {_endpoint.Username}");
        }
        catch (NpgsqlException ex)
        {
            await conn.DisposeAsync();
            throw new SourceException(Explain(host, ex.Message));
        }
    }

    /// <summary>
    /// A failed open as the probe reports it. The sign-in refusals of <see cref="ConnectionString"/> say what to change,
    /// and that a server set up right means something else answered.
    /// </summary>
    internal string Explain(string host, string message)
    {
        // Npgsql: "\"MD5\" authentication method is not allowed. Allowed methods: ScramSHA256"
        if (message.Contains("authentication method is not allowed", StringComparison.Ordinal))
        {
            var asked = message.StartsWith('"') && message.IndexOf('"', 1) is > 1 and var end ? message[1..end] : "";
            var what = asked switch
            {
                "Password" => "the password in clear text",
                "MD5" => "an MD5 password",
                "None" => "no password",
                _ => "another sign-in method",
            };
            return $"PostgreSQL {host}: the server asked for {what}, and the agent signs in with SCRAM-SHA-256 only. Give "
                + "the role a SCRAM password (SET password_encryption = 'scram-sha-256'; \\password " + PsqlName(_endpoint.Username) + ") "
                + "and use scram-sha-256 in pg_hba.conf. If both are so already, something between the agent and the server "
                + "answered in its place.";
        }
        // Npgsql: "... the server doesn't support SCRAM-SHA-256-PLUS", or "Unable to bind to SCRAM-SHA-256-PLUS ...".
        if (message.Contains("SCRAM-SHA-256-PLUS", StringComparison.Ordinal))
        {
            return $"PostgreSQL {host}: no channel binding (SCRAM-SHA-256-PLUS), which the agent requires with tls: true so "
                + "that nothing between it and the server can pass its sign-in on. Point the agent at the server itself, not "
                + "at a connection pooler. If it is the server itself, something between the agent and the server answered "
                + "in its place.";
        }
        return $"PostgreSQL {host}: {message}";
    }

    /// <summary>A role as psql's \password takes it: quoted unless psql would read it unchanged.</summary>
    private static string PsqlName(string role) =>
        System.Text.RegularExpressions.Regex.IsMatch(role, @"^[a-z_][a-z0-9_]*\z") ? role : "\"" + role + "\"";

    /// <summary>The password was rejected: remembered, so that no other sign-in is tried until the agent restarts.</summary>
    internal SourceException Reject(string message)
    {
        _rejected = message;
        return Rejected(message);
    }

    private static SourceException Rejected(string message) => new(message) { AuthenticationRejected = true };

    /// <summary>
    /// The snapshot statement for an installed <c>pg_stat_statements</c> version (<c>extversion</c>); null when the
    /// extension is not created. 1.9 (PostgreSQL 14) adds <c>toplevel</c> and the info view; 1.8 renamed
    /// <c>total_time</c> to <c>total_exec_time</c>.
    /// </summary>
    public static string? SnapshotSql(string? extensionVersion) => ExtensionVersion(extensionVersion) switch
    {
        null => null,
        var v when v >= new Version(1, 9) => PgSql.Snapshot,
        var v when v >= new Version(1, 8) => PgSql.SnapshotWithoutInfo,
        _ => PgSql.SnapshotTotalTime,
    };

    /// <summary><c>extversion</c> as a version ("1.10" is after "1.9"); null when absent or unreadable.</summary>
    public static Version? ExtensionVersion(string? extversion) =>
        Version.TryParse(extversion, out var v) ? v : int.TryParse(extversion, out var major) ? new Version(major, 0) : null;

    /// <summary>
    /// One <c>pg_stat_snapshot</c> per instance, taken now: counters cannot be read for the past. An instance whose
    /// database has no <c>pg_stat_statements</c> is skipped; with none at all the task fails as "not configured".
    /// </summary>
    public async Task SnapshotAsync(IReadOnlyList<PgInstance> instances, PgRows output, CancellationToken ct)
    {
        var read = 0;
        await EachInstanceAsync(instances, async instance =>
        {
            await using var conn = await OpenAsync(instance.Host, _database, ct);
            if (SnapshotSql(await ScalarStringAsync(conn, PgSql.StatStatementsVersion, ct)) is not { } sql)
            {
                return;
            }
            try
            {
                await WriteAsync(conn, sql, output, ct, ("instance", (object?)instance.Id), ("taken_at", (object?)null));
                read++;
            }
            catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.UndefinedFunction or PostgresErrorCodes.UndefinedTable
                or PostgresErrorCodes.ObjectNotInPrerequisiteState)
            {
                // Created but not loaded (not in shared_preload_libraries): nothing to read on this instance.
            }
        });
        if (read == 0)
        {
            throw new SourceException($"not configured: pg_stat_statements is not created in database {_database} "
                + "(CREATE EXTENSION pg_stat_statements; it must also be in shared_preload_libraries)") { MissingOptionalGrant = true };
        }
    }

    /// <summary><c>catalog_settings</c>, from every instance: a replica can run with other settings.</summary>
    public async Task SettingsAsync(IReadOnlyList<PgInstance> instances, PgRows output, CancellationToken ct)
    {
        var date = Today();
        await EachInstanceAsync(instances, async instance =>
        {
            await using var conn = await OpenAsync(instance.Host, _database, ct);
            await WriteAsync(conn, PgSql.Settings, output, ct, ("instance", (object?)instance.Id), ("snapshot_date", (object?)date));
        });
    }

    /// <summary><c>catalog_statement_text</c>, from every instance: a statement run only on a replica has its text only there. Passwords are removed.</summary>
    public async Task StatementTextAsync(IReadOnlyList<PgInstance> instances, PgRows output, CancellationToken ct)
    {
        if (!_endpoint.StoreRawQueryText)
        {
            return;
        }
        var date = Today();
        await EachInstanceAsync(instances, async instance =>
        {
            await using var conn = await OpenAsync(instance.Host, _database, ct);
            try
            {
                await WriteAsync(conn, PgSql.StatementText, output, ct, ("instance", (object?)instance.Id), ("snapshot_date", (object?)date),
                    transform: (column, text) => column == "query" ? PgPasswordRedactor.Redact(text) : text);
            }
            catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.UndefinedFunction or PostgresErrorCodes.ObjectNotInPrerequisiteState)
            {
                // No pg_stat_statements on this instance: no text to read.
            }
        });
    }

    /// <summary>
    /// Reads every instance in turn. One that cannot be signed in to (deleted, unreachable) is skipped, so it does not cost
    /// the others their rows; the task fails, as the first of them did, only when no instance could be read.
    /// </summary>
    /// <remarks>⚠ A rejected password is never skipped: it stops every sign-in (<see cref="OpenAsync"/>).</remarks>
    internal static async Task EachInstanceAsync(IReadOnlyList<PgInstance> instances, Func<PgInstance, Task> read)
    {
        SourceException? first = null;
        var done = 0;
        foreach (var instance in instances)
        {
            try
            {
                await read(instance);
                done++;
            }
            catch (SourceException ex) when (!ex.AuthenticationRejected)
            {
                first ??= ex;
            }
            TaskHeartbeat.Beat();
        }
        if (done == 0 && first != null)
        {
            throw first;
        }
    }

    /// <summary>
    /// The first of <paramref name="candidates"/> that can be signed in to: the catalog is read on the first replica, and on
    /// the primary while that replica cannot be reached.
    /// </summary>
    internal static async Task<T> FirstAsync<T>(IReadOnlyList<PgInstance> candidates, Func<PgInstance, Task<T>> open)
    {
        SourceException? first = null;
        foreach (var candidate in candidates)
        {
            try
            {
                return await open(candidate);
            }
            catch (SourceException ex) when (!ex.AuthenticationRejected)
            {
                first ??= ex;
            }
        }
        throw first ?? new SourceException("no PostgreSQL server to read the catalog on");
    }

    /// <summary><c>catalog_users</c>: roles belong to the server, so one database is enough.</summary>
    public async Task UsersAsync(IReadOnlyList<PgInstance> candidates, PgRows output, CancellationToken ct)
    {
        var date = Today();
        await using var conn = await FirstAsync(candidates, instance => OpenAsync(instance.Host, _database, ct));
        await WriteAsync(conn, PgSql.Users, output, ct, ("snapshot_date", (object?)date));
    }

    /// <summary>
    /// A catalog stream that lives in each database (tables, columns, grants): every database the role may connect to, one
    /// at a time, at most <see cref="PgSql.MaxDatabases"/>. A database that refuses the connection is skipped.
    /// </summary>
    public async Task PerDatabaseAsync(IReadOnlyList<PgInstance> candidates, string stream, PgRows output, CancellationToken ct)
    {
        // One date for the whole task: a sweep that crosses midnight is still one day's catalog.
        var day = Today();
        var (instance, databases) = await FirstAsync(candidates, async candidate =>
        {
            await using var conn = await OpenAsync(candidate.Host, _database, ct);
            return (candidate, await ListAsync(conn, PgSql.Databases, ct));
        });
        var columns = 0L;
        var listCut = false;
        foreach (var database in databases.Take(PgSql.MaxDatabases))
        {
            NpgsqlConnection conn;
            try
            {
                conn = await OpenAsync(instance.Host, database, ct);
            }
            catch (SourceException ex) when (!ex.AuthenticationRejected)
            {
                continue;
            }
            await using (conn)
            {
                var date = ("snapshot_date", (object?)day);
                var db = ("database", (object?)database);
                switch (stream)
                {
                    case Protocol.ProtocolInfo.Streams.CatalogTables:
                        listCut |= AtLimit(await WriteAsync(conn, PgSql.Tables, output, ct, new object[] { PgSql.MaxTablesPerDatabase }, null, date, db),
                            PgSql.MaxTablesPerDatabase);
                        break;
                    case Protocol.ProtocolInfo.Streams.CatalogGrants:
                        listCut |= AtLimit(await WriteAsync(conn, PgSql.Grants, output, ct, new object[] { PgSql.MaxGrantsPerDatabase }, null, date, db),
                            PgSql.MaxGrantsPerDatabase);
                        break;
                    case Protocol.ProtocolInfo.Streams.CatalogColumns:
                        bool tablesCut;
                        (columns, tablesCut) = await ColumnsAsync(conn, output, columns, date, db, ct);
                        listCut |= tablesCut;
                        break;
                }
            }
            TaskHeartbeat.Beat();
            if (columns >= PgSql.MaxColumnsPerSweep)
            {
                break;
            }
        }
        if (Cut(databases.Count, columns, listCut))
        {
            Truncated(output.Sink, stream, day);
        }
    }

    /// <summary>A list that returned as many rows as its limit: taken as cut, as one more row cannot be told from none.</summary>
    internal static bool AtLimit(long rows, int limit) => rows >= limit;

    /// <summary>Whether a per-database stream ends with its <c>truncated</c> row: the databases, a list or the columns over a limit.</summary>
    internal static bool Cut(int databases, long columns, bool listCut) =>
        listCut || databases > PgSql.MaxDatabases || columns >= PgSql.MaxColumnsPerSweep;

    /// <summary>
    /// The columns of one database, in pages of <see cref="PgSql.ColumnPageTables"/> tables, so no statement holds the server
    /// long; and whether its tables reached <see cref="PgSql.MaxTablesPerDatabase"/>, so some had no columns read.
    /// </summary>
    private static async Task<(long Written, bool TablesCut)> ColumnsAsync(NpgsqlConnection conn, PgRows output, long written,
        (string, object?) date, (string, object?) db, CancellationToken ct)
    {
        var ids = new List<long>();
        await using (var cmd = new NpgsqlCommand(PgSql.RelationIds, conn))
        {
            cmd.Parameters.AddWithValue(PgSql.MaxTablesPerDatabase);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                ids.Add(reader.GetInt64(0));
            }
        }
        for (var i = 0; i < ids.Count && written < PgSql.MaxColumnsPerSweep; i += PgSql.ColumnPageTables)
        {
            var page = ids.Skip(i).Take(PgSql.ColumnPageTables).Select(id => (uint)id).ToArray();
            written += await WriteAsync(conn, PgSql.Columns, output, ct, new object[] { page }, null, date, db);
            TaskHeartbeat.Beat();
        }
        return (written, AtLimit(ids.Count, PgSql.MaxTablesPerDatabase));
    }

    /// <summary>
    /// One row that says the catalog was cut at its limits: a full row of the stream, every column sent, whose kind
    /// (<c>kind</c>, <c>data_type</c> or <c>object_kind</c>) is <c>truncated</c>. It is not a table, column or grant.
    /// </summary>
    internal static void Truncated(RowSink sink, string stream, string snapshotDate)
    {
        var w = sink.Writer;
        w.WriteStartObject();
        switch (stream)
        {
            case Protocol.ProtocolInfo.Streams.CatalogTables:
                w.WriteString("schema_name"u8, "");
                w.WriteString("table_name"u8, "");
                w.WriteString("kind"u8, TruncatedKind);
                w.WriteNumber("row_estimate"u8, 0);
                w.WriteNumber("approx_bytes"u8, 0);
                w.WriteString("owner"u8, "");
                break;
            case Protocol.ProtocolInfo.Streams.CatalogColumns:
                w.WriteString("schema_name"u8, "");
                w.WriteString("table_name"u8, "");
                w.WriteNumber("position"u8, 0);
                w.WriteString("column_name"u8, "");
                w.WriteString("data_type"u8, TruncatedKind);
                break;
            default:
                w.WriteString("object_kind"u8, TruncatedKind);
                w.WriteString("schema_name"u8, "");
                w.WriteString("object_name"u8, "");
                w.WriteString("grantee"u8, "");
                w.WriteString("privilege_type"u8, "");
                break;
        }
        w.WriteString("snapshot_date"u8, snapshotDate);
        w.WriteString("database"u8, "");
        w.WriteEndObject();
        sink.EndRow();
    }

    public const string TruncatedKind = "truncated";

    private static async Task<string?> ScalarStringAsync(NpgsqlConnection conn, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        return await cmd.ExecuteScalarAsync(ct) as string;
    }

    private static async Task<List<string>> ListAsync(NpgsqlConnection conn, string sql, CancellationToken ct)
    {
        var list = new List<string>();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            list.Add(reader.GetString(0));
        }
        return list;
    }

    private static Task<long> WriteAsync(NpgsqlConnection conn, string sql, PgRows output, CancellationToken ct,
        params (string Name, object? Value)[] extra) => WriteAsync(conn, sql, output, ct, Array.Empty<object>(), null, extra);

    private static Task<long> WriteAsync(NpgsqlConnection conn, string sql, PgRows output, CancellationToken ct,
        (string Name, object? Value) a, (string Name, object? Value) b, Func<string, string, string> transform) =>
        WriteAsync(conn, sql, output, ct, Array.Empty<object>(), transform, a, b);

    /// <summary>
    /// Runs one statement and writes each row as JSON, then the <paramref name="extra"/> columns. An extra column with
    /// no value is the time now. Returns the rows written.
    /// </summary>
    private static async Task<long> WriteAsync(NpgsqlConnection conn, string sql, PgRows output, CancellationToken ct, object[] parameters,
        Func<string, string, string>? transform, params (string Name, object? Value)[] extra)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        foreach (var p in parameters)
        {
            // Npgsql sends no unsigned array untyped: the relation ids go as oid[].
            cmd.Parameters.Add(p is uint[] oids
                ? new NpgsqlParameter { Value = oids, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Oid }
                : new NpgsqlParameter { Value = p });
        }
        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
        var now = DateTime.UtcNow;
        long rows = 0;
        var sink = output.Sink;
        while (await reader.ReadAsync(ct))
        {
            var w = sink.Writer;
            w.WriteStartObject();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                WriteValue(w, reader.GetName(i), reader.IsDBNull(i) ? null : reader.GetValue(i), transform);
            }
            foreach (var (name, value) in extra)
            {
                WriteValue(w, name, value ?? now, null);
            }
            w.WriteEndObject();
            sink.EndRow();
            if (++rows % 1000 == 0)
            {
                await output.FlushAsync(ct);
            }
        }
        await output.FlushAsync(ct);
        return rows;
    }

    internal static void WriteValue(Utf8JsonWriter w, string name, object? value, Func<string, string, string>? transform)
    {
        switch (value)
        {
            case null:
                w.WriteNull(name);
                break;
            case string s:
                w.WriteString(name, transform == null ? s : transform(name, s));
                break;
            case bool b:
                w.WriteBoolean(name, b);
                break;
            case short or int or long or uint:
                w.WriteNumber(name, Convert.ToInt64(value, CultureInfo.InvariantCulture));
                break;
            case float or double:
                w.WriteNumber(name, Convert.ToDouble(value, CultureInfo.InvariantCulture));
                break;
            case decimal d:
                w.WriteNumber(name, d);
                break;
            // Infinity ('infinity' in rolvaliduntil reads as DateTime.MaxValue), or out of the range a time column holds.
            case DateTime t when t.Year is < 1900 or > 2299:
                w.WriteNull(name);
                break;
            case DateTime t:
                RowSink.Time(w, System.Text.Encoding.UTF8.GetBytes(name), (DateTime.SpecifyKind(t, DateTimeKind.Utc) - DateTime.UnixEpoch).Ticks / 10);
                break;
            default:
                w.WriteString(name, Convert.ToString(value, CultureInfo.InvariantCulture));
                break;
        }
    }

    private static string Today() => DateTime.UtcNow.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
}
