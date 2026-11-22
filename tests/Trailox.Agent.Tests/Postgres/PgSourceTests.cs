using System.Globalization;
using System.Text.Json;
using Npgsql;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.Postgres;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests.Postgres;

/// <summary>A <c>pg_log</c> task end to end, from the RDS API of two instances to the rows the upload reads.</summary>
public class PgSourceTests
{
    private static readonly DateTime Day = new(2026, 10, 22, 0, 0, 0, DateTimeKind.Utc);

    private static long Micros(DateTime at) => (at - DateTime.UnixEpoch).Ticks / 10;

    private static string Line(DateTime at, int pid, string message) =>
        at.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + $" UTC:192.0.2.10({50000 + pid}):app@appdb:[{pid}]:LOG:  " + message;

    private static EndpointConfig Rds() => new()
    {
        Alias = "orders",
        Engine = "postgres",
        Host = "orders.abc123def456.eu-west-1.rds.amazonaws.com",
        Port = 5432,
        Username = "trailox_monitor",
        StoreRawQueryText = true,
        Options = new Dictionary<string, string> { ["log_source"] = "rds_api", ["instances"] = "orders, orders-replica-1", ["rds_region"] = "eu-west-1" },
    };

    private static FakeRds TwoInstances()
    {
        var rds = new FakeRds();
        // The fake answers every instance from the same files, so each instance ships its own copy of the window.
        rds.Add("error/postgresql.log.2026-10-22-0610", (Micros(Day.AddHours(6).AddMinutes(20)) / 1000) - 1, new[]
        {
            Line(Day.AddHours(6).AddMinutes(11), 1, "connection received: host=192.0.2.10 port=50001"),
            Line(Day.AddHours(6).AddMinutes(12), 2, "statement: ALTER ROLE etl PASSWORD 'S3cret'"),
        });
        rds.Add("error/postgresql.log.2026-10-22-0620", Micros(Day.AddHours(6).AddMinutes(30)) / 1000, new[]
        {
            Line(Day.AddHours(6).AddMinutes(20).AddSeconds(30), 3, "connection received: host=192.0.2.10 port=50003"),
        });
        return rds;
    }

    private static async Task<List<JsonElement>> RowsAsync(RowStream stream)
    {
        await using (stream)
        {
            using var text = new StreamReader(stream.Body);
            var body = await text.ReadToEndAsync();
            return body.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(l => JsonDocument.Parse(l).RootElement.Clone()).ToList();
        }
    }

    private static AgentTask Task(string stream, DateTime start, DateTime end) =>
        new() { ChunkId = "c1", EndpointId = 1, Stream = stream, StartMicros = Micros(start), EndMicros = Micros(end) };

    [Fact]
    public async Task A_log_task_reads_every_instance_and_ships_a_count_row_for_each()
    {
        var source = new PgSource(Rds(), TwoInstances(), () => Day.AddHours(7));

        var rows = await RowsAsync((await source.OpenAsync(Task(ProtocolInfo.Streams.PgLog, Day.AddHours(6).AddMinutes(10), Day.AddHours(6).AddMinutes(20)), CancellationToken.None))!);

        Assert.Equal(new[] { "orders", "orders", "orders", "orders-replica-1", "orders-replica-1", "orders-replica-1" },
            rows.Select(r => r.GetProperty("instance").GetString()));
        Assert.Equal(2, rows.Count(r => r.GetProperty("level").GetString() == "summary"));
        Assert.DoesNotContain(rows, r => r.GetRawText().Contains("S3cret", StringComparison.Ordinal));
        Assert.Contains(rows, r => r.GetProperty("command").GetString() == "ALTER ROLE");
    }

    [Fact]
    public async Task A_window_that_ended_seconds_ago_is_not_yet_published()
    {
        var source = new PgSource(Rds(), TwoInstances(), () => Day.AddHours(6).AddMinutes(20).AddSeconds(10));

        var ex = await Assert.ThrowsAsync<SourceException>(() =>
            source.OpenAsync(Task(ProtocolInfo.Streams.PgLog, Day.AddHours(6).AddMinutes(10), Day.AddHours(6).AddMinutes(20)), CancellationToken.None));

        Assert.True(ex.NotYetPublished);
    }

    [Fact]
    public async Task A_stream_of_another_engine_is_not_produced()
    {
        var source = new PgSource(Rds(), TwoInstances(), () => Day.AddHours(7));

        Assert.Null(await source.OpenAsync(Task(ProtocolInfo.Streams.MongoOps, Day, Day.AddMinutes(10)), CancellationToken.None));
    }

    [Theory]
    [InlineData("s", "hello", "\"s\":\"hello\"")]
    [InlineData("b", true, "\"b\":true")]
    [InlineData("n", 42L, "\"n\":42")]
    [InlineData("o", 16384u, "\"o\":16384")]
    [InlineData("d", 1.5, "\"d\":1.5")]
    [InlineData("x", null, "\"x\":null")]
    public void Sql_values_are_written_as_json(string name, object? value, string json)
    {
        using var buffer = new MemoryStream();
        using (var w = new Utf8JsonWriter(buffer))
        {
            w.WriteStartObject();
            PgCatalog.WriteValue(w, name, value, null);
            w.WriteEndObject();
        }

        Assert.Equal("{" + json + "}", System.Text.Encoding.UTF8.GetString(buffer.ToArray()));
    }

    [Fact]
    public void A_time_is_written_in_utc_with_microseconds()
    {
        using var buffer = new MemoryStream();
        using (var w = new Utf8JsonWriter(buffer))
        {
            w.WriteStartObject();
            PgCatalog.WriteValue(w, "t", new DateTime(2026, 10, 22, 6, 10, 0, 250, DateTimeKind.Utc), null);
            w.WriteEndObject();
        }

        Assert.Equal("{\"t\":\"2026-10-22T06:10:00.250000Z\"}", System.Text.Encoding.UTF8.GetString(buffer.ToArray()));
    }

    [Theory]
    [InlineData(ProtocolInfo.Streams.CatalogTables, "kind", "schema_name,table_name,kind,row_estimate,approx_bytes,owner,snapshot_date,database")]
    [InlineData(ProtocolInfo.Streams.CatalogColumns, "data_type", "schema_name,table_name,position,column_name,data_type,snapshot_date,database")]
    [InlineData(ProtocolInfo.Streams.CatalogGrants, "object_kind", "object_kind,schema_name,object_name,grantee,privilege_type,snapshot_date,database")]
    public void A_cut_catalog_ends_with_a_full_row_of_its_stream(string stream, string kindColumn, string columns)
    {
        var buffer = new System.Buffers.ArrayBufferWriter<byte>();
        var sink = new Trailox.Agent.Engines.MongoDb.RowSink(buffer);

        PgCatalog.Truncated(sink, stream, "2026-10-22");

        var row = JsonDocument.Parse(System.Text.Encoding.UTF8.GetString(buffer.WrittenSpan)).RootElement;
        Assert.Equal(columns.Split(','), row.EnumerateObject().Select(p => p.Name));
        Assert.Equal("truncated", row.GetProperty(kindColumn).GetString());
        Assert.Equal("2026-10-22", row.GetProperty("snapshot_date").GetString());
    }

    [Theory]
    [InlineData(10, 0L, false, false)]
    [InlineData(501, 0L, false, true)]                                    // more databases than are read
    [InlineData(10, PgSql.MaxColumnsPerSweep, false, true)]               // the day's columns
    [InlineData(10, 0L, true, true)]                                      // one database's tables or grants at their limit
    public void A_per_database_stream_is_cut_by_any_of_its_limits(int databases, long columns, bool listCut, bool cut)
    {
        Assert.Equal(cut, PgCatalog.Cut(databases, columns, listCut));
    }

    [Fact]
    public void A_list_as_long_as_its_limit_is_taken_as_cut()
    {
        Assert.True(PgCatalog.AtLimit(PgSql.MaxTablesPerDatabase, PgSql.MaxTablesPerDatabase));
        Assert.True(PgCatalog.AtLimit(PgSql.MaxGrantsPerDatabase, PgSql.MaxGrantsPerDatabase));
        Assert.False(PgCatalog.AtLimit(PgSql.MaxTablesPerDatabase - 1, PgSql.MaxTablesPerDatabase));
    }

    [Theory]
    [InlineData(9999)]   // 'infinity', as Npgsql reads it (DateTime.MaxValue)
    [InlineData(1)]      // '-infinity'
    [InlineData(2300)]   // past what a time column holds
    public void A_time_out_of_range_is_written_as_null(int year)
    {
        using var buffer = new MemoryStream();
        using (var w = new Utf8JsonWriter(buffer))
        {
            w.WriteStartObject();
            PgCatalog.WriteValue(w, "rolvaliduntil", year == 9999 ? DateTime.MaxValue : year == 1 ? DateTime.MinValue : new DateTime(year, 1, 1), null);
            w.WriteEndObject();
        }

        Assert.Equal("{\"rolvaliduntil\":null}", System.Text.Encoding.UTF8.GetString(buffer.ToArray()));
    }

    /// <summary>
    /// 🔴 Aurora is told by calling aurora_version(): Aurora PostgreSQL 17.11 answers it (17.11.1) but lists no such function
    ///    in pg_proc, so the lookup this replaces reported every Aurora source as RDS. "Function does not exist"
    ///    (42883) is a definite no; any other failure is not known, and the next probe asks again.
    /// </summary>
    [Theory]
    [InlineData("17.11.1", null, "17.11.1")]
    [InlineData(null, "42883", "")]
    [InlineData(null, "57014", null)]    // statement timeout: not known
    [InlineData(null, "42501", null)]    // insufficient privilege: not known
    public void Aurora_answers_its_version_and_only_function_does_not_exist_is_a_no(string? value, string? sqlState, string? expected)
    {
        Assert.Equal(expected, PgSource.AuroraAnswer(value, sqlState));
    }

    [Theory]
    [InlineData(true, "17.11.1", "aurora")]
    [InlineData(true, "", "rds")]
    [InlineData(true, null, "rds")]
    [InlineData(false, "17.11.1", "self_managed")]   // a file source is never asked
    [InlineData(false, null, "self_managed")]
    public void The_platform_follows_the_log_source_and_the_aurora_answer(bool rdsApi, string? auroraVersion, string expected)
    {
        Assert.Equal(expected, PgSource.PlatformOf(rdsApi, auroraVersion));
    }

    /// <summary>
    /// 🔴 A planted Aurora function is never run: the agent's Aurora calls go through search_path, and Aurora lists neither
    ///    function in pg_proc, so a listed one is a database user's. Only a definite "none is listed" lets the agent call
    ///    them; a check that failed (null) calls nothing either.
    /// </summary>
    [Theory]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(null, false)]
    public void An_aurora_function_is_called_only_when_none_is_listed(bool? listed, bool mayCall)
    {
        Assert.Equal(mayCall, PgSource.MayCallAurora(listed));
    }

    /// <summary>Every Aurora function any statement of the agent calls is one the listing check looks for.</summary>
    [Fact]
    public void The_listing_check_covers_every_aurora_function_the_agent_calls()
    {
        var statements = typeof(PgSql).GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name != nameof(PgSql.AuroraFunctionListed))
            .Select(f => (string)f.GetRawConstantValue()!);
        var called = statements
            .SelectMany(s => System.Text.RegularExpressions.Regex.Matches(s, @"\b(aurora_\w+)\s*\(").Select(m => m.Groups[1].Value))
            .Distinct().Order().ToArray();

        Assert.Equal(new[] { "aurora_replica_status", "aurora_version" }, called);
        Assert.All(called, f => Assert.Contains($"'{f}'", PgSql.AuroraFunctionListed));
        Assert.Contains("FROM pg_catalog.pg_proc WHERE proname IN (", PgSql.AuroraFunctionListed);
    }

    /// <summary>
    /// 🔴 Live, when <c>TRAILOX_PG_LIVE</c> holds a connection string to a throwaway server as its superuser: a
    ///    function planted in public answers an unqualified call, but never the agent's Aurora call, which
    ///    leaves the session as it found it.
    /// </summary>
    [Fact]
    public async Task A_planted_aurora_function_never_answers_the_agents_call()
    {
        var live = Environment.GetEnvironmentVariable("TRAILOX_PG_LIVE");
        if (string.IsNullOrWhiteSpace(live))
        {
            return;
        }
        var ct = CancellationToken.None;
        await using var admin = new NpgsqlConnection(live);
        await admin.OpenAsync(ct);
        await LiveScalar(admin, "CREATE OR REPLACE FUNCTION public.aurora_version() RETURNS text LANGUAGE sql AS $$SELECT 'planted'$$");
        try
        {
            await using var agent = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(live) { Options = PgSql.SessionOptions }.ConnectionString);
            await agent.OpenAsync(ct);
            Assert.Equal("planted", await LiveScalar(agent, "SELECT aurora_version()"));   // what an unqualified call allows
            Assert.False(PgSource.MayCallAurora(await LiveScalar(agent, PgSql.AuroraFunctionListed)));

            var refused = await Assert.ThrowsAsync<PostgresException>(() => PgSource.AuroraCallAsync(agent, PgSql.AuroraVersion, ct));
            Assert.Equal(PostgresErrorCodes.UndefinedFunction, refused.SqlState);
            Assert.StartsWith("PostgreSQL ", (string?)await PgSource.AuroraCallAsync(agent, "SELECT version()", ct));
            Assert.Equal("\"$user\", public", await LiveScalar(agent, "SHOW search_path"));
            Assert.Equal("on", await LiveScalar(agent, "SHOW default_transaction_read_only"));
        }
        finally
        {
            await LiveScalar(admin, "DROP FUNCTION IF EXISTS public.aurora_version()");
        }
    }

    private static async Task<object?> LiveScalar(NpgsqlConnection conn, string sql)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        return await cmd.ExecuteScalarAsync();
    }

    private static PgInstance Instance(string id) => new(id, id + ".abc123def456.eu-west-1.rds.amazonaws.com", "replica");

    [Fact]
    public async Task An_instance_that_cannot_be_signed_in_to_does_not_cost_the_others_their_rows()
    {
        var read = new List<string>();

        await PgCatalog.EachInstanceAsync(new[] { Instance("orders"), Instance("gone"), Instance("orders-replica-1") }, instance =>
        {
            if (instance.Id == "gone")
            {
                throw new SourceException("PostgreSQL gone: name not found");
            }
            read.Add(instance.Id);
            return System.Threading.Tasks.Task.CompletedTask;
        });

        Assert.Equal(new[] { "orders", "orders-replica-1" }, read);
    }

    [Fact]
    public async Task A_task_fails_when_no_instance_can_be_signed_in_to()
    {
        var ex = await Assert.ThrowsAsync<SourceException>(() => PgCatalog.EachInstanceAsync(new[] { Instance("a"), Instance("b") },
            instance => throw new SourceException($"PostgreSQL {instance.Id}: unreachable")));

        Assert.Equal("PostgreSQL a: unreachable", ex.Message);
    }

    [Fact]
    public async Task A_rejected_password_is_never_skipped()
    {
        var read = 0;
        var ex = await Assert.ThrowsAsync<SourceException>(() => PgCatalog.EachInstanceAsync(new[] { Instance("a"), Instance("b") }, _ =>
        {
            read++;
            throw new SourceException("password authentication failed") { AuthenticationRejected = true };
        }));

        Assert.True(ex.AuthenticationRejected);
        Assert.Equal(1, read);
        await Assert.ThrowsAsync<SourceException>(() => PgCatalog.FirstAsync<int>(new[] { Instance("a"), Instance("b") },
            _ => throw new SourceException("password authentication failed") { AuthenticationRejected = true }));
    }

    [Fact]
    public async Task The_catalog_falls_back_to_the_primary_when_the_replica_cannot_be_reached()
    {
        var used = await PgCatalog.FirstAsync(new[] { Instance("orders-replica-1"), Instance("orders") }, instance =>
            instance.Id == "orders-replica-1" ? throw new SourceException("PostgreSQL orders-replica-1: name not found") : System.Threading.Tasks.Task.FromResult(instance.Id));

        Assert.Equal("orders", used);
    }

    [Fact]
    public async Task After_a_rejected_password_no_sign_in_is_tried_until_the_agent_restarts()
    {
        var catalog = new PgCatalog(Rds());
        catalog.Reject("PostgreSQL orders: password authentication failed for trailox_monitor");

        // A documentation address: had the agent tried to connect, the open would fail on the network, not on the latch.
        var ex = await Assert.ThrowsAsync<SourceException>(() => catalog.OpenAsync("192.0.2.1", "postgres", CancellationToken.None));

        Assert.True(ex.AuthenticationRejected);
        Assert.Contains("password authentication failed", ex.Message);
    }

    [Fact]
    public async Task A_failure_before_any_row_is_the_tasks_own_failure_with_its_flags()
    {
        var ex = await Assert.ThrowsAsync<SourceException>(() => PgSource.StreamedAsync(
            (_, _) => throw new SourceException("not configured: pg_stat_statements is not created in database postgres") { MissingOptionalGrant = true },
            CancellationToken.None));

        Assert.True(ex.MissingOptionalGrant);
    }

    [Fact]
    public async Task Rows_written_before_the_upload_starts_are_all_uploaded()
    {
        var stream = await PgSource.StreamedAsync(async (rows, token) =>
        {
            for (var i = 0; i < 3; i++)
            {
                rows.Sink.Writer.WriteStartObject();
                PgCatalog.WriteValue(rows.Sink.Writer, "n", i, null);
                rows.Sink.Writer.WriteEndObject();
                rows.Sink.EndRow();
            }
            await rows.FlushAsync(token);
        }, CancellationToken.None);

        await using (stream)
        {
            using var text = new StreamReader(stream.Body);
            Assert.Equal("{\"n\":0}\n{\"n\":1}\n{\"n\":2}\n", await text.ReadToEndAsync());
        }
    }

    [Fact]
    public void The_connection_bounds_every_session()
    {
        var builder = new Npgsql.NpgsqlConnectionStringBuilder(new PgCatalog(Rds()).ConnectionString("h", "postgres"));

        Assert.Equal("trailox-agent", builder.ApplicationName);
        Assert.False(builder.Pooling);
        Assert.Equal(Npgsql.SslMode.Require, builder.SslMode);
        Assert.Equal(Npgsql.GssEncryptionMode.Disable, builder.GssEncryptionMode);
        Assert.Contains("statement_timeout=30000", builder.Options);
    }

    /// <summary>
    /// 🔴 tls: true checks no certificate, so a machine in the path can answer for the server. SCRAM-SHA-256 only keeps
    ///    the password from it (no clear text, no MD5, no sign-in without one); channel binding keeps it from passing the
    ///    sign-in on to the real server.
    /// </summary>
    [Fact]
    public void Every_sign_in_is_scram_and_over_tls_bound_to_the_channel()
    {
        var tls = new Npgsql.NpgsqlConnectionStringBuilder(new PgCatalog(Rds()).ConnectionString("h", "postgres"));
        var plain = Rds();
        plain.Tls = false;
        var local = new Npgsql.NpgsqlConnectionStringBuilder(new PgCatalog(plain).ConnectionString("127.0.0.1", "postgres"));

        Assert.Equal("ScramSHA256", tls.RequireAuth);
        Assert.Equal(Npgsql.ChannelBinding.Require, tls.ChannelBinding);
        Assert.Equal("ScramSHA256", local.RequireAuth);
        Assert.Equal(Npgsql.SslMode.Disable, local.SslMode);
        Assert.Equal(Npgsql.ChannelBinding.Disable, local.ChannelBinding);
    }

    [Theory]
    [InlineData("\"Password\" authentication method is not allowed. Allowed methods: ScramSHA256", "asked for the password in clear text")]
    [InlineData("\"MD5\" authentication method is not allowed. Allowed methods: ScramSHA256", "asked for an MD5 password")]
    [InlineData("\"None\" authentication method is not allowed. Allowed methods: ScramSHA256", "asked for no password")]
    [InlineData("\"GSS\" authentication method is not allowed. Allowed methods: ScramSHA256", "asked for another sign-in method")]
    public void A_refused_sign_in_method_says_what_to_change_and_what_else_it_can_mean(string npgsql, string expected)
    {
        var message = new PgCatalog(Rds()).Explain("orders", npgsql);

        Assert.StartsWith("PostgreSQL orders: the server " + expected + ", and the agent signs in with SCRAM-SHA-256 only.", message);
        Assert.Contains("\\password trailox_monitor)", message);
        Assert.Contains("something between the agent and the server answered in its place", message);
    }

    [Theory]
    [InlineData("Couldn't connect because ChannelBinding is set to Require but the server doesn't support SCRAM-SHA-256-PLUS")]
    [InlineData("Unable to bind to SCRAM-SHA-256-PLUS, check logs for more information")]
    public void No_channel_binding_says_to_point_the_agent_at_the_server_itself(string npgsql)
    {
        var message = new PgCatalog(Rds()).Explain("orders", npgsql);

        Assert.StartsWith("PostgreSQL orders: no channel binding (SCRAM-SHA-256-PLUS)", message);
        Assert.Contains("not at a connection pooler", message);
    }

    [Fact]
    public void A_role_psql_would_fold_is_quoted_in_the_password_hint()
    {
        var endpoint = Rds();
        endpoint.Username = "Agent.Mon";

        Assert.Contains("\\password \"Agent.Mon\")", new PgCatalog(endpoint).Explain("orders", "\"MD5\" authentication method is not allowed."));
        Assert.Equal("PostgreSQL orders: timeout", new PgCatalog(endpoint).Explain("orders", "timeout"));
    }
}
