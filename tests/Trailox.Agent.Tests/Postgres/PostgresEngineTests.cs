using Microsoft.Extensions.Logging.Abstractions;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.Postgres;
using Trailox.Agent.Protocol;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Tests.Postgres;

/// <summary>agent.yaml for a PostgreSQL source, the probe's checks, the statements the agent runs, and a rejected password.</summary>
public class PostgresEngineTests
{
    private static EndpointConfig Rds(string instances = "orders, orders-replica-1", string region = "eu-west-1") => new()
    {
        Alias = "orders",
        Engine = "postgres",
        Host = "orders.abc123def456.eu-west-1.rds.amazonaws.com",
        Username = "trailox_monitor",
        Options = new Dictionary<string, string> { ["log_source"] = "rds_api", ["instances"] = instances, ["rds_region"] = region },
    };

    private static EndpointConfig File(string dir = "/var/log/trailox-pg") => new()
    {
        Alias = "billing",
        Engine = "postgres",
        Host = "127.0.0.1",
        Username = "trailox_monitor",
        Options = new Dictionary<string, string> { ["log_source"] = "file", ["log_directory"] = dir },
    };

    [Fact]
    public void An_rds_source_is_valid_and_its_replica_hosts_come_from_the_primary()
    {
        var e = Rds();

        Assert.Empty(new PostgresEngine().Validate(e));
        Assert.Equal(5432, e.Port);
        Assert.Equal("cloud", e.Kind);
        var instances = PostgresEngine.Instances(e, out var problems);
        Assert.Empty(problems);
        Assert.Equal(new[] { ("orders", "orders.abc123def456.eu-west-1.rds.amazonaws.com", "primary"),
            ("orders-replica-1", "orders-replica-1.abc123def456.eu-west-1.rds.amazonaws.com", "replica") },
            instances.Select(i => (i.Id, i.Host, i.Role)));
    }

    [Fact]
    public void An_aurora_instance_can_name_its_own_host()
    {
        var e = Rds("writer-1=writer-1.xyz.eu-west-1.rds.amazonaws.com, reader-1=reader-1.xyz.eu-west-1.rds.amazonaws.com");
        e.Host = "app.cluster-xyz.eu-west-1.rds.amazonaws.com";

        Assert.Empty(new PostgresEngine().Validate(e));
        Assert.Equal("reader-1.xyz.eu-west-1.rds.amazonaws.com", PostgresEngine.Instances(e, out _)[1].Host);
    }

    [Theory]
    [InlineData("192.0.2.5", "orders")]                                             // an address
    [InlineData("orders-db.internal.example.com", "orders")]                        // a CNAME
    [InlineData("app.cluster-xyz.eu-west-1.rds.amazonaws.com", "app")]              // an Aurora cluster endpoint
    [InlineData("app.cluster-ro-xyz.eu-west-1.rds.amazonaws.com", "app")]
    [InlineData("other.abc123def456.eu-west-1.rds.amazonaws.com", "orders")]        // another instance's endpoint
    public void A_replica_host_is_never_guessed_from_a_host_that_is_not_the_primarys_rds_endpoint(string host, string primary)
    {
        var e = Rds($"{primary}, {primary}-replica-1");
        e.Host = host;

        Assert.Contains(new PostgresEngine().Validate(e), p => p.Contains($"list {primary}-replica-1 as {primary}-replica-1=<its host>", StringComparison.Ordinal));
        Assert.Null(PostgresEngine.ReplicaHost(host, primary, primary + "-replica-1"));
    }

    [Fact]
    public void A_replica_host_follows_the_primarys_rds_endpoint_in_any_partition()
    {
        Assert.Equal("orders-replica-1.abc123def456.cn-north-1.rds.amazonaws.com.cn",
            PostgresEngine.ReplicaHost("orders.abc123def456.cn-north-1.rds.amazonaws.com.cn", "orders", "orders-replica-1"));
        Assert.Equal("orders-replica-1.abc123def456.eu-west-1.rds.amazonaws.com",
            PostgresEngine.ReplicaHost("ORDERS.abc123def456.eu-west-1.rds.amazonaws.com", "orders", "orders-replica-1"));
    }

    [Fact]
    public void A_file_source_is_valid()
    {
        Assert.Empty(new PostgresEngine().Validate(File()));
    }

    [Theory]
    [InlineData("log_source", "cloudwatch", "options.log_source")]
    [InlineData("rds_region", "Ireland", "options.rds_region")]
    [InlineData("instances", "", "options.instances")]
    [InlineData("instances", "1orders", "options.instances")]
    [InlineData("instances", "orders, orders", "listed twice")]
    [InlineData("log_directory", "/var/log/pg", "only with log_source: file")]
    [InlineData("password", "x", "options.password: not an option")]
    [InlineData("database", "app db", "options.database")]
    public void An_rds_source_says_what_is_wrong(string key, string value, string problem)
    {
        var e = Rds();
        e.Options[key] = value;

        Assert.Contains(new PostgresEngine().Validate(e), p => p.Contains(problem, StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("log_directory", "var/log/pg", "options.log_directory")]
    [InlineData("instances", "x", "only with log_source: rds_api")]
    public void A_file_source_says_what_is_wrong(string key, string value, string problem)
    {
        var e = File();
        e.Options[key] = value;

        Assert.Contains(new PostgresEngine().Validate(e), p => p.Contains(problem, StringComparison.Ordinal));
    }

    [Fact]
    public void The_engine_is_registered_and_loads_from_agent_yaml()
    {
        const string yaml = """
            version: 1
            gateway: https://agent.trailox.io
            endpoints:
              - alias: orders
                engine: postgres
                host: orders.abc123def456.eu-west-1.rds.amazonaws.com
                username: trailox_monitor
                password_env: PG_PW
                options:
                  log_source: rds_api
                  instances: orders
                  rds_region: eu-west-1
            """;

        var (config, _) = ConfigLoader.Parse(yaml, new Secrets());

        Assert.Equal("postgres", config.Endpoints.Single().Engine);
        Assert.NotNull(EngineRegistry.Find("postgres"));
    }

    private sealed class Secrets : ISecretReader
    {
        public string? FromEnvironment(string variable) => variable == "PG_PW" ? "pw" : null;

        public string FromFile(string path) => throw new FileNotFoundException(path);
    }

    [Theory]
    [InlineData(false, 2, true, true, "ok")]
    [InlineData(true, 2, true, true, "warn")]
    [InlineData(false, -1, true, true, "warn")]
    [InlineData(false, 2, false, true, "warn")]
    public void The_role_check_warns_about_more_than_the_agent_needs(bool super, int limit, bool stats, bool settings, string status)
    {
        Assert.Equal(status, PgProbeChecks.Role(super, limit, stats, settings).Status);
    }

    private static Dictionary<string, string> Settings(string connections = "on", string statement = "ddl", string lc = "en_US.UTF-8",
        string zone = "UTC", string collector = "on") => new()
    {
        ["log_connections"] = connections,
        ["log_disconnections"] = "on",
        ["log_statement"] = statement,
        ["lc_messages"] = lc,
        ["log_timezone"] = zone,
        ["logging_collector"] = collector,
    };

    private static string StatusOf(IEnumerable<PostgresCheck> checks, string name) => checks.Single(c => c.Name == name).Status;

    [Fact]
    public void Logs_only_settings_pass_and_postgresql_18_lists_count_as_on()
    {
        var checks = PgProbeChecks.Settings(Settings(connections: "receipt,authentication,authorization"), "rds_api").ToList();

        Assert.All(checks, c => Assert.Equal("ok", c.Status));
    }

    [Theory]
    [InlineData("off", "ddl", "en_US.UTF-8", "UTC", "logging", "warn")]
    [InlineData("on", "none", "C", "UTC", "logging", "warn")]
    [InlineData("on", "ddl", "de_DE.UTF-8", "UTC", "lc_messages", "warn")]
    [InlineData("on", "ddl", "C.UTF-8", "Etc/UTC", "lc_messages", "ok")]
    [InlineData("on", "ddl", "", "Asia/Jerusalem", "log_timezone", "fail")]
    public void Settings_say_what_to_change(string connections, string statement, string lc, string zone, string check, string status)
    {
        Assert.Equal(status, StatusOf(PgProbeChecks.Settings(Settings(connections, statement, lc, zone), "rds_api"), check));
    }

    /// <summary>terse drops every CONTEXT, so a function's RAISE LOG 'AUDIT: ...' passes as pgaudit's line.</summary>
    [Theory]
    [InlineData("pg_stat_statements,pgaudit", "terse", "warn")]
    [InlineData("pgaudit", "TERSE", "warn")]
    [InlineData("pgaudit", "default", "ok")]
    [InlineData("pgaudit", "verbose", "ok")]
    public void Pgaudit_needs_more_than_terse_log_lines(string libraries, string verbosity, string status)
    {
        var settings = new Dictionary<string, string>(Settings())
        {
            ["shared_preload_libraries"] = libraries,
            ["log_error_verbosity"] = verbosity,
        };

        Assert.Equal(status, StatusOf(PgProbeChecks.Settings(settings, "rds_api"), "log_error_verbosity"));
    }

    [Fact]
    public void Without_pgaudit_the_log_verbosity_is_not_checked()
    {
        var settings = new Dictionary<string, string>(Settings()) { ["log_error_verbosity"] = "terse" };

        Assert.DoesNotContain(PgProbeChecks.Settings(settings, "rds_api"), c => c.Name == "log_error_verbosity");
    }

    [Fact]
    public void A_self_managed_server_without_the_logging_collector_fails()
    {
        Assert.Equal("fail", StatusOf(PgProbeChecks.Settings(Settings(collector: "off"), "file"), "logging_collector"));
        Assert.DoesNotContain(PgProbeChecks.Settings(Settings(collector: "off"), "rds_api"), c => c.Name == "logging_collector");
    }

    [Fact]
    public void Pg_stat_statements_says_whether_it_needs_a_restart()
    {
        var notLoaded = PgProbeChecks.StatStatements(null, new Dictionary<string, string> { ["shared_preload_libraries"] = "pgaudit" }, "postgres");
        var loaded = PgProbeChecks.StatStatements(null, new Dictionary<string, string> { ["shared_preload_libraries"] = "rdsutils, pg_stat_statements" }, "postgres");

        Assert.Contains("one restart", notLoaded.Message);
        Assert.Contains("CREATE EXTENSION", loaded.Message);
        Assert.DoesNotContain("restart", loaded.Message);
        Assert.Equal("ok", PgProbeChecks.StatStatements("1.10", new Dictionary<string, string>(), "postgres").Status);
        Assert.Equal("ok", PgProbeChecks.StatStatements("1.8", new Dictionary<string, string>(), "postgres").Status);
        var old = PgProbeChecks.StatStatements("1.7", new Dictionary<string, string>(), "postgres");
        Assert.Equal("warn", old.Status);
        Assert.Contains("ALTER EXTENSION pg_stat_statements UPDATE", old.Message);
    }

    [Fact]
    public void A_replica_the_agent_cannot_sign_in_to_is_a_warning()
    {
        Assert.Equal("ok", PgProbeChecks.ReplicasAnswer(1, Array.Empty<string>()).Status);
        var check = PgProbeChecks.ReplicasAnswer(2, new[] { "PostgreSQL orders-replica-2.abc123def456.eu-west-1.rds.amazonaws.com: name not found" });
        Assert.Equal("warn", check.Status);
        Assert.Contains("1 of 2 replicas", check.Message);
    }

    [Fact]
    public void More_streaming_replicas_than_listed_is_a_warning()
    {
        Assert.Equal("warn", PgProbeChecks.Replicas(2, 1).Status);
        Assert.Equal("ok", PgProbeChecks.Replicas(1, 1).Status);
    }

    [Fact]
    public void File_length_comes_from_the_newest_names()
    {
        const long Minute = 60_000_000;
        Assert.Equal(10, PgProbeChecks.FileMinutes(new[] { 0L, 10 * Minute, 20 * Minute, 30 * Minute }));
        Assert.Equal(60, PgProbeChecks.FileMinutes(new[] { 0L, 60 * Minute, 120 * Minute }));
        Assert.Equal(0, PgProbeChecks.FileMinutes(new[] { 0L }));
    }

    [Theory]
    [InlineData(0, 10, "fail")]
    [InlineData(144, 10, "ok")]
    [InlineData(2, 12, "ok")]      // a reboot's irregular gap: it warned "up to 1 times"
    [InlineData(72, 20, "warn")]
    [InlineData(24, 60, "warn")]
    [InlineData(3, 1440, "fail")]
    public void Files_longer_than_ten_minutes_warn_and_longer_than_an_hour_fail(int files, int minutes, string status)
    {
        Assert.Equal(status, PgProbeChecks.LogFiles("orders", files, minutes).Status);
    }

    /// <summary>
    /// With no rotation by time, a deleted log file cannot be told from a quiet server; with a long one, a short deletion
    /// is not told. A directory says so, and a check that already warns or fails keeps its own words.
    /// </summary>
    [Theory]
    [InlineData(144, 10, 10, "ok")]
    [InlineData(144, 10, 0, "warn")]
    [InlineData(144, 10, 19, "ok")]
    [InlineData(144, 10, 60, "warn")]
    [InlineData(144, 10, 1440, "warn")]
    [InlineData(3, 1440, 0, "fail")]
    [InlineData(0, 0, 0, "fail")]
    public void A_directory_with_no_rotation_by_time_warns(int files, int minutes, int rotationMinutes, string status)
    {
        var check = PgProbeChecks.LogFiles("orders", files, minutes, rotationMinutes);

        Assert.Equal(status, check.Status);
        Assert.Equal(status == "warn", check.Message.Contains("log_rotation_age is " + rotationMinutes + " ", StringComparison.Ordinal)
            || check.Message.Contains("log_rotation_age is 0 or", StringComparison.Ordinal));
    }

    [Fact]
    public void No_statement_calls_a_size_function_or_resets_the_counters()
    {
        var statements = typeof(PgSql).GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
            .Where(f => f.FieldType == typeof(string)).Select(f => (string)f.GetValue(null)!).ToList();

        Assert.NotEmpty(statements);
        foreach (var forbidden in new[] { "pg_relation_size", "pg_total_relation_size", "pg_table_size", "pg_indexes_size", "pg_database_size",
                     "pg_stat_statements_reset", "pg_read_file", "pg_ls_logdir" })
        {
            Assert.DoesNotContain(statements, s => s.Contains(forbidden, StringComparison.Ordinal));
        }
        foreach (var snapshot in new[] { PgSql.Snapshot, PgSql.SnapshotWithoutInfo, PgSql.SnapshotTotalTime })
        {
            Assert.Contains("showtext := false", snapshot);
        }
        foreach (var older in new[] { PgSql.SnapshotWithoutInfo, PgSql.SnapshotTotalTime })
        {
            Assert.DoesNotContain("pg_stat_statements_info", older);
            Assert.DoesNotContain("s.toplevel", older);
        }
        Assert.Contains("s.total_time AS total_exec_time", PgSql.SnapshotTotalTime);
        Assert.Contains("string_agg(DISTINCT b.rolname", PgSql.Users);
        Assert.Contains("ANY($1::oid[])", PgSql.Columns);
        Assert.Equal(1000, PgSql.ColumnPageTables);
        Assert.Contains("lock_timeout=1000", PgSql.SessionOptions);
        Assert.Contains("default_transaction_read_only=on", PgSql.SessionOptions);
    }

    [Theory]
    [InlineData("1.11", "full")]
    [InlineData("1.10", "full")]
    [InlineData("1.9", "full")]
    [InlineData("1.8", "without info")]
    [InlineData("1.7", "total_time")]
    [InlineData("1.4", "total_time")]
    [InlineData(null, "none")]
    public void The_snapshot_follows_the_extensions_version_not_the_servers(string? extversion, string expected)
    {
        var sql = PgCatalog.SnapshotSql(extversion);

        Assert.Equal(expected switch
        {
            "full" => PgSql.Snapshot,
            "without info" => PgSql.SnapshotWithoutInfo,
            "total_time" => PgSql.SnapshotTotalTime,
            _ => null,
        }, sql);
    }

    private sealed class RejectingSource : IEngineSource
    {
        public bool Rejected { get; init; }

        public int Probes { get; private set; }

        public Task<EndpointCaps> ProbeAsync(CancellationToken ct)
        {
            Probes++;
            throw new SourceException("PostgreSQL db: password authentication failed for trailox_monitor") { AuthenticationRejected = Rejected };
        }

        public Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct) => Task.FromResult<RowStream?>(null);
    }

    [Fact]
    public async Task A_rejected_password_stops_the_probes_until_the_agent_restarts()
    {
        var session = new EndpointSession(Rds(), new RejectingSource { Rejected = true }, TimeSpan.FromMinutes(60));

        await session.ProbeAsync(NullLogger.Instance, CancellationToken.None);

        Assert.False(session.NeedsProbe);
        Assert.Contains("restart the agent", session.ProbeError);
    }

    [Fact]
    public async Task Any_other_probe_failure_is_retried()
    {
        var session = new EndpointSession(Rds(), new RejectingSource { Rejected = false }, TimeSpan.FromMinutes(60));

        await session.ProbeAsync(NullLogger.Instance, CancellationToken.None);

        Assert.True(session.NeedsProbe);
    }
}
