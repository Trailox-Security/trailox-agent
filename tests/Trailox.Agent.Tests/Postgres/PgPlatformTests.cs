using Trailox.Agent.Engines.Postgres;

namespace Trailox.Agent.Tests.Postgres;

/// <summary>
/// The probe on each platform: Azure Database for PostgreSQL told apart from a self-managed server, the checks that differ
/// there, <c>log_line_prefix</c>, and the platform's own roles and databases left out of the counters and the catalog.
/// </summary>
public class PgPlatformTests
{
    private static Dictionary<string, string> Settings(string track) => new()
    {
        ["shared_preload_libraries"] = "pg_stat_statements",
        ["pg_stat_statements.track"] = track,
    };

    /// <summary>
    /// 🔴 A created extension that tracks nothing is no read volume: Azure ships <c>track = none</c>, and the counters
    ///    then read as an idle server.
    /// </summary>
    [Theory]
    [InlineData("1.11", "none", "warn")]
    [InlineData("1.11", "NONE", "warn")]
    [InlineData("1.11", "top", "ok")]
    [InlineData("1.11", "all", "ok")]
    [InlineData("1.11", "", "ok")]     // a setting the role cannot read is not taken for none
    [InlineData("1.7", "none", "warn")]
    public void Pg_stat_statements_that_tracks_nothing_warns_on_every_platform(string extversion, string track, string status)
    {
        var check = PgProbeChecks.StatStatements(extversion, Settings(track), "postgres");

        Assert.Equal(status, check.Status);
        Assert.Equal(track.Equals("none", StringComparison.OrdinalIgnoreCase), check.Message.Contains("pg_stat_statements.track = top", StringComparison.Ordinal));
        Assert.Equal(status == "ok", check.Message == "read volume per user is collected");
    }

    [Fact]
    public void A_loaded_but_not_created_extension_that_tracks_nothing_says_both()
    {
        var check = PgProbeChecks.StatStatements(null, Settings("none"), "postgres");

        Assert.Equal("warn", check.Status);
        Assert.Equal("pg_stat_statements is loaded but not created in database postgres: CREATE EXTENSION pg_stat_statements; "
            + "and set pg_stat_statements.track = top, which is none", check.Message);
    }

    [Theory]
    [InlineData(false, null, true, "azure")]
    [InlineData(false, null, false, "self_managed")]
    [InlineData(true, "", true, "rds")]   // an RDS API source reads RDS's log, whatever the server says
    public void Azures_own_setting_makes_a_server_azure(bool rdsApi, string? auroraVersion, bool azure, string expected)
    {
        Assert.Equal(expected, PgSource.PlatformOf(rdsApi, auroraVersion, azure));
    }

    [Fact]
    public void The_azure_marker_is_a_setting_the_server_defines_never_a_placeholder()
    {
        Assert.Contains("FROM pg_catalog.pg_settings WHERE name = 'azure.extensions' AND context <> 'user'", PgSql.AzureMarker);
    }

    /// <summary>Azure runs with no logging collector and lets none be turned on: no check asks for one there.</summary>
    [Fact]
    public void Azure_has_no_logging_collector_check()
    {
        var settings = new Dictionary<string, string>
        {
            ["log_connections"] = "on", ["log_disconnections"] = "on", ["log_statement"] = "ddl", ["lc_messages"] = "en_US.utf8",
            ["log_timezone"] = "UTC", ["logging_collector"] = "off",
        };

        Assert.All(PgProbeChecks.Settings(settings, "file", PgPlatform.Azure), c => Assert.Equal("ok", c.Status));
        Assert.Null(PgProbeChecks.LoggingCollector(settings, "file", PgPlatform.Azure, filesFound: false));
        Assert.Equal("fail", PgProbeChecks.LoggingCollector(settings, "file", PgPlatform.SelfManaged, filesFound: false)!.Status);
    }

    /// <summary>
    /// With the collector off, a service manager or a package's start script can save the server's stderr to files: the
    /// agent reads them where they are, and asks for the collector only when it finds none.
    /// </summary>
    [Theory]
    [InlineData("off", "file", true, "ok")]
    [InlineData("off", "file", false, "fail")]
    [InlineData("on", "file", false, null)]
    [InlineData("off", "rds_api", false, null)]
    public void The_logging_collector_is_asked_for_only_when_no_log_file_is_found(string collector, string source, bool found, string? status)
    {
        var check = PgProbeChecks.LoggingCollector(new Dictionary<string, string> { ["logging_collector"] = collector }, source,
            PgPlatform.SelfManaged, found);

        Assert.Equal(status, check?.Status);
    }

    [Fact]
    public void An_azure_server_with_no_log_directory_says_its_log_is_not_read()
    {
        var azure = PgProbeChecks.NoLogFiles("/var/log/pg", PgPlatform.Azure);
        var other = PgProbeChecks.NoLogFiles("/var/log/pg", PgPlatform.SelfManaged);

        Assert.Equal("fail", azure.Status);
        Assert.StartsWith("Azure Database for PostgreSQL flexible server has no log directory to mount", azure.Message);
        Assert.Equal("no log file in /var/log/pg: check logging_collector, log_directory and the mount", other.Message);
    }

    [Theory]
    [InlineData("%t:%r:%u@%d:[%p]:", "stderr", "ok", " (it has no %c)")]
    [InlineData("%m:%r:%u@%d:[%p]:%l:%e:%s:%v:%x:%c:%q%a:", "stderr", "ok", "")]
    [InlineData("%t-%c-", "stderr", "ok", " (it has no %u, %p)")]
    [InlineData("%t-%c-", "", "ok", " (it has no %u, %p)")]
    [InlineData("%m [%p] ", "stderr", "ok", " (it has no %u, %c)")]
    [InlineData("%m [%p] %q%u@%d ", "stderr", "ok", " (it has no %c)")]
    [InlineData("%m %d ", "stderr", "warn", " (it has no %u, %c, %p)")]
    [InlineData("[%p] %u ", "stderr", "fail", " (it has no %c)")]
    [InlineData("", "jsonlog", "ok", " (it has no %u, %c, %p)")]
    [InlineData("%t-%c-", "csvlog", "ok", " (it has no %u, %p)")]
    public void The_line_prefix_is_reported_with_the_fields_it_lacks(string prefix, string format, string status, string lacks)
    {
        var check = PgProbeChecks.LinePrefix(prefix, format);

        Assert.Equal("log_line_prefix", check.Name);
        Assert.Equal(status, check.Status);
        Assert.StartsWith($"log_line_prefix is '{prefix}'{lacks}: ", check.Message);
    }

    [Fact]
    public void A_long_prefix_is_cut_in_the_message()
    {
        var check = PgProbeChecks.LinePrefix(new string('x', 500) + "%u", "stderr");

        Assert.Contains("'" + new string('x', 80) + "...'", check.Message);
        Assert.True(check.Message.Length < 300);
    }

    /// <summary>As PostgreSQL reads a prefix: a padding before the escape, <c>%%</c> for a percent sign, a lone <c>%</c> at the end.</summary>
    [Theory]
    [InlineData("%t-%c-", "tc")]
    [InlineData("%-10u@%5d %%p [%p]", "udp")]
    [InlineData("%m %", "m")]
    [InlineData("%-", "")]
    [InlineData("no escape", "")]
    public void A_prefix_is_read_as_postgresql_reads_it(string prefix, string escapes)
    {
        Assert.Equal(escapes, string.Concat(PgLinePrefix.Escapes(prefix)));
    }

    [Fact]
    public void A_prefix_keeps_its_text_between_escapes()
    {
        var tokens = PgLinePrefix.Tokens("%-10u@%5d %%[%p]:");

        Assert.Equal(new PgLinePrefix.Token[]
        {
            new('u', "", -10), new('\0', "@", 0), new('d', "", 5), new('\0', " %[", 0), new('p', "", 0), new('\0', "]:", 0),
        }, tokens);
    }

    [Fact]
    public void Azure_keeps_its_own_role_and_databases_out()
    {
        Assert.Equal(new[] { "azuresu" }, PgPlatform.Users(PgPlatform.Azure));
        Assert.Equal(new[] { "rdsadmin", "azure_sys", "azure_maintenance" }, PgPlatform.Databases(PgPlatform.Azure));
        foreach (var other in new[] { PgPlatform.Rds, PgPlatform.Aurora, PgPlatform.SelfManaged, "" })
        {
            Assert.Empty(PgPlatform.Users(other));
            Assert.Equal(new[] { "rdsadmin" }, PgPlatform.Databases(other));
        }
    }

    /// <summary>Every statement that reads the counters, the texts or the catalog takes the platform's own roles and databases.</summary>
    [Fact]
    public void The_counters_texts_and_catalog_leave_the_platforms_own_out()
    {
        foreach (var snapshot in new[] { PgSql.Snapshot, PgSql.SnapshotWithoutInfo, PgSql.SnapshotTotalTime })
        {
            Assert.EndsWith("WHERE coalesce(r.rolname, '') <> ALL($1::text[]) AND coalesce(d.datname, '') <> ALL($2::text[])", snapshot);
        }
        Assert.Contains("userid NOT IN (SELECT oid FROM pg_roles WHERE rolname = ANY($1::text[]))", PgSql.StatementText);
        Assert.Contains("dbid NOT IN (SELECT oid FROM pg_database WHERE datname = ANY($2::text[]))", PgSql.StatementText);
        Assert.Contains("WHERE r.rolname <> ALL($1::text[])", PgSql.Users);
        Assert.Contains("datname <> ALL($1::text[])", PgSql.Databases);
    }
}
