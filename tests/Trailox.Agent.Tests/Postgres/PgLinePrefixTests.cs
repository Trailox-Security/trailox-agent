using System.Diagnostics;
using Trailox.Agent.Engines.Postgres;

namespace Trailox.Agent.Tests.Postgres;

/// <summary>
/// A stderr line of any <c>log_line_prefix</c>: the pattern built from the prefix, Azure's server log download, and the
/// parser that reads both.
/// </summary>
public class PgLinePrefixTests
{
    private static long Micros(string utc) => (DateTime.SpecifyKind(DateTime.Parse(utc, System.Globalization.CultureInfo.InvariantCulture), DateTimeKind.Utc)
        - DateTime.UnixEpoch).Ticks / 10;

    [Fact]
    public void Azures_prefix_gives_the_time_the_session_and_its_process()
    {
        var f = PgLineFormat.Of("%t-%c-").Match("2026-01-15 10:57:18 UTC-6968c80d.423-LOG:  statement: CREATE ROLE x LOGIN")!;

        Assert.Equal(Micros("2026-01-15 10:57:18"), f.TsMicros);
        Assert.Equal("6968c80d.423", f.SessionId);
        Assert.Equal(0x423, f.Pid);
        Assert.Equal("LOG", f.Level);
        Assert.Equal("statement: CREATE ROLE x LOGIN", f.Text);
        Assert.Null(f.User);
        Assert.Null(f.Remote);
    }

    [Fact]
    public void A_session_id_is_its_start_and_its_process_in_hex()
    {
        Assert.Equal(Micros("2026-01-15 10:57:17"), PgSessionId.StartMicros("6968c80d.423"));
        Assert.Equal(1059, PgSessionId.Pid("6968c80d.423"));
        Assert.Null(PgSessionId.StartMicros("nope"));
    }

    [Theory]
    [InlineData("2026-01-15 10:57:18 UTC:192.0.2.10(52128):app@shop:[1059]:LOG:  statement: x", "192.0.2.10", "52128", "app", "shop", 1059)]
    [InlineData("2026-01-15 10:57:18 UTC:[local]:app@shop:[1059]:LOG:  statement: x", "[local]", "", "app", "shop", 1059)]
    [InlineData("2026-01-15 10:57:18 UTC:2001:db8::1(5432):app@shop:[1059]:LOG:  statement: x", "2001:db8::1", "5432", "app", "shop", 1059)]
    [InlineData("2026-01-15 10:57:18 UTC::@:[7]:LOG:  checkpoint starting: time", "", "", "", "", 7)]
    public void The_rds_prefix_reads_as_rds_writes_it(string line, string host, string port, string user, string database, int pid)
    {
        var f = PgLineFormat.Of(StderrLogParser.RdsPrefix).Match(line)!;

        Assert.Equal((host, port), f.Remote);
        Assert.Equal((user, database, pid), (f.User, f.Database, f.Pid));
        Assert.Equal(Micros("2026-01-15 10:57:18"), f.TsMicros);
    }

    /// <summary>Debian's prefix: a process that is no session writes nothing after %q.</summary>
    [Fact]
    public void After_q_only_a_session_writes()
    {
        var format = PgLineFormat.Of("%m [%p] %q%u@%d ");

        var session = format.Match("2026-01-15 10:57:18.123 UTC [1059] app@shop LOG:  statement: SELECT 1")!;
        var background = format.Match("2026-01-15 10:57:18.123 UTC [31] LOG:  checkpoint starting: time")!;

        Assert.Equal(Micros("2026-01-15 10:57:18") + 123_000, session.TsMicros);
        Assert.Equal(("app", "shop", 1059), (session.User, session.Database, session.Pid));
        Assert.Equal("statement: SELECT 1", session.Text);
        Assert.Equal((null, 31, "checkpoint starting: time"), (background.User, background.Pid, background.Text));
    }

    [Theory]
    [InlineData("%m [%p] ", "2026-01-15 10:57:18.123 UTC [1059] ERROR:  boom", "ERROR", "boom")]
    [InlineData("%n|%-6u|%a|", "1768474638.250|app   |psql|LOG:  connection authorized: x", "LOG", "connection authorized: x")]
    [InlineData("<%t %e> ", "<2026-01-15 10:57:18 UTC 42501> ERROR:  permission denied", "ERROR", "permission denied")]
    public void Any_prefix_reads_by_its_escapes(string prefix, string line, string level, string text)
    {
        var f = PgLineFormat.Of(prefix).Match(line);

        Assert.NotNull(f);
        Assert.Equal((level, text), (f!.Level, f.Text));
        Assert.NotNull(f.TsMicros);
    }

    [Fact]
    public void Padding_epoch_time_and_sqlstate_are_read()
    {
        var f = PgLineFormat.Of("%n|%-6u|%a|%e ").Match("1768474638.250|app   |psql|28P01 FATAL:  password authentication failed")!;

        Assert.Equal(1768474638_250_000L, f.TsMicros);
        Assert.Equal(("app", "psql", "28P01"), (f.User, f.Application, f.SqlState));
    }

    [Theory]
    [InlineData("%t-%c-", "LOG:  statement: x")]               // no prefix at all: a continuation line
    [InlineData("%t-%c-", "2026-01-15 10:57:18 UTC-6968c80d.423-NOTALEVEL:  x")]
    [InlineData("%m [%p] ", "2026-01-15 10:57:18 UTC [1059] LOG:  x")]   // %m has milliseconds
    public void A_line_that_does_not_fit_the_prefix_starts_no_record(string prefix, string line)
    {
        Assert.Null(PgLineFormat.Of(prefix).Match(line));
    }

    [Fact]
    public void A_prefix_with_no_time_gives_no_time()
    {
        var format = PgLineFormat.Of("[%p] ");

        Assert.False(format.HasTime);
        Assert.Null(format.Match("[1059] LOG:  statement: x")!.TsMicros);
    }

    /// <summary>🔴 Anyone's text reaches the pattern: a long line of what free text could match costs linear time.</summary>
    [Fact]
    public void A_megabyte_line_is_matched_in_linear_time()
    {
        var format = PgLineFormat.Of("%m [%p] %q%u@%d %a ");
        var line = "2026-01-15 10:57:18.123 UTC [1059] " + string.Concat(Enumerable.Repeat("a@b c ", 200_000));

        var clock = Stopwatch.StartNew();
        Assert.Null(format.Match(line));
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(2), clock.Elapsed.ToString());
    }

    [Fact]
    public void Azures_line_carries_an_errors_statement_as_a_field()
    {
        var (text, fields) = PgAzureLine.Split(
            "2026-01-15 10:57:19 UTC-6968c80d.423-ERROR:  permission denied to set parameter \\\"log_statement\\\"\",\"statement\": \"ALTER SYSTEM SET log_statement = 'ddl'");

        Assert.Equal("2026-01-15 10:57:19 UTC-6968c80d.423-ERROR:  permission denied to set parameter \"log_statement\"", text);
        Assert.Equal(new[] { new PgAzureLine.Field("statement", "ALTER SYSTEM SET log_statement = 'ddl'") }, fields);
    }

    [Fact]
    public void Azures_line_carries_a_refusals_detail_with_its_newline()
    {
        var (text, fields) = PgAzureLine.Split(
            "x-FATAL:  password authentication failed for user \\\"u\\\"\",\"detail_log\": \"Role \\\"u\\\" does not exist.\\nConnection matched line 29: \\\"hostssl all all  0.0.0.0/0 md5\\\"");

        Assert.Equal("x-FATAL:  password authentication failed for user \"u\"", text);
        Assert.Equal("Role \"u\" does not exist.\nConnection matched line 29: \"hostssl all all  0.0.0.0/0 md5\"", Assert.Single(fields).Value);
    }

    /// <summary>🔴 A statement's own text cannot forge a field: its quotes are escaped, so no separator is unescaped.</summary>
    [Theory]
    [InlineData("statement: SELECT '\\\",\\\"statement\\\": \\\"DROP'", "statement: SELECT '\",\"statement\": \"DROP'")]
    [InlineData("statement: SELECT 'a\\\\'", "statement: SELECT 'a\\'")]
    [InlineData("tab\\there \\u00e9 \\/ \\q", "tab\there é / \\q")]
    public void Only_an_unescaped_quote_separates_a_field(string line, string expected)
    {
        var (text, fields) = PgAzureLine.Split(line);

        Assert.Equal(expected, text);
        Assert.Empty(fields);
    }

    [Fact]
    public void A_backslash_at_the_end_of_a_value_does_not_hide_the_separator()
    {
        var (text, fields) = PgAzureLine.Split("ERROR:  bad \\\\\",\"statement\": \"SELECT 1");

        Assert.Equal("ERROR:  bad \\", text);
        Assert.Equal("SELECT 1", Assert.Single(fields).Value);
    }

    [Fact]
    public void The_azure_download_parses_into_records_with_their_statement_and_detail()
    {
        var parser = new PrefixLogParser(PgLineFormat.Of("%t-%c-"), azureFile: true);
        var records = new List<PgLogRecord>();
        foreach (var line in File.ReadAllLines(PgFixtures.AzureDownload))
        {
            parser.Feed(line, records);
        }
        parser.Finish(records);

        Assert.Equal(0, parser.Malformed);
        Assert.Equal(479, records.Count);   // one record a line: Azure writes a record's parts on its own line
        var error = records.Single(r => r.Level == "ERROR" && r.Message.StartsWith("permission denied to set parameter", StringComparison.Ordinal));
        Assert.Equal("permission denied to set parameter \"log_statement\"", error.Message);
        Assert.Equal("ALTER SYSTEM SET log_statement = 'ddl'", error.Statement);
        var unknown = records.Single(r => r.Level == "FATAL" && r.Message.Contains("no_such_user", StringComparison.Ordinal));
        Assert.StartsWith("Role \"no_such_user\" does not exist.\nConnection matched file", unknown.Detail);
        var audit = records.First(r => r.Kind == PgRecordKind.Audit);
        Assert.Equal("6968c811.425", audit.SessionId);
        Assert.Equal(0x425, audit.Pid);
        Assert.NotNull(PgAuditLine.Parse(audit.Message));
        Assert.All(records, r => Assert.NotEqual(long.MinValue, r.TsMicros));
    }

    [Theory]
    [InlineData(StderrLogParser.RdsPrefix, false, typeof(StderrLogParser))]
    [InlineData(null, false, typeof(StderrLogParser))]
    [InlineData("%t-%c-", false, typeof(PrefixLogParser))]
    [InlineData(StderrLogParser.RdsPrefix, true, typeof(PrefixLogParser))]
    public void Stderr_of_any_prefix_but_rdss_is_read_by_the_prefix(string? prefix, bool azureFile, Type parser)
    {
        Assert.IsType(parser, PgLogParser.For(PgLogFormat.Stderr, null, prefix == null ? null : PgLineFormat.Of(prefix), azureFile));
        Assert.IsType<JsonLogParser>(PgLogParser.For(PgLogFormat.Json, null, PgLineFormat.Of("%t-%c-")));
    }

    [Fact]
    public void A_prefix_names_a_detail_by_its_process()
    {
        var parser = new PrefixLogParser(PgLineFormat.Of("%m [%p] "));
        var records = new List<PgLogRecord>();
        foreach (var line in new[]
                 {
                     "2026-01-15 10:57:18.001 UTC [10] FATAL:  password authentication failed for user \"u\"",
                     "2026-01-15 10:57:18.001 UTC [10] DETAIL:  Role \"u\" does not exist.",
                     "\tConnection matched line 29",
                     "2026-01-15 10:57:18.002 UTC [11] ERROR:  boom",
                     "2026-01-15 10:57:18.002 UTC [11] STATEMENT:  SELECT",
                     "  1",
                     "2026-01-15 10:57:18.003 UTC [12] DETAIL:  not the error's",
                 })
        {
            parser.Feed(line, records);
        }
        parser.Finish(records);

        Assert.Equal(3, records.Count);
        Assert.Equal("Role \"u\" does not exist.\n\tConnection matched line 29", records[0].Detail);
        Assert.Equal(("boom", "", "SELECT\n  1"), (records[1].Message, records[1].Detail, records[1].Statement));
        Assert.Equal(("DETAIL", 12), (records[2].Level, records[2].Pid));   // another process's: a record of its own
    }
}

/// <summary>The test files: cut from real server logs, with every name, address and date replaced.</summary>
internal static class PgFixtures
{
    /// <summary>An hour of Azure Database for PostgreSQL's server log download (prefix %t-%c-), its first 479 lines.</summary>
    public static string AzureDownload => Path.Combine(Root(), "tests", "Trailox.Agent.Tests", "Fixtures", "pg-azure-download.log");

    /// <summary>
    /// The same stretch of one server's log read from both of Azure's sources: <c>.json</c> is Log Analytics' answer to
    /// the agent's query (<c>PGSQLServerLogs</c>), <c>.log</c> the download's lines. <c>traffic</c> (15:30 to 15:40)
    /// holds sign-ins, refused sign-ins, DDL and pgaudit rows; <c>odd</c> (16:50 to 16:55) statements with newlines, a
    /// tab, quotes, a backslash and a field separator written in their text.
    /// </summary>
    public static string AzureBoth(string name, string extension) =>
        Path.Combine(Root(), "tests", "Trailox.Agent.Tests", "Fixtures", $"pg-azure-la-{name}.{extension}");

    private static string Root()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !File.Exists(Path.Combine(dir.FullName, "trailox-agent.sln")))
        {
            dir = dir.Parent;
        }
        Assert.NotNull(dir);
        return dir!.FullName;
    }
}
