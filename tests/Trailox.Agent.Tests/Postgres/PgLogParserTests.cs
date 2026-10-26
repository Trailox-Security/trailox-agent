using Trailox.Agent.Engines.Postgres;

namespace Trailox.Agent.Tests.Postgres;

/// <summary>
/// The three log formats, as PostgreSQL 13-18 and RDS write them. Every address is from
/// the documentation ranges; nothing here comes from a real server.
/// </summary>
public class PgLogParserTests
{
    private static readonly long T0 = new DateTimeOffset(2026, 10, 22, 6, 10, 1, TimeSpan.Zero).ToUnixTimeMilliseconds() * 1000;

    private static List<PgLogRecord> Parse(PgLogFormat format, string text, out long malformed)
    {
        var parser = PgLogParser.For(format);
        var done = new List<PgLogRecord>();
        foreach (var line in text.Split('\n'))
        {
            parser.Feed(line, done);
        }
        parser.Finish(done);
        malformed = parser.Malformed;
        return done;
    }

    private static List<PgLogRecord> Stderr(string text) => Parse(PgLogFormat.Stderr, text, out _);

    [Fact]
    public void Rds_prefix_gives_time_address_user_database_pid_and_level()
    {
        var r = Stderr("2026-10-22 06:10:01 UTC:192.0.2.10(54170):[unknown]@[unknown]:[7642]:LOG:  connection received: host=192.0.2.10 port=54170").Single();

        Assert.Equal(T0, r.TsMicros);
        Assert.Equal("192.0.2.10", r.RemoteHost);
        Assert.Equal("54170", r.RemotePort);
        Assert.Equal("[unknown]", r.User);
        Assert.Equal("[unknown]", r.Database);
        Assert.Equal(7642, r.Pid);
        Assert.Equal("LOG", r.Level);
        Assert.Equal(PgRecordKind.ConnectionReceived, r.Kind);
        Assert.Null(r.ApplicationName);
    }

    [Fact]
    public void Milliseconds_ipv6_local_and_background_prefixes_are_read()
    {
        var records = Stderr(string.Join('\n',
            "2026-10-22 06:10:01.250 UTC:2001:db8::5(5432):app_user@appdb:[11]:LOG:  disconnection: session time: 0:00:00.046 user=app_user database=appdb host=2001:db8::5 port=5432",
            "2026-10-22 06:10:01 UTC:[local]:postgres@postgres:[12]:LOG:  statement: CREATE TABLE t (a int)",
            "2026-10-22 06:10:01 UTC::@:[13]:LOG:  checkpoint starting: time"));

        Assert.Equal(T0 + 250_000, records[0].TsMicros);
        Assert.Equal("2001:db8::5", records[0].RemoteHost);
        Assert.Equal("5432", records[0].RemotePort);
        Assert.Equal(PgRecordKind.Disconnection, records[0].Kind);
        Assert.Equal("[local]", records[1].RemoteHost);
        Assert.Equal("postgres", records[1].User);
        Assert.Equal(PgRecordKind.Statement, records[1].Kind);
        Assert.Equal("", records[2].RemoteHost);
        Assert.Equal("", records[2].User);
        Assert.Equal(PgRecordKind.Other, records[2].Kind);
    }

    [Fact]
    public void A_user_name_with_at_signs_splits_at_the_last_one()
    {
        var r = Stderr("2026-10-22 06:10:01 UTC:192.0.2.10(1):alice@example.com@appdb:[7]:LOG:  connection authorized: user=alice@example.com database=appdb").Single();

        Assert.Equal("alice@example.com", r.User);
        Assert.Equal("appdb", r.Database);
        Assert.Equal(PgRecordKind.ConnectionAuthorized, r.Kind);
    }

    [Theory]
    // 14 and 15 (and 13).
    [InlineData("Connection matched pg_hba.conf line 97: \"host all all 192.0.2.0/24 scram-sha-256\"")]
    // 16, 17, 18 and RDS 17.
    [InlineData("Connection matched file \"/rdsdbdata/config/pg_hba.conf\" line 13: \"host all all 0.0.0.0/0 md5\"")]
    public void A_fatal_and_its_detail_are_one_record_in_both_hint_wordings(string hint)
    {
        var records = Stderr(string.Join('\n',
            "2026-10-22 06:10:01 UTC:192.0.2.10(54170):nobody@appdb:[7642]:FATAL:  password authentication failed for user \"nobody\"",
            "2026-10-22 06:10:01 UTC:192.0.2.10(54170):nobody@appdb:[7642]:DETAIL:  Role \"nobody\" does not exist.",
            "\t" + hint,
            "2026-10-22 06:10:02 UTC:192.0.2.10(54171):app@appdb:[7643]:LOG:  connection received: host=192.0.2.10 port=54171"));

        Assert.Equal(2, records.Count);
        var fatal = records[0];
        Assert.Equal(PgRecordKind.Fatal, fatal.Kind);
        Assert.Equal("nobody", fatal.User);
        Assert.Equal("Role \"nobody\" does not exist.\n\t" + hint, fatal.Detail);
    }

    [Fact]
    public void A_detail_of_another_pid_is_not_joined()
    {
        var records = Stderr(string.Join('\n',
            "2026-10-22 06:10:01 UTC:192.0.2.10(1):a@d:[1]:FATAL:  password authentication failed for user \"a\"",
            "2026-10-22 06:10:01 UTC:192.0.2.11(2):b@d:[2]:DETAIL:  Role \"b\" does not exist."));

        Assert.Equal(2, records.Count);
        Assert.Equal("", records[0].Detail);
        Assert.Equal("DETAIL", records[1].Level);
        Assert.Equal(PgRecordKind.Other, records[1].Kind);
    }

    [Fact]
    public void An_error_keeps_its_statement_apart_and_a_statement_keeps_its_lines()
    {
        var records = Stderr(string.Join('\n',
            "2026-10-22 06:10:01 UTC:192.0.2.10(1):app@appdb:[5]:ERROR:  permission denied for table orders",
            "2026-10-22 06:10:01 UTC:192.0.2.10(1):app@appdb:[5]:STATEMENT:  SELECT * FROM orders WHERE card = '4111'",
            "2026-10-22 06:10:02 UTC:192.0.2.10(1):app@appdb:[5]:LOG:  statement: CREATE TABLE t (",
            "\ta int,",
            "\tb text",
            ")"));

        Assert.Equal("SELECT * FROM orders WHERE card = '4111'", records[0].Statement);
        Assert.Equal("permission denied for table orders", records[0].Message);
        Assert.Equal(PgRecordKind.Other, records[0].Kind);
        Assert.Equal("statement: CREATE TABLE t (\n\ta int,\n\tb text\n)", records[1].Message);
    }

    [Theory]
    [InlineData("execute <unnamed>: CREATE TABLE flyway_schema_history (a int)")]
    [InlineData("execute S_1: ALTER TABLE t ADD COLUMN b int")]
    [InlineData("execute fetch from S_2/C_3: CREATE INDEX i ON t (a)")]
    public void Ddl_from_the_extended_protocol_is_an_execute_record(string message)
    {
        var r = Stderr("2026-10-22 06:10:01 UTC:192.0.2.10(1):app@appdb:[5]:LOG:  " + message).Single();

        Assert.Equal(PgRecordKind.Execute, r.Kind);
        Assert.DoesNotContain("execute", PgRecordKinds.StatementText(r.Message), StringComparison.Ordinal);
    }

    [Fact]
    public void Rds_long_prefix_gives_sqlstate_session_and_an_application_name_with_colons()
    {
        var records = Stderr(string.Join('\n',
            "2026-10-22 06:10:01.500 UTC:192.0.2.10(54170):app@appdb:[7642]:4:00000:2026-10-22 06:00:00 UTC:3/12:0:6712a.1b:etl:job:7:LOG:  statement: GRANT SELECT ON t TO r",
            "2026-10-22 06:10:01.600 UTC:::@:[99]:1:00000:2026-10-22 06:00:00 UTC::0:6712a.2c:LOG:  checkpoint complete"));

        Assert.Equal("etl:job:7", records[0].ApplicationName);
        Assert.Equal("00000", records[0].SqlState);
        Assert.Equal("6712a.1b", records[0].SessionId);
        Assert.Equal(PgRecordKind.Statement, records[0].Kind);
        Assert.Equal("", records[1].ApplicationName);
        Assert.Equal("6712a.2c", records[1].SessionId);
    }

    [Fact]
    public void A_zone_that_is_not_utc_cannot_be_read_and_counts_once()
    {
        var records = Parse(PgLogFormat.Stderr, "2026-10-22 09:10:01 IDT:192.0.2.10(1):a@d:[1]:LOG:  connection received: host=192.0.2.10 port=1", out var malformed);

        // The record with no time is the window's one unreadable record; the parser does not count it again.
        Assert.Equal(long.MinValue, records.Single().TsMicros);
        Assert.Equal(0, malformed);
    }

    /// <summary>
    /// 🔴 A new RDS replica's first file starts with its restore script's lines, which have no prefix. They
    ///    are a preamble, not unreadable records, at the time of the file's first record, so one window counts them.
    /// </summary>
    [Fact]
    public void Lines_before_the_first_record_are_a_preamble_at_its_time()
    {
        var done = Parse(PgLogFormat.Stderr, string.Join('\n',
            "recovering 00000002.history",
            "archive 000000010000000000000004 is not yet downloaded, exiting restore_command",
            "2026-10-22 06:10:01 UTC::@:[431]:LOG:  database system is ready to accept read-only connections"), out var malformed);

        Assert.Equal((2, true, T0), (done[0].UnreadLines, done[0].Preamble, done[0].TsMicros));
        Assert.Equal("database system is ready to accept read-only connections", done[1].Message);
        Assert.Equal(0, malformed);
    }

    [Fact]
    public void A_file_of_preamble_only_takes_the_files_time_or_counts_as_before()
    {
        var parser = new StderrLogParser(fileStartMicros: T0 - 1_000_000);
        var done = new List<PgLogRecord>();
        parser.Feed("recovering 00000002.history", done);
        parser.Finish(done);
        Assert.Equal((1, true, T0 - 1_000_000), (done.Single().UnreadLines, done.Single().Preamble, done.Single().TsMicros));

        // A name with no time (Red Hat's postgresql-Mon.log): no window to place the line in, so it counts as it did.
        Assert.Empty(Parse(PgLogFormat.Stderr, "recovering 00000002.history", out var malformed));
        Assert.Equal(1, malformed);
    }

    [Fact]
    public void A_record_with_a_prefix_we_do_not_know_counts_once_with_its_lines()
    {
        var done = Parse(PgLogFormat.Stderr, string.Join('\n',
            "2026-10-22 06:10:01 UTC:192.0.2.10(1):a@d:[9]:SOMETHING ELSE",
            "  its second line",
            "  its third line"), out var malformed);

        Assert.Equal((1, false, T0), (done.Single().UnreadLines, done.Single().Preamble, done.Single().TsMicros));
        Assert.Equal(0, malformed);
    }

    private static string Csv(params string[] fields) =>
        string.Join(',', fields.Select(f => f.IndexOfAny(new[] { ',', '"', '\n' }) >= 0 ? "\"" + f.Replace("\"", "\"\"") + "\"" : f));

    private static string[] CsvFields(int columns, string message, string app = "psql", string detail = "", string from = "192.0.2.10:54170")
    {
        var f = new string[columns];
        Array.Fill(f, "");
        f[0] = "2026-10-22 06:10:01.250 UTC";
        f[1] = "alice";
        f[2] = "appdb";
        f[3] = "7642";
        f[4] = from;
        f[5] = "6712a.1b";
        f[11] = message.StartsWith("password", StringComparison.Ordinal) ? "FATAL" : "LOG";
        f[12] = "00000";
        f[13] = message;
        f[14] = detail;
        f[22] = app;
        if (columns > 23)
        {
            f[23] = "client backend";
        }
        return f;
    }

    [Theory]
    [InlineData(24)] // PostgreSQL 13
    [InlineData(26)] // PostgreSQL 14-18
    public void Csvlog_reads_13_and_14_to_18(int columns)
    {
        var r = Parse(PgLogFormat.Csv, Csv(CsvFields(columns, "connection authorized: user=alice database=appdb application_name=psql")), out var malformed).Single();

        Assert.Equal(0, malformed);
        Assert.Equal(T0 + 250_000, r.TsMicros);
        Assert.Equal("192.0.2.10", r.RemoteHost);
        Assert.Equal("54170", r.RemotePort);
        Assert.Equal("psql", r.ApplicationName);
        Assert.Equal("6712a.1b", r.SessionId);
        Assert.Equal(PgRecordKind.ConnectionAuthorized, r.Kind);
    }

    [Fact]
    public void Csvlog_fields_span_lines_and_keep_doubled_quotes_and_an_empty_application_name()
    {
        var statement = "statement: CREATE VIEW v AS\nSELECT 'a,b' AS \"quoted\"\nFROM t";
        var text = Csv(CsvFields(26, statement, app: "", from: "2001:db8::5:5432"));

        var r = Parse(PgLogFormat.Csv, text, out var malformed).Single();

        Assert.Equal(0, malformed);
        Assert.Equal(statement, r.Message);
        Assert.Equal("", r.ApplicationName);
        Assert.Equal("2001:db8::5", r.RemoteHost);
        Assert.Equal("5432", r.RemotePort);
    }

    [Fact]
    public void Csvlog_keeps_a_one_and_a_half_megabyte_statement_whole()
    {
        var big = "statement: CREATE VIEW v AS SELECT '" + new string('x', 1_500_000) + "'";
        var r = Parse(PgLogFormat.Csv, Csv(CsvFields(26, big)), out _).Single();

        Assert.Equal(big.Length, r.Message.Length);
    }

    [Fact]
    public void A_csvlog_record_cut_by_the_end_of_the_file_is_malformed()
    {
        var text = Csv(CsvFields(26, "statement: CREATE TABLE a (b int)"));
        var cut = text[..(text.Length / 2)] + "\"\nmore";

        Parse(PgLogFormat.Csv, cut, out var malformed);

        Assert.Equal(1, malformed);
    }

    [Fact]
    public void Jsonlog_reads_a_record_and_leaves_an_absent_application_name_empty()
    {
        var text = string.Join('\n',
            """{"timestamp":"2026-10-22 06:10:01.250 UTC","user":"alice","dbname":"appdb","pid":7642,"remote_host":"192.0.2.10","remote_port":54170,"session_id":"6712a.1b","error_severity":"FATAL","state_code":"28P01","message":"password authentication failed for user \"alice\"","detail":"Connection matched file \"/etc/postgresql/16/main/pg_hba.conf\" line 120: \"host all all 192.0.2.0/24 scram-sha-256\"","backend_type":"client backend"}""",
            "{not json",
            "");

        var records = Parse(PgLogFormat.Json, text, out var malformed);

        var r = records.Single();
        Assert.Equal(1, malformed);
        Assert.Equal(T0 + 250_000, r.TsMicros);
        Assert.Equal("54170", r.RemotePort);
        Assert.Equal("28P01", r.SqlState);
        Assert.Equal("", r.ApplicationName);
        Assert.Equal(PgRecordKind.Fatal, r.Kind);
        Assert.StartsWith("Connection matched file", r.Detail);
    }

    [Theory]
    [InlineData("connection authenticated: identity=\"alice\" method=scram-sha-256 (/etc/postgresql/17/main/pg_hba.conf:120)", PgRecordKind.ConnectionAuthenticated)]
    [InlineData("replication connection authorized: user=repl application_name=walreceiver", PgRecordKind.ConnectionAuthorized)]
    [InlineData("AUDIT: SESSION,1,1,READ,SELECT,,,SELECT 1,<not logged>", PgRecordKind.Audit)]
    [InlineData("duration: 1.234 ms  statement: SELECT 1", PgRecordKind.Other)]
    [InlineData("execute: not a prepared statement", PgRecordKind.Other)]
    public void Kinds_come_from_the_level_and_the_start_of_the_message(string message, PgRecordKind kind)
    {
        Assert.Equal(kind, PgRecordKinds.Of("LOG", message));
    }

    [Fact]
    public void A_fatal_or_panic_is_always_fatal_and_other_levels_are_other()
    {
        Assert.Equal(PgRecordKind.Fatal, PgRecordKinds.Of("PANIC", "could not write to file"));
        Assert.Equal(PgRecordKind.Other, PgRecordKinds.Of("ERROR", "statement: looks like a statement"));
    }

    [Fact]
    public void Statement_text_drops_the_prefix_of_both_forms()
    {
        Assert.Equal("CREATE TABLE t (a int)", PgRecordKinds.StatementText("statement: CREATE TABLE t (a int)"));
        Assert.Equal("ALTER TABLE t ADD b int", PgRecordKinds.StatementText("execute S_1: ALTER TABLE t ADD b int"));
        Assert.Equal("CREATE TABLE x (a int)", PgRecordKinds.StatementText("execute <unnamed>: CREATE TABLE x (a int)"));
    }

    [Fact]
    public void A_message_in_another_language_is_counted_as_other()
    {
        // lc_messages = 'de_DE': "Verbindung autorisiert" is PostgreSQL's German "connection authorized".
        var r = Stderr("2026-10-22 06:10:01 UTC:192.0.2.10(1):a@d:[1]:LOG:  Verbindung autorisiert: Benutzer=a Datenbank=d").Single();

        Assert.Equal(PgRecordKind.Other, r.Kind);
        Assert.Equal("Verbindung autorisiert: Benutzer=a Datenbank=d", r.Message);
    }
}
