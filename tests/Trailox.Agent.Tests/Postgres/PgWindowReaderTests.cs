using System.Buffers;
using System.Text;
using System.Text.Json;
using Trailox.Agent.Engines.MongoDb;
using Trailox.Agent.Engines.Postgres;

namespace Trailox.Agent.Tests.Postgres;

/// <summary>What one window ships: the Logs only kinds, by their own time, redacted and cut; everything else counted.</summary>
public class PgWindowReaderTests
{
    private const long Start = 1_792_649_400_000_000; // 2026-10-22 06:10:00 UTC
    private const long End = Start + 600_000_000;

    private static PgLogRecord R(long ts, string level, string message, string detail = "", string statement = "", int pid = 7) => new()
    {
        TsMicros = ts,
        Pid = pid,
        User = "app",
        Database = "appdb",
        RemoteHost = "192.0.2.10",
        RemotePort = "54170",
        Level = level,
        Message = message,
        Detail = detail,
        Statement = statement,
        Format = PgLogFormat.Stderr,
    };

    private static (List<JsonElement> Rows, PgWindowReader Reader) Run(bool shipText, params PgLogRecord[] records)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var reader = new PgWindowReader("db1", Start, End, shipText, new RowSink(buffer));
        foreach (var r in records)
        {
            reader.Add(r);
        }
        reader.WriteCounts();
        var rows = Encoding.UTF8.GetString(buffer.WrittenSpan).Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => JsonDocument.Parse(l).RootElement.Clone()).ToList();
        return (rows, reader);
    }

    private static JsonElement Counts(List<JsonElement> rows) => JsonDocument.Parse(rows[^1].GetProperty("message").GetString()!).RootElement;

    /// <summary>One stderr file read through its parser into one window, as the RDS reader does: the window's counts.</summary>
    private static JsonElement ReadFile(long start, long end, params string[] lines)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var reader = new PgWindowReader("db1", start, end, true, new RowSink(buffer));
        var parser = new StderrLogParser();
        var done = new List<PgLogRecord>();
        foreach (var line in lines)
        {
            parser.Feed(line, done);
        }
        parser.Finish(done);
        done.ForEach(reader.Add);
        reader.WriteCounts();
        return Counts(Encoding.UTF8.GetString(buffer.WrittenSpan).Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => JsonDocument.Parse(l).RootElement.Clone()).ToList()).Clone();
    }

    /// <summary>
    /// 🔴 Every window that reads an RDS file reads it from its start, so the restore script's preamble
    ///    reached each of them and warned for a day. It counts once, in the window of its time, apart from unreadable.
    /// </summary>
    [Fact]
    public void A_files_preamble_counts_once_in_its_own_window_and_apart_from_unreadable()
    {
        string[] file =
        {
            "recovering 00000002.history",
            "archive 000000010000000000000004 is not yet downloaded",
            "2026-10-22 06:10:01 UTC::@:[431]:LOG:  database system is ready to accept read-only connections",
            "2026-10-22 06:20:01 UTC::@:[431]:LOG:  checkpoint starting: time",
        };

        var first = ReadFile(Start, End, file);
        var second = ReadFile(End, End + 600_000_000, file);

        Assert.Equal((2L, 0L, 2L), (first.GetProperty("preamble").GetInt64(), first.GetProperty("unreadable").GetInt64(),
            first.GetProperty("records").GetInt64()));
        Assert.Equal((0L, 0L), (second.GetProperty("preamble").GetInt64(), second.GetProperty("unreadable").GetInt64()));
    }

    /// <summary>
    /// 🔴 A server that writes German. Its sign-ins read as LOG records of no kind we know, so they are
    ///    counted unrecognised. On an English server unknown LOG records (checkpoints) stay other;
    ///    a translated level is no LOG record either way.
    /// </summary>
    [Theory]
    [InlineData(false, 1, 1)]
    [InlineData(true, 0, 2)]
    public void A_log_record_of_no_known_kind_is_unrecognised_only_where_the_server_is_not_english(bool english, int unrecognised, int other)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var reader = new PgWindowReader("db1", Start, End, true, new RowSink(buffer), english: english);
        reader.Add(R(Start, "LOG", "Verbindung autorisiert: Benutzer=app Datenbank=appdb"));
        reader.Add(R(Start, "FEHLER", "Relation t existiert nicht"));
        reader.WriteCounts();

        var counted = Counts(Encoding.UTF8.GetString(buffer.WrittenSpan).Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => JsonDocument.Parse(l).RootElement.Clone()).ToList()).GetProperty("counted");
        Assert.Equal(unrecognised, counted.TryGetProperty("unrecognised", out var u) ? u.GetInt32() : 0);
        Assert.Equal(other, counted.GetProperty("other").GetInt32());
    }

    [Fact]
    public void Records_belong_to_the_window_by_their_own_time()
    {
        var (rows, reader) = Run(true,
            R(Start - 1, "LOG", "statement: CREATE TABLE before (a int)"),
            R(Start, "LOG", "statement: CREATE TABLE first (a int)"),
            R(End - 1, "LOG", "statement: CREATE TABLE last (a int)"),
            R(End, "LOG", "statement: CREATE TABLE after (a int)"));

        Assert.Equal(new[] { "statement: CREATE TABLE first (a int)", "statement: CREATE TABLE last (a int)" },
            rows.SkipLast(1).Select(r => r.GetProperty("message").GetString()));
        Assert.Equal(2, Counts(rows).GetProperty("out_of_window").GetInt64());
        Assert.False(reader.Past);
    }

    [Fact]
    public void A_record_five_seconds_past_the_end_stops_the_read()
    {
        var (_, reader) = Run(true, R(End + PgWindowReader.StopMarginMicros - 1, "LOG", "x"));
        Assert.False(reader.Past);

        (_, reader) = Run(true, R(End + PgWindowReader.StopMarginMicros, "LOG", "x"));
        Assert.True(reader.Past);
    }

    [Fact]
    public void Only_the_logs_only_kinds_ship_and_the_rest_is_counted()
    {
        var (rows, _) = Run(true,
            R(Start, "LOG", "connection received: host=192.0.2.10 port=54170"),
            R(Start, "LOG", "connection authorized: user=app database=appdb application_name=psql"),
            R(Start, "LOG", "disconnection: session time: 0:00:01.000 user=app database=appdb host=192.0.2.10 port=54170"),
            R(Start, "FATAL", "password authentication failed for user \"app\"", "Connection matched file \"/etc/postgresql/17/main/pg_hba.conf\" line 120: \"host all all all scram-sha-256\""),
            R(Start, "ERROR", "permission denied for table cards", statement: "SELECT number FROM cards WHERE id = 4111"),
            R(Start, "LOG", "AUDIT: SESSION,1,1,READ,SELECT,,,SELECT 1,<not logged>"),
            R(Start, "LOG", "checkpoint starting: time"),
            new PgLogRecord { TsMicros = long.MinValue, Level = "LOG", Message = "unreadable time" });

        Assert.Equal(new[] { "LOG", "LOG", "LOG", "FATAL", "summary" }, rows.Select(r => r.GetProperty("level").GetString()));
        Assert.DoesNotContain(rows, r => r.GetRawText().Contains("4111", StringComparison.Ordinal));
        var counts = Counts(rows);
        Assert.Equal(1, counts.GetProperty("counted").GetProperty("audit").GetInt64());
        Assert.Equal(2, counts.GetProperty("counted").GetProperty("other").GetInt64());
        Assert.Equal(1, counts.GetProperty("unreadable").GetInt64());
        Assert.Equal(4, counts.GetProperty("shipped").GetInt64());
        Assert.StartsWith("Connection matched file", rows[3].GetProperty("detail").GetString());
    }

    [Fact]
    public void A_statement_ships_its_command_redacted_text_and_never_its_parameters()
    {
        var (rows, _) = Run(true,
            R(Start, "LOG", "statement: CREATE ROLE etl LOGIN PASSWORD 'S3cret'"),
            R(Start, "LOG", "execute <unnamed>: ALTER ROLE etl PASSWORD 'An0ther'", detail: "Parameters: $1 = 'value'"));

        Assert.Equal("statement: CREATE ROLE etl LOGIN PASSWORD '<redacted>'", rows[0].GetProperty("message").GetString());
        Assert.Equal("CREATE ROLE", rows[0].GetProperty("command").GetString());
        Assert.Equal("ALTER ROLE", rows[1].GetProperty("command").GetString());
        Assert.Equal("", rows[1].GetProperty("detail").GetString());
        Assert.DoesNotContain(rows, r => r.GetRawText().Contains("S3cret", StringComparison.Ordinal) || r.GetRawText().Contains("An0ther", StringComparison.Ordinal));
    }

    [Fact]
    public void Without_statement_text_a_statement_ships_its_command_only()
    {
        var (rows, _) = Run(false, R(Start, "LOG", "statement: GRANT SELECT ON salaries TO analyst"));

        Assert.Equal("", rows[0].GetProperty("message").GetString());
        Assert.Equal("GRANT", rows[0].GetProperty("command").GetString());
    }

    [Fact]
    public void A_long_text_is_cut_and_marked()
    {
        var (rows, _) = Run(true, R(Start, "LOG", "statement: CREATE VIEW v AS SELECT '" + new string('x', PgWindowReader.MaxTextChars) + "'"));

        Assert.Equal(PgWindowReader.MaxTextChars, rows[0].GetProperty("message").GetString()!.Length);
        Assert.True(rows[0].GetProperty("truncated").GetBoolean());
    }

    [Fact]
    public void Every_column_is_in_every_row_and_stderr_has_no_application_name()
    {
        var (rows, _) = Run(true, R(Start, "LOG", "connection received: host=192.0.2.10 port=54170"));

        var names = rows[0].EnumerateObject().Select(p => p.Name).ToArray();
        Assert.Equal(new[] { "instance", "ts", "pid", "user", "database", "remote_host", "remote_port", "level", "sql_state",
            "application_name", "session_id", "message", "detail", "command", "format", "truncated" }, names);
        Assert.Equal(names, rows[1].EnumerateObject().Select(p => p.Name).ToArray());
        Assert.Equal(JsonValueKind.Null, rows[0].GetProperty("application_name").ValueKind);
        Assert.Equal("2026-10-22T06:10:00.000000Z", rows[0].GetProperty("ts").GetString());
        Assert.Equal("stderr", rows[0].GetProperty("format").GetString());
    }

    [Theory]
    [InlineData("CREATE OR REPLACE VIEW v AS SELECT 1", "CREATE VIEW")]
    [InlineData("  -- migration 42\n  grant select on t to r", "GRANT")]
    [InlineData("/* tool */ alter role r password 'x'", "ALTER ROLE")]
    [InlineData("DROP TABLE IF EXISTS t", "DROP TABLE")]
    [InlineData("CREATE UNIQUE INDEX CONCURRENTLY i ON t (a)", "CREATE INDEX")]
    [InlineData("CREATE MATERIALIZED VIEW m AS SELECT 1", "CREATE MATERIALIZED")]
    [InlineData("CREATE USER MAPPING FOR app SERVER s OPTIONS (password 'x')", "CREATE USER MAPPING")]
    [InlineData("ALTER DEFAULT PRIVILEGES IN SCHEMA s GRANT SELECT ON TABLES TO r", "ALTER DEFAULT PRIVILEGES")]
    [InlineData("CREATE USER app_reader", "CREATE USER")]
    [InlineData("", "")]
    public void The_command_is_the_first_keyword_and_the_object_kind(string sql, string command)
    {
        Assert.Equal(command, PgCommand.Of(sql));
    }

    [Theory]
    [InlineData("2026-10-22 06:10:00 UTC:192.0.2.10(1):a@d:[1]:LOG:  x", 1_792_649_400_000_000L)]
    [InlineData("2026-10-22 06:10:00.250 UTC:192.0.2.10(1):a@d:[1]:LOG:  x", 1_792_649_400_250_000L)]
    public void The_time_of_a_stderr_line_is_read_from_its_prefix(string line, long micros)
    {
        Assert.Equal(micros, PgWindowReader.StderrTime(line));
    }

    [Theory]
    [InlineData("\tcontinuation of a statement")]
    [InlineData("2026 was a year, UTC: in a statement's text")]
    [InlineData("")]
    public void A_continuation_line_has_no_time(string line)
    {
        Assert.Null(PgWindowReader.StderrTime(line));
    }
}
