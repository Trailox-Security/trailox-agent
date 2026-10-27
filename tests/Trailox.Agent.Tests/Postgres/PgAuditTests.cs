using System.Buffers;
using System.Text;
using System.Text.Json;
using Trailox.Agent.Engines.MongoDb;
using Trailox.Agent.Engines.Postgres;

namespace Trailox.Agent.Tests.Postgres;

/// <summary>pgaudit's lines: read as CSV, joined into statements, grouped per minute by shape, and never a value shipped.</summary>
public class PgAuditTests
{
    private const long Start = 1_792_649_400_000_000; // 2026-10-22 06:10:00 UTC
    private const long End = Start + 600_000_000;

    private static PgLogRecord A(long ts, string audit, int pid = 7, PgLogFormat format = PgLogFormat.Stderr, string? app = null) => new()
    {
        TsMicros = ts,
        Pid = pid,
        User = "analyst",
        Database = "appdb",
        RemoteHost = "192.0.2.10",
        RemotePort = "54170",
        Level = "LOG",
        Message = "AUDIT: " + audit,
        Format = format,
        ApplicationName = format == PgLogFormat.Stderr ? null : app ?? "",
    };

    private static List<JsonElement> Run(bool shipText, params PgLogRecord[] records)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var reader = new PgWindowReader("db1", Start, End, shipText, new RowSink(buffer));
        foreach (var r in records)
        {
            reader.Add(r);
        }
        reader.WriteCounts();
        return Encoding.UTF8.GetString(buffer.WrittenSpan).Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => JsonDocument.Parse(l).RootElement.Clone()).ToList();
    }

    private static JsonElement Counts(List<JsonElement> rows) => JsonDocument.Parse(rows[^1].GetProperty("message").GetString()!).RootElement;

    private static List<JsonElement> AuditRows(List<JsonElement> rows) =>
        rows.Where(r => r.GetProperty("audit_class").GetString()!.Length > 0).ToList();

    private static string[] Objects(JsonElement row) => row.GetProperty("objects").EnumerateArray().Select(o => o.GetString()!).ToArray();

    [Fact]
    public void A_session_line_reads_as_its_fields_and_never_its_parameter()
    {
        var line = PgAuditLine.Parse("AUDIT: SESSION,3,1,READ,SELECT,TABLE,public.cards,SELECT number FROM cards WHERE id = $1,4111111111111111")!;

        Assert.Equal(("SESSION", 3L, 1L, "READ", "SELECT", "TABLE", "public.cards"),
            (line.AuditType, line.StatementId, line.SubstatementId, line.Class, line.Command, line.ObjectType, line.ObjectName));
        Assert.Equal("SELECT number FROM cards WHERE id = $1", line.Statement);
        Assert.False(line.Truncated);
        Assert.DoesNotContain("4111", line.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void A_quoted_statement_keeps_its_commas_quotes_and_line_breaks()
    {
        var line = PgAuditLine.Parse("AUDIT: OBJECT,1,1,WRITE,INSERT,TABLE,public.t,\"INSERT INTO t (a, b)\n\tVALUES ('x', \"\"y\"\")\",<not logged>")!;

        Assert.Equal("INSERT INTO t (a, b)\n\tVALUES ('x', \"y\")", line.Statement);
        Assert.Equal("OBJECT", line.AuditType);
    }

    [Fact]
    public void A_cut_statement_is_kept_as_far_as_it_goes_and_marked()
    {
        var line = PgAuditLine.Parse("AUDIT: SESSION,1,1,READ,SELECT,,,\"SELECT a, b FROM t WHERE c = 'x")!;

        Assert.True(line.Truncated);
        Assert.Equal("SELECT a, b FROM t WHERE c = 'x", line.Statement);
    }

    [Theory]
    [InlineData("statement: SELECT 1")]
    [InlineData("AUDIT: SESSION,x,1,READ,SELECT,,,SELECT 1,<none>")]
    [InlineData("AUDIT: SESSION,1,1")]
    public void A_line_that_is_not_whole_pgaudit_is_not_read(string message) => Assert.Null(PgAuditLine.Parse(message));

    [Theory]
    [InlineData("SELECT * FROM t WHERE id = 42 AND name = 'bob'", "SELECT * FROM t WHERE id = ? AND name = ?")]
    [InlineData("SELECT * FROM t WHERE id IN (1, 2, 3)", "SELECT * FROM t WHERE id IN (?)")]
    [InlineData("SELECT * FROM t WHERE id IN (1,2,3)", "SELECT * FROM t WHERE id IN (?)")]
    [InlineData("UPDATE t SET a = $1, b = $2 WHERE id = $3", "UPDATE t SET a = $1, b = $2 WHERE id = $3")]
    [InlineData("SELECT 1.5e10, .5, -7", "SELECT ?, -?")]
    [InlineData("SELECT E'it\\'s', $$x$$, $tag$y$tag$, U&'d\\0061t'", "SELECT ?")]
    [InlineData("SELECT a  -- note 'x'\n  FROM /* c */ t1", "SELECT a FROM t1")]
    [InlineData("SELECT \"Col 1\" FROM \"My Table\"", "SELECT \"Col 1\" FROM \"My Table\"")]
    [InlineData("SELECT * FROM t WHERE p = 'never ends", "SELECT * FROM t WHERE p = ?")]
    public void A_shape_has_no_value(string sql, string shape) => Assert.Equal(shape, PgStatementShape.Of(sql));

    [Fact]
    public void Statements_of_one_shape_in_one_minute_are_one_row_with_their_count()
    {
        var rows = Run(true,
            A(Start + 1_000_000, "SESSION,1,1,READ,SELECT,TABLE,public.orders,SELECT * FROM orders WHERE id = 1,<not logged>", pid: 10, format: PgLogFormat.Csv, app: "billing"),
            A(Start + 2_000_000, "SESSION,1,1,READ,SELECT,TABLE,public.orders,SELECT * FROM orders WHERE id = 2,<not logged>", pid: 11, format: PgLogFormat.Csv, app: "billing"),
            A(Start + 3_000_000, "SESSION,2,1,READ,SELECT,TABLE,public.orders,SELECT * FROM orders WHERE id = 3,<not logged>", pid: 10, format: PgLogFormat.Csv, app: "billing"));

        var audit = Assert.Single(AuditRows(rows));
        Assert.Equal(3, audit.GetProperty("event_count").GetInt64());
        Assert.Equal("SELECT * FROM orders WHERE id = ?", audit.GetProperty("message").GetString());
        Assert.Equal("READ", audit.GetProperty("audit_class").GetString());
        Assert.Equal("SELECT", audit.GetProperty("command").GetString());
        Assert.Equal(new[] { "public.orders" }, Objects(audit));
        Assert.Equal("billing", audit.GetProperty("application_name").GetString());
        Assert.Equal("2026-10-22T06:10:01.000000Z", audit.GetProperty("ts").GetString());
        var counts = Counts(rows);
        Assert.Equal(3, counts.GetProperty("audit_lines").GetInt64());
        Assert.Equal(1, counts.GetProperty("audit_rows").GetInt64());
    }

    /// <summary>🔴 On stderr only a session's connection line names its application, so a group never holds two sessions.</summary>
    [Fact]
    public void On_stderr_each_pid_is_its_own_row()
    {
        var rows = Run(true,
            A(Start + 1_000_000, "SESSION,1,1,READ,SELECT,TABLE,public.orders,SELECT * FROM orders WHERE id = 1,<not logged>", pid: 10),
            A(Start + 2_000_000, "SESSION,1,1,READ,SELECT,TABLE,public.orders,SELECT * FROM orders WHERE id = 2,<not logged>", pid: 11),
            A(Start + 3_000_000, "SESSION,2,1,READ,SELECT,TABLE,public.orders,SELECT * FROM orders WHERE id = 3,<not logged>", pid: 10));

        var audit = AuditRows(rows);
        Assert.Equal(new long[] { 2, 1 }, audit.Select(r => r.GetProperty("event_count").GetInt64()));
        Assert.Equal(new[] { 10, 11 }, audit.Select(r => r.GetProperty("pid").GetInt32()));
        Assert.All(audit, r => Assert.Equal(JsonValueKind.Null, r.GetProperty("application_name").ValueKind));
    }

    /// <summary>🔴 One statement, one line per relation, and a relation under both object and session audit twice.</summary>
    [Fact]
    public void A_statements_lines_are_one_statement_naming_each_relation_once()
    {
        const string join = "SELECT * FROM a JOIN b USING (id) JOIN c USING (id)";
        var rows = Run(true,
            A(Start, $"SESSION,5,1,READ,SELECT,TABLE,public.a,{join},<not logged>"),
            A(Start, $"OBJECT,5,1,READ,SELECT,TABLE,public.a,{join},<not logged>"),
            A(Start, "SESSION,1,1,READ,SELECT,TABLE,public.other,SELECT 1 FROM other,<not logged>", pid: 99),
            A(Start, $"SESSION,5,1,READ,SELECT,TABLE,public.c,{join},<not logged>"),
            A(Start, $"SESSION,5,1,READ,SELECT,TABLE,public.b,{join},<not logged>"));

        var audit = AuditRows(rows);
        var joined = Assert.Single(audit, r => r.GetProperty("pid").GetInt32() == 7);
        Assert.Equal(new[] { "public.a", "public.b", "public.c" }, Objects(joined));
        Assert.Equal(1, joined.GetProperty("event_count").GetInt64());
        Assert.Equal(5, Counts(rows).GetProperty("audit_lines").GetInt64());
    }

    [Fact]
    public void Each_minute_is_its_own_row()
    {
        var rows = Run(true,
            A(Start + 1_000_000, "SESSION,1,1,WRITE,UPDATE,TABLE,public.t,UPDATE t SET a = 1,<not logged>", format: PgLogFormat.Json),
            A(Start + 61_000_000, "SESSION,2,1,WRITE,UPDATE,TABLE,public.t,UPDATE t SET a = 2,<not logged>", format: PgLogFormat.Json));

        Assert.Equal(new[] { "2026-10-22T06:10:01.000000Z", "2026-10-22T06:11:01.000000Z" },
            AuditRows(rows).Select(r => r.GetProperty("ts").GetString()));
    }

    /// <summary>DDL and role changes come from log_statement: shipped from pgaudit too, each would come twice.</summary>
    [Fact]
    public void Lines_of_other_classes_and_unreadable_lines_are_only_counted()
    {
        var rows = Run(true,
            A(Start, "SESSION,1,1,DDL,CREATE TABLE,,,CREATE TABLE t (a int),<not logged>"),
            A(Start, "SESSION,2,1,ROLE,GRANT,,,GRANT SELECT ON t TO bi,<not logged>"),
            A(Start, "SESSION,x,1,READ,SELECT,,,SELECT 1,<none>"));

        Assert.Empty(AuditRows(rows));
        var counted = Counts(rows).GetProperty("counted");
        Assert.Equal(2, counted.GetProperty("audit").GetInt64());
        Assert.Equal(1, counted.GetProperty("audit_unreadable").GetInt64());
    }

    [Fact]
    public void No_value_and_no_parameter_ever_ships()
    {
        var rows = Run(true,
            A(Start, "SESSION,1,1,WRITE,INSERT,TABLE,public.users,\"INSERT INTO users (email, pw) VALUES ('a@example.com', 'Hunter2!')\",\"'a@example.com', 'S3cret'\""),
            A(Start, "SESSION,2,1,READ,SELECT,TABLE,public.cards,SELECT * FROM cards WHERE number = 4111111111111111,<not logged>"));

        var text = string.Join('\n', rows.Select(r => r.GetRawText()));
        foreach (var value in new[] { "example.com", "Hunter2", "S3cret", "4111" })
        {
            Assert.DoesNotContain(value, text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Without_statement_text_a_group_ships_no_text()
    {
        var rows = Run(false, A(Start, "SESSION,1,1,READ,SELECT,TABLE,public.t,SELECT * FROM t,<not logged>"));

        var audit = Assert.Single(AuditRows(rows));
        Assert.Equal("", audit.GetProperty("message").GetString());
        Assert.Equal("SELECT", audit.GetProperty("command").GetString());
    }

    [Fact]
    public void Every_row_carries_the_audit_columns()
    {
        var rows = Run(true,
            new PgLogRecord { TsMicros = Start, Level = "LOG", Message = "connection received: host=192.0.2.10 port=54170" });

        Assert.All(rows, r =>
        {
            Assert.Equal("", r.GetProperty("audit_class").GetString());
            Assert.Empty(Objects(r));
            Assert.Equal(1, r.GetProperty("event_count").GetInt64());
        });
    }

    // ------------------------------------------------------------------ review fixes ----

    /// <summary>🔴 INSERT ... SELECT reads one relation and writes another: one row each, so the read relation is never written.</summary>
    [Fact]
    public void A_statement_that_reads_and_writes_is_a_row_per_class()
    {
        const string sql = "INSERT INTO t SELECT * FROM s";
        var rows = Run(true,
            A(Start, $"SESSION,4,1,WRITE,INSERT,TABLE,public.t,{sql},<not logged>"),
            A(Start, $"SESSION,4,1,READ,SELECT,TABLE,public.s,{sql},<not logged>"));

        var audit = AuditRows(rows).OrderBy(r => r.GetProperty("audit_class").GetString()).ToList();
        Assert.Equal(new[] { "READ SELECT public.s", "WRITE INSERT public.t" }, audit.Select(r =>
            $"{r.GetProperty("audit_class").GetString()} {r.GetProperty("command").GetString()} {string.Join(",", Objects(r))}"));
    }

    /// <summary>🔴 pgaudit names a COPY's lines by the rights it uses; the statement says which way the data went.</summary>
    [Theory]
    [InlineData("READ,SELECT,TABLE,public.cards,COPY cards TO STDOUT", "COPY TO")]
    [InlineData("READ,SELECT,TABLE,public.a,COPY (SELECT * FROM a) TO STDOUT", "COPY TO")]
    [InlineData("WRITE,INSERT,TABLE,public.t,COPY t FROM STDIN", "COPY FROM")]
    [InlineData("READ,SELECT,TABLE,public.t,SELECT * FROM t", "SELECT")]
    public void A_copy_is_named_by_its_direction(string fields, string command)
    {
        var rows = Run(true, A(Start, $"SESSION,1,1,{fields},<not logged>"));

        Assert.Equal(command, Assert.Single(AuditRows(rows)).GetProperty("command").GetString());
    }

    /// <summary>🔴 Only a quoted name can spell TO or FROM, and it is the table's: COPY "from" TO is still an export.</summary>
    [Theory]
    [InlineData("""COPY "from" TO STDOUT""", "TO")]
    [InlineData("""COPY "to" FROM STDIN""", "FROM")]
    [InlineData("""COPY s."from" (a, "to") TO STDOUT""", "TO")]
    [InlineData("""COPY U&"from" TO STDOUT""", "TO")]
    public void A_quoted_name_is_never_a_copy_direction(string sql, string direction) =>
        Assert.Equal(direction, PgStatementShape.CopyDirection(sql));

    /// <summary>🔴 Anyone can RAISE LOG 'AUDIT: ...' from a function; pgaudit's own lines carry no CONTEXT.</summary>
    [Fact]
    public void An_audit_line_written_from_a_function_is_never_shipped()
    {
        var forged = A(Start, "SESSION,1,1,READ,SELECT,TABLE,public.t,SELECT 1,<not logged>");
        forged.HasContext = true;

        var rows = Run(true, forged);

        Assert.Empty(AuditRows(rows));
        Assert.Equal(1, Counts(rows).GetProperty("counted").GetProperty("audit_context").GetInt64());
    }

    /// <summary>🔴 One user's many shapes cannot push another user's statements out of their own row.</summary>
    [Fact]
    public void Past_its_shape_limit_a_users_statements_keep_who_and_what_but_not_their_shape()
    {
        var flood = Enumerable.Range(0, PgAuditGroups.UserShapeCap + 5).Select(i =>
            A(Start + 1_000_000, $"SESSION,{i + 1},1,READ,SELECT,TABLE,public.t,SELECT c{i} FROM t,<not logged>", pid: 50, format: PgLogFormat.Csv, app: "x")).ToList();
        flood.Add(A(Start + 2_000_000, "SESSION,1,1,READ,SELECT,TABLE,public.cards,SELECT * FROM cards,<not logged>", pid: 60, format: PgLogFormat.Csv, app: "x"));
        flood[^1].User = "bob";

        var rows = Run(true, flood.ToArray());

        var audit = AuditRows(rows);
        var many = Assert.Single(audit, r => r.GetProperty("message").GetString() == PgAuditGroups.ManyShapes);
        Assert.Equal(("analyst", 5L), (many.GetProperty("user").GetString(), many.GetProperty("event_count").GetInt64()));
        Assert.Equal(new[] { "public.t" }, Objects(many));
        Assert.Equal("SELECT * FROM cards", Assert.Single(audit, r => r.GetProperty("user").GetString() == "bob").GetProperty("message").GetString());
        Assert.Equal(5, Counts(rows).GetProperty("audit_shapeless").GetInt64());
    }

    /// <summary>The overflow row is a count with nothing else: not cut text, never a user.</summary>
    [Fact]
    public void A_minutes_overflow_row_is_a_count_only()
    {
        var lines = Enumerable.Range(0, PgAuditGroups.GroupCap + 3).Select(i =>
        {
            var r = A(Start + 1_000_000, "SESSION,1,1,READ,SELECT,TABLE,public.t,SELECT * FROM t,<not logged>", pid: i + 1, format: PgLogFormat.Csv, app: "x");
            r.User = $"u{i}";
            return r;
        }).ToArray();

        var rows = Run(true, lines);

        var other = Assert.Single(rows, r => r.GetProperty("audit_class").GetString() == MinuteAggregator.Other);
        Assert.Equal((3L, false, ""), (other.GetProperty("event_count").GetInt64(), other.GetProperty("truncated").GetBoolean(),
            other.GetProperty("user").GetString()));
    }

    /// <summary>Two groups that differ in their shape alone stay two rows with two hashes, with or without the text.</summary>
    [Fact]
    public void The_shape_hash_ships_with_or_without_the_text()
    {
        var rows = Run(false,
            A(Start, "SESSION,1,1,READ,SELECT,TABLE,public.t,SELECT a FROM t,<not logged>"),
            A(Start, "SESSION,2,1,READ,SELECT,TABLE,public.t,SELECT b FROM t,<not logged>"));

        var hashes = AuditRows(rows).Select(r => r.GetProperty("detail").GetString()!).ToList();
        Assert.Equal(2, hashes.Distinct().Count());
        Assert.All(hashes, h => Assert.Matches("^[0-9a-f]{16}$", h));
        Assert.All(AuditRows(rows), r => Assert.Equal("", r.GetProperty("message").GetString()));
    }

    /// <summary>A window read again ships the same rows: grouping depends on the lines, never on the order of a dictionary.</summary>
    [Fact]
    public void A_window_read_again_ships_the_same_rows()
    {
        PgLogRecord[] Lines() => Enumerable.Range(0, 300).Select(i =>
            A(Start + i * 2_000_000L, $"SESSION,{i + 1},1,{(i % 3 == 0 ? "WRITE,UPDATE" : "READ,SELECT")},TABLE,public.t{i % 7},SELECT * FROM t{i % 7} WHERE id = {i},<not logged>",
                pid: 10 + i % 4)).ToArray();

        Assert.Equal(Run(true, Lines()).Select(r => r.GetRawText()), Run(true, Lines()).Select(r => r.GetRawText()));
    }

    /// <summary>Steady traffic over minutes: every statement counted once, in its own minute.</summary>
    [Fact]
    public void Steady_traffic_is_counted_once_per_statement_across_minutes()
    {
        var lines = Enumerable.Range(0, 30).Select(i =>
            A(Start + i * 10_000_000L, $"SESSION,{i + 1},1,READ,SELECT,TABLE,public.t,SELECT * FROM t WHERE id = {i},<not logged>",
                format: PgLogFormat.Csv, app: "api")).ToArray();

        var audit = AuditRows(Run(true, lines));

        Assert.Equal(new long[] { 6, 6, 6, 6, 6 }, audit.Select(r => r.GetProperty("event_count").GetInt64()));
    }

    [Theory]
    [InlineData("SELECT * FROM t WHERE id = 0xDEADBEEF OR m = 0o777 OR b = 0b1011", "SELECT * FROM t WHERE id = ? OR m = ? OR b = ?")]
    [InlineData("SELECT 'it\\'s secret' FROM t", "SELECT ?")]
    [InlineData("""INSERT INTO t VALUES ('C:\', 'john secret')""", "INSERT INTO t VALUES (?")]
    [InlineData("""SELECT * FROM t WHERE p = 'C:\temp' AND q = 'secret'""", "SELECT * FROM t WHERE p = ? AND q = ?")]
    [InlineData("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')", "INSERT INTO t VALUES (?)")]
    public void A_shape_keeps_no_value_in_any_form(string sql, string shape)
    {
        Assert.Equal(shape, PgStatementShape.Of(sql));
        Assert.DoesNotContain("secret", PgStatementShape.Of(sql), StringComparison.Ordinal);
    }

    /// <summary>pgaudit.log_statement off writes seven fields: the line is still a statement, with no text.</summary>
    [Fact]
    public void A_line_without_its_statement_is_read()
    {
        var line = PgAuditLine.Parse("AUDIT: SESSION,2,1,READ,SELECT,TABLE,public.t")!;

        Assert.Equal(("READ", "public.t", ""), (line.Class, line.ObjectName, line.Statement));
    }
}
