using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Trailox.Agent.Engines.Postgres;
using Trailox.Agent.Engines.MongoDb;

namespace Trailox.Agent.Tests.Postgres;

/// <summary>
/// A prefix that does not name the user on every line: each record takes its user from its session's sign-in line, read in
/// the window, or before it by a look-back.
/// </summary>
public sealed class PgSessionMapTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("pg-sessions").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static long Micros(string utc) =>
        (DateTime.SpecifyKind(DateTime.Parse(utc, CultureInfo.InvariantCulture), DateTimeKind.Utc) - DateTime.UnixEpoch).Ticks / 10;

    private PgLogDirectory AzureFile(int probeBytes = PgLogDirectory.ProbeBytes)
    {
        File.Copy(PgFixtures.AzureDownload, Path.Combine(_dir, "postgresql-2026-01-15_100000.log"));
        return new PgLogDirectory(_dir, probeBytes);
    }

    private static async Task<List<JsonElement>> ReadAsync(PgLogDirectory directory, string prefix, string from, string to,
        PgSessionMap? sessions = null, bool azureFile = true)
    {
        var format = PgLineFormat.Of(prefix);
        sessions ??= new PgSessionMap(format);
        var buffer = new ArrayBufferWriter<byte>();
        var reader = new PgWindowReader("db1", Micros(from), Micros(to), true, new RowSink(buffer), prefix: format, sessions: sessions,
            lines: directory, azureFile: azureFile);
        await reader.ReadAsync(directory, CancellationToken.None);
        reader.WriteCounts();
        return Encoding.UTF8.GetString(buffer.WrittenSpan).Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => JsonDocument.Parse(l).RootElement.Clone()).ToList();
    }

    private static string S(JsonElement row, string column) => row.GetProperty(column).ValueKind == JsonValueKind.Null ? "<null>" : row.GetProperty(column).ToString();

    private static JsonElement Counts(List<JsonElement> rows) => JsonDocument.Parse(rows.Last().GetProperty("message").GetString()!).RootElement.Clone();

    private static int Counted(JsonElement counts, string key) =>
        counts.GetProperty("counted").TryGetProperty(key, out var v) ? v.GetInt32() : 0;

    /// <summary>
    /// 🔴 The collector of a window needs a user on every event. With Azure's prefix no line names one: each takes its
    ///    session's, from the session's connection authorized line.
    /// </summary>
    [Fact]
    public async Task Every_row_of_azures_download_has_its_user_from_its_sessions_sign_in()
    {
        var rows = await ReadAsync(AzureFile(), "%t-%c-", "2026-01-15 10:57:00", "2026-01-15 10:58:00");

        var shipped = rows.SkipLast(1).ToList();
        Assert.NotEmpty(shipped);
        Assert.All(shipped, r => Assert.NotEqual("", S(r, "user")));
        Assert.Equal(0, Counted(Counts(rows), "no_user"));

        var ddl = shipped.Where(r => S(r, "message").StartsWith("statement: CREATE ROLE", StringComparison.Ordinal)).ToList();
        Assert.Equal(2, ddl.Count);
        Assert.All(ddl, r => Assert.Equal(
            ("dbadmin", "postgres", "192.0.2.10", "52128", "setup-script", "6968c80d.423", "1059", "stderr"),
            (S(r, "user"), S(r, "database"), S(r, "remote_host"), S(r, "remote_port"), S(r, "application_name"), S(r, "session_id"),
                S(r, "pid"), S(r, "format"))));

        var received = shipped.First(r => S(r, "message") == "connection received: host=192.0.2.10 port=52128");
        Assert.Equal(("[unknown]", "[unknown]", "192.0.2.10", "52128"), (S(received, "user"), S(received, "database"), S(received, "remote_host"),
            S(received, "remote_port")));

        var refused = shipped.Where(r => S(r, "level") == "FATAL" && S(r, "message").StartsWith("password authentication failed", StringComparison.Ordinal))
            .Select(r => (S(r, "user"), S(r, "remote_host"), S(r, "remote_port"))).ToList();
        Assert.Equal(new[] { ("app_user", "192.0.2.10", "58871"), ("no_such_user", "192.0.2.10", "58872") }, refused);

        var audit = shipped.Where(r => S(r, "audit_class") is "READ" or "WRITE").ToList();
        Assert.Equal(2, audit.Count);
        Assert.All(audit, r => Assert.Equal(("app_user", "postgres", "6968c811.425"), (S(r, "user"), S(r, "database"), S(r, "session_id"))));

        // Azure's own sessions are its superuser's.
        Assert.Contains(shipped, r => S(r, "message").StartsWith("execute _p3: create table if not exists public.lsnmover", StringComparison.Ordinal)
            && S(r, "user") == "azuresu" && S(r, "database") == "azure_maintenance");
        Assert.All(shipped.Where(r => S(r, "message").StartsWith("disconnection:", StringComparison.Ordinal)), r => Assert.NotEqual("", S(r, "user")));
    }

    /// <summary>
    /// A session that signed in long before the window: its id says when it started, so a look-back reads the minute
    /// after that and finds its sign-in.
    /// </summary>
    [Fact]
    public async Task A_session_that_signed_in_before_the_read_is_found_by_a_look_back()
    {
        // A small step reads the window from just before it, so the session's 10:55:46 sign-in is not read with it.
        var rows = await ReadAsync(AzureFile(probeBytes: 512), "%t-%c-", "2026-01-15 10:57:30", "2026-01-15 10:57:45");

        var lsnmover = rows.SkipLast(1).Where(r => S(r, "session_id") == "6968c7b2.3fa").ToList();
        Assert.NotEmpty(lsnmover);
        Assert.All(lsnmover, r => Assert.Equal(("azuresu", "azure_maintenance", "127.0.0.1", "57526"),
            (S(r, "user"), S(r, "database"), S(r, "remote_host"), S(r, "remote_port"))));
        var counts = Counts(rows);
        Assert.InRange(counts.GetProperty("look_backs").GetInt32(), 1, PgWindowReader.MaxLookBacks);
        Assert.Equal(0, Counted(counts, "no_user"));
    }

    /// <summary>The sessions live as long as the source: a window after another needs no look-back for a session the first read.</summary>
    [Fact]
    public async Task A_session_read_in_one_window_is_known_in_the_next()
    {
        var directory = AzureFile(probeBytes: 512);
        var sessions = new PgSessionMap(PgLineFormat.Of("%t-%c-"));
        await ReadAsync(directory, "%t-%c-", "2026-01-15 10:55:40", "2026-01-15 10:57:30", sessions);

        var rows = await ReadAsync(directory, "%t-%c-", "2026-01-15 10:57:30", "2026-01-15 10:57:45", sessions);

        Assert.Equal(0, Counts(rows).GetProperty("look_backs").GetInt32());
        Assert.All(rows.SkipLast(1).Where(r => S(r, "session_id") == "6968c7b2.3fa"), r => Assert.Equal("azuresu", S(r, "user")));
    }

    /// <summary>
    /// 🔴 A session whose sign-in is gone (the server's own processes, a session older than the log) is looked for once,
    ///    and its rows ship without a user, counted.
    /// </summary>
    [Fact]
    public async Task A_session_whose_sign_in_is_not_in_the_log_is_counted_and_looked_for_once()
    {
        // A session that started at 09:00, an hour before the file.
        var session = $"{Micros("2026-01-15 09:00:00") / 1_000_000:x}.10";
        var lines = new[]
        {
            $"2026-01-15 10:00:01 UTC-{session}-LOG:  statement: DROP TABLE t",
            $"2026-01-15 10:00:02 UTC-{session}-LOG:  statement: DROP TABLE u",
        };
        await File.WriteAllLinesAsync(Path.Combine(_dir, "postgresql-2026-01-15_100000.log"), lines);

        var rows = await ReadAsync(new PgLogDirectory(_dir), "%t-%c-", "2026-01-15 10:00:00", "2026-01-15 10:01:00");

        Assert.All(rows.SkipLast(1), r => Assert.Equal("", S(r, "user")));
        var counts = Counts(rows);
        Assert.Equal(2, Counted(counts, "no_user"));
        Assert.Equal(1, counts.GetProperty("look_backs").GetInt32());
    }

    private const string PgDefault = "%m [%p] ";

    private static string Vm(string time, int pid, string level, string text) => $"2026-01-15 {time} UTC [{pid}] {level}:  {text}";

    /// <summary>
    /// PostgreSQL's own default prefix names the process only. 🔴 A process id is reused: a record takes the newest
    /// sign-in of its process before it, and none after that process disconnected.
    /// </summary>
    [Fact]
    public async Task With_a_process_only_a_record_takes_its_processes_newest_sign_in()
    {
        await File.WriteAllLinesAsync(Path.Combine(_dir, "postgresql-2026-01-15_100000.log"), new[]
        {
            Vm("10:00:01.000", 100, "LOG", "connection received: host=192.0.2.20 port=40001"),
            Vm("10:00:01.010", 100, "LOG", "connection authorized: user=alice database=shop application_name=psql"),
            Vm("10:00:02.000", 100, "LOG", "statement: CREATE TABLE a (x int)"),
            Vm("10:00:03.000", 100, "LOG", "disconnection: session time: 0:00:02.000 user=alice database=shop host=192.0.2.20 port=40001"),
            Vm("10:00:04.000", 100, "LOG", "statement: CREATE TABLE orphan (x int)"),
            Vm("10:00:05.000", 100, "LOG", "connection received: host=192.0.2.21 port=40002"),
            Vm("10:00:05.010", 100, "LOG", "connection authorized: user=bob database=crm"),
            Vm("10:00:06.000", 100, "LOG", "statement: CREATE TABLE b (x int)"),
            Vm("10:00:07.000", 101, "FATAL", "password authentication failed for user \"mallory\""),
        });

        var rows = (await ReadAsync(new PgLogDirectory(_dir), PgDefault, "2026-01-15 10:00:00", "2026-01-15 10:01:00", azureFile: false))
            .SkipLast(1).ToList();
        JsonElement Row(string start) => rows.Single(r => S(r, "message").StartsWith(start, StringComparison.Ordinal));

        var a = Row("statement: CREATE TABLE a ");
        Assert.Equal(("alice", "shop", "192.0.2.20", "40001", "psql"),
            (S(a, "user"), S(a, "database"), S(a, "remote_host"), S(a, "remote_port"), S(a, "application_name")));
        Assert.Equal("", S(Row("statement: CREATE TABLE orphan"), "user"));
        var b = Row("statement: CREATE TABLE b ");
        Assert.Equal(("bob", "crm", "192.0.2.21", ""), (S(b, "user"), S(b, "database"), S(b, "remote_host"), S(b, "application_name")));
        Assert.Equal("mallory", S(Row("password authentication failed"), "user"));
    }

    /// <summary>A process's sign-in before the read is found by one look-back over the hours before it.</summary>
    [Fact]
    public async Task With_a_process_only_a_look_back_reads_the_hours_before()
    {
        var lines = new List<string>
        {
            Vm("08:30:00.000", 200, "LOG", "connection received: host=192.0.2.30 port=40003"),
            Vm("08:30:00.010", 200, "LOG", "connection authorized: user=carol database=shop"),
        };
        for (var minute = 31; minute < 60; minute++)
        {
            lines.Add(Vm($"08:{minute}:00.000", 31, "LOG", "checkpoint starting: time " + new string('x', 200)));
        }
        lines.Add(Vm("09:59:00.000", 200, "LOG", "statement: DROP TABLE old"));
        await File.WriteAllLinesAsync(Path.Combine(_dir, "postgresql-2026-01-15_080000.log"), lines);

        var rows = await ReadAsync(new PgLogDirectory(_dir, probeBytes: 512), PgDefault, "2026-01-15 09:58:00", "2026-01-15 10:00:00", azureFile: false);

        var drop = rows.Single(r => S(r, "message") == "statement: DROP TABLE old");
        Assert.Equal(("carol", "shop"), (S(drop, "user"), S(drop, "database")));
        Assert.Equal(1, Counts(rows).GetProperty("look_backs").GetInt32());
    }

    /// <summary>A prefix that names no user, no session and no process: a statement cannot be told whose it is, and counts as unread.</summary>
    [Fact]
    public async Task A_prefix_with_no_user_session_or_process_counts_statements_as_unread()
    {
        await File.WriteAllLinesAsync(Path.Combine(_dir, "postgresql-2026-01-15_100000.log"), new[]
        {
            "2026-01-15 10:00:01.000 UTC LOG:  connection authorized: user=alice database=shop",
            "2026-01-15 10:00:02.000 UTC LOG:  statement: CREATE TABLE a (x int)",
            "2026-01-15 10:00:03.000 UTC FATAL:  password authentication failed for user \"mallory\"",
        });

        var rows = await ReadAsync(new PgLogDirectory(_dir), "%m ", "2026-01-15 10:00:00", "2026-01-15 10:01:00", azureFile: false);

        Assert.Equal(new[] { ("connection authorized: user=alice database=shop", "alice"), ("password authentication failed for user \"mallory\"", "mallory") },
            rows.SkipLast(1).Select(r => (S(r, "message"), S(r, "user"))));
        Assert.Equal(1, Counts(rows).GetProperty("unreadable").GetInt32());
    }

    /// <summary>
    /// 🔴 A refused client chooses the user name it asks for, and Debian's prefix writes it before the database: a name
    ///    holding a prefix's rest, a level and a sign-in read from the left as that sign-in. The real level follows in the
    ///    text, so the line is counted forged, and neither shipped nor learned from.
    /// </summary>
    [Fact]
    public async Task A_sign_in_forged_inside_a_user_name_is_counted_and_not_read()
    {
        await File.WriteAllLinesAsync(Path.Combine(_dir, "postgresql-2026-01-15_100000.log"), new[]
        {
            "2026-01-15 10:00:01.000 UTC [300] a@b LOG:  connection authorized: user=admin database=prod@postgres FATAL:  "
                + "password authentication failed for user \"a@b LOG:  connection authorized: user=admin database=prod\"",
            "2026-01-15 10:00:02.000 UTC [301] alice@shop LOG:  connection authorized: user=alice database=shop",
        });

        var rows = await ReadAsync(new PgLogDirectory(_dir), "%m [%p] %q%u@%d ", "2026-01-15 10:00:00", "2026-01-15 10:01:00", azureFile: false);

        Assert.Equal(new[] { "alice" }, rows.SkipLast(1).Select(r => S(r, "user")));
        var counts = Counts(rows);
        Assert.Equal((1, 1), (Counted(counts, "forged"), counts.GetProperty("unreadable").GetInt32()));
    }

    /// <summary>
    /// 🔴 Both ways round: a DDL statement forged in a refused client's user name is caught by the refusal that ends its
    ///    line, and a real one that quotes a level in its own text is read, so no one hides a statement that way.
    /// </summary>
    [Fact]
    public async Task A_statement_forged_in_a_user_name_is_caught_and_a_real_one_quoting_a_level_is_read()
    {
        await File.WriteAllLinesAsync(Path.Combine(_dir, "postgresql-2026-01-15_100000.log"), new[]
        {
            "2026-01-15 10:00:01.000 UTC [300] admin@prod LOG:  statement: DROP TABLE accounts@postgres FATAL:  "
                + "password authentication failed for user \"admin@prod LOG:  statement: DROP TABLE accounts\"",
            "2026-01-15 10:00:02.000 UTC [301] alice@shop LOG:  statement: CREATE TABLE notes AS SELECT 'x@y ERROR:  z' AS t",
        });

        var rows = await ReadAsync(new PgLogDirectory(_dir), "%m [%p] %q%u@%d ", "2026-01-15 10:00:00", "2026-01-15 10:01:00", azureFile: false);

        Assert.Equal(new[] { ("alice", "statement: CREATE TABLE notes AS SELECT 'x@y ERROR:  z' AS t") },
            rows.SkipLast(1).Select(r => (S(r, "user"), S(r, "message"))));
        Assert.Equal(1, Counted(Counts(rows), "forged"));
    }

    /// <summary>
    /// 🔴 Any refusal ends a refused client's line with the rest of the prefix and a FATAL, whatever its text: a forgery
    ///    ending in a refusal of no listed kind is caught too.
    /// </summary>
    [Fact]
    public async Task A_forgery_ending_in_any_refusal_is_caught()
    {
        await File.WriteAllLinesAsync(Path.Combine(_dir, "postgresql-2026-01-15_100000.log"), new[]
        {
            "2026-01-15 10:00:01.000 UTC [300] admin@prod LOG:  statement: DROP TABLE accounts@postgres FATAL:  sorry, too many clients already",
        });

        var rows = await ReadAsync(new PgLogDirectory(_dir), "%m [%p] %q%u@%d ", "2026-01-15 10:00:00", "2026-01-15 10:01:00", azureFile: false);

        Assert.Single(rows);
        Assert.Equal(1, Counted(Counts(rows), "forged"));
    }

    /// <summary>
    /// With the session before the application name, a forgery rewrites only the client's own lines, which keep its
    /// real user: they are read. A sign-in forged there is still caught.
    /// </summary>
    [Fact]
    public async Task With_the_session_before_the_application_name_a_forgery_keeps_its_real_user()
    {
        var session = $"{Micros("2026-01-15 10:00:00") / 1_000_000:x}.12c";
        await File.WriteAllLinesAsync(Path.Combine(_dir, "postgresql-2026-01-15_100000.log"), new[]
        {
            $"2026-01-15 10:00:00 UTC-{session}- LOG:  connection authorized: user=mallory database=shop application_name=psql",
            $"2026-01-15 10:00:01 UTC-{session}-x LOG:  statement: DROP TABLE accounts psql LOG:  statement: SELECT 1",
            $"2026-01-15 10:00:02 UTC-{session}-x LOG:  connection authorized: user=admin database=prod psql LOG:  statement: SELECT 2",
        });

        var rows = await ReadAsync(new PgLogDirectory(_dir), "%t-%c-%a ", "2026-01-15 10:00:00", "2026-01-15 10:01:00", azureFile: false);

        Assert.Equal(new[]
            {
                ("connection authorized: user=mallory database=shop application_name=psql", "mallory"),
                ("statement: DROP TABLE accounts psql LOG:  statement: SELECT 1", "mallory"),
            },
            rows.SkipLast(1).Select(r => (S(r, "message"), S(r, "user"))));
        Assert.Equal(1, Counted(Counts(rows), "forged"));
    }

    /// <summary>
    /// 🔴 With the user after the application name, a forgery can name another user: a level near the start of a
    ///    record's text is taken as forged. A level far into a long statement is the statement's own, and is read.
    /// </summary>
    [Fact]
    public async Task With_the_user_after_the_application_name_a_forgery_is_caught_near_the_start_only()
    {
        var longDdl = "statement: CREATE TABLE notes AS SELECT '" + new string('n', 600) + " ERROR:  kept' AS t";
        await File.WriteAllLinesAsync(Path.Combine(_dir, "postgresql-2026-01-15_100000.log"), new[]
        {
            "2026-01-15 10:00:01.000 UTC [300] x admin@prod LOG:  statement: DROP TABLE accounts psql mallory@shop LOG:  statement: SELECT 1",
            $"2026-01-15 10:00:02.000 UTC [301] psql alice@shop LOG:  {longDdl}",
        });

        var rows = await ReadAsync(new PgLogDirectory(_dir), "%m [%p] %a %u@%d ", "2026-01-15 10:00:00", "2026-01-15 10:01:00", azureFile: false);

        Assert.Equal(new[] { ("alice", longDdl) }, rows.SkipLast(1).Select(r => (S(r, "user"), S(r, "message"))));
        Assert.Equal(1, Counted(Counts(rows), "forged"));
    }

    [Fact]
    public void A_prefix_that_stores_no_session_never_fails_its_bound()
    {
        var sessions = new PgSessionMap(PgLineFormat.Of("%m "));
        for (var i = 0; i <= PgSessionMap.MaxSessions + 10; i++)
        {
            var r = new PgLogRecord { TsMicros = i, Level = "LOG", Message = "connection received: host=192.0.2.1 port=1", Format = PgLogFormat.Stderr };
            Assert.Equal(PgSessionMap.Outcome.Complete, sessions.Take(r));
        }
    }

    [Fact]
    public void Past_its_bound_the_map_keeps_the_newer_half()
    {
        var sessions = new PgSessionMap(PgLineFormat.Of("%t-%c-"));
        for (var i = 0; i <= PgSessionMap.MaxSessions; i++)
        {
            sessions.Take(new PgLogRecord { TsMicros = i * 1_000_000L, SessionId = $"{i:x}.1", Level = "LOG", Message = "connection received: host=h port=1", Format = PgLogFormat.Stderr });
            sessions.Take(new PgLogRecord { TsMicros = i * 1_000_000L + 1, SessionId = $"{i:x}.1", Level = "LOG", Message = "connection authorized: user=u" + i, Format = PgLogFormat.Stderr });
        }

        PgLogRecord Statement(int i) => new() { TsMicros = i * 1_000_000L + 2, SessionId = $"{i:x}.1", Level = "LOG", Message = "statement: DROP TABLE t", Format = PgLogFormat.Stderr };
        var newest = Statement(PgSessionMap.MaxSessions);
        var oldest = Statement(0);
        sessions.Take(newest);
        sessions.Take(oldest);

        Assert.Equal("u" + PgSessionMap.MaxSessions, newest.User);
        Assert.Equal("", oldest.User);
    }

    /// <summary>A daily file, or one named with no time, is read from where the window begins, not from its start.</summary>
    [Fact]
    public async Task A_file_that_starts_long_before_the_window_is_read_from_the_window()
    {
        var lines = Enumerable.Range(0, 2000).Select(i => Vm(TimeSpan.FromSeconds(i * 10).ToString(@"hh\:mm\:ss") + ".000", 31, "LOG",
            "checkpoint starting: time")).ToList();
        await File.WriteAllLinesAsync(Path.Combine(_dir, "postgresql-17-main.log"), lines);
        File.SetLastWriteTimeUtc(Path.Combine(_dir, "postgresql-17-main.log"), new DateTime(2026, 1, 15, 6, 0, 0, DateTimeKind.Utc));

        var rows = await ReadAsync(new PgLogDirectory(_dir, probeBytes: 1024), PgDefault, "2026-01-15 05:00:00", "2026-01-15 05:01:00", azureFile: false);

        var counts = Counts(rows);
        Assert.Equal(6, Counted(counts, "other"));
        Assert.InRange(counts.GetProperty("bytes").GetInt64(), 1, new FileInfo(Path.Combine(_dir, "postgresql-17-main.log")).Length / 4);
    }
}
