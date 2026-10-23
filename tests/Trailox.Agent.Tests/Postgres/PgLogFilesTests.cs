using System.Buffers;
using System.Text;
using System.Text.Json;
using Trailox.Agent.Engines.MongoDb;
using Trailox.Agent.Engines.Postgres;

namespace Trailox.Agent.Tests.Postgres;

/// <summary>
/// Which files hold a window, from their names and last writes, and the self-managed reader over a real directory
/// (PostgreSQL's default names, rotation inside a window, a record stamped before a rotation).
/// </summary>
public class PgLogFilesTests : IDisposable
{
    private static readonly DateTime Day = new(2026, 10, 22, 0, 0, 0, DateTimeKind.Utc);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "trailox-pg-" + Guid.NewGuid().ToString("N"));

    public PgLogFilesTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    private static long At(int hour, int minute, int second = 0) =>
        new DateTimeOffset(2026, 10, 22, hour, minute, second, TimeSpan.Zero).ToUnixTimeMilliseconds() * 1000;

    [Theory]
    [InlineData("postgresql-2026-10-22_061000.log", 6, 10)]
    [InlineData("postgresql-2026-10-22_061000.json", 6, 10)]
    [InlineData("postgresql-2026-10-22_061000.csv", 6, 10)]
    [InlineData("error/postgresql.log.2026-10-22-06", 6, 0)]
    [InlineData("error/postgresql.log.2026-10-22-0610", 6, 10)]
    [InlineData("error/postgresql.log.2026-10-22", 0, 0)]
    [InlineData("error/postgresql.log.2026-10-22-06.csv", 6, 0)]
    public void The_name_gives_the_start_of_the_file(string name, int hour, int minute)
    {
        Assert.Equal(At(hour, minute), PgLogFileNames.StartMicros(name));
    }

    [Theory]
    [InlineData("postgresql-Mon.log")]
    [InlineData("postgresql.log")]
    public void A_name_without_a_date_gives_no_start(string name)
    {
        Assert.Null(PgLogFileNames.StartMicros(name));
    }

    [Fact]
    public void A_window_takes_its_files_in_one_format_and_the_file_after_it()
    {
        var files = new[]
        {
            new PgLogFile("postgresql-2026-10-22_060000.json", At(6, 0), At(6, 9, 59), 10, PgLogFormat.Json),
            new PgLogFile("postgresql-2026-10-22_061000.json", At(6, 10), At(6, 19, 59), 10, PgLogFormat.Json),
            new PgLogFile("postgresql-2026-10-22_061000.log", At(6, 10), At(6, 19, 59), 10, PgLogFormat.Stderr),
            new PgLogFile("postgresql-2026-10-22_062000.json", At(6, 20), At(6, 29, 59), 10, PgLogFormat.Json),
            new PgLogFile("postgresql-2026-10-22_063000.json", At(6, 30), At(6, 39, 59), 10, PgLogFormat.Json),
        };

        var picked = PgLogFileNames.ForWindow(files, At(6, 10), At(6, 20), PgWindowReader.StopMarginMicros);

        Assert.Equal(new[] { "postgresql-2026-10-22_061000.json", "postgresql-2026-10-22_062000.json" }, picked.Select(f => f.Name));
    }

    [Fact]
    public void The_format_is_chosen_among_the_files_of_the_window()
    {
        // csvlog until an upgrade to 15 at noon, jsonlog after it.
        var files = new[]
        {
            new PgLogFile("postgresql-2026-10-22_000000.csv", At(0, 0), At(11, 59), 10, PgLogFormat.Csv),
            new PgLogFile("postgresql-2026-10-22_120000.json", At(12, 0), At(13, 0), 10, PgLogFormat.Json),
        };

        Assert.Equal(new[] { "postgresql-2026-10-22_000000.csv" }, PgLogFileNames.ForWindow(files, At(6, 10), At(6, 20), 0).Select(f => f.Name));
        Assert.Equal(new[] { "postgresql-2026-10-22_120000.json" }, PgLogFileNames.ForWindow(files, At(12, 10), At(12, 20), 0).Select(f => f.Name));
    }

    [Fact]
    public void A_name_without_a_date_is_kept_while_it_was_written_after_the_window_started()
    {
        var files = new[]
        {
            new PgLogFile("postgresql-Wed.log", null, At(6, 15), 10, PgLogFormat.Stderr),
            new PgLogFile("postgresql-Tue.log", null, At(5, 0), 10, PgLogFormat.Stderr),
        };

        Assert.Equal(new[] { "postgresql-Wed.log" }, PgLogFileNames.ForWindow(files, At(6, 10), At(6, 20), 0).Select(f => f.Name));
    }

    private static string Json(DateTime at, int pid, string severity, string message, string? app = "psql") =>
        JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["timestamp"] = at.ToString("yyyy-MM-dd HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture) + " UTC",
            ["user"] = "app",
            ["dbname"] = "appdb",
            ["pid"] = pid,
            ["remote_host"] = "192.0.2.10",
            ["remote_port"] = 50000 + pid,
            ["error_severity"] = severity,
            ["message"] = message,
            ["application_name"] = app,
        }.Where(kv => kv.Value != null).ToDictionary(kv => kv.Key, kv => kv.Value));

    private void Write(string name, DateTime lastWrite, params string[] lines)
    {
        var path = Path.Combine(_dir, name);
        File.WriteAllText(path, string.Join('\n', lines) + "\n");
        File.SetLastWriteTimeUtc(path, lastWrite);
    }

    private static List<JsonElement> Rows(ArrayBufferWriter<byte> buffer) =>
        Encoding.UTF8.GetString(buffer.WrittenSpan).Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => JsonDocument.Parse(l).RootElement.Clone()).ToList();

    [Fact]
    public async Task The_directory_reader_finds_a_record_stamped_before_the_rotation_in_the_next_file()
    {
        Write("postgresql-2026-10-22_061000.json", Day.AddHours(6).AddMinutes(19).AddSeconds(58),
            Json(Day.AddHours(6).AddMinutes(9).AddSeconds(59), 1, "LOG", "connection received: host=192.0.2.10 port=1"),
            Json(Day.AddHours(6).AddMinutes(10), 2, "LOG", "statement: CREATE TABLE a (b int)"),
            Json(Day.AddHours(6).AddMinutes(15), 3, "LOG", "checkpoint starting: time"));
        Write("postgresql-2026-10-22_062000.json", Day.AddHours(6).AddMinutes(25),
            // Stamped by its backend before 06:20, written by the logging collector after the rotation.
            Json(Day.AddHours(6).AddMinutes(19).AddSeconds(59).AddMilliseconds(900), 4, "FATAL", "password authentication failed for user \"app\""),
            Json(Day.AddHours(6).AddMinutes(20).AddSeconds(1), 5, "LOG", "statement: DROP TABLE a"),
            Json(Day.AddHours(6).AddMinutes(20).AddSeconds(6), 6, "LOG", "statement: DROP TABLE b"),
            Json(Day.AddHours(6).AddMinutes(21), 7, "LOG", "statement: DROP TABLE c"));
        var buffer = new ArrayBufferWriter<byte>();
        var reader = new PgWindowReader("db1", At(6, 10), At(6, 20), shipText: true, new RowSink(buffer));

        await reader.ReadAsync(new PgLogDirectory(_dir), CancellationToken.None);
        reader.WriteCounts();

        var rows = Rows(buffer);
        Assert.Equal(new[] { "LOG", "FATAL", "summary" }, rows.Select(r => r.GetProperty("level").GetString()));
        Assert.Equal(new[] { "CREATE TABLE", "", "" }, rows.Select(r => r.GetProperty("command").GetString()));
        Assert.True(reader.Past);
        var counts = JsonDocument.Parse(rows[2].GetProperty("message").GetString()!).RootElement;
        Assert.Equal(1, counts.GetProperty("counted").GetProperty("other").GetInt64());
        Assert.Equal(2, counts.GetProperty("files").GetInt32());
    }

    [Fact]
    public async Task A_window_before_the_oldest_file_says_where_the_log_starts()
    {
        Write("postgresql-2026-10-22_090000.json", Day.AddHours(9).AddMinutes(10),
            Json(Day.AddHours(9).AddMinutes(1), 1, "LOG", "statement: CREATE TABLE x (a int)"));
        var buffer = new ArrayBufferWriter<byte>();
        var reader = new PgWindowReader("db1", At(6, 10), At(6, 20), shipText: true, new RowSink(buffer));

        await reader.ReadAsync(new PgLogDirectory(_dir), CancellationToken.None);
        reader.WriteCounts();

        var counts = JsonDocument.Parse(Rows(buffer).Single().GetProperty("message").GetString()!).RootElement;
        Assert.Equal("2026-10-22T09:00:00.000000Z", counts.GetProperty("first_log").GetString());
    }

    [Fact]
    public async Task A_large_file_that_starts_before_the_window_is_read_from_its_end()
    {
        // 2 MB of hourly file before the window, then the window's records at its end.
        var lines = Enumerable.Range(0, 9000).Select(i => Json(Day.AddHours(6).AddSeconds(i * 0.06), 1, "LOG", "connection received: host=192.0.2.10 port=1 " + new string('x', 120)))
            .Concat(new[] { Json(Day.AddHours(6).AddMinutes(10).AddSeconds(5), 2, "LOG", "statement: CREATE TABLE in_window (a int)") })
            .ToArray();
        Write("postgresql-2026-10-22_060000.json", Day.AddHours(6).AddMinutes(25), lines);
        var directory = new PgLogDirectory(_dir);
        var file = directory.List().Single();

        Assert.NotNull(directory.Tail(file, 1 << 20, At(6, 10) - PgWindowReader.StopMarginMicros));

        var buffer = new ArrayBufferWriter<byte>();
        var reader = new PgWindowReader("db1", At(6, 10), At(6, 20), shipText: true, new RowSink(buffer));
        await reader.ReadAsync(directory, CancellationToken.None);
        reader.WriteCounts();

        var rows = Rows(buffer);
        Assert.Contains(rows, r => r.GetProperty("command").GetString() == "CREATE TABLE");
        var counts = JsonDocument.Parse(rows[^1].GetProperty("message").GetString()!).RootElement;
        Assert.True(counts.GetProperty("bytes").GetInt64() <= (1 << 20) + 1000);
    }

    [Fact]
    public void A_missing_directory_says_where_to_mount_the_log()
    {
        var ex = Assert.Throws<Trailox.Agent.Engines.SourceException>(() => new PgLogDirectory(Path.Combine(_dir, "missing")).List());

        Assert.Contains("options.log_directory", ex.Message);
    }

    [Fact]
    public async Task Csvlog_files_are_read_when_there_is_no_jsonlog()
    {
        var fields = new string[24];
        Array.Fill(fields, "");
        fields[0] = "2026-10-22 06:12:00.000 UTC";
        fields[1] = "app";
        fields[2] = "appdb";
        fields[3] = "9";
        fields[4] = "192.0.2.10:5000";
        fields[11] = "LOG";
        fields[13] = "\"statement: GRANT SELECT ON t TO r\"";
        fields[22] = "psql";
        Write("postgresql-2026-10-22_061000.csv", Day.AddHours(6).AddMinutes(13), string.Join(',', fields));
        Write("postgresql-2026-10-22_061000.log", Day.AddHours(6).AddMinutes(13), "noise from a library");
        var buffer = new ArrayBufferWriter<byte>();
        var reader = new PgWindowReader("db1", At(6, 10), At(6, 20), shipText: true, new RowSink(buffer));

        await reader.ReadAsync(new PgLogDirectory(_dir), CancellationToken.None);

        var row = Rows(buffer).Single();
        Assert.Equal("GRANT", row.GetProperty("command").GetString());
        Assert.Equal("csvlog", row.GetProperty("format").GetString());
        Assert.Equal("psql", row.GetProperty("application_name").GetString());
    }
}
