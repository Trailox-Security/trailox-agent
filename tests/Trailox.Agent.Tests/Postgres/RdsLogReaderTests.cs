using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Trailox.Agent.Engines.MongoDb;
using Trailox.Agent.Engines.Postgres;

namespace Trailox.Agent.Tests.Postgres;

/// <summary>
/// An RDS instance's log files in memory, behaving as <c>DownloadDBLogFilePortion</c> does: pages of lines from a
/// marker, at most 1 MB a page (a larger page is cut and says so, and its marker skips the lost lines), and the file's
/// last lines when no marker is given. The marker is a line number here; the reader never reads it.
/// </summary>
internal sealed class FakeRds : IRdsLogs
{
    private readonly Dictionary<string, (string[] Lines, long LastWrittenMs)> _files = new(StringComparer.Ordinal);

    public List<string> Calls { get; } = new();

    public HashSet<string> Gone { get; } = new(StringComparer.Ordinal);

    public bool InstanceGone { get; set; }

    public void Add(string name, long lastWrittenMs, IEnumerable<string> lines) => _files[name] = (lines.ToArray(), lastWrittenMs);

    public Task<IReadOnlyList<RdsLogFile>> ListAsync(string instance, long writtenSinceMs, string? nameContains, CancellationToken ct)
    {
        Calls.Add($"list {nameContains}");
        if (InstanceGone)
        {
            throw new RdsInstanceGoneException($"there is no RDS instance {instance}");
        }
        IReadOnlyList<RdsLogFile> files = _files
            .Where(f => f.Value.LastWrittenMs >= writtenSinceMs && (nameContains == null || f.Key.Contains(nameContains, StringComparison.Ordinal)))
            .Select(f => new RdsLogFile(f.Key, f.Value.LastWrittenMs, f.Value.Lines.Sum(l => Encoding.UTF8.GetByteCount(l) + 1)))
            .ToList();
        return Task.FromResult(files);
    }

    public Task<RdsPortion> DownloadAsync(string instance, string file, string? marker, int lines, CancellationToken ct)
    {
        Calls.Add($"download {file} {marker ?? "tail"} {lines}");
        if (Gone.Contains(file))
        {
            throw new RdsLogFileGoneException("the log file is gone");
        }
        var all = _files[file].Lines;
        if (marker == null)
        {
            var from = Math.Max(0, all.Length - lines);
            return Task.FromResult(Page(all, from, all.Length - from, all.Length.ToString(CultureInfo.InvariantCulture)));
        }
        var start = int.Parse(marker, CultureInfo.InvariantCulture);
        var count = Math.Min(lines, all.Length - start);
        return Task.FromResult(Page(all, start, count, (start + count).ToString(CultureInfo.InvariantCulture), pending: start + count < all.Length));
    }

    private static RdsPortion Page(string[] all, int from, int count, string next, bool pending = false)
    {
        var data = string.Concat(all.Skip(from).Take(count).Select(l => l + "\n"));
        if (Encoding.UTF8.GetByteCount(data) > RdsLogReader.PageCapBytes)
        {
            data = data[..(RdsLogReader.PageCapBytes - 100)] + RdsLogReader.TruncatedNote;
        }
        return new RdsPortion(data, next, pending);
    }
}

/// <summary>The RDS API reader: which files, where in each file, the 1 MB cap, and what it reports.</summary>
public class RdsLogReaderTests
{
    private static readonly DateTime Day = new(2026, 10, 22, 0, 0, 0, DateTimeKind.Utc);

    private static DateTime At(int hour, int minute) => Day.AddHours(hour).AddMinutes(minute);

    private static long Micros(DateTime at) => (at - DateTime.UnixEpoch).Ticks / 10;

    private static long Ms(DateTime at) => Micros(at) / 1000;

    private static string Line(DateTime at, int pid, string message) =>
        at.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + $" UTC:192.0.2.10({50000 + pid}):app@appdb:[{pid}]:LOG:  " + message;

    /// <summary>One line every <paramref name="step"/> from <paramref name="from"/> to <paramref name="to"/>, a DDL marker each minute.</summary>
    private static IEnumerable<string> Lines(DateTime from, DateTime to, TimeSpan step, int padding = 0, bool brief = false)
    {
        var pid = 1;
        for (var at = from; at < to; at += step)
        {
            var message = at.Second == 0 && at.Millisecond == 0 ? $"statement: CREATE TABLE m_{at:HHmm} (a int)"
                : brief ? "x" : "connection received: host=192.0.2.10 port=1";
            yield return Line(at, pid++ % 1000, message + new string(' ', padding));
        }
    }

    private static Task NoDelay(TimeSpan _, CancellationToken __) => Task.CompletedTask;

    private static async Task<(List<JsonElement> Rows, RdsLogReader Reader)> ReadAsync(FakeRds rds, DateTime start, DateTime end, bool shipText = true)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var window = new PgWindowReader("db1", Micros(start), Micros(end), shipText, new RowSink(buffer));
        var reader = new RdsLogReader(rds, "db1", callsPerSecond: 4, NoDelay);
        await window.ReadAsync(reader, CancellationToken.None);
        window.WriteCounts();
        var rows = Encoding.UTF8.GetString(buffer.WrittenSpan).Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => JsonDocument.Parse(l).RootElement.Clone()).ToList();
        return (rows, reader);
    }

    private static string[] Ddl(List<JsonElement> rows) =>
        rows.Where(r => r.GetProperty("command").GetString() == "CREATE TABLE").Select(r => r.GetProperty("message").GetString()!).ToArray();

    [Fact]
    public async Task Ten_minute_files_read_the_window_file_from_its_start_and_stop_in_the_next()
    {
        var rds = new FakeRds();
        // The logging collector wrote 06:10:00's first record into the 06:00 file before it rotated: that file was last
        // written after the window started, and its end is read in one call.
        rds.Add("error/postgresql.log.2026-10-22-0600", Ms(At(6, 10).AddMilliseconds(400)),
            Lines(At(6, 0), At(6, 10), TimeSpan.FromSeconds(10))
                .Append(Line(At(6, 10), 999, "statement: CREATE TABLE written_before_rotation (a int)")));
        rds.Add("error/postgresql.log.2026-10-22-0610", Ms(At(6, 20)), Lines(At(6, 10), At(6, 20), TimeSpan.FromSeconds(10)));
        rds.Add("error/postgresql.log.2026-10-22-0620", Ms(At(6, 30)), Lines(At(6, 20), At(6, 30), TimeSpan.FromSeconds(10)));

        var (rows, reader) = await ReadAsync(rds, At(6, 10), At(6, 20));

        Assert.Equal(11, Ddl(rows).Length);
        Assert.Contains("statement: CREATE TABLE written_before_rotation (a int)", Ddl(rows));
        Assert.Contains("download error/postgresql.log.2026-10-22-0610 0 3000", rds.Calls);
        Assert.Contains("download error/postgresql.log.2026-10-22-0620 0 3000", rds.Calls);
        Assert.Contains("download error/postgresql.log.2026-10-22-0600 tail 10000", rds.Calls);
        Assert.DoesNotContain("download error/postgresql.log.2026-10-22-0600 0 3000", rds.Calls);
        Assert.Equal(1, reader.Stats.TailHits);
        Assert.Null(reader.Stats.FirstLogMicros);
    }

    /// <summary>
    /// 🔴 RDS can list a file as last written a few milliseconds before the window while it holds the window's first
    ///    records: under load the log rotates every minute, and the logging collector writes records stamped at the
    ///    window's first second into the previous file before it rotates. Listed from the window's start, that file was
    ///    skipped, and its records fell in no window: the window before stops at its end.
    /// </summary>
    [Fact]
    public async Task The_file_before_the_window_is_read_when_listed_as_written_just_before_it()
    {
        var rds = new FakeRds();
        rds.Add("error/postgresql.log.2026-10-22-0609", Ms(At(6, 10).AddMilliseconds(-4)),
            Lines(At(6, 9), At(6, 10), TimeSpan.FromSeconds(1))
                .Append(Line(At(6, 10), 997, "statement: CREATE TABLE stamped_at_the_start_1 (a int)"))
                .Append(Line(At(6, 10), 998, "statement: CREATE TABLE stamped_at_the_start_2 (a int)")));
        rds.Add("error/postgresql.log.2026-10-22-0610", Ms(At(6, 11).AddMilliseconds(-9)), Lines(At(6, 10), At(6, 11), TimeSpan.FromSeconds(1)));
        rds.Add("error/postgresql.log.2026-10-22-0611", Ms(At(6, 20)), Lines(At(6, 11), At(6, 20), TimeSpan.FromSeconds(1)));
        rds.Add("error/postgresql.log.2026-10-22-0620", Ms(At(6, 30)), Lines(At(6, 20), At(6, 30), TimeSpan.FromSeconds(1)));

        var (rows, reader) = await ReadAsync(rds, At(6, 10), At(6, 20));

        Assert.Contains("statement: CREATE TABLE stamped_at_the_start_1 (a int)", Ddl(rows));
        Assert.Contains("statement: CREATE TABLE stamped_at_the_start_2 (a int)", Ddl(rows));
        Assert.Equal(12, Ddl(rows).Length);
        // Its end, in one call: not the whole file.
        Assert.Contains("download error/postgresql.log.2026-10-22-0609 tail 10000", rds.Calls);
        Assert.DoesNotContain("download error/postgresql.log.2026-10-22-0609 0 3000", rds.Calls);
        Assert.Equal(1, reader.Stats.TailHits);
    }

    [Fact]
    public async Task An_hourly_file_whose_tail_reaches_the_window_is_read_in_one_call()
    {
        var rds = new FakeRds();
        // 20,000 short lines: the file is over 1 MB, so the newest file is tried from its end first, and its last
        // 10,000 lines (12.5 minutes) fit in one page.
        var lines = Lines(At(6, 0), At(6, 25), TimeSpan.FromMilliseconds(75), brief: true).ToList();
        rds.Add("error/postgresql.log.2026-10-22-06", Ms(At(6, 25)), lines);

        var (rows, reader) = await ReadAsync(rds, At(6, 20), At(6, 24));

        Assert.Equal(4, Ddl(rows).Length);
        Assert.Equal(1, reader.Stats.TailHits);
        Assert.DoesNotContain(rds.Calls, c => c.EndsWith(" 0 3000", StringComparison.Ordinal));
    }

    [Fact]
    public async Task An_hourly_file_whose_tail_starts_after_the_window_is_read_from_its_start()
    {
        var rds = new FakeRds();
        var lines = Lines(At(6, 0), At(7, 0), TimeSpan.FromSeconds(1), padding: 200).ToList();
        rds.Add("error/postgresql.log.2026-10-22-06", Ms(At(7, 0)), lines);

        var (rows, reader) = await ReadAsync(rds, At(6, 10), At(6, 20));

        Assert.Equal(10, Ddl(rows).Length);
        Assert.Equal(0, reader.Stats.TailHits);
        Assert.Contains("download error/postgresql.log.2026-10-22-06 tail 10000", rds.Calls);
        Assert.Contains("download error/postgresql.log.2026-10-22-06 0 3000", rds.Calls);
    }

    [Fact]
    public async Task A_capped_page_is_read_again_with_half_the_lines_and_nothing_is_lost()
    {
        var rds = new FakeRds();
        // 3,000 lines of 600 bytes are 1.8 MB: the first page is cut, so the reader asks again for 1,500.
        var lines = Lines(At(6, 10), At(6, 20), TimeSpan.FromMilliseconds(150), padding: 520).ToList();
        rds.Add("error/postgresql.log.2026-10-22-0610", Ms(At(6, 20)), lines);

        var (rows, reader) = await ReadAsync(rds, At(6, 10), At(6, 20));

        Assert.True(reader.Stats.CappedPages >= 1);
        Assert.Equal(lines.Count, rows.Count(r => r.GetProperty("level").GetString() == "LOG"));
    }

    [Fact]
    public async Task A_line_over_one_megabyte_ships_cut_and_marked()
    {
        var rds = new FakeRds();
        var at = At(6, 12);
        rds.Add("error/postgresql.log.2026-10-22-0610", Ms(At(6, 20)), new[]
        {
            Line(at, 1, "statement: CREATE VIEW v AS SELECT '" + new string('x', 1_500_000) + "'"),
            Line(at.AddSeconds(1), 2, "statement: CREATE TABLE after_the_big_one (a int)"),
        });

        var (rows, _) = await ReadAsync(rds, At(6, 10), At(6, 20));

        var view = rows.Single(r => r.GetProperty("command").GetString() == "CREATE VIEW");
        Assert.True(view.GetProperty("truncated").GetBoolean());
        Assert.True(view.GetProperty("message").GetString()!.Length <= PgWindowReader.MaxTextChars);
        Assert.Single(Ddl(rows));
    }

    [Fact]
    public async Task A_window_whose_file_is_gone_says_where_the_log_starts()
    {
        var rds = new FakeRds();
        // Retention deleted 06:10; the oldest file left starts at 06:20.
        rds.Add("error/postgresql.log.2026-10-22-0620", Ms(At(6, 30)), Lines(At(6, 20), At(6, 30), TimeSpan.FromSeconds(10)));

        var (rows, reader) = await ReadAsync(rds, At(6, 10), At(6, 20));

        Assert.Equal(Micros(At(6, 20)), reader.Stats.FirstLogMicros);
        var counts = JsonDocument.Parse(rows.Single().GetProperty("message").GetString()!).RootElement;
        Assert.Equal("2026-10-22T06:20:00.000000Z", counts.GetProperty("first_log").GetString());
    }

    [Fact]
    public async Task A_deleted_instance_is_reported_in_the_count_row_and_does_not_throw()
    {
        var rds = new FakeRds { InstanceGone = true };

        var (rows, _) = await ReadAsync(rds, At(6, 10), At(6, 20));

        var counts = JsonDocument.Parse(rows.Single().GetProperty("message").GetString()!).RootElement;
        Assert.Contains("no RDS instance", counts.GetProperty("missing").GetString());
    }

    [Fact]
    public async Task A_file_deleted_while_it_was_read_is_counted_and_the_next_file_is_read()
    {
        var rds = new FakeRds();
        rds.Add("error/postgresql.log.2026-10-22-0610", Ms(At(6, 20)), Lines(At(6, 10), At(6, 20), TimeSpan.FromSeconds(10)));
        rds.Add("error/postgresql.log.2026-10-22-0620", Ms(At(6, 30)),
            new[] { Line(At(6, 19).AddSeconds(59), 9, "statement: CREATE TABLE late (a int)") });
        rds.Gone.Add("error/postgresql.log.2026-10-22-0610");

        var (rows, reader) = await ReadAsync(rds, At(6, 10), At(6, 20));

        Assert.Equal(1, reader.Stats.FilesGone);
        Assert.Equal(new[] { "statement: CREATE TABLE late (a int)" }, Ddl(rows));
        var counts = JsonDocument.Parse(rows[^1].GetProperty("message").GetString()!).RootElement;
        Assert.Equal(1, counts.GetProperty("files_gone").GetInt32());
    }

    [Fact]
    public async Task A_window_across_midnight_lists_both_dates()
    {
        var rds = new FakeRds();
        rds.Add("error/postgresql.log.2026-10-22-2350", Ms(Day.AddDays(1)), Lines(At(23, 50), Day.AddDays(1), TimeSpan.FromSeconds(10)));
        rds.Add("error/postgresql.log.2026-10-23-0000", Ms(Day.AddDays(1).AddMinutes(10)),
            Lines(Day.AddDays(1), Day.AddDays(1).AddMinutes(10), TimeSpan.FromSeconds(10)));

        var (rows, _) = await ReadAsync(rds, At(23, 50), Day.AddDays(1));

        Assert.Contains("list postgresql.log.2026-10-22", rds.Calls);
        Assert.Contains("list postgresql.log.2026-10-23", rds.Calls);
        Assert.Equal(10, Ddl(rows).Length);
    }

    [Fact]
    public async Task A_tail_that_starts_inside_the_margin_before_the_window_is_not_trusted()
    {
        var rds = new FakeRds();
        // The file's last 10,000 lines start 2 s before the window: records of the window's first seconds can sit before
        // them, out of order, so the file is read from its start.
        var lines = Lines(At(6, 10).AddSeconds(-2), At(6, 10).AddSeconds(8), TimeSpan.FromMilliseconds(1), brief: true).ToList();
        rds.Add("error/postgresql.log.2026-10-22-06", Ms(At(6, 10).AddSeconds(8)), lines);

        var (_, reader) = await ReadAsync(rds, At(6, 10), At(6, 20));

        Assert.Equal(0, reader.Stats.TailHits);
        Assert.Contains("download error/postgresql.log.2026-10-22-06 0 3000", rds.Calls);
    }

    [Fact]
    public async Task The_first_window_after_midnight_reads_the_end_of_the_last_file_of_the_day_before()
    {
        var rds = new FakeRds();
        rds.Add("error/postgresql.log.2026-10-22-2350", Ms(Day.AddDays(1).AddMilliseconds(300)),
            Lines(At(23, 50), Day.AddDays(1), TimeSpan.FromSeconds(10))
                .Append(Line(Day.AddDays(1), 7, "statement: CREATE TABLE at_midnight (a int)")));
        rds.Add("error/postgresql.log.2026-10-23-0000", Ms(Day.AddDays(1).AddMinutes(10)),
            Lines(Day.AddDays(1), Day.AddDays(1).AddMinutes(10), TimeSpan.FromSeconds(10)));

        var (rows, _) = await ReadAsync(rds, Day.AddDays(1), Day.AddDays(1).AddMinutes(10));

        Assert.Contains("list postgresql.log.2026-10-22", rds.Calls);
        Assert.Contains("statement: CREATE TABLE at_midnight (a int)", Ddl(rows));
    }

    [Fact]
    public async Task A_window_whose_date_has_no_file_left_finds_where_the_log_starts()
    {
        var rds = new FakeRds();
        rds.Add("error/postgresql.log.2026-10-25-0000", Ms(Day.AddDays(3).AddMinutes(10)),
            Lines(Day.AddDays(3), Day.AddDays(3).AddMinutes(10), TimeSpan.FromSeconds(10)));

        var (_, reader) = await ReadAsync(rds, At(6, 10), At(6, 20));

        Assert.Equal(Micros(Day.AddDays(3)), reader.Stats.FirstLogMicros);
    }

    [Fact]
    public async Task After_a_cut_page_the_reader_keeps_the_size_that_fit()
    {
        var rds = new FakeRds();
        var lines = Lines(At(6, 10), At(6, 20), TimeSpan.FromMilliseconds(100), padding: 520).ToList();
        rds.Add("error/postgresql.log.2026-10-22-0610", Ms(At(6, 20)), lines);

        await ReadAsync(rds, At(6, 10), At(6, 20));

        var sizes = rds.Calls.Where(c => c.StartsWith("download", StringComparison.Ordinal)).Select(c => c.Split(' ')[^1]).ToList();
        Assert.Equal(new[] { "3000", "1500", "1500", "1500", "1500" }, sizes);
    }

    [Fact]
    public async Task Calls_are_paced()
    {
        var rds = new FakeRds();
        rds.Add("error/postgresql.log.2026-10-22-0610", Ms(At(6, 20)), Lines(At(6, 10), At(6, 20), TimeSpan.FromSeconds(10)));
        var waits = new List<TimeSpan>();
        var reader = new RdsLogReader(rds, "db1", callsPerSecond: 2, (d, _) =>
        {
            waits.Add(d);
            return Task.CompletedTask;
        });

        await foreach (var _ in reader.LinesAsync(Micros(At(6, 10)), Micros(At(6, 20)), 0, PgWindowReader.StderrTime, CancellationToken.None))
        {
        }

        Assert.Equal(rds.Calls.Count - 1, waits.Count);
        Assert.All(waits, w => Assert.InRange(w.TotalMilliseconds, 1, 500));
    }

    /// <summary>The Region's budget caps the sum of every reader there: three calls at one instant take three slots.</summary>
    [Fact]
    public void A_regions_budget_gives_each_call_its_own_slot()
    {
        var budget = new RdsCallBudget(callsPerSecond: 2);
        var at = new DateTime(2026, 10, 22, 6, 10, 0, DateTimeKind.Utc);

        Assert.Equal(new[] { 0.0, 500, 1000 }, new[] { budget.Reserve(at), budget.Reserve(at), budget.Reserve(at) }.Select(w => w.TotalMilliseconds));
        Assert.Equal(TimeSpan.Zero, budget.Reserve(at.AddSeconds(5)));
    }

    /// <summary>A reader faster than its Region's budget waits for the budget.</summary>
    [Fact]
    public async Task A_reader_waits_for_its_regions_budget()
    {
        var rds = new FakeRds();
        rds.Add("error/postgresql.log.2026-10-22-0610", Ms(At(6, 20)), Lines(At(6, 10), At(6, 20), TimeSpan.FromSeconds(10)));
        var waits = new List<TimeSpan>();
        var reader = new RdsLogReader(rds, "db1", callsPerSecond: 100, (d, _) =>
        {
            waits.Add(d);
            return Task.CompletedTask;
        }, new RdsCallBudget(callsPerSecond: 0.5));

        await foreach (var _ in reader.LinesAsync(Micros(At(6, 10)), Micros(At(6, 20)), 0, PgWindowReader.StderrTime, CancellationToken.None))
        {
        }

        Assert.NotEmpty(waits);
        Assert.All(waits, w => Assert.True(w.TotalMilliseconds > 1000, w.ToString()));
    }

    /// <summary>
    /// 🔴 At pgaudit's volumes the end of the file before a window is more than a page holds. The end is read again with
    ///    half the lines until it fits, never the whole file from its start: that downloaded every window twice.
    /// </summary>
    [Fact]
    public async Task A_capped_end_read_is_read_again_with_half_the_lines()
    {
        var rds = new FakeRds();
        rds.Add("error/postgresql.log.2026-10-22-0600", Ms(At(6, 10).AddMilliseconds(400)),
            Lines(At(6, 0), At(6, 10), TimeSpan.FromMilliseconds(100), padding: 200));
        rds.Add("error/postgresql.log.2026-10-22-0610", Ms(At(6, 20)), Lines(At(6, 10), At(6, 20), TimeSpan.FromSeconds(10)));

        var (rows, reader) = await ReadAsync(rds, At(6, 10), At(6, 20));

        var tail = rds.Calls.Where(c => c.StartsWith("download error/postgresql.log.2026-10-22-0600 tail", StringComparison.Ordinal)).ToList();
        Assert.Equal(new[] { "10000", "5000", "2500" }, tail.Select(c => c.Split(' ')[^1]));
        Assert.DoesNotContain(rds.Calls, c => c.StartsWith("download error/postgresql.log.2026-10-22-0600 0 ", StringComparison.Ordinal));
        Assert.Equal(1, reader.Stats.TailHits);
        Assert.Equal(10, Ddl(rows).Length);
    }

    /// <summary>The count row's log_bytes are the window's own lines; bytes are everything downloaded, the end read included.</summary>
    [Fact]
    public async Task Log_bytes_are_the_windows_own_lines()
    {
        var rds = new FakeRds();
        var before = Lines(At(6, 0), At(6, 10), TimeSpan.FromSeconds(10)).ToList();
        var window = Lines(At(6, 10), At(6, 20), TimeSpan.FromSeconds(10)).ToList();
        rds.Add("error/postgresql.log.2026-10-22-0600", Ms(At(6, 10).AddMilliseconds(400)), before);
        rds.Add("error/postgresql.log.2026-10-22-0610", Ms(At(6, 20)), window);

        var (rows, _) = await ReadAsync(rds, At(6, 10), At(6, 20));

        var counts = JsonDocument.Parse(rows[^1].GetProperty("message").GetString()!).RootElement;
        Assert.Equal(window.Sum(l => Encoding.UTF8.GetByteCount(l) + 1), counts.GetProperty("log_bytes").GetInt64());
        Assert.True(counts.GetProperty("bytes").GetInt64() > counts.GetProperty("log_bytes").GetInt64());
    }
}
