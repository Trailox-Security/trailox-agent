using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>One log file: its name, the time in its name, when it was last written, and its format.</summary>
public sealed record PgLogFile(string Name, long? StartMicros, long LastWrittenMicros, long Size, PgLogFormat Format);

/// <summary>What a log file's name says: when the file starts, and its format.</summary>
public static class PgLogFileNames
{
    // postgresql-2026-10-22_061000.log/.csv/.json (PostgreSQL's default name), postgresql.log.2026-10-22-0610 and
    // postgresql.log.2026-10-22-06 (RDS), postgresql.log.2026-10-22 (RDS, daily).
    private static readonly Regex Time = new(@"(\d{4})-(\d{2})-(\d{2})(?:[_-](\d{2})(\d{2})?(\d{2})?)?(?=\.(?:log|csv|json)$|$)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The time in the name, UTC, in microseconds; null for a name with no date (Red Hat's <c>postgresql-Mon.log</c>).</summary>
    public static long? StartMicros(string name)
    {
        var m = Time.Match(name);
        if (!m.Success)
        {
            return null;
        }
        int Part(int group) => m.Groups[group].Success ? int.Parse(m.Groups[group].ValueSpan, CultureInfo.InvariantCulture) : 0;
        try
        {
            var at = new DateTime(Part(1), Part(2), Part(3), Part(4), Part(5), Part(6), DateTimeKind.Utc);
            return (at - DateTime.UnixEpoch).Ticks / 10;
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }
    }

    public static PgLogFormat FormatOf(string name) =>
        name.EndsWith(".csv", StringComparison.Ordinal) ? PgLogFormat.Csv
        : name.EndsWith(".json", StringComparison.Ordinal) ? PgLogFormat.Json
        : PgLogFormat.Stderr;

    /// <summary>
    /// The files that can hold records of [<paramref name="startMicros"/>, <paramref name="endMicros"/>), in order, in
    /// one format: jsonlog before csvlog before stderr, chosen among the files of the window (after an upgrade to 15,
    /// the older windows are csvlog; with the logging collector on, a stderr file can sit beside the others and hold only
    /// what bypassed the logger). A file covers [the time in its name, its last write]. The files that start up to
    /// <paramref name="marginMicros"/> after the window's end are kept too: the collector rotates on its own timer, so a
    /// record stamped just before a rotation can be in the next file.
    /// </summary>
    public static IReadOnlyList<PgLogFile> ForWindow(IEnumerable<PgLogFile> files, long startMicros, long endMicros, long marginMicros)
    {
        var candidates = files
            .Where(f => f.LastWrittenMicros >= startMicros && (f.StartMicros ?? long.MinValue) <= endMicros + marginMicros)
            .ToList();
        var format = candidates.Any(f => f.Format == PgLogFormat.Json) ? PgLogFormat.Json
            : candidates.Any(f => f.Format == PgLogFormat.Csv) ? PgLogFormat.Csv
            : PgLogFormat.Stderr;
        return candidates
            .Where(f => f.Format == format)
            .OrderBy(f => f.StartMicros ?? long.MinValue)
            .ThenBy(f => f.Name, StringComparer.Ordinal)
            .ToList();
    }
}

/// <summary>
/// A self-managed server's log directory, mounted read-only in the agent's container. The agent only lists and reads
/// it. Setup gives the directory the agent's group (mode 2750) and the files mode 0640, so the agent's user can read them.
/// </summary>
public sealed class PgLogDirectory
{
    private readonly string _path;

    public PgLogDirectory(string path)
    {
        _path = path;
    }

    /// <summary>Every log file in the directory (not its subdirectories). Compressed copies (logrotate's .gz) are not read.</summary>
    public IReadOnlyList<PgLogFile> List()
    {
        try
        {
            return new DirectoryInfo(_path).EnumerateFiles()
                .Where(f => IsLogName(f.Name))
                .Select(f => new PgLogFile(f.Name, PgLogFileNames.StartMicros(f.Name), Micros(f.LastWriteTimeUtc), f.Length,
                    PgLogFileNames.FormatOf(f.Name)))
                .ToList();
        }
        catch (DirectoryNotFoundException)
        {
            throw new SourceException($"the log directory {_path} does not exist in the agent's container: "
                + "mount the server's log directory there (options.log_directory)");
        }
        catch (UnauthorizedAccessException)
        {
            throw new SourceException($"the agent cannot list {_path}: give the directory the agent's group with mode 2750");
        }
    }

    /// <summary>The file, opened for reading while PostgreSQL may still write to it or rotate it away.</summary>
    public Stream Open(PgLogFile file)
    {
        try
        {
            return new FileStream(Path.Combine(_path, file.Name), FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete, 1 << 16, FileOptions.SequentialScan | FileOptions.Asynchronous);
        }
        catch (UnauthorizedAccessException)
        {
            throw new SourceException($"the agent cannot read {file.Name}: set log_file_mode = '0640', give the directory the agent's "
                + "group, and rotate once (SELECT pg_rotate_logfile()), because a new file mode reaches only the next file");
        }
    }

    /// <summary>
    /// The last <paramref name="bytes"/> of a jsonlog or stderr file as lines, the first partial line left out, when
    /// they reach back before <paramref name="beforeMicros"/>; null otherwise (then the file is read from its start).
    /// csvlog is never read so: a record can span lines, so a line start is not a record start.
    /// </summary>
    public IReadOnlyList<string>? Tail(PgLogFile file, int bytes, long beforeMicros)
    {
        if (file.Format == PgLogFormat.Csv || file.Size <= bytes)
        {
            return null;
        }
        using var stream = Open(file);
        stream.Seek(-bytes, SeekOrigin.End);
        using var reader = new StreamReader(stream, Encoding.UTF8);
        _ = reader.ReadLine();
        var lines = new List<string>();
        while (reader.ReadLine() is { } line)
        {
            lines.Add(line);
        }
        var first = lines.Select(l => file.Format == PgLogFormat.Json ? JsonTime(l) : PgWindowReader.StderrTime(l))
            .FirstOrDefault(t => t != null);
        return first != null && first < beforeMicros ? lines : null;
    }

    /// <summary>The time of a jsonlog line, read from its <c>timestamp</c> key without parsing the line.</summary>
    private static long? JsonTime(string line)
    {
        const string key = "\"timestamp\":\"";
        var at = line.IndexOf(key, StringComparison.Ordinal);
        if (at < 0)
        {
            return null;
        }
        var start = at + key.Length;
        var end = line.IndexOf('"', start);
        return end < 0 ? null : PgTime.Micros(line.AsSpan(start, end - start));
    }

    private static bool IsLogName(string name) =>
        !name.StartsWith('.') && (name.EndsWith(".log", StringComparison.Ordinal) || name.EndsWith(".csv", StringComparison.Ordinal)
            || name.EndsWith(".json", StringComparison.Ordinal) || name.StartsWith("postgresql.log.", StringComparison.Ordinal));

    private static long Micros(DateTime utc) => (utc - DateTime.UnixEpoch).Ticks / 10;
}
