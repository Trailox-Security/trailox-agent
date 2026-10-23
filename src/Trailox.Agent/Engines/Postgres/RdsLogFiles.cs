using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Amazon;
using Amazon.RDS;
using Amazon.RDS.Model;
using Amazon.Runtime;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>One RDS log file as <c>DescribeDBLogFiles</c> lists it.</summary>
public sealed record RdsLogFile(string Name, long LastWrittenMs, long Size);

/// <summary>One answer of <c>DownloadDBLogFilePortion</c>: at most 1 MB of lines, and where to go on.</summary>
public sealed record RdsPortion(string Data, string? Marker, bool Pending);

/// <summary>The two RDS calls the agent makes. A seam, so tests run without AWS.</summary>
public interface IRdsLogs
{
    /// <summary>
    /// The instance's log files written at or after <paramref name="writtenSinceMs"/>, whose names hold
    /// <paramref name="nameContains"/>.
    /// </summary>
    Task<IReadOnlyList<RdsLogFile>> ListAsync(string instance, long writtenSinceMs, string? nameContains, CancellationToken ct);

    /// <summary>
    /// Up to <paramref name="lines"/> lines from <paramref name="marker"/> ("0" is the file's start). With no marker, the
    /// file's last lines.
    /// </summary>
    Task<RdsPortion> DownloadAsync(string instance, string file, string? marker, int lines, CancellationToken ct);
}

/// <summary>The instance does not exist (a replica that was deleted). The reader skips it; the other instances go on.</summary>
public sealed class RdsInstanceGoneException : SourceException
{
    public RdsInstanceGoneException(string message) : base(message)
    {
    }
}

/// <summary>The log file is gone (RDS deleted it at the end of its retention while the agent read it).</summary>
public sealed class RdsLogFileGoneException : SourceException
{
    public RdsLogFileGoneException(string message) : base(message)
    {
    }
}

/// <summary>
/// The RDS API, as the agent's own AWS identity from the SDK's default chain (IRSA, EKS Pod Identity, ECS, the
/// instance profile, or AWS_* variables). It needs <c>rds:DescribeDBLogFiles</c> and
/// <c>rds:DownloadDBLogFilePortion</c> on each instance, and nothing else.
/// </summary>
public sealed class AwsRdsLogs : IRdsLogs
{
    private readonly string _region;
    private readonly Lazy<IAmazonRDS> _client;

    public AwsRdsLogs(string region)
    {
        _region = region;
        _client = new Lazy<IAmazonRDS>(() =>
            new AmazonRDSClient(new AmazonRDSConfig { RegionEndpoint = RegionEndpoint.GetBySystemName(region) }));
    }

    public async Task<IReadOnlyList<RdsLogFile>> ListAsync(string instance, long writtenSinceMs, string? nameContains, CancellationToken ct)
    {
        var files = new List<RdsLogFile>();
        string? marker = null;
        do
        {
            var request = new DescribeDBLogFilesRequest
            {
                DBInstanceIdentifier = instance,
                FileLastWritten = writtenSinceMs,
                FilenameContains = nameContains,
                Marker = marker,
            };
            var page = await Call(() => _client.Value.DescribeDBLogFilesAsync(request, ct), instance, "list the log files of");
            foreach (var f in page.DescribeDBLogFiles ?? new List<DescribeDBLogFilesDetails>())
            {
                files.Add(new RdsLogFile(f.LogFileName, f.LastWritten ?? 0, f.Size ?? 0));
            }
            marker = page.Marker;
        }
        while (!string.IsNullOrEmpty(marker));
        return files;
    }

    public async Task<RdsPortion> DownloadAsync(string instance, string file, string? marker, int lines, CancellationToken ct)
    {
        var request = new DownloadDBLogFilePortionRequest
        {
            DBInstanceIdentifier = instance,
            LogFileName = file,
            Marker = marker,
            NumberOfLines = lines,
        };
        var r = await Call(() => _client.Value.DownloadDBLogFilePortionAsync(request, ct), instance, $"read {file} of");
        return new RdsPortion(r.LogFileData ?? "", r.Marker, r.AdditionalDataPending ?? false);
    }

    private async Task<T> Call<T>(Func<Task<T>> call, string instance, string what)
    {
        try
        {
            return await call();
        }
        catch (Exception ex) when (Worded(ex, instance, what) is { } worded)
        {
            throw worded;
        }
    }

    /// <summary>An AWS failure as what to fix; null for anything else, which keeps its own message.</summary>
    private SourceException? Worded(Exception ex, string instance, string what) => ex switch
    {
        AmazonServiceException s when s.ErrorCode is "DBInstanceNotFound" or "DBInstanceNotFoundFault" =>
            new RdsInstanceGoneException(
                $"{what} {instance}: there is no RDS instance {instance} in {_region} (options.instances, options.rds_region)"),
        AmazonServiceException s when s.ErrorCode is "DBLogFileNotFoundFault" or "DBLogFileNotFound" =>
            new RdsLogFileGoneException($"{what} {instance}: the log file is gone"),
        AmazonServiceException s when s.ErrorCode is "AccessDenied" or "AccessDeniedException" or "UnauthorizedOperation"
            || s.StatusCode == System.Net.HttpStatusCode.Forbidden =>
            new SourceException($"{what} {instance}: access denied. The agent's AWS identity needs rds:DescribeDBLogFiles and "
                + $"rds:DownloadDBLogFilePortion on arn:aws:rds:{_region}:<account>:db:{instance}"),
        AmazonServiceException s when s.ErrorCode is "DBInstanceNotReady" or "InvalidDBInstanceState" =>
            new SourceException($"{what} {instance}: the instance is not available now ({s.ErrorCode})"),
        AmazonServiceException service => new SourceException($"{what} {instance} failed: {service.ErrorCode} {service.Message}".TrimEnd()),
        AmazonClientException client when client.Message.Contains("credential", StringComparison.OrdinalIgnoreCase) =>
            new SourceException($"{what} {instance}: no AWS credentials ({client.Message}). Give the agent an AWS identity that can read "
                + "the instance's log files: IRSA or EKS Pod Identity on Kubernetes, an instance profile, or AWS_* variables"),
        AmazonClientException client => new SourceException($"{what} {instance}: AWS client error ({client.Message})"),
        _ => null,
    };
}

/// <summary>One line of an RDS log file, with the file it came from.</summary>
public readonly record struct RdsLine(string File, string Text);

/// <summary>What one instance's read did.</summary>
public sealed class RdsReadStats
{
    public int Files { get; internal set; }
    public long Bytes { get; internal set; }
    public int TailHits { get; internal set; }
    public int CappedPages { get; internal set; }

    /// <summary>The start of the first file found, when the files found start after the window: the log before it was not found.</summary>
    public long? FirstLogMicros { get; internal set; }

    /// <summary>Files that RDS deleted while the agent read them.</summary>
    public int FilesGone { get; internal set; }
}

/// <summary>
/// Reads the lines of one RDS instance's log for one time window, through <see cref="IRdsLogs"/>. The agent keeps no
/// state, so the window's place in a file comes from the file names and the lines' own times:
/// <list type="bullet">
/// <item>a file that starts at or after the window's start is read from its start ("0");</item>
/// <item>a file that starts before the window is first read from its end, one call: if those lines reach back before
///   the window's start, nothing else of it is read; otherwise it is read from its start;</item>
/// <item>the caller drops the lines before the window and stops reading once a line is past it.</item>
/// </list>
/// A page of 1 MB, or one that holds RDS's truncation note, may have lost lines: it is read again from the same marker
/// with half the lines. Calls are paced to at most <c>callsPerSecond</c>.
/// </summary>
public sealed class RdsLogReader
{
    public const int PageLines = 3000;
    public const int TailLines = 10000;
    public const int PageCapBytes = 1_000_000;
    public const string TruncatedNote = "[Your log message was truncated]";

    private readonly IRdsLogs _api;
    private readonly string _instance;
    private readonly TimeSpan _interval;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private DateTime _nextCall = DateTime.MinValue;

    public RdsLogReader(IRdsLogs api, string instance, double callsPerSecond, Func<TimeSpan, CancellationToken, Task>? delay = null)
    {
        _api = api;
        _instance = instance;
        _interval = TimeSpan.FromSeconds(1 / Math.Max(0.1, callsPerSecond));
        _delay = delay ?? Task.Delay;
    }

    public RdsReadStats Stats { get; } = new();

    /// <param name="timeOf">The time of a line that starts a record, or null for a continuation line.</param>
    public async IAsyncEnumerable<RdsLine> LinesAsync(long startMicros, long endMicros, long marginMicros, Func<string, long?> timeOf,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var untilMicros = endMicros + marginMicros;
        var listed = await FilesAsync(startMicros, untilMicros, ct);
        if (listed.Count == 0)
        {
            // Nothing on the window's dates: RDS deleted them (retention), or the instance wrote nothing since. A later
            // file says which.
            await PaceAsync(ct);
            listed = Stderr(await _api.ListAsync(_instance, startMicros / 1000, "postgresql.log.", ct));
        }
        // RDS starts a file at each rotation, idle or not, so a window with no file of its own lost its log (retention).
        if (listed.Count > 0 && PgLogFileNames.StartMicros(listed[0].Name) is { } first && first > startMicros)
        {
            Stats.FirstLogMicros = first;
        }
        foreach (var file in listed.Where(f => StartOf(f) <= untilMicros))
        {
            Stats.Files++;
            // A file that starts before the window holds it at its end (the window starts inside it, or the collector
            // wrote the window's first records into it before it rotated): one call from its end usually covers that.
            if (StartOf(file) < startMicros && await TailAsync(file, startMicros - marginMicros, timeOf, ct) is { } tail)
            {
                Stats.TailHits++;
                foreach (var line in tail)
                {
                    yield return new RdsLine(file.Name, line);
                }
                continue;
            }
            await foreach (var line in FromStartAsync(file.Name, ct))
            {
                yield return new RdsLine(file.Name, line);
            }
        }
    }

    /// <summary>
    /// The file's lines from its start, a page at a time. A capped page may have lost lines, so it is read again from the
    /// same marker with half the lines. A file RDS deletes meanwhile ends the read and is counted.
    /// </summary>
    private async IAsyncEnumerable<string> FromStartAsync(string file, [EnumeratorCancellation] CancellationToken ct)
    {
        var marker = "0";
        var lines = PageLines;
        var good = PageLines;
        while (true)
        {
            RdsPortion portion;
            try
            {
                portion = await DownloadAsync(file, marker, lines, ct);
            }
            catch (RdsLogFileGoneException)
            {
                Stats.FilesGone++;
                break;
            }
            if (Capped(portion.Data))
            {
                Stats.CappedPages++;
                if (lines > 1)
                {
                    lines = Math.Max(1, lines / 2);
                    continue;
                }
                // One line over 1 MB: it ships cut, and the pages go back to the last size that fit.
                lines = good;
            }
            else
            {
                good = lines;
            }
            foreach (var line in Lines(portion.Data))
            {
                yield return line;
            }
            if (!portion.Pending || string.IsNullOrEmpty(portion.Marker) || portion.Marker == marker)
            {
                break;
            }
            marker = portion.Marker;
        }
    }

    /// <summary>
    /// The stderr files written since the window's start, on the window's dates and the hour before it (a file of the
    /// day before can hold the first records after midnight), in order. RDS writes csvlog and jsonlog files beside stderr
    /// when <c>log_destination</c> names them; the agent reads stderr only.
    /// </summary>
    private async Task<List<RdsLogFile>> FilesAsync(long startMicros, long untilMicros, CancellationToken ct)
    {
        var dates = new SortedSet<string>(StringComparer.Ordinal)
        {
            Date(startMicros - 3_600_000_000),
            Date(startMicros),
            Date(untilMicros),
        };
        var found = new Dictionary<string, RdsLogFile>(StringComparer.Ordinal);
        foreach (var date in dates)
        {
            await PaceAsync(ct);
            foreach (var f in await _api.ListAsync(_instance, startMicros / 1000, "postgresql.log." + date, ct))
            {
                found[f.Name] = f;
            }
        }
        return Stderr(found.Values);
    }

    private static List<RdsLogFile> Stderr(IEnumerable<RdsLogFile> files) => files
        .Where(f => PgLogFileNames.FormatOf(f.Name) == PgLogFormat.Stderr && !f.Name.EndsWith(".gz", StringComparison.Ordinal))
        .OrderBy(StartOf)
        .ThenBy(f => f.Name, StringComparer.Ordinal)
        .ToList();

    /// <summary>The time in the file's name; a name with none sorts first.</summary>
    private static long StartOf(RdsLogFile file) => PgLogFileNames.StartMicros(file.Name) ?? long.MinValue;

    /// <summary>
    /// The file's last lines, when they reach back before <paramref name="beforeMicros"/> (the window's start less the
    /// margin, for records out of order within a second); null otherwise.
    /// </summary>
    private async Task<string[]?> TailAsync(RdsLogFile file, long beforeMicros, Func<string, long?> timeOf, CancellationToken ct)
    {
        RdsPortion portion;
        try
        {
            portion = await DownloadAsync(file.Name, null, TailLines, ct);
        }
        catch (RdsLogFileGoneException)
        {
            return null;
        }
        if (Capped(portion.Data))
        {
            return null;
        }
        var lines = Lines(portion.Data);
        var firstTime = lines.Select(timeOf).FirstOrDefault(t => t != null);
        return firstTime != null && firstTime < beforeMicros ? lines : null;
    }

    private async Task<RdsPortion> DownloadAsync(string file, string? marker, int lines, CancellationToken ct)
    {
        await PaceAsync(ct);
        var portion = await _api.DownloadAsync(_instance, file, marker, lines, ct);
        Stats.Bytes += Encoding.UTF8.GetByteCount(portion.Data);
        Runner.TaskHeartbeat.Beat();
        return portion;
    }

    private async Task PaceAsync(CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        if (_nextCall > now)
        {
            await _delay(_nextCall - now, ct);
            now = _nextCall;
        }
        _nextCall = now + _interval;
    }

    private static bool Capped(string data) =>
        data.Contains(TruncatedNote, StringComparison.Ordinal) || Encoding.UTF8.GetByteCount(data) >= PageCapBytes;

    /// <summary>The lines of a portion; the empty part after its last newline is no line.</summary>
    private static string[] Lines(string data)
    {
        var parts = data.Split('\n');
        return parts[^1].Length == 0 ? parts[..^1] : parts;
    }

    private static string Date(long micros) =>
        DateTime.UnixEpoch.AddTicks(micros * 10).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
}
