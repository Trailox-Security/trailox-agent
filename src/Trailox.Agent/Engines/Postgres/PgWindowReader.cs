using System.Globalization;
using System.IO.Pipelines;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Trailox.Agent.Engines.MongoDb;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>Rows on their way to the upload: the sink, and the pipe it writes to, flushed as it fills so a task holds little.</summary>
public sealed class PgRows
{
    private const int FlushBytes = 256 * 1024;

    private readonly PipeWriter? _pipe;

    public PgRows(RowSink sink, PipeWriter? pipe = null)
    {
        Sink = sink;
        _pipe = pipe;
    }

    public RowSink Sink { get; }

    public async ValueTask FlushAsync(CancellationToken ct)
    {
        if (_pipe != null && _pipe.UnflushedBytes > FlushBytes)
        {
            await _pipe.FlushAsync(ct);
        }
    }
}

/// <summary>
/// The <c>pg_log</c> rows of one instance for one window [start, end). Each record belongs to the window by its own
/// time. A record of a shipped kind (<see cref="PgRecordKinds.Shipped"/>) becomes a row, its text redacted and cut at
/// <see cref="MaxTextChars"/>; every other record is only counted. One count row per instance and window closes the
/// read: what was counted, what could not be read, and whether the log before the window's start was found.
/// </summary>
/// <remarks>
/// Reading stops at the first record <see cref="StopMarginMicros"/> past the window's end: records written in the same
/// second can be out of order in the file, so a record of the window can follow one stamped just after it.
/// </remarks>
public sealed class PgWindowReader
{
    public const long StopMarginMicros = 5_000_000;
    public const int MaxTextChars = 256 * 1024;
    private const int HeartbeatLines = 5000;
    private const int TailBytes = 1 << 20;

    private static readonly JsonSerializerOptions CountsJson = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    private readonly string _instance;
    private readonly long _start;
    private readonly long _end;
    private readonly bool _shipText;
    private readonly RowSink _sink;
    private readonly PgRows? _rows;
    private readonly SortedDictionary<string, long> _counted = new(StringComparer.Ordinal);

    private long _records;
    private long _shipped;
    private long _outOfWindow;
    private long _unreadable;
    private long _malformed;
    private int _files;
    private long _bytes;
    private long? _firstLogMicros;
    private int _filesGone;
    private string? _missing;

    /// <param name="shipText">The source's <c>storeRawQueryText</c>: when false, a statement record ships its command only.</param>
    /// <param name="rows">The upload's pipe, flushed as rows build up; null when the rows go to memory.</param>
    public PgWindowReader(string instance, long startMicros, long endMicros, bool shipText, RowSink sink, PgRows? rows = null)
    {
        _instance = instance;
        _start = startMicros;
        _end = endMicros;
        _shipText = shipText;
        _sink = sink;
        _rows = rows;
    }

    /// <summary>A record past the window's end and its margin was seen: nothing after it can belong to the window.</summary>
    public bool Past { get; private set; }

    public void Add(PgLogRecord r)
    {
        _records++;
        if (r.TsMicros == long.MinValue)
        {
            _unreadable++;
            return;
        }
        if (r.TsMicros >= _end + StopMarginMicros)
        {
            Past = true;
        }
        if (r.TsMicros < _start || r.TsMicros >= _end)
        {
            _outOfWindow++;
            return;
        }
        var kind = r.Kind;
        if (!PgRecordKinds.Shipped(kind))
        {
            var key = kind.ToString().ToLowerInvariant();
            _counted[key] = _counted.GetValueOrDefault(key) + 1;
            return;
        }
        Write(r, kind);
        _shipped++;
    }

    /// <summary>The instance could not be read for this window (a deleted replica); said in the count row.</summary>
    public void Missing(string reason) => _missing = reason;

    /// <summary>Reads the window from a self-managed server's log directory.</summary>
    public async Task ReadAsync(PgLogDirectory directory, CancellationToken ct)
    {
        var all = directory.List();
        var files = PgLogFileNames.ForWindow(all, _start, _end, StopMarginMicros);
        var format = files.FirstOrDefault()?.Format;
        var oldest = all.Where(f => format == null || f.Format == format).Select(f => f.StartMicros).OfType<long>()
            .DefaultIfEmpty(long.MinValue).Min();
        if (oldest > _start)
        {
            _firstLogMicros = oldest;
        }
        var done = new List<PgLogRecord>();
        foreach (var file in files)
        {
            _files++;
            var parser = PgLogParser.For(file.Format);
            // A file that starts before the window holds it at its end: its last megabyte usually covers that.
            var tail = (file.StartMicros ?? long.MinValue) < _start ? directory.Tail(file, TailBytes, _start - StopMarginMicros) : null;
            if (tail != null)
            {
                foreach (var line in tail)
                {
                    _bytes += line.Length + 1;
                    parser.Feed(line, done);
                    Drain(done);
                    if (Past)
                    {
                        break;
                    }
                }
            }
            else
            {
                await ReadWholeAsync(directory, file, parser, done, ct);
            }
            Close(parser, done);
            await FlushAsync(ct);
            if (Past)
            {
                break;
            }
        }
    }

    private async Task ReadWholeAsync(PgLogDirectory directory, PgLogFile file, PgLogParser parser, List<PgLogRecord> done,
        CancellationToken ct)
    {
        await using var stream = directory.Open(file);
        using var lines = new LogLines(stream, gzip: false);
        var sinceBeat = 0;
        while (!Past && await lines.FillAsync(ct))
        {
            while (!Past && lines.TryReadLine(out var span))
            {
                _bytes += span.Length + 1;
                parser.Feed(Encoding.UTF8.GetString(span), done);
                Drain(done);
                if (++sinceBeat == HeartbeatLines)
                {
                    sinceBeat = 0;
                    TaskHeartbeat.Beat();
                }
            }
            await FlushAsync(ct);
        }
    }

    /// <summary>Reads the window from an RDS instance through the RDS API (stderr only).</summary>
    public async Task ReadAsync(RdsLogReader rds, CancellationToken ct)
    {
        var done = new List<PgLogRecord>();
        PgLogParser? parser = null;
        string? file = null;
        var lines = 0L;
        try
        {
            await foreach (var line in rds.LinesAsync(_start, _end, StopMarginMicros, StderrTime, ct))
            {
                if (line.File != file)
                {
                    // A message never spans a rotation: the record in progress ends with its file.
                    Close(parser, done);
                    parser = new StderrLogParser();
                    file = line.File;
                }
                parser!.Feed(line.Text, done);
                Drain(done);
                if (Past)
                {
                    break;
                }
                if (++lines % 1000 == 0)
                {
                    await FlushAsync(ct);
                }
            }
        }
        catch (RdsInstanceGoneException ex)
        {
            Missing(ex.Message);
        }
        Close(parser, done);
        _files += rds.Stats.Files;
        _bytes += rds.Stats.Bytes;
        _firstLogMicros = rds.Stats.FirstLogMicros;
        _filesGone += rds.Stats.FilesGone;
    }

    /// <summary>The count row: <c>level</c> 'summary', and the counts as JSON in <c>message</c>.</summary>
    public void WriteCounts()
    {
        var counts = new Dictionary<string, object?>
        {
            ["records"] = _records,
            ["shipped"] = _shipped,
            ["out_of_window"] = _outOfWindow,
            ["unreadable"] = _unreadable + _malformed,
            ["counted"] = _counted,
            ["files"] = _files,
            ["bytes"] = _bytes,
            ["files_gone"] = _filesGone,
            ["first_log"] = _firstLogMicros is { } f ? Iso(f) : null,
            ["missing"] = _missing,
        };
        var w = _sink.Writer;
        w.WriteStartObject();
        w.WriteString("instance"u8, _instance);
        RowSink.Time(w, "ts"u8, _start);
        w.WriteNumber("pid"u8, 0);
        w.WriteString("user"u8, "");
        w.WriteString("database"u8, "");
        w.WriteString("remote_host"u8, "");
        w.WriteString("remote_port"u8, "");
        w.WriteString("level"u8, "summary");
        w.WriteString("sql_state"u8, "");
        w.WriteNull("application_name"u8);
        w.WriteString("session_id"u8, "");
        w.WriteString("message"u8, JsonSerializer.Serialize(counts, CountsJson));
        w.WriteString("detail"u8, "");
        w.WriteString("command"u8, "");
        w.WriteString("format"u8, "");
        w.WriteBoolean("truncated"u8, false);
        w.WriteEndObject();
        _sink.EndRow();
    }

    /// <summary>The time of a stderr line that starts a record; null for a continuation line.</summary>
    public static long? StderrTime(string line) =>
        line.Length >= 23 && char.IsAsciiDigit(line[0]) && line.IndexOf(" UTC:", StringComparison.Ordinal) is var z and >= 19 and <= 26
            ? PgTime.Micros(line.AsSpan(0, z + 4))
            : null;

    private ValueTask FlushAsync(CancellationToken ct) => _rows?.FlushAsync(ct) ?? ValueTask.CompletedTask;

    private void Close(PgLogParser? parser, List<PgLogRecord> done)
    {
        if (parser == null)
        {
            return;
        }
        parser.Finish(done);
        Drain(done);
        _malformed += parser.Malformed;
    }

    private void Drain(List<PgLogRecord> done)
    {
        foreach (var r in done)
        {
            Add(r);
        }
        done.Clear();
    }

    private void Write(PgLogRecord r, PgRecordKind kind)
    {
        var truncated = r.Message.Contains(RdsLogReader.TruncatedNote, StringComparison.Ordinal);
        string message, detail, command = "";
        if (kind is PgRecordKind.Statement or PgRecordKind.Execute)
        {
            command = PgCommand.Of(PgRecordKinds.StatementText(r.Message));
            message = _shipText ? Cut(PgPasswordRedactor.Redact(r.Message), ref truncated) : "";
            // The DETAIL of a statement record is its bound values ("Parameters: $1 = ..."): never shipped.
            detail = "";
        }
        else
        {
            message = Cut(PgPasswordRedactor.Redact(r.Message), ref truncated);
            detail = Cut(PgPasswordRedactor.Redact(r.Detail), ref truncated);
        }

        var w = _sink.Writer;
        w.WriteStartObject();
        w.WriteString("instance"u8, _instance);
        RowSink.Time(w, "ts"u8, r.TsMicros);
        w.WriteNumber("pid"u8, r.Pid);
        w.WriteString("user"u8, r.User);
        w.WriteString("database"u8, r.Database);
        w.WriteString("remote_host"u8, r.RemoteHost);
        w.WriteString("remote_port"u8, r.RemotePort);
        w.WriteString("level"u8, r.Level);
        w.WriteString("sql_state"u8, r.SqlState);
        if (r.ApplicationName is null)
        {
            w.WriteNull("application_name"u8);
        }
        else
        {
            w.WriteString("application_name"u8, r.ApplicationName);
        }
        w.WriteString("session_id"u8, r.SessionId);
        w.WriteString("message"u8, message);
        w.WriteString("detail"u8, detail);
        w.WriteString("command"u8, command);
        w.WriteString("format"u8, FormatName(r.Format));
        w.WriteBoolean("truncated"u8, truncated);
        w.WriteEndObject();
        _sink.EndRow();
    }

    private static string Cut(string text, ref bool truncated)
    {
        if (text.Length <= MaxTextChars)
        {
            return text;
        }
        truncated = true;
        return text[..MaxTextChars];
    }

    internal static string FormatName(PgLogFormat f) => f switch
    {
        PgLogFormat.Csv => "csvlog",
        PgLogFormat.Json => "jsonlog",
        _ => "stderr",
    };

    private static string Iso(long micros) =>
        DateTime.UnixEpoch.AddTicks(micros * 10).ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture);
}

/// <summary>The command of a statement: its first keyword, and for CREATE, ALTER and DROP the kind of object.</summary>
public static class PgCommand
{
    private const int MaxLength = 64;

    private static readonly HashSet<string> TakesObject = new(StringComparer.Ordinal) { "CREATE", "ALTER", "DROP" };

    private static readonly HashSet<string> Modifiers = new(StringComparer.Ordinal)
    {
        "OR", "REPLACE", "TEMP", "TEMPORARY", "UNLOGGED", "UNIQUE", "GLOBAL", "LOCAL", "IF", "NOT", "EXISTS", "CONCURRENTLY",
        "DEFAULT", "TRUSTED", "PROCEDURAL", "RECURSIVE", "CONSTRAINT",
    };

    private static readonly (string First, string Second)[] Phrases = { ("USER", "MAPPING"), ("DEFAULT", "PRIVILEGES") };

    private static string NextWord(string sql, int i)
    {
        while (i < sql.Length && char.IsWhiteSpace(sql[i]))
        {
            i++;
        }
        var j = i;
        while (j < sql.Length && (char.IsLetter(sql[j]) || sql[j] == '_'))
        {
            j++;
        }
        return sql[i..j].ToUpperInvariant();
    }

    public static string Of(string sql)
    {
        var words = new List<string>(2);
        var i = 0;
        while (i < sql.Length && words.Count < 2)
        {
            var c = sql[i];
            if (char.IsWhiteSpace(c))
            {
                i++;
            }
            else if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                var nl = sql.IndexOf('\n', i);
                i = nl < 0 ? sql.Length : nl + 1;
            }
            else if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                var close = sql.IndexOf("*/", i + 2, StringComparison.Ordinal);
                i = close < 0 ? sql.Length : close + 2;
            }
            else if (char.IsLetter(c))
            {
                var j = i;
                while (j < sql.Length && (char.IsLetter(sql[j]) || sql[j] == '_'))
                {
                    j++;
                }
                var word = sql[i..j].ToUpperInvariant();
                if (words.Count == 0)
                {
                    words.Add(word);
                    if (!TakesObject.Contains(word))
                    {
                        break;
                    }
                }
                else if (Phrases.FirstOrDefault(p => p.First == word && NextWord(sql, j) == p.Second) is { First: not null } phrase)
                {
                    // CREATE USER MAPPING holds a foreign server's credentials, not a role; ALTER DEFAULT PRIVILEGES is grants.
                    words.Add(phrase.First + " " + phrase.Second);
                    break;
                }
                else if (!Modifiers.Contains(word))
                {
                    words.Add(word);
                }
                i = j;
            }
            else
            {
                break;
            }
        }
        var command = string.Join(' ', words);
        return command.Length <= MaxLength ? command : command[..MaxLength];
    }
}
