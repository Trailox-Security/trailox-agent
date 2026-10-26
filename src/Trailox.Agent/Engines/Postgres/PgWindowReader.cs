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

    /// <summary>The start of the first file after a gap that tells a stopped server: PostgreSQL's own startup line.</summary>
    internal const string ShutDownLine = "database system was shut down at";

    private const int HeadBytes = 64 * 1024;

    private readonly string _instance;
    private readonly long _start;
    private readonly long _end;
    private readonly bool _shipText;
    private readonly bool _english;
    private readonly long _rotationMicros;
    private readonly RowSink _sink;
    private readonly PgRows? _rows;
    private readonly SortedDictionary<string, long> _counted = new(StringComparer.Ordinal);

    private long _records;
    private long _shipped;
    private long _outOfWindow;
    private long _unreadable;
    private long _malformed;
    private long _preamble;
    private int _files;
    private long _bytes;
    private long? _firstLogMicros;
    private int _filesGone;
    private string? _missing;

    /// <param name="shipText">The source's <c>storeRawQueryText</c>: when false, a statement record ships its command only.</param>
    /// <param name="rows">The upload's pipe, flushed as rows build up; null when the rows go to memory.</param>
    /// <param name="english">
    /// The server writes its log in English (<c>lc_messages</c>, from the probe). When it does not, a LOG record of no kind
    /// we know is counted <c>unrecognised</c>: it may be a sign-in or a statement the agent cannot read.
    /// An English server's other LOG records (checkpoints, autovacuum) stay <c>other</c>.
    /// </param>
    /// <param name="rotationMinutes">
    /// <c>log_rotation_age</c>, from the probe: a new file at least this often. 0 (unknown, or no rotation by time): a
    /// directory window is never said to be missing.
    /// </param>
    public PgWindowReader(string instance, long startMicros, long endMicros, bool shipText, RowSink sink, PgRows? rows = null,
        bool english = true, int rotationMinutes = 0)
    {
        _instance = instance;
        _start = startMicros;
        _end = endMicros;
        _shipText = shipText;
        _english = english;
        _rotationMicros = Math.Max(0, rotationMinutes) * 60_000_000L;
        _sink = sink;
        _rows = rows;
    }

    /// <summary>A record past the window's end and its margin was seen: nothing after it can belong to the window.</summary>
    public bool Past { get; private set; }

    public void Add(PgLogRecord r)
    {
        if (r.UnreadLines > 0)
        {
            // Lines the parser could not read count once, in the window of their time.
            if (r.TsMicros >= _start && r.TsMicros < _end)
            {
                if (r.Preamble)
                {
                    _preamble += r.UnreadLines;
                }
                else
                {
                    _malformed += r.UnreadLines;
                }
            }
            return;
        }
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
            var key = kind == PgRecordKind.Other && r.Level == "LOG" && !_english ? "unrecognised" : kind.ToString().ToLowerInvariant();
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
        else if (Gap(directory, all) is { } gap)
        {
            Missing(gap);
        }
        var done = new List<PgLogRecord>();
        foreach (var file in files)
        {
            _files++;
            var parser = PgLogParser.For(file.Format, file.StartMicros);
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

    /// <summary>
    /// Why a window that no file holds is missing, when log files start both before and after it: the
    /// server was stopped, which its next file says at its start ("database system was shut down at"), or the window's
    /// files were deleted (a retention that ran out). Null when no file is newer: the window is not written yet.
    /// </summary>
    /// <remarks>
    /// 🔴 NO FILE = NO FILE STARTED FOR IT, NOT NONE WRITTEN IN IT. A file's last write says
    ///    nothing on a quiet server, and the next file, which starts at the window's end, is read for the window's last
    ///    seconds. A file starts at least every <c>log_rotation_age</c>, so the window is missing when the newest file
    ///    that starts before its end started a whole rotation before its start.
    /// 🔴 THE SERVER'S OWN FORMAT ONLY. Beside each jsonlog or csvlog file, the logging collector keeps a .log of the same
    ///    name for what it wrote to stderr ("ending log output to stderr"): never the file that says how the server started.
    /// </remarks>
    internal string? Gap(PgLogDirectory directory, IReadOnlyList<PgLogFile> all)
    {
        if (_rotationMicros == 0)
        {
            return null;
        }
        var format = all.Any(f => f.Format == PgLogFormat.Json) ? PgLogFormat.Json
            : all.Any(f => f.Format == PgLogFormat.Csv) ? PgLogFormat.Csv
            : PgLogFormat.Stderr;
        var dated = all.Where(f => f.Format == format && f.StartMicros != null).ToList();
        var before = dated.Where(f => f.StartMicros < _end).Select(f => f.StartMicros!.Value).DefaultIfEmpty(long.MinValue).Max();
        var next = dated.Where(f => f.StartMicros >= _end).MinBy(f => f.StartMicros);
        if (next == null || before == long.MinValue || before + _rotationMicros > _start)
        {
            return null;
        }
        return StartedAfterShutdown(directory.Head(next, HeadBytes), next) ? "stopped" : "deleted";
    }

    /// <summary>
    /// Whether a file's first records include the startup process's "database system was shut down at": a LOG record whose
    /// backend_type is "startup", read by the file's own parser. Never for stderr, or a record with no backend_type.
    /// </summary>
    /// <remarks>
    /// 🔴 NEVER A SUBSTRING OF THE FILE, AND NEVER BY FIELDS THAT ARE EMPTY.
    ///    Any user can put those words in the log, in a statement's text or a RAISE LOG, and "stopped" is info where
    ///    "deleted" warns: a log deletion would hide behind a planted line. The server names the process that wrote each
    ///    record; no client sets it. A client before sign-in, or a background worker, has no user and no database too.
    /// 🔴 NEVER stderr. Its prefix is free text: the server writes the user name a client
    ///    asked for as it is, a newline included, so a refused client can begin a line of its own with any prefix at all.
    ///    jsonlog and csvlog quote their fields. Our setup sets one of the two; a stderr log someone set up themselves
    ///    reads a stopped server as "deleted", which warns.
    /// </remarks>
    internal static bool StartedAfterShutdown(string head, PgLogFile file)
    {
        if (file.Format == PgLogFormat.Stderr)
        {
            return false;
        }
        var parser = PgLogParser.For(file.Format, file.StartMicros);
        var records = new List<PgLogRecord>();
        foreach (var line in head.Split('\n').Take(StartupRecords))
        {
            parser.Feed(line.TrimEnd('\r'), records);
        }
        parser.Finish(records);
        return records.Take(StartupRecords).Any(r => r.UnreadLines == 0 && r.Level == "LOG" && r.BackendType == "startup"
            && r.Message.StartsWith(ShutDownLine, StringComparison.Ordinal));
    }

    /// <summary>How far into a file the startup lines are: "starting PostgreSQL", "listening on" (one per address), then this.</summary>
    private const int StartupRecords = 20;

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
                    parser = new StderrLogParser(PgLogFileNames.StartMicros(line.File));
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
            ["preamble"] = _preamble,
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
