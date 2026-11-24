using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Trailox.Agent.Engines.MongoDb;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// The <c>pg_log</c> rows of one instance for one window [start, end). Each record belongs to the window by its own
/// time. A record of a shipped kind (<see cref="PgRecordKinds.Shipped"/>) becomes a row, its text redacted and cut at
/// <see cref="MaxTextChars"/>; a statement record also carries its shape (<see cref="PgStatementShape"/>) as its
/// detail. pgaudit's READ and WRITE lines become statements, one row per minute and shape
/// (<see cref="PgAuditGroups"/>). Every other record is only counted. One count row per instance and window closes the
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
    private const int FlushLines = 1000;
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
    private readonly RowWriter? _rows;
    private readonly SortedDictionary<string, long> _counted = new(StringComparer.Ordinal);
    private readonly PgAuditGroups _audit;

    private long _records;
    private long _shipped;
    private long _auditRows;

    /// <summary>The bytes of the lines whose time is in the window, as the server wrote them: what it logged, not what was read.</summary>
    private long _logBytes;
    private long _outOfWindow;
    private long _unreadable;
    private long _malformed;
    private long _preamble;
    private int _files;
    private long _bytes;
    private long? _firstLogMicros;
    private int _filesGone;
    private string? _missing;

    private readonly PgLineFormat? _prefix;
    private readonly PgSessionMap? _sessions;
    private readonly IPgLogLines? _lines;
    private readonly bool _azureFile;
    private long? _readFrom;
    private int _lookBacks;

    /// <param name="shipText">
    /// The source's <c>storeRawQueryText</c>: when false, a statement record ships its command and shape, never its text.
    /// </param>
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
    /// <param name="prefix">The server's <c>log_line_prefix</c>, from the probe; null for RDS's.</param>
    /// <param name="sessions">
    /// The source's sessions, when the prefix names no user: each record takes its user from its session's sign-in line.
    /// </param>
    /// <param name="lines">Where a look-back reads the log before the window, for a session that signed in before it.</param>
    /// <param name="azureFile">The lines are Azure's server log download (<see cref="PgAzureLine"/>).</param>
    public PgWindowReader(string instance, long startMicros, long endMicros, bool shipText, RowSink sink, RowWriter? rows = null,
        bool english = true, int rotationMinutes = 0, PgLineFormat? prefix = null, PgSessionMap? sessions = null,
        IPgLogLines? lines = null, bool azureFile = false)
    {
        _prefix = prefix;
        _sessions = sessions;
        _lines = lines;
        _azureFile = azureFile;
        _instance = instance;
        _start = startMicros;
        _end = endMicros;
        _shipText = shipText;
        _english = english;
        _rotationMicros = Math.Max(0, rotationMinutes) * 60_000_000L;
        _sink = sink;
        _rows = rows;
        _audit = new PgAuditGroups(WriteAudit);
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
        if (r.Forged)
        {
            _unreadable++;
            _counted["forged"] = _counted.GetValueOrDefault("forged") + 1;
            return;
        }
        if (r.NoUser)
        {
            // The prefix names no user, no session and no process: the record cannot be told whose it is.
            if (_prefix is { KnowsUser: false })
            {
                _unreadable++;
                return;
            }
            _counted["no_user"] = _counted.GetValueOrDefault("no_user") + 1;
        }
        var kind = r.Kind;
        _audit.Advance(r.TsMicros);
        if (kind == PgRecordKind.Audit)
        {
            AddAudit(r);
            return;
        }
        if (!PgRecordKinds.Shipped(kind))
        {
            var key = kind == PgRecordKind.Other && r.Level == "LOG" && !_english ? "unrecognised" : kind.ToString().ToLowerInvariant();
            _counted[key] = _counted.GetValueOrDefault(key) + 1;
            return;
        }
        Write(r, kind);
        _shipped++;
    }

    private void AddAudit(PgLogRecord r)
    {
        if (r.HasContext)
        {
            // Written from inside a function (RAISE LOG 'AUDIT: ...'): anyone's text, never pgaudit's.
            _counted["audit_context"] = _counted.GetValueOrDefault("audit_context") + 1;
        }
        else if (PgAuditLine.Parse(r.Message) is not { } line)
        {
            _counted["audit_unreadable"] = _counted.GetValueOrDefault("audit_unreadable") + 1;
        }
        else if (!_audit.Add(r, line))
        {
            // Another class than READ and WRITE: DDL and role changes already come from log_statement.
            _counted["audit"] = _counted.GetValueOrDefault("audit") + 1;
        }
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
        _sessions?.BeginRead();
        try
        {
            foreach (var file in files)
            {
                _files++;
                var parser = PgLogParser.For(file.Format, file.StartMicros, _prefix, _azureFile);
                var before = (file.StartMicros ?? long.MinValue) < _start;
                if (before && parser is PrefixLogParser)
                {
                    // A file of any prefix that starts before the window (a daily file, a file named without a time): read
                    // from where the window's records begin, found on the times of its lines.
                    var offset = directory.OffsetOf(file, _start - StopMarginMicros, _prefix!.TimeOf);
                    await ReadLinesAsync(directory.LinesFromAsync(file, offset, ct), parser, done, ct);
                }
                // A file that starts before the window holds it at its end: its last megabyte usually covers that.
                else if ((before ? directory.Tail(file, TailBytes, _start - StopMarginMicros) : null) is { } tail)
                {
                    foreach (var line in tail)
                    {
                        _bytes += line.Length + 1;
                        parser.Feed(line, done);
                        await DrainAsync(done, ct);
                        if (Past)
                        {
                            break;
                        }
                    }
                }
                else
                {
                    await ReadLinesAsync(directory.LinesFromAsync(file, 0, ct), parser, done, ct);
                }
                await CloseAsync(parser, done, ct);
                await FlushAsync(ct);
                if (Past)
                {
                    break;
                }
            }
        }
        finally
        {
            _sessions?.EndRead();
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

    /// <summary>Feeds a file's lines to its parser until a record past the window; flushes the rows as they build up.</summary>
    private async Task ReadLinesAsync(IAsyncEnumerable<string> lines, PgLogParser parser, List<PgLogRecord> done, CancellationToken ct)
    {
        var sinceBeat = 0;
        await foreach (var line in lines.WithCancellation(ct))
        {
            _bytes += Encoding.UTF8.GetByteCount(line) + 1;
            parser.Feed(line, done);
            await DrainAsync(done, ct);
            if (Past)
            {
                break;
            }
            if (++sinceBeat % FlushLines == 0)
            {
                await FlushAsync(ct);
            }
            if (sinceBeat == HeartbeatLines)
            {
                sinceBeat = 0;
                TaskHeartbeat.Beat();
            }
        }
    }

    /// <summary>Reads the window from an RDS instance through the RDS API (stderr only).</summary>
    public async Task ReadAsync(RdsLogReader rds, CancellationToken ct)
    {
        var done = new List<PgLogRecord>();
        PgLogParser? parser = null;
        string? file = null;
        var lines = 0L;
        var inWindow = false;
        try
        {
            await foreach (var line in rds.LinesAsync(_start, _end, StopMarginMicros, StderrTime, ct))
            {
                // A continuation line belongs where the line it continues does.
                if (StderrTime(line.Text) is { } at)
                {
                    inWindow = at >= _start && at < _end;
                }
                if (inWindow)
                {
                    _logBytes += Encoding.UTF8.GetByteCount(line.Text) + 1;
                }
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
        _audit.Finish();
        var counts = new Dictionary<string, object?>
        {
            ["records"] = _records,
            ["shipped"] = _shipped,
            ["audit_lines"] = _audit.Lines,
            ["audit_rows"] = _auditRows,
            ["audit_overflow"] = _audit.OverflowLines,
            ["audit_shapeless"] = _audit.ShapelessLines,
            ["audit_late"] = _audit.LateLines,
            ["log_bytes"] = _logBytes,
            ["out_of_window"] = _outOfWindow,
            ["unreadable"] = _unreadable + _malformed,
            ["preamble"] = _preamble,
            ["counted"] = _counted,
            ["files"] = _files,
            ["bytes"] = _bytes,
            ["files_gone"] = _filesGone,
            ["look_backs"] = _lookBacks,
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
        WriteAuditColumns(w, "", Array.Empty<string>(), 1);
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

    private async ValueTask CloseAsync(PgLogParser parser, List<PgLogRecord> done, CancellationToken ct)
    {
        parser.Finish(done);
        await DrainAsync(done, ct);
        _malformed += parser.Malformed;
    }

    /// <summary>The look-backs one window makes at most; a session not found by then has no user in this window.</summary>
    public const int MaxLookBacks = 200;

    /// <summary>
    /// <see cref="Drain"/>, with each record first given what its prefix lacks from its session (<see cref="PgSessionMap"/>).
    /// A record of a session that signed in before the read waits for a look-back: one record at a time, so the rows keep
    /// the log's order.
    /// </summary>
    private async ValueTask DrainAsync(List<PgLogRecord> done, CancellationToken ct)
    {
        if (_sessions == null)
        {
            Drain(done);
            return;
        }
        foreach (var r in done)
        {
            if (_readFrom == null && r.TsMicros != long.MinValue && r.UnreadLines == 0)
            {
                _readFrom = r.TsMicros;
            }
            var outcome = _sessions.Take(r);
            if (outcome == PgSessionMap.Outcome.SessionUnknown && _lines != null && _lookBacks < MaxLookBacks
                && _sessions.LookBackFor(r, _readFrom ?? r.TsMicros) is { } range)
            {
                _lookBacks++;
                await LookBackAsync(range, ct);
                outcome = _sessions.Retake(r);
            }
            r.NoUser = outcome != PgSessionMap.Outcome.Complete;
            Add(r);
        }
        done.Clear();
    }

    /// <summary>Reads the log of <paramref name="range"/> into the sessions only: no record of it is the window's.</summary>
    private async Task LookBackAsync((long From, long To) range, CancellationToken ct)
    {
        var saved = _sessions!.BeginLookBack();
        try
        {
            var done = new List<PgLogRecord>();
            PgLogParser? parser = null;
            string? file = null;
            await foreach (var line in _lines!.LinesAsync(range.From, range.To, _prefix!.TimeOf, ct))
            {
                if (line.File != file)
                {
                    parser?.Finish(done);
                    parser = PgLogParser.For(PgLogFormat.Stderr, PgLogFileNames.StartMicros(line.File), _prefix, _azureFile);
                    file = line.File;
                }
                parser!.Feed(line.Text, done);
                foreach (var r in done)
                {
                    _sessions.Take(r);
                }
                done.Clear();
            }
            parser?.Finish(done);
            foreach (var r in done)
            {
                _sessions.Take(r);
            }
            TaskHeartbeat.Beat();
        }
        finally
        {
            _sessions.EndLookBack(saved);
        }
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
            var statement = PgRecordKinds.StatementText(r.Message);
            command = PgCommand.Of(statement);
            message = _shipText ? Cut(PgPasswordRedactor.Redact(r.Message), ref truncated) : "";
            // The DETAIL of a statement record is its bound values ("Parameters: $1 = ..."): never shipped. The column
            // carries the statement's shape instead, with or without the text, as a pgaudit row's message does.
            detail = Shape(statement, ref truncated);
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
        WriteAuditColumns(w, "", Array.Empty<string>(), 1);
        w.WriteEndObject();
        _sink.EndRow();
    }

    /// <summary>
    /// One row per minute group: the first statement's session, the shape as its text (with or without statement
    /// text), pgaudit's class and command, the objects, and how many statements. The row of a minute's overflow keeps
    /// only its count: its class is <see cref="MinuteAggregator.Other"/>.
    /// </summary>
    private void WriteAudit(MinuteGroup g)
    {
        var l = g.Labels;
        var truncated = !g.IsOther && l[PgAuditGroups.Truncated].Length > 0;
        // The shape masks every value, so it ships with or without the statement text.
        var message = g.IsOther ? "" : Cut(l[PgAuditGroups.Shape], ref truncated);
        var w = _sink.Writer;
        w.WriteStartObject();
        w.WriteString("instance"u8, _instance);
        RowSink.Time(w, "ts"u8, g.FirstMicros);
        w.WriteNumber("pid"u8, g.IsOther ? 0 : int.Parse(l[PgAuditGroups.Pid], CultureInfo.InvariantCulture));
        w.WriteString("user"u8, g.IsOther ? "" : l[PgAuditGroups.User]);
        w.WriteString("database"u8, g.IsOther ? "" : l[PgAuditGroups.Database]);
        w.WriteString("remote_host"u8, g.IsOther ? "" : l[PgAuditGroups.RemoteHost]);
        w.WriteString("remote_port"u8, g.IsOther ? "" : l[PgAuditGroups.RemotePort]);
        w.WriteString("level"u8, "LOG");
        w.WriteString("sql_state"u8, "");
        if (g.IsOther || l[PgAuditGroups.App] == PgAuditGroups.NoApp)
        {
            w.WriteNull("application_name"u8);
        }
        else
        {
            w.WriteString("application_name"u8, l[PgAuditGroups.App]);
        }
        w.WriteString("session_id"u8, g.IsOther ? "" : l[PgAuditGroups.SessionId]);
        w.WriteString("message"u8, message);
        // The shape's hash, with or without the text: it tells apart two groups that differ in their shape alone.
        w.WriteString("detail"u8, g.IsOther ? "" : l[PgAuditGroups.ShapeHash]);
        w.WriteString("command"u8, g.IsOther ? "" : l[PgAuditGroups.Command]);
        w.WriteString("format"u8, g.IsOther ? "" : l[PgAuditGroups.Format]);
        w.WriteBoolean("truncated"u8, truncated);
        WriteAuditColumns(w, g.IsOther ? MinuteAggregator.Other : l[PgAuditGroups.Class],
            g.IsOther || l[PgAuditGroups.Objects].Length == 0 ? Array.Empty<string>() : l[PgAuditGroups.Objects].Split(PgAuditGroups.ObjectSeparator),
            g.Count);
        w.WriteEndObject();
        _sink.EndRow();
        _auditRows++;
    }

    /// <summary>pgaudit's class ('' when the row is no pgaudit statement), the objects it names, and the statements it stands for.</summary>
    private static void WriteAuditColumns(Utf8JsonWriter w, string auditClass, IReadOnlyList<string> objects, long count)
    {
        w.WriteString("audit_class"u8, auditClass);
        w.WriteStartArray("objects"u8);
        foreach (var o in objects)
        {
            w.WriteStringValue(o);
        }
        w.WriteEndArray();
        w.WriteNumber("event_count"u8, count);
    }

    /// <summary>
    /// A statement record's shape (<see cref="PgStatementShape"/>), cut where a pgaudit row's is, so that a statement
    /// logged both ways has one shape.
    /// </summary>
    /// <remarks>
    /// 🔴 THE SHAPE OF THE STATEMENT AS LOGGED, NEVER OF THE REDACTED TEXT. The shape masks every string, a password's
    ///    too. The redactor reads on past a string whose end is in doubt (a backslash in a plain string), so the text
    ///    after it can hold a value as words; the shape stops there instead.
    /// </remarks>
    private static string Shape(string statement, ref bool truncated)
    {
        var shape = PgStatementShape.Of(statement);
        if (shape.Length <= PgAuditGroups.MaxShapeChars)
        {
            return shape;
        }
        truncated = true;
        return shape[..PgAuditGroups.MaxShapeChars];
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
