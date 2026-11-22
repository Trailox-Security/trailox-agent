using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// Turns the lines of one log file into records. A record is complete when the next one starts or the file ends, so
/// call <see cref="Finish"/> after the last line of each file. One parser reads one file: a message never spans a
/// rotation.
/// </summary>
public abstract class PgLogParser
{
    /// <summary>Lines (stderr, jsonlog) or records (csvlog) that could not be read.</summary>
    public long Malformed { get; protected set; }

    /// <summary>Adds one line of the file; completed records go to <paramref name="done"/>.</summary>
    public abstract void Feed(string line, List<PgLogRecord> done);

    /// <summary>The end of the file: the record in progress is complete.</summary>
    public abstract void Finish(List<PgLogRecord> done);

    /// <param name="fileStartMicros">The time in the file's name, for stderr lines that no record follows.</param>
    public static PgLogParser For(PgLogFormat format, long? fileStartMicros = null) => format switch
    {
        PgLogFormat.Stderr => new StderrLogParser(fileStartMicros),
        PgLogFormat.Csv => new CsvLogParser(),
        PgLogFormat.Json => new JsonLogParser(),
        _ => throw new ArgumentOutOfRangeException(nameof(format)),
    };
}

/// <summary>
/// stderr with RDS's prefix <c>%t:%r:%u@%d:[%p]:</c> (the prefix our self-managed setup also prints), or RDS's other
/// choice <c>%m:%r:%u@%d:[%p]:%l:%e:%s:%v:%x:%c:%q%a:</c>. A message's DETAIL, HINT and STATEMENT are lines of their
/// own with the same pid; the backend writes them as one unit, so they follow the message in the file.
/// </summary>
/// <remarks>
/// Lines it cannot read go to the window reader as <see cref="PgLogRecord.UnreadLines"/> with a time, so each counts in
/// one window only: lines before the file's first record take that record's time (or the
/// file's, when no record follows) and are a <see cref="PgLogRecord.Preamble"/>; a record with a prefix we do not know
/// takes its own time, and its continuation lines count with it, once.
/// </remarks>
public sealed class StderrLogParser : PgLogParser
{
    private const string Levels = "DEBUG[1-5]|LOG|INFO|NOTICE|WARNING|ERROR|FATAL|PANIC|DETAIL|HINT|CONTEXT|STATEMENT|QUERY|LOCATION";

    // The time, then everything up to the first ":[pid]:". A user or database name holding ":[digits]:" would mis-split.
    private static readonly Regex Head = new(@"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d{1,6})? [A-Za-z0-9+\-]+):(.*?):\[(\d+)\]:(.*)$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline);

    private static readonly Regex DefaultTail = new($@"^({Levels}):  ?(.*)$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline);

    // %l:%e:%s:%v:%x:%c: then, for a session only, %a: (%q ends the prefix of a background process). The application name
    // may hold ':', so the level is the first ":LEVEL:  " after %c.
    private static readonly Regex LongTail = new(
        $@"^\d+:([0-9A-Z]{{5}}):\d{{4}}-\d\d-\d\d \d\d:\d\d:\d\d [A-Za-z0-9+\-]+:[^:]*:\d*:([0-9a-f]+\.[0-9a-f]+):(?:(.*?):)?({Levels}):  ?(.*)$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline);

    private enum Part
    {
        Message,
        Detail,
        Statement,
        Ignored,
    }

    private readonly long? _fileStartMicros;
    private PgLogRecord? _current;
    private Part _part;
    private bool _started;
    private int _preamble;

    /// <param name="fileStartMicros">The time in the file's name: where lines go that no record follows.</param>
    public StderrLogParser(long? fileStartMicros = null)
    {
        _fileStartMicros = fileStartMicros;
    }

    public override void Feed(string line, List<PgLogRecord> done)
    {
        var head = Head.Match(line);
        if (!head.Success)
        {
            Continue(line);
            return;
        }
        var headTs = PgTime.Micros(head.Groups[1].ValueSpan);
        if (!_started)
        {
            _started = true;
            EndPreamble(headTs, done);
        }
        var rest = head.Groups[4].Value;
        string level, text, sqlState = "", session = "";
        string? app = null;
        var m = DefaultTail.Match(rest);
        if (m.Success)
        {
            level = m.Groups[1].Value;
            text = m.Groups[2].Value;
        }
        else if ((m = LongTail.Match(rest)).Success)
        {
            sqlState = m.Groups[1].Value;
            session = m.Groups[2].Value;
            app = m.Groups[3].Success ? m.Groups[3].Value : "";
            level = m.Groups[4].Value;
            text = m.Groups[5].Value;
        }
        else
        {
            // A prefix we do not know: the record cannot be classified, but it is not part of the one before. It counts
            // in its own window; its continuation lines count with it.
            Flush(done);
            Unread(headTs, 1, preamble: false, done);
            _part = Part.Ignored;
            return;
        }
        var pid = int.Parse(head.Groups[3].ValueSpan, CultureInfo.InvariantCulture);

        if (level is "DETAIL" or "HINT" or "CONTEXT" or "STATEMENT" or "QUERY" or "LOCATION" && _current != null && _current.Pid == pid)
        {
            Attach(level, text);
            return;
        }

        Flush(done);
        // A time that cannot be read (a zone that is not UTC) is counted once, as the record's: the window reader counts a
        // record with no time as unreadable.
        var record = new PgLogRecord
        {
            TsMicros = headTs ?? long.MinValue,
            Pid = pid,
            Level = level,
            SqlState = sqlState,
            SessionId = session,
            ApplicationName = app,
            Message = text,
            Format = PgLogFormat.Stderr,
        };
        SplitRemote(head.Groups[2].Value, record);
        _current = record;
        _part = Part.Message;
    }

    public override void Finish(List<PgLogRecord> done)
    {
        Flush(done);
        if (!_started)
        {
            EndPreamble(_fileStartMicros, done);
        }
    }

    /// <summary>The lines before the file's first record, at <paramref name="ts"/> (that record's time, or the file's).</summary>
    private void EndPreamble(long? ts, List<PgLogRecord> done)
    {
        if (_preamble > 0)
        {
            Unread(ts, _preamble, preamble: true, done);
            _preamble = 0;
        }
    }

    /// <summary>
    /// Lines that are no record, counted in the window of <paramref name="ts"/>. With no time to place them, they are
    /// counted as before: in <see cref="PgLogParser.Malformed"/>, by every window that reads the file.
    /// </summary>
    private void Unread(long? ts, int lines, bool preamble, List<PgLogRecord> done)
    {
        if (ts is { } at)
        {
            done.Add(new PgLogRecord { TsMicros = at, UnreadLines = lines, Preamble = preamble, Format = PgLogFormat.Stderr });
        }
        else
        {
            Malformed += lines;
        }
    }

    private void Flush(List<PgLogRecord> done)
    {
        if (_current != null)
        {
            done.Add(_current);
            _current = null;
        }
    }

    private void Attach(string level, string text)
    {
        switch (level)
        {
            case "DETAIL":
                _current!.Detail = _current.Detail.Length == 0 ? text : _current.Detail + "\n" + text;
                _part = Part.Detail;
                break;
            case "STATEMENT":
                _current!.Statement = text;
                _part = Part.Statement;
                break;
            case "CONTEXT":
                _current!.HasContext = true;
                _part = Part.Ignored;
                break;
            default:
                _part = Part.Ignored;
                break;
        }
    }

    /// <summary>
    /// A line without the prefix continues the part before it: a statement over several lines, a DETAIL's second line.
    /// Before the file's first record it is preamble; after a record we cannot read, that record's own text.
    /// </summary>
    private void Continue(string line)
    {
        if (_current == null)
        {
            if (!_started && line.Length > 0)
            {
                _preamble++;
            }
            return;
        }
        switch (_part)
        {
            case Part.Message:
                _current.Message += "\n" + line;
                break;
            case Part.Detail:
                _current.Detail += "\n" + line;
                break;
            case Part.Statement:
                _current.Statement += "\n" + line;
                break;
        }
    }

    /// <summary>
    /// <c>%r:%u@%d</c>: <c>address(port)</c>, <c>[local]</c> or nothing, then <c>user@database</c>. An IPv6 address holds
    /// colons, so the address ends at its <c>)</c>. A user name may hold '@' (IAM and e-mail-like names), so user and
    /// database split at the last '@'.
    /// </summary>
    private static void SplitRemote(string head, PgLogRecord r)
    {
        string rest;
        if (head.StartsWith(':'))
        {
            rest = head[1..];
        }
        else if (head.StartsWith("[local]:", StringComparison.Ordinal))
        {
            r.RemoteHost = "[local]";
            rest = head["[local]:".Length..];
        }
        else
        {
            var close = head.IndexOf("):", StringComparison.Ordinal);
            if (close > 0 && head.LastIndexOf('(', close) is var open and > 0)
            {
                r.RemoteHost = head[..open];
                r.RemotePort = head[(open + 1)..close];
                rest = head[(close + 2)..];
            }
            else
            {
                var colon = head.IndexOf(':');
                r.RemoteHost = colon < 0 ? head : head[..colon];
                rest = colon < 0 ? "" : head[(colon + 1)..];
            }
        }
        var at = rest.LastIndexOf('@');
        r.User = at < 0 ? rest : rest[..at];
        r.Database = at < 0 ? "" : rest[(at + 1)..];
    }
}

/// <summary>
/// csvlog (13-18): RFC 4180 records, whose quoted fields can span lines and be megabytes long. PostgreSQL 13 writes 24
/// columns, 14-18 write 26; the first 23 are read.
/// </summary>
public sealed class CsvLogParser : PgLogParser
{
    private const int MinColumns = 23;

    private readonly List<string> _fields = new();
    private readonly StringBuilder _field = new();
    private bool _inQuotes;

    public override void Feed(string line, List<PgLogRecord> done)
    {
        for (var i = 0; i <= line.Length; i++)
        {
            // The line's end is a newline: inside quotes it belongs to the field, outside it ends the record.
            var ch = i < line.Length ? line[i] : '\n';
            if (_inQuotes)
            {
                if (ch != '"')
                {
                    _field.Append(ch);
                }
                else if (i + 1 < line.Length && line[i + 1] == '"')
                {
                    _field.Append('"');
                    i++;
                }
                else
                {
                    _inQuotes = false;
                }
            }
            else if (ch == '"' && _field.Length == 0)
            {
                _inQuotes = true;
            }
            else if (ch == ',')
            {
                EndField();
            }
            else if (ch == '\n')
            {
                EndField();
                EndRecord(done);
            }
            else
            {
                _field.Append(ch);
            }
        }
    }

    public override void Finish(List<PgLogRecord> done)
    {
        if (_inQuotes || _fields.Count > 0 || _field.Length > 0)
        {
            // A record cut by the end of the file: the server was still writing it.
            Malformed++;
        }
        _fields.Clear();
        _field.Clear();
        _inQuotes = false;
    }

    private void EndField()
    {
        _fields.Add(_field.ToString());
        _field.Clear();
    }

    private void EndRecord(List<PgLogRecord> done)
    {
        // An empty line is no record.
        var empty = _fields.Count == 1 && _fields[0].Length == 0;
        if (!empty)
        {
            if (ToRecord(_fields) is { } record)
            {
                done.Add(record);
            }
            else
            {
                Malformed++;
            }
        }
        _fields.Clear();
    }

    /// <summary>The record of one row's fields; null when it has too few columns, or no readable time or pid.</summary>
    private static PgLogRecord? ToRecord(List<string> f)
    {
        if (f.Count < MinColumns)
        {
            return null;
        }
        var ts = PgTime.Micros(f[0]);
        if (ts == null || !int.TryParse(f[3], NumberStyles.None, CultureInfo.InvariantCulture, out var pid))
        {
            return null;
        }
        var record = new PgLogRecord
        {
            TsMicros = ts.Value,
            User = f[1],
            Database = f[2],
            Pid = pid,
            SessionId = f[5],
            Level = f[11],
            SqlState = f[12],
            Message = f[13],
            Detail = f[14],
            Statement = f[19],
            ApplicationName = f[22],
            BackendType = f.Count > 23 ? f[23] : "",
            HasContext = f[18].Length > 0,
            Format = PgLogFormat.Csv,
        };
        SplitConnectionFrom(f[4], record);
        return record;
    }

    /// <summary>
    /// <c>connection_from</c> is <c>host:port</c>, or <c>[local]</c>; an IPv6 host holds colons, so the port is after the
    /// last one.
    /// </summary>
    private static void SplitConnectionFrom(string from, PgLogRecord r)
    {
        var colon = from.LastIndexOf(':');
        if (colon > 0 && colon < from.Length - 1 && !from.AsSpan(colon + 1).ContainsAnyExceptInRange('0', '9'))
        {
            r.RemoteHost = from[..colon];
            r.RemotePort = from[(colon + 1)..];
        }
        else
        {
            r.RemoteHost = from;
        }
    }
}

/// <summary>jsonlog (15-18): one object per line. A key with no value is left out, <c>application_name</c> included.</summary>
public sealed class JsonLogParser : PgLogParser
{
    public override void Feed(string line, List<PgLogRecord> done)
    {
        if (string.IsNullOrWhiteSpace(line))
        {
            return;
        }
        try
        {
            using var doc = JsonDocument.Parse(line);
            var o = doc.RootElement;
            var ts = PgTime.Micros(Str(o, "timestamp"));
            if (ts == null)
            {
                Malformed++;
                return;
            }
            done.Add(new PgLogRecord
            {
                TsMicros = ts.Value,
                User = Str(o, "user"),
                Database = Str(o, "dbname"),
                Pid = o.TryGetProperty("pid", out var pid) && pid.TryGetInt32(out var p) ? p : 0,
                RemoteHost = Str(o, "remote_host"),
                RemotePort = o.TryGetProperty("remote_port", out var port) ? port.ToString() : "",
                SessionId = Str(o, "session_id"),
                Level = Str(o, "error_severity"),
                SqlState = Str(o, "state_code"),
                Message = Str(o, "message"),
                Detail = Str(o, "detail"),
                Statement = Str(o, "statement"),
                ApplicationName = Str(o, "application_name"),
                BackendType = Str(o, "backend_type"),
                HasContext = Str(o, "context").Length > 0,
                Format = PgLogFormat.Json,
            });
        }
        catch (JsonException)
        {
            Malformed++;
        }
    }

    public override void Finish(List<PgLogRecord> done)
    {
    }

    private static string Str(JsonElement o, string key) =>
        o.TryGetProperty(key, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() ?? "" : "";
}
