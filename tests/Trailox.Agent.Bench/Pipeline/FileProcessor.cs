using System.Text.Json;

namespace Trailox.Agent.Bench.Pipeline;

/// <summary>What one pass did with every line: each line read lands in exactly one bucket.</summary>
internal sealed class Counts
{
    public long LinesIn, OutOfWindow, Filtered, Malformed, Ops, SlowOps, Connections, SigninFailures;
    public long LateLines, OverflowLines, PeakGroups, RawBytesIn, GzipBytesIn;
    public long OpsRows, OpsEvents, SlowRows, SlowEvents, ConnectionRows, FailureRows, FailureEvents, RawBytesOut, WireBytesOut;
    public long SlowNreturned, SlowDurationMs;

    public long Accounted => OutOfWindow + Filtered + Malformed + Ops + SlowOps + Connections + SigninFailures;

    public void Add(Counts o)
    {
        LinesIn += o.LinesIn; OutOfWindow += o.OutOfWindow; Filtered += o.Filtered; Malformed += o.Malformed; Ops += o.Ops;
        SlowOps += o.SlowOps; Connections += o.Connections; SigninFailures += o.SigninFailures; LateLines += o.LateLines;
        OverflowLines += o.OverflowLines; PeakGroups += o.PeakGroups; RawBytesIn += o.RawBytesIn; GzipBytesIn += o.GzipBytesIn;
        OpsRows += o.OpsRows; OpsEvents += o.OpsEvents; SlowRows += o.SlowRows; SlowEvents += o.SlowEvents;
        ConnectionRows += o.ConnectionRows; FailureRows += o.FailureRows; FailureEvents += o.FailureEvents;
        RawBytesOut += o.RawBytesOut; WireBytesOut += o.WireBytesOut;
        SlowNreturned += o.SlowNreturned; SlowDurationMs += o.SlowDurationMs;
    }
}

/// <summary>Lines processed so far by every reader, for the meter and the pacer.</summary>
internal static class Progress
{
    private static long _lines;

    public static long Lines => Interlocked.Read(ref _lines);

    public static void Add(long lines) => Interlocked.Add(ref _lines, lines);
}

/// <summary>Holds the readers to a line rate, for the sustained run (null = as fast as possible).</summary>
internal sealed class Pacer
{
    private readonly double _linesPerSecond;
    private readonly long _startLines;
    private readonly long _startTicks = Environment.TickCount64;

    public Pacer(double linesPerSecond)
    {
        _linesPerSecond = linesPerSecond;
        _startLines = Progress.Lines;
    }

    public void Wait()
    {
        var due = (Progress.Lines - _startLines) / _linesPerSecond * 1000;
        var ahead = due - (Environment.TickCount64 - _startTicks);
        if (ahead > 1)
        {
            Thread.Sleep((int)ahead);
        }
    }
}

/// <summary>Fields of one server-log or audit line, as spans into the line or the unescape scratch.</summary>
internal ref struct LineFields
{
    public ReadOnlySpan<byte> Ctx, Ns, App, Remote, Client, CommandName, User, Db, Mechanism, Uuid, Host, Driver, DriverVersion, Ip;
    public long ConnectionId, Pid, Result, ErrCode;
    public bool IsClusterMember, HasShape;
    public OpMetrics Metrics;
}

/// <summary>
/// The prototype pipeline over one node's file: gunzip → window check and id/atype filter on the raw
/// bytes → Utf8JsonReader over the lines kept → shape → per-minute aggregates and connection rows.
/// </summary>
internal sealed class FileProcessor : IDisposable
{
    private const int Batch = 4096;

    private static readonly byte[][] DdlTypes =
    [
        "createCollection"u8.ToArray(), "dropCollection"u8.ToArray(), "createDatabase"u8.ToArray(), "dropDatabase"u8.ToArray(),
        "createIndex"u8.ToArray(), "dropIndex"u8.ToArray(), "renameCollection"u8.ToArray(), "createUser"u8.ToArray(),
        "dropUser"u8.ToArray(), "updateUser"u8.ToArray(), "grantRolesToUser"u8.ToArray(), "revokeRolesFromUser"u8.ToArray(),
        "createRole"u8.ToArray(), "dropRole"u8.ToArray(), "updateRole"u8.ToArray(), "grantPrivilegesToRole"u8.ToArray(),
        "revokePrivilegesFromRole"u8.ToArray(), "dropAllUsersFromDatabase"u8.ToArray(), "dropAllRolesFromDatabase"u8.ToArray(),
    ];

    private readonly string _process;
    private readonly long _windowStart;
    private readonly long _windowEnd;
    private readonly Pacer? _pacer;
    private readonly ShapeNormalizer _shape = new();
    private readonly MinuteAggregator _ops;
    private readonly MinuteAggregator _slow;
    private readonly MinuteAggregator _failures;
    private readonly RowSink _opsSink, _slowSink, _connectionSink, _failureSink;
    private readonly Counts _c = new();
    private byte[] _scratch = new byte[1 << 16];
    private int _scratchUsed;
    private long _maxTs = long.MinValue;
    private bool _parseOnly;

    public FileProcessor(string process, long windowStart, long windowEnd, int groupCap, Pacer? pacer, bool captureRows)
    {
        _process = process;
        _windowStart = windowStart;
        _windowEnd = windowEnd;
        _pacer = pacer;
        _opsSink = new RowSink("mongo_ops", captureRows);
        _slowSink = new RowSink("mongo_slow_ops", captureRows);
        _connectionSink = new RowSink("mongo_connections", captureRows);
        _failureSink = new RowSink("mongo_signin_failures", captureRows);
        _ops = new MinuteAggregator(process, groupCap, _opsSink);
        _slow = new MinuteAggregator(process, groupCap, _slowSink);
        _failures = new MinuteAggregator(process, groupCap, _failureSink);
    }

    public IEnumerable<RowSink> Sinks => [_opsSink, _slowSink, _connectionSink, _failureSink];

    /// <summary>Stops each line after this stage, to split the cost: gunzip (read lines only), prefilter
    /// (window check and id/atype on the raw bytes), parse (+ field extraction and shape), full (+ aggregation
    /// and rows; the only one verified).</summary>
    public string Stage { get; init; } = "full";

    public void Read(string path)
    {
        using var reader = new GzipLineReader(path);
        var batch = 0;
        var stage = Stage == "gunzip" ? 0 : Stage == "prefilter" ? 1 : 2;
        _parseOnly = Stage == "parse";
        while (reader.TryReadLine(out var line))
        {
            if (stage == 0)
            {
                _c.LinesIn++;
            }
            else if (stage == 1)
            {
                Prefilter(line);
            }
            else
            {
                ProcessLine(line);
            }
            if (++batch == Batch)
            {
                batch = 0;
                Progress.Add(Batch);
                _pacer?.Wait();
                _ops.FlushBefore(_maxTs);
                _slow.FlushBefore(_maxTs);
            }
        }
        Progress.Add(batch);
        _c.RawBytesIn += reader.RawBytes;
        _c.GzipBytesIn += reader.CompressedBytes;
    }

    /// <summary>Ships what is left and returns the counts; the sinks are complete after this.</summary>
    public Counts Finish()
    {
        _ops.FlushAll();
        _slow.FlushAll();
        _failures.FlushAll();
        _c.LateLines = _ops.LateLines + _slow.LateLines + _failures.LateLines;
        _c.OverflowLines = _ops.OverflowLines + _slow.OverflowLines + _failures.OverflowLines;
        _c.PeakGroups = _ops.PeakGroups + _slow.PeakGroups + _failures.PeakGroups;
        _c.SlowNreturned = _slow.ShippedNreturned;
        _c.SlowDurationMs = _slow.ShippedDurationMs;
        foreach (var sink in Sinks)
        {
            sink.Writer.Flush();
        }
        Dispose();
        _c.OpsRows = _opsSink.Rows; _c.OpsEvents = _opsSink.EventCount;
        _c.SlowRows = _slowSink.Rows; _c.SlowEvents = _slowSink.EventCount;
        _c.ConnectionRows = _connectionSink.Rows;
        _c.FailureRows = _failureSink.Rows; _c.FailureEvents = _failureSink.EventCount;
        _c.RawBytesOut = Sinks.Sum(s => s.RawBytes);
        _c.WireBytesOut = Sinks.Sum(s => s.WireBytes);
        return _c;
    }

    public void Dispose()
    {
        foreach (var sink in Sinks)
        {
            sink.Dispose();
        }
    }

    private void ProcessLine(ReadOnlySpan<byte> line)
    {
        _c.LinesIn++;
        _scratchUsed = 0;
        var audit = line.Length > 10 && line[..Math.Min(24, line.Length)].IndexOf("\"atype\""u8) >= 0;
        if (!LineScan.TryTimestamp(line, audit, out var ts))
        {
            _c.Malformed++;
            return;
        }
        if (ts < _windowStart || ts >= _windowEnd)
        {
            _c.OutOfWindow++;
            return;
        }
        _maxTs = Math.Max(_maxTs, ts);
        try
        {
            if (audit)
            {
                ProcessAudit(line, ts);
            }
            else
            {
                ProcessServer(line, ts);
            }
        }
        catch (JsonException)
        {
            // Parsing happens before anything is counted or aggregated, so a bad line is counted once, here.
            _c.Malformed++;
        }
    }

    private void Prefilter(ReadOnlySpan<byte> line)
    {
        _c.LinesIn++;
        var audit = line.Length > 10 && line[..Math.Min(24, line.Length)].IndexOf("\"atype\""u8) >= 0;
        if (!LineScan.TryTimestamp(line, audit, out var ts) || ts < _windowStart || ts >= _windowEnd)
        {
            return;
        }
        // The results feed a counter only so that the JIT cannot drop the work; the stage is not verified.
        if (audit)
        {
            _c.Filtered += LineScan.QuotedAt(line, LineScan.ValueOf(line, "\"atype\""u8)).Length;
        }
        else if (LineScan.TryInt(line, Math.Max(0, LineScan.ValueOf(line, "\"id\""u8)), out var id))
        {
            _c.Filtered += id & 1;
        }
    }

    private void ProcessServer(ReadOnlySpan<byte> line, long ts)
    {
        var at = LineScan.ValueOf(line, "\"id\""u8);
        if (at < 0 || !LineScan.TryInt(line, at, out var id))
        {
            _c.Malformed++;
            return;
        }
        if (id is not (51803 or 5286306 or 5286307 or 22943 or 22944 or 4615611))
        {
            _c.Filtered++;
            return;
        }
        var f = new LineFields();
        ParseServer(line, ref f);
        if (_parseOnly)
        {
            return;
        }
        switch (id)
        {
            case 51803:
                _c.SlowOps++;
                _slow.Add(ts, 1, default, default, IpOf(f.Remote), ConnOf(f.Ctx), f.CommandName, f.Ns,
                    f.HasShape ? _shape.Hash : 0, f.HasShape ? _shape.Shape : default, f.ErrCode, f.App, f.Metrics);
                break;
            case 5286307:
                _c.SigninFailures++;
                _failures.Add(ts, 2, f.User, f.Db, IpOf(f.Client), ConnOf(f.Ctx), f.Mechanism, default, 0, default, f.Result, f.App, default);
                break;
            default:
                _c.Connections++;
                WriteConnection(ts, id switch { 5286306 => "signin"u8, 22943 => "accepted"u8, 22944 => "ended"u8, _ => "process_start"u8 }, ref f);
                break;
        }
    }

    private void ProcessAudit(ReadOnlySpan<byte> line, long ts)
    {
        var atype = LineScan.QuotedAt(line, LineScan.ValueOf(line, "\"atype\""u8));
        var isOp = atype.SequenceEqual("authCheck"u8);
        var isDdl = !isOp && IsDdl(atype);
        var isAuth = atype.SequenceEqual("authenticate"u8);
        var isMetadata = atype.SequenceEqual("clientMetadata"u8);
        if (!(isOp || isDdl || isAuth || isMetadata))
        {
            _c.Filtered++;
            return;
        }
        var f = new LineFields();
        ParseAudit(line, ref f);
        if (_parseOnly)
        {
            return;
        }
        if (isOp || isDdl)
        {
            _c.Ops++;
            _ops.Add(ts, 0, f.User, f.Db, f.Ip, f.Uuid, isOp ? f.CommandName : atype, f.Ns,
                f.HasShape ? _shape.Hash : 0, f.HasShape ? _shape.Shape : default, f.Result, default, default);
        }
        else if (isAuth && f.Result != 0)
        {
            _c.SigninFailures++;
            _failures.Add(ts, 3, f.User, f.Db, f.Ip, f.Uuid, f.Mechanism, default, 0, default, f.Result, default, default);
        }
        else
        {
            _c.Connections++;
            WriteConnection(ts, isAuth ? "audit_authenticate"u8 : "audit_client_metadata"u8, ref f);
        }
    }

    private void ParseServer(ReadOnlySpan<byte> line, ref LineFields f)
    {
        var r = new Utf8JsonReader(line);
        r.Read();
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            if (name.SequenceEqual("ctx"u8))
            {
                f.Ctx = Str(ref r);
            }
            else if (name.SequenceEqual("attr"u8) && r.TokenType == JsonTokenType.StartObject)
            {
                ParseServerAttr(ref r, ref f);
            }
            else
            {
                r.Skip();
            }
        }
    }

    private void ParseServerAttr(scoped ref Utf8JsonReader r, ref LineFields f)
    {
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            if (r.TokenType == JsonTokenType.Number)
            {
                var v = r.GetInt64Lenient();
                if (name.SequenceEqual("nreturned"u8)) { f.Metrics.Nreturned = v; f.Metrics.Present = true; }
                else if (name.SequenceEqual("docsExamined"u8)) { f.Metrics.DocsExamined = v; }
                else if (name.SequenceEqual("keysExamined"u8)) { f.Metrics.KeysExamined = v; }
                else if (name.SequenceEqual("reslen"u8)) { f.Metrics.Reslen = v; f.Metrics.Present = true; }
                else if (name.SequenceEqual("durationMillis"u8)) { f.Metrics.DurationMs = v; f.Metrics.Present = true; }
                else if (name.SequenceEqual("errCode"u8)) { f.ErrCode = v; }
                else if (name.SequenceEqual("connectionId"u8)) { f.ConnectionId = v; }
                else if (name.SequenceEqual("pid"u8)) { f.Pid = v; }
                else if (name.SequenceEqual("result"u8)) { f.Result = v; }
            }
            else if (r.TokenType == JsonTokenType.String)
            {
                if (name.SequenceEqual("ns"u8)) { f.Ns = Str(ref r); }
                else if (name.SequenceEqual("appName"u8)) { f.App = Str(ref r); }
                else if (name.SequenceEqual("remote"u8)) { f.Remote = Str(ref r); }
                else if (name.SequenceEqual("client"u8)) { f.Client = Str(ref r); }
                else if (name.SequenceEqual("user"u8)) { f.User = Str(ref r); }
                else if (name.SequenceEqual("db"u8)) { f.Db = Str(ref r); }
                else if (name.SequenceEqual("mechanism"u8)) { f.Mechanism = Str(ref r); }
                else if (name.SequenceEqual("host"u8)) { f.Host = Str(ref r); }
            }
            else if (r.TokenType is JsonTokenType.True or JsonTokenType.False)
            {
                if (name.SequenceEqual("isClusterMember"u8)) { f.IsClusterMember = r.TokenType == JsonTokenType.True; }
            }
            else if (r.TokenType == JsonTokenType.StartObject)
            {
                if (name.SequenceEqual("command"u8)) { Command(ref r, ref f); }
                else if (name.SequenceEqual("doc"u8)) { ClientDoc(ref r, ref f); }
                else if (name.SequenceEqual("uuid"u8)) { f.Uuid = Nested(ref r, "uuid"u8, "$uuid"u8); }
                else { r.Skip(); }
            }
            else
            {
                r.Skip();
            }
        }
    }

    private void ParseAudit(ReadOnlySpan<byte> line, ref LineFields f)
    {
        var r = new Utf8JsonReader(line);
        r.Read();
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            if (name.SequenceEqual("uuid"u8) && r.TokenType == JsonTokenType.StartObject)
            {
                f.Uuid = Member(ref r, "$binary"u8);
            }
            else if (name.SequenceEqual("remote"u8) && r.TokenType == JsonTokenType.StartObject)
            {
                f.Ip = Member(ref r, "ip"u8);
            }
            else if (name.SequenceEqual("users"u8) && r.TokenType == JsonTokenType.StartArray)
            {
                // The first authenticated user; more than one is legacy multi-user auth.
                if (r.Read() && r.TokenType == JsonTokenType.StartObject)
                {
                    UserAndDb(ref r, ref f);
                }
                while (r.TokenType != JsonTokenType.EndArray && r.Read())
                {
                    r.Skip();
                }
            }
            else if (name.SequenceEqual("param"u8) && r.TokenType == JsonTokenType.StartObject)
            {
                ParseParam(ref r, ref f);
            }
            else if (name.SequenceEqual("result"u8) && r.TokenType == JsonTokenType.Number)
            {
                f.Result = r.GetInt64Lenient();
            }
            else
            {
                r.Skip();
            }
        }
    }

    private void ParseParam(scoped ref Utf8JsonReader r, ref LineFields f)
    {
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            if (name.SequenceEqual("args"u8) && r.TokenType == JsonTokenType.StartObject) { Command(ref r, ref f); }
            else if (r.TokenType == JsonTokenType.String && name.SequenceEqual("command"u8)) { f.CommandName = Str(ref r); }
            else if (r.TokenType == JsonTokenType.String && name.SequenceEqual("ns"u8)) { f.Ns = Str(ref r); }
            else if (r.TokenType == JsonTokenType.String && name.SequenceEqual("user"u8)) { f.User = Str(ref r); }
            else if (r.TokenType == JsonTokenType.String && name.SequenceEqual("db"u8)) { f.Db = Str(ref r); }
            else if (r.TokenType == JsonTokenType.String && name.SequenceEqual("mechanism"u8)) { f.Mechanism = Str(ref r); }
            else if (r.TokenType == JsonTokenType.StartObject && name.SequenceEqual("clientMetadata"u8)) { ClientDoc(ref r, ref f); }
            else { r.Skip(); }
        }
    }

    /// <summary>The command document: its name (first key) and its shape.</summary>
    private void Command(scoped ref Utf8JsonReader r, ref LineFields f)
    {
        var peek = r;
        if (f.CommandName.IsEmpty && peek.Read() && peek.TokenType == JsonTokenType.PropertyName)
        {
            f.CommandName = peek.ValueSpan;
        }
        _shape.Normalize(ref r);
        f.HasShape = true;
    }

    /// <summary>Client metadata: application.name, driver.name, driver.version.</summary>
    private void ClientDoc(scoped ref Utf8JsonReader r, ref LineFields f)
    {
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            if (r.TokenType != JsonTokenType.StartObject)
            {
                r.Skip();
                continue;
            }
            if (name.SequenceEqual("application"u8))
            {
                f.App = Member(ref r, "name"u8);
            }
            else if (name.SequenceEqual("driver"u8))
            {
                while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
                {
                    var key = r.ValueSpan;
                    r.Read();
                    if (r.TokenType == JsonTokenType.String && key.SequenceEqual("name"u8)) { f.Driver = Str(ref r); }
                    else if (r.TokenType == JsonTokenType.String && key.SequenceEqual("version"u8)) { f.DriverVersion = Str(ref r); }
                    else { r.Skip(); }
                }
            }
            else
            {
                r.Skip();
            }
        }
    }

    private void UserAndDb(scoped ref Utf8JsonReader r, ref LineFields f)
    {
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var key = r.ValueSpan;
            r.Read();
            if (r.TokenType == JsonTokenType.String && key.SequenceEqual("user"u8)) { f.User = Str(ref r); }
            else if (r.TokenType == JsonTokenType.String && key.SequenceEqual("db"u8)) { f.Db = Str(ref r); }
            else { r.Skip(); }
        }
    }

    /// <summary>The string member <paramref name="key"/> of the object the reader is on; skips the rest of it.</summary>
    private ReadOnlySpan<byte> Member(scoped ref Utf8JsonReader r, ReadOnlySpan<byte> key)
    {
        ReadOnlySpan<byte> value = default;
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            if (r.TokenType == JsonTokenType.String && name.SequenceEqual(key))
            {
                value = Str(ref r);
            }
            else
            {
                r.Skip();
            }
        }
        return value;
    }

    /// <summary>{"outer": {"inner": "value"}} on the object the reader is on.</summary>
    private ReadOnlySpan<byte> Nested(scoped ref Utf8JsonReader r, ReadOnlySpan<byte> outer, ReadOnlySpan<byte> inner)
    {
        ReadOnlySpan<byte> value = default;
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            if (r.TokenType == JsonTokenType.StartObject && name.SequenceEqual(outer))
            {
                value = Member(ref r, inner);
            }
            else
            {
                r.Skip();
            }
        }
        return value;
    }

    /// <summary>The string token's text: the raw span when it has no escapes, else unescaped into the line's scratch.</summary>
    private ReadOnlySpan<byte> Str(scoped ref Utf8JsonReader r)
    {
        if (!r.ValueIsEscaped)
        {
            return r.ValueSpan;
        }
        var need = r.ValueSpan.Length;
        if (_scratchUsed + need > _scratch.Length)
        {
            // Earlier spans of this line keep pointing at the old array, which stays alive through them.
            _scratch = new byte[Math.Max(_scratch.Length * 2, need)];
            _scratchUsed = 0;
        }
        var written = r.CopyString(_scratch.AsSpan(_scratchUsed));
        var text = _scratch.AsSpan(_scratchUsed, written);
        _scratchUsed += written;
        return text;
    }

    private void WriteConnection(long ts, ReadOnlySpan<byte> evt, ref LineFields f)
    {
        var w = _connectionSink.Writer;
        w.WriteStartObject();
        w.WriteNumber("ts"u8, ts);
        w.WriteString("process"u8, _process);
        w.WriteString("event"u8, evt);
        w.WriteString("connection"u8, f.ConnectionId > 0 ? default : ConnOf(f.Ctx));
        if (f.ConnectionId > 0)
        {
            w.WriteNumber("connectionId"u8, f.ConnectionId);
        }
        w.WriteString("uuid"u8, f.Uuid);
        w.WriteString("user"u8, f.User);
        w.WriteString("authDb"u8, f.Db);
        w.WriteString("mechanism"u8, f.Mechanism);
        w.WriteString("clientIp"u8, IpOf(f.Remote.IsEmpty ? f.Client : f.Remote));
        w.WriteString("ip"u8, f.Ip);
        w.WriteString("appName"u8, f.App);
        w.WriteString("driver"u8, f.Driver);
        w.WriteString("driverVersion"u8, f.DriverVersion);
        w.WriteBoolean("isClusterMember"u8, f.IsClusterMember);
        if (f.Pid > 0)
        {
            w.WriteNumber("pid"u8, f.Pid);
            w.WriteString("host"u8, f.Host);
        }
        w.WriteEndObject();
        _connectionSink.EndRow(1);
    }

    private static bool IsDdl(ReadOnlySpan<byte> atype)
    {
        foreach (var t in DdlTypes)
        {
            if (atype.SequenceEqual(t))
            {
                return true;
            }
        }
        return false;
    }

    /// <summary>"conn123" → "123"; an internal thread's name stays as it is.</summary>
    private static ReadOnlySpan<byte> ConnOf(ReadOnlySpan<byte> ctx) => ctx.StartsWith("conn"u8) ? ctx[4..] : ctx;

    /// <summary>"1.2.3.4:5678" → "1.2.3.4"; "[::1]:5678" → "::1".</summary>
    private static ReadOnlySpan<byte> IpOf(ReadOnlySpan<byte> address)
    {
        if (address.IsEmpty)
        {
            return address;
        }
        if (address[0] == (byte)'[')
        {
            var close = address.IndexOf((byte)']');
            return close > 0 ? address[1..close] : address;
        }
        var colon = address.LastIndexOf((byte)':');
        return colon > 0 && address[..colon].IndexOf((byte)':') < 0 ? address[..colon] : address;
    }
}

internal static class JsonReaderExtensions
{
    /// <summary>An integer, or a double truncated (mongod writes some counters as doubles).</summary>
    public static long GetInt64Lenient(this ref Utf8JsonReader r) => r.TryGetInt64(out var v) ? v : (long)r.GetDouble();
}
