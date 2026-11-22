using System.Buffers;
using System.Buffers.Text;
using System.Text;

namespace Trailox.Agent.Engines.MongoDb;

/// <summary>The rows of each stream: the group key of a line, the labels of a new group, and the row it ships.</summary>
public sealed partial class MongoLogProcessor
{
    // ---- rows --------------------------------------------------------------------------------------

    // Labels per stream, in the order the row writers read them.
    private const int OpsLabels = 9, SlowLabels = 6, FailureLabels = 8;

    private void AddOp(long ts, ReadOnlySpan<byte> command, ReadOnlySpan<byte> atype, ref LineFields f)
    {
        var shape = f.HasShape ? _shape.Hash : 0UL;
        _key.Clear();
        Key(atype);
        Key(f.User);
        Key(f.Db);
        Key(f.IsSystemUser ? "1"u8 : "0"u8);
        Key(f.Ip);
        Key(DirectIp(ref f));
        Key(f.Uuid);
        Key(command);
        Key(f.Ns);
        KeyNumber((long)shape);
        KeyNumber(f.Result);
        var group = _aggregator!.Group(ts, MinuteAggregator.KeyOf(_key.WrittenSpan), out var created);
        if (created)
        {
            var l = group.Labels;
            l[0] = Text(atype);
            l[1] = Text(f.User);
            l[2] = Text(f.Db);
            l[3] = f.IsSystemUser ? "1" : "0";
            l[4] = Text(f.Ip);
            l[5] = Text(DirectIp(ref f));
            l[6] = UuidText(f.Uuid);
            l[7] = Text(command);
            l[8] = Text(f.Ns);
            group.ShapeHash = shape;
            group.Shape = f.HasShape ? ShapeText(shape) : "";
            group.Result = f.Result;
        }
        group.Add(ts);
    }

    private void WriteOps(MinuteGroup g)
    {
        var w = _sink.Writer;
        var l = g.Labels;
        w.WriteStartObject();
        RowSink.Time(w, "minute"u8, g.MinuteMicros);
        w.WriteNumber("part"u8, g.Part);
        w.WriteString("process"u8, _process);
        w.WriteString("process_type"u8, _processType);
        w.WriteString("atype"u8, l[0]);
        w.WriteString("user"u8, l[1]);
        w.WriteString("auth_db"u8, l[2]);
        w.WriteNumber("is_system_user"u8, l[3] == "1" ? 1 : 0);
        w.WriteString("client_ip"u8, l[4]);
        w.WriteString("direct_ip"u8, l[5]);
        w.WriteString("connection_uuid"u8, l[6]);
        w.WriteString("command"u8, l[7]);
        w.WriteString("ns"u8, l[8]);
        w.WriteNumber("shape_hash"u8, g.IsOther ? 0UL : g.ShapeHash);
        w.WriteString("shape"u8, g.IsOther ? MinuteAggregator.Other : g.Shape);
        w.WriteNumber("shape_version"u8, MongoShape.Version);
        w.WriteNumber("result"u8, g.IsOther ? -1 : g.Result);
        w.WriteNumber("event_count"u8, g.Count);
        RowSink.Time(w, "first_ts"u8, g.FirstMicros);
        RowSink.Time(w, "last_ts"u8, g.LastMicros);
        w.WriteString("audit_filter"u8, "");
        w.WriteNumber("audit_successes"u8, -1);
        w.WriteEndObject();
        _sink.EndRow();
    }

    /// <summary>The audit startup line: one mongo_ops row that states the active audit filter (not an operation).</summary>
    private void WriteStartup(long ts, ref LineFields f)
    {
        var w = _sink.Writer;
        var minute = ts - ts % MinuteAggregator.MinuteMicros;
        w.WriteStartObject();
        RowSink.Time(w, "minute"u8, minute);
        w.WriteNumber("part"u8, 0);
        w.WriteString("process"u8, _process);
        w.WriteString("process_type"u8, _processType);
        w.WriteString("atype"u8, "startup");
        w.WriteString("user"u8, "");
        w.WriteString("auth_db"u8, "");
        w.WriteNumber("is_system_user"u8, 1);
        w.WriteString("client_ip"u8, "");
        w.WriteString("direct_ip"u8, "");
        w.WriteString("connection_uuid"u8, UuidText(f.Uuid));
        w.WriteString("command"u8, "startup");
        w.WriteString("ns"u8, "");
        w.WriteNumber("shape_hash"u8, 0UL);
        w.WriteString("shape"u8, "");
        w.WriteNumber("shape_version"u8, MongoShape.Version);
        w.WriteNumber("result"u8, f.Result);
        w.WriteNumber("event_count"u8, 1);
        RowSink.Time(w, "first_ts"u8, ts);
        RowSink.Time(w, "last_ts"u8, ts);
        w.WriteString("audit_filter"u8, f.AuditFilter);
        w.WriteNumber("audit_successes"u8, f.AuditSuccesses);
        w.WriteEndObject();
        _sink.EndRow();
    }

    private void AddSlow(long ts, ref LineFields f)
    {
        var shape = f.HasShape ? _shape.Hash : 0UL;
        var conn = ConnOf(f.Ctx);
        var ip = IpOf(f.Remote);
        var userConnection = !f.IsFromUserConnectionSet || f.IsFromUserConnection;
        _key.Clear();
        Key(conn);
        Key(ip);
        Key(f.CommandName);
        Key(f.Ns);
        KeyNumber((long)shape);
        KeyNumber(f.ErrCode);
        Key(userConnection ? "1"u8 : "0"u8);
        var group = _aggregator!.Group(ts, MinuteAggregator.KeyOf(_key.WrittenSpan), out var created);
        if (created)
        {
            var l = group.Labels;
            l[0] = Text(conn);
            l[1] = Text(ip);
            l[2] = Text(f.App);
            l[3] = Text(f.CommandName);
            l[4] = Text(f.Ns);
            l[5] = Text(f.QueryShapeHash);
            group.ShapeHash = shape;
            group.Shape = f.HasShape ? ShapeText(shape) : "";
            group.ErrCode = f.ErrCode;
            group.UserConnection = userConnection;
        }
        group.Add(ts);
        if (f.HasMetrics)
        {
            group.Nreturned += f.Nreturned;
            group.DocsExamined += f.DocsExamined;
            group.KeysExamined += f.KeysExamined;
            group.Reslen += f.Reslen;
            group.DurationSum += f.DurationMs;
            group.DurationMax = Math.Max(group.DurationMax, f.DurationMs);
        }
    }

    private void WriteSlow(MinuteGroup g)
    {
        var w = _sink.Writer;
        var l = g.Labels;
        w.WriteStartObject();
        RowSink.Time(w, "minute"u8, g.MinuteMicros);
        w.WriteNumber("part"u8, g.Part);
        w.WriteString("process"u8, _process);
        w.WriteString("process_type"u8, _processType);
        w.WriteString("conn"u8, l[0]);
        w.WriteString("client_ip"u8, l[1]);
        w.WriteString("app_name"u8, l[2]);
        w.WriteString("command"u8, l[3]);
        w.WriteString("ns"u8, l[4]);
        w.WriteNumber("shape_hash"u8, g.IsOther ? 0UL : g.ShapeHash);
        w.WriteString("shape"u8, g.IsOther ? MinuteAggregator.Other : g.Shape);
        w.WriteNumber("shape_version"u8, MongoShape.Version);
        w.WriteString("server_shape_hash"u8, g.IsOther ? "" : l[5]);
        w.WriteNumber("err_code"u8, g.IsOther ? 0 : g.ErrCode);
        w.WriteNumber("is_user_connection"u8, g.IsOther || g.UserConnection ? 1 : 0);
        w.WriteNumber("event_count"u8, g.Count);
        RowSink.Time(w, "first_ts"u8, g.FirstMicros);
        RowSink.Time(w, "last_ts"u8, g.LastMicros);
        w.WriteNumber("nreturned"u8, g.Nreturned);
        w.WriteNumber("docs_examined"u8, g.DocsExamined);
        w.WriteNumber("keys_examined"u8, g.KeysExamined);
        w.WriteNumber("reslen"u8, g.Reslen);
        w.WriteNumber("duration_ms_sum"u8, g.DurationSum);
        w.WriteNumber("duration_ms_max"u8, g.DurationMax);
        w.WriteEndObject();
        _sink.EndRow();
    }

    private void AddFailure(long ts, ref LineFields f)
    {
        var conn = ConnOf(f.Ctx);
        var ip = IpOf(f.Client);
        var reason = ReasonOf(f.Error);
        _key.Clear();
        Key(f.User);
        Key(f.Db);
        Key(ip);
        Key(f.App);
        Key(f.Driver);
        Key(f.Mechanism);
        KeyNumber(f.Result);
        Key(reason);
        Key(conn);
        var group = _aggregator!.Group(ts, MinuteAggregator.KeyOf(_key.WrittenSpan), out var created);
        if (created)
        {
            var l = group.Labels;
            l[0] = Text(f.User);
            l[1] = Text(f.Db);
            l[2] = Text(ip);
            l[3] = Text(f.App);
            l[4] = Text(f.Driver);
            l[5] = Text(f.Mechanism);
            l[6] = Text(reason);
            l[7] = Text(conn);
            group.Result = f.Result;
        }
        group.Add(ts);
    }

    private void WriteFailure(MinuteGroup g)
    {
        var w = _sink.Writer;
        var l = g.Labels;
        w.WriteStartObject();
        RowSink.Time(w, "minute"u8, g.MinuteMicros);
        w.WriteNumber("part"u8, g.Part);
        w.WriteString("process"u8, _process);
        w.WriteString("user"u8, l[0]);
        w.WriteString("auth_db"u8, l[1]);
        w.WriteString("client_ip"u8, l[2]);
        w.WriteString("app_name"u8, l[3]);
        w.WriteString("driver_name"u8, l[4]);
        w.WriteString("mechanism"u8, l[5]);
        w.WriteNumber("result"u8, g.IsOther ? -1 : g.Result);
        w.WriteString("reason"u8, l[6]);
        w.WriteString("conn"u8, l[7]);
        w.WriteNumber("event_count"u8, g.Count);
        w.WriteEndObject();
        _sink.EndRow();
    }

    /// <summary>One mongo_connections row per line: accepted, ended, signin, process_start.</summary>
    private void WriteConnection(long ts, long id, ref LineFields f)
    {
        var w = _sink.Writer;
        var address = f.Remote.IsEmpty ? f.Client : f.Remote;
        w.WriteStartObject();
        RowSink.Time(w, "ts"u8, ts);
        w.WriteString("process"u8, _process);
        w.WriteString("event"u8, id switch { 22943 => "accepted"u8, 22944 => "ended"u8, 5286306 => "signin"u8, _ => "process_start"u8 });
        w.WriteNumber("conn_id"u8, f.ConnectionId > 0 ? f.ConnectionId : ConnNumber(f.Ctx));
        w.WriteString("connection_uuid"u8, f.Uuid);
        w.WriteString("user"u8, f.User);
        w.WriteString("auth_db"u8, f.Db);
        w.WriteString("mechanism"u8, f.Mechanism);
        w.WriteString("client_ip"u8, IpOf(address));
        w.WriteNumber("client_port"u8, PortOf(address));
        w.WriteString("app_name"u8, f.App);
        w.WriteString("driver_name"u8, f.Driver);
        w.WriteString("driver_version"u8, f.DriverVersion);
        w.WriteNumber("is_cluster_member"u8, f.IsClusterMember ? 1 : 0);
        w.WriteNumber("pid"u8, f.Pid);
        w.WriteString("host"u8, f.Host);
        w.WriteEndObject();
        _sink.EndRow();
    }

    // ---- helpers -----------------------------------------------------------------------------------

    private static ReadOnlySpan<byte> Separator => [0x1F];

    private void Key(ReadOnlySpan<byte> field)
    {
        _key.Write(field);
        _key.Write(Separator);
    }

    private void KeyNumber(long value)
    {
        Span<byte> b = stackalloc byte[8];
        BitConverter.TryWriteBytes(b, value);
        _key.Write(b);
        _key.Write(Separator);
    }

    private string ShapeText(ulong hash)
    {
        if (!_shapes.TryGetValue(hash, out var text))
        {
            text = _shape.Text();
            _shapes[hash] = text;
        }
        return text;
    }

    private static string Text(ReadOnlySpan<byte> utf8) => utf8.IsEmpty ? "" : Encoding.UTF8.GetString(utf8);

    /// <summary>The audit uuid (base64 of 16 bytes) as the canonical UUID string the server log writes. BSON UUIDs are big-endian.</summary>
    internal static string UuidText(ReadOnlySpan<byte> base64)
    {
        if (base64.IsEmpty)
        {
            return "";
        }
        Span<byte> bytes = stackalloc byte[24];
        if (Base64.DecodeFromUtf8(base64, bytes, out _, out var written) != OperationStatus.Done || written != 16)
        {
            return Text(base64);
        }
        return new Guid(bytes[..16], bigEndian: true).ToString();
    }

    private static ReadOnlySpan<byte> DirectIp(ref LineFields f) => f.DirectIp.SequenceEqual(f.Ip) ? default : f.DirectIp;

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

    private static long ConnNumber(ReadOnlySpan<byte> ctx) =>
        ctx.StartsWith("conn"u8) && System.Buffers.Text.Utf8Parser.TryParse(ctx[4..], out long n, out var used) && used == ctx.Length - 4 ? n : 0;

    /// <summary>"1.2.3.4:5678" → "1.2.3.4"; "[::1]:5678" → "::1".</summary>
    internal static ReadOnlySpan<byte> IpOf(ReadOnlySpan<byte> address)
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

    private static int PortOf(ReadOnlySpan<byte> address)
    {
        var colon = address.LastIndexOf((byte)':');
        return colon > 0 && System.Buffers.Text.Utf8Parser.TryParse(address[(colon + 1)..], out int port, out var used) && used == address.Length - colon - 1 ? port : 0;
    }

    /// <summary>The error's code name ("UserNotFound", "AuthenticationFailed"), never its message, which repeats the user name.</summary>
    private static ReadOnlySpan<byte> ReasonOf(ReadOnlySpan<byte> error)
    {
        var colon = error.IndexOf((byte)':');
        var code = colon > 0 ? error[..colon] : error;
        return code.Length <= 64 && code.IndexOf((byte)' ') < 0 ? code : default;
    }
}
