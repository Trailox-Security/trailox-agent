using System.IO.Hashing;
using System.Runtime.InteropServices;
using System.Text;

namespace Trailox.Agent.Bench.Pipeline;

/// <summary>The per-operation metrics of a slow-query line; audit lines carry none.</summary>
internal struct OpMetrics
{
    public long Nreturned;
    public long DocsExamined;
    public long KeysExamined;
    public long Reslen;
    public long DurationMs;
    public bool Present;
}

/// <summary>
/// One row per (minute, part, source, process, user, IP, connection, command, namespace, shape,
/// result) with a count and summed metrics (D11, PLAN §6.3). Text is materialised once per group,
/// never per line. A minute is flushed to the upload as soon as the file has moved past it; a line
/// for a minute already flushed opens the next part of that minute instead of replacing it.
/// <para>Cardinality guard: past <c>groupCap</c> groups in a (minute, part), every further line of
/// that minute goes to one "__other__" group per source with its real totals.</para>
/// </summary>
internal sealed class MinuteAggregator
{
    public const string Other = "__other__";
    private const long FlushGraceMs = 2_000;

    private readonly string _process;
    private readonly int _groupCap;
    private readonly RowSink _sink;
    private readonly Dictionary<GroupKey, Group> _groups = new();
    private readonly Dictionary<(long Minute, int Part), int> _groupsPerMinute = new();
    private readonly Dictionary<long, int> _openPart = new();
    private readonly Dictionary<ulong, string> _shapes = new();
    private readonly Stack<Group> _pool = new();
    private readonly List<GroupKey> _flushing = new();
    private long _closedBefore = long.MinValue;

    public long LateLines { get; private set; }

    public long OverflowLines { get; private set; }

    public long PeakGroups { get; private set; }

    /// <summary>Sums over the rows shipped, for the no-loss check against the generator's own sums.</summary>
    public long ShippedNreturned { get; private set; }

    public long ShippedDurationMs { get; private set; }

    public MinuteAggregator(string process, int groupCap, RowSink sink)
    {
        _process = process;
        _groupCap = groupCap;
        _sink = sink;
    }

    public void Add(long ts, byte source, ReadOnlySpan<byte> user, ReadOnlySpan<byte> db, ReadOnlySpan<byte> ip,
        ReadOnlySpan<byte> connection, ReadOnlySpan<byte> command, ReadOnlySpan<byte> ns, ulong shapeHash,
        ReadOnlySpan<byte> shape, long result, ReadOnlySpan<byte> app, in OpMetrics metrics)
    {
        var minute = Math.DivRem(ts, 60_000L, out var rem) - (rem < 0 ? 1 : 0);
        _openPart.TryGetValue(minute, out var part);
        if (part > 0)
        {
            LateLines++;
        }

        var key = new GroupKey(minute, part, source, Hash(user, db), Hash(ip), Hash(connection), Hash(command), Hash(ns), shapeHash, result);
        ref var group = ref CollectionsMarshal.GetValueRefOrAddDefault(_groups, key, out var exists);
        if (!exists)
        {
            ref var count = ref CollectionsMarshal.GetValueRefOrAddDefault(_groupsPerMinute, (minute, part), out _);
            if (count >= _groupCap)
            {
                // Over the cap: this line, and every later new key of this minute, goes to __other__.
                _groups.Remove(key);
                OverflowLines++;
                var other = new GroupKey(minute, part, source, 0, 0, 0, 0, 0, 0, -1);
                ref var otherGroup = ref CollectionsMarshal.GetValueRefOrAddDefault(_groups, other, out var otherExists);
                if (!otherExists)
                {
                    otherGroup = Rent(other, Other, Other, Other, Other, Other, Other, Other, Other, Other);
                }
                otherGroup!.Add(ts, metrics);
                return;
            }
            count++;
            group = Rent(key, Text(user), Text(db), Text(ip), Text(connection), Text(command), Text(ns),
                Shape(shapeHash, shape), shapeHash.ToString("x16"), Text(app));
            PeakGroups = Math.Max(PeakGroups, _groups.Count);
        }
        group!.Add(ts, metrics);
    }

    /// <summary>Ships every minute the file has moved past by more than the grace.</summary>
    public void FlushBefore(long maxTs)
    {
        var closedBefore = (maxTs - FlushGraceMs) / 60_000L;
        if (closedBefore <= _closedBefore)
        {
            return;
        }
        _closedBefore = closedBefore;
        Flush(k => k.Minute < closedBefore);
    }

    public void FlushAll() => Flush(_ => true);

    private void Flush(Func<GroupKey, bool> due)
    {
        _flushing.Clear();
        foreach (var key in _groups.Keys)
        {
            if (due(key))
            {
                _flushing.Add(key);
            }
        }
        foreach (var key in _flushing)
        {
            var group = _groups[key];
            _groups.Remove(key);
            Write(group);
            _groupsPerMinute.Remove((key.Minute, key.Part));
            // A later line of this minute opens the next part.
            _openPart[key.Minute] = key.Part + 1;
            _pool.Push(group);
        }
    }

    private void Write(Group g)
    {
        var w = _sink.Writer;
        w.WriteStartObject();
        w.WriteString("minute"u8, DateTimeOffset.FromUnixTimeMilliseconds(g.Key.Minute * 60_000L).UtcDateTime);
        w.WriteNumber("part"u8, g.Key.Part);
        w.WriteString("process"u8, _process);
        w.WriteString("source"u8, g.Key.Source switch { 0 => "audit"u8, 1 => "slow_query"u8, 2 => "log_signin"u8, _ => "audit_authenticate"u8 });
        w.WriteString("user"u8, g.User);
        w.WriteString("authDb"u8, g.Db);
        w.WriteString("clientIp"u8, g.Ip);
        w.WriteString("connection"u8, g.Connection);
        w.WriteString("appName"u8, g.App);
        if (g.Key.Source >= 2)
        {
            // Sign-in failures: one group per connection, so Trailox counts attempts, not lines (an
            // unknown user writes two lines per attempt). The mechanism rides in the command slot.
            w.WriteString("mechanism"u8, g.Command);
        }
        else
        {
            w.WriteString("command"u8, g.Command);
            w.WriteString("ns"u8, g.Ns);
            w.WriteString("shapeHash"u8, g.ShapeHash);
            w.WriteString("shape"u8, g.Shape);
            w.WriteNumber("shapeVersion"u8, ShapeNormalizer.Version);
        }
        w.WriteNumber("result"u8, g.Key.Result);
        w.WriteNumber("eventCount"u8, g.Count);
        w.WriteNumber("firstMs"u8, g.FirstMs);
        w.WriteNumber("lastMs"u8, g.LastMs);
        if (g.HasMetrics)
        {
            ShippedNreturned += g.Nreturned;
            ShippedDurationMs += g.DurationSum;
            w.WriteNumber("nreturned"u8, g.Nreturned);
            w.WriteNumber("docsExamined"u8, g.DocsExamined);
            w.WriteNumber("keysExamined"u8, g.KeysExamined);
            w.WriteNumber("reslen"u8, g.Reslen);
            w.WriteNumber("durationMsSum"u8, g.DurationSum);
            w.WriteNumber("durationMsMax"u8, g.DurationMax);
        }
        w.WriteEndObject();
        _sink.EndRow(g.Count);
    }

    private Group Rent(GroupKey key, string user, string db, string ip, string connection, string command, string ns, string shape, string shapeHash, string app)
    {
        var g = _pool.Count > 0 ? _pool.Pop() : new Group();
        g.Reset(key, user, db, ip, connection, command, ns, shape, shapeHash, app);
        return g;
    }

    private string Shape(ulong hash, ReadOnlySpan<byte> shape)
    {
        ref var text = ref CollectionsMarshal.GetValueRefOrAddDefault(_shapes, hash, out var known);
        if (!known)
        {
            text = Encoding.UTF8.GetString(shape);
        }
        return text!;
    }

    private static string Text(ReadOnlySpan<byte> utf8) => utf8.IsEmpty ? "" : Encoding.UTF8.GetString(utf8);

    private static ulong Hash(ReadOnlySpan<byte> a) => a.IsEmpty ? 1 : XxHash3.HashToUInt64(a);

    private static ulong Hash(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => a.IsEmpty ? 1 : XxHash3.HashToUInt64(a, (long)XxHash3.HashToUInt64(b));

    internal readonly record struct GroupKey(long Minute, int Part, byte Source, ulong User, ulong Ip, ulong Connection, ulong Command, ulong Ns, ulong Shape, long Result);

    private sealed class Group
    {
        public GroupKey Key;
        public string User = "", Db = "", Ip = "", Connection = "", Command = "", Ns = "", Shape = "", ShapeHash = "", App = "";
        public long Count, FirstMs, LastMs, Nreturned, DocsExamined, KeysExamined, Reslen, DurationSum, DurationMax;
        public bool HasMetrics;

        public void Reset(GroupKey key, string user, string db, string ip, string connection, string command, string ns, string shape, string shapeHash, string app)
        {
            Key = key;
            (User, Db, Ip, Connection, Command, Ns, Shape, ShapeHash, App) = (user, db, ip, connection, command, ns, shape, shapeHash, app);
            Count = 0;
            FirstMs = long.MaxValue;
            LastMs = long.MinValue;
            Nreturned = DocsExamined = KeysExamined = Reslen = DurationSum = DurationMax = 0;
            HasMetrics = false;
        }

        public void Add(long ts, in OpMetrics m)
        {
            Count++;
            FirstMs = Math.Min(FirstMs, ts);
            LastMs = Math.Max(LastMs, ts);
            if (m.Present)
            {
                HasMetrics = true;
                Nreturned += m.Nreturned;
                DocsExamined += m.DocsExamined;
                KeysExamined += m.KeysExamined;
                Reslen += m.Reslen;
                DurationSum += m.DurationMs;
                DurationMax = Math.Max(DurationMax, m.DurationMs);
            }
        }
    }
}
