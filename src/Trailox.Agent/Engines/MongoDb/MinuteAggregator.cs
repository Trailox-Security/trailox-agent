using System.IO.Hashing;
using System.Runtime.InteropServices;

namespace Trailox.Agent.Engines.MongoDb;

/// <summary>One group of one minute: the lines that share every key field, counted, with their metrics summed.</summary>
public sealed class MinuteGroup
{
    public long MinuteMicros;
    public int Part;
    public bool IsOther;
    public string[] Labels = Array.Empty<string>();
    public long Count, FirstMicros, LastMicros;
    public long Nreturned, DocsExamined, KeysExamined, Reslen, DurationSum, DurationMax;
    public ulong ShapeHash;
    public string Shape = "";

    /// <summary>Key fields kept as numbers: the audit result, a slow query's error code, whether it ran on a client connection.</summary>
    public long Result, ErrCode;
    public bool UserConnection;

    internal void Reset(long minute, int part, int labelCount)
    {
        MinuteMicros = minute;
        Part = part;
        IsOther = false;
        if (Labels.Length != labelCount)
        {
            Labels = new string[labelCount];
        }
        Array.Fill(Labels, "");
        Count = 0;
        FirstMicros = long.MaxValue;
        LastMicros = long.MinValue;
        Nreturned = DocsExamined = KeysExamined = Reslen = DurationSum = DurationMax = 0;
        ShapeHash = 0;
        Shape = "";
        Result = ErrCode = 0;
        UserConnection = true;
    }

    public void Add(long tsMicros)
    {
        Count++;
        if (tsMicros < FirstMicros)
        {
            FirstMicros = tsMicros;
        }
        if (tsMicros > LastMicros)
        {
            LastMicros = tsMicros;
        }
    }
}

/// <summary>
/// One row per minute and key (D11): the caller hashes every key field of a line into one 128-bit key,
/// and fills a group's labels only when the group is new, so text is made once per group, never per line.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>A minute ships as soon as the file is 2 seconds past it (measured disorder: at most 0.1 s), which
/// keeps memory flat however long the window.</item>
/// <item>A line for a minute already shipped opens the next PART of that minute; it never replaces a row.</item>
/// <item>Cardinality guard: past <c>groupCap</c> groups in one (minute, part), every new key of that minute
/// goes into one "__other__" group with its real counts. The guard admits keys in file order, so a
/// window read again admits the same keys.</item>
/// </list>
/// </remarks>
public sealed class MinuteAggregator
{
    public const string Other = "__other__";
    public const long MinuteMicros = 60_000_000;
    private const long FlushGraceMicros = 2_000_000;

    private readonly int _groupCap;
    private readonly int _labelCount;
    private readonly Action<MinuteGroup> _emit;
    private readonly Dictionary<(long Minute, int Part, UInt128 Key), MinuteGroup> _groups = new();
    private readonly Dictionary<(long Minute, int Part), int> _groupsPerMinute = new();
    private readonly Dictionary<long, int> _openPart = new();
    private readonly Stack<MinuteGroup> _pool = new();
    private readonly List<(long, int, UInt128)> _due = new();
    private long _flushedBefore = long.MinValue;

    public long OverflowLines { get; private set; }

    public long LateLines { get; private set; }

    public MinuteAggregator(int groupCap, int labelCount, Action<MinuteGroup> emit)
    {
        _groupCap = groupCap;
        _labelCount = labelCount;
        _emit = emit;
    }

    public static UInt128 KeyOf(ReadOnlySpan<byte> keyBytes) => XxHash128.HashToUInt128(keyBytes);

    /// <summary>The group for this line's minute and key. <paramref name="created"/>: fill its labels now.</summary>
    public MinuteGroup Group(long tsMicros, UInt128 key, out bool created)
    {
        var minute = tsMicros - (tsMicros % MinuteMicros + MinuteMicros) % MinuteMicros;
        _openPart.TryGetValue(minute, out var part);
        if (part > 0)
        {
            LateLines++;
        }
        ref var group = ref CollectionsMarshal.GetValueRefOrAddDefault(_groups, (minute, part, key), out var exists);
        if (exists)
        {
            created = false;
            return group!;
        }
        ref var count = ref CollectionsMarshal.GetValueRefOrAddDefault(_groupsPerMinute, (minute, part), out _);
        if (count >= _groupCap)
        {
            _groups.Remove((minute, part, key));
            OverflowLines++;
            ref var other = ref CollectionsMarshal.GetValueRefOrAddDefault(_groups, (minute, part, UInt128.Zero), out var otherExists);
            if (!otherExists)
            {
                other = Rent(minute, part);
                other.IsOther = true;
                Array.Fill(other.Labels, Other);
            }
            created = false;
            return other!;
        }
        count++;
        group = Rent(minute, part);
        created = true;
        return group;
    }

    /// <summary>Ships every minute the file has moved past by more than the grace.</summary>
    public void FlushBefore(long maxTsMicros)
    {
        var closedBefore = maxTsMicros - FlushGraceMicros;
        if (closedBefore <= _flushedBefore)
        {
            return;
        }
        _flushedBefore = closedBefore;
        Flush(minuteEnd => minuteEnd <= closedBefore);
    }

    public void FlushAll() => Flush(_ => true);

    private void Flush(Func<long, bool> due)
    {
        _due.Clear();
        foreach (var key in _groups.Keys)
        {
            if (due(key.Minute + MinuteMicros))
            {
                _due.Add(key);
            }
        }
        // Rows in minute order: the gateway sees time move forward within a chunk.
        _due.Sort((a, b) => a.Item1 != b.Item1 ? a.Item1.CompareTo(b.Item1) : a.Item2.CompareTo(b.Item2));
        foreach (var key in _due)
        {
            var group = _groups[key];
            _groups.Remove(key);
            _emit(group);
            _groupsPerMinute.Remove((key.Item1, key.Item2));
            _openPart[key.Item1] = key.Item2 + 1;
            _pool.Push(group);
        }
    }

    private MinuteGroup Rent(long minute, int part)
    {
        var g = _pool.Count > 0 ? _pool.Pop() : new MinuteGroup();
        g.Reset(minute, part, _labelCount);
        return g;
    }
}
