using System.Buffers;
using System.IO.Hashing;
using System.Text.Json;

namespace Trailox.Agent.Engines.MongoDb;

/// <summary>
/// The shape of a command: every value replaced by its type, so the shape and its hash can leave the
/// customer's network and the literals cannot. This is the PII boundary: no value byte of the input is
/// ever copied to the output, only key names and the fixed tokens below.
/// </summary>
/// <remarks>
/// <para>Rules (version <see cref="Version"/>):</para>
/// <list type="bullet">
/// <item>scalars become "?string", "?number", "?bool"; extended-JSON wrappers ({"$date": ...}, {"$binary": ..., "$type": ...})
///   one token for their type, whatever the order of their keys;</item>
/// <item>an array of scalars becomes "?array&lt;type&gt;" whatever its length; an array of documents keeps each element's
///   shape, consecutive equal shapes written once (a pipeline keeps its stages; a 1- and a 500-document insert are one shape);</item>
/// <item>the driver's envelope (session, cluster time, read and write concern, $db, comment) is left out of the command;</item>
/// <item>🔴 THE SAME SHAPE FROM BOTH LOG SOURCES. The Atlas S3 export rewrites every line (measured 2026-10-19): keys come in a
///   new order at every depth, every array becomes an object keyed "1".."n", and null fields disappear. So keys are sorted
///   at every depth, an object keyed exactly "1".."n" is read as an array, a null-valued key is left out, and an empty array
///   and an empty object are one token. API lines get the same treatment, so a query has one shape whichever source read it.</item>
/// </list>
/// </remarks>
public sealed class MongoShape
{
    /// <summary>Travels with every row, so a change to these rules is visible in the data.</summary>
    public const int Version = 1;

    private const int MaxDepth = 32;

    private static readonly byte[][] Envelope =
    [
        "lsid"u8.ToArray(), "$clusterTime"u8.ToArray(), "txnNumber"u8.ToArray(), "$db"u8.ToArray(), "$readPreference"u8.ToArray(),
        "writeConcern"u8.ToArray(), "readConcern"u8.ToArray(), "maxTimeMS"u8.ToArray(), "autocommit"u8.ToArray(),
        "startTransaction"u8.ToArray(), "apiVersion"u8.ToArray(), "apiStrict"u8.ToArray(), "apiDeprecationErrors"u8.ToArray(),
        "$replData"u8.ToArray(), "$oplogQueryData"u8.ToArray(), "$configTime"u8.ToArray(), "$topologyTime"u8.ToArray(),
        "comment"u8.ToArray(), "$audit"u8.ToArray(), "mayBypassWriteBlocking"u8.ToArray(), "$client"u8.ToArray(),
    ];

    // Keys that may sit beside a wrapper's own key: {"$binary": .., "$type": ..}, {"$regex": .., "$options": ..}.
    private static readonly byte[][] WrapperCompanions = ["$type"u8.ToArray(), "$options"u8.ToArray(), "$scope"u8.ToArray()];

    private enum Kind : byte { None, String, Number, Bool, Date, ObjectId, Binary, Uuid, Timestamp, Regex, MinKey, MaxKey, Code, Other, Empty, Structured }

    private struct Member
    {
        public int KeyStart, KeyLength, ValueStart, ValueLength;
        public Kind Kind;
    }

    private readonly Buffer _out = new(4096);
    private readonly Buffer _keys = new(1024);
    private readonly List<Member>[] _members = Enumerable.Range(0, MaxDepth + 2).Select(_ => new List<Member>(16)).ToArray();

    public ReadOnlySpan<byte> Shape => _out.Span;

    public ulong Hash { get; private set; }

    /// <summary>Normalises the value the reader is on (normally the command's StartObject), then hashes it.</summary>
    public void Normalize(ref Utf8JsonReader reader)
    {
        _out.Clear();
        _keys.Clear();
        WriteValue(ref reader, 0, topLevel: true);
        Hash = XxHash3.HashToUInt64(_out.Span);
    }

    /// <summary>The shape's text, for the row. Allocates; call once per new shape, not per line.</summary>
    public string Text() => System.Text.Encoding.UTF8.GetString(_out.Span);

    private Kind WriteValue(ref Utf8JsonReader r, int depth, bool topLevel = false)
    {
        switch (r.TokenType)
        {
            case JsonTokenType.String: return Scalar(Kind.String);
            case JsonTokenType.Number: return Scalar(Kind.Number);
            case JsonTokenType.True:
            case JsonTokenType.False: return Scalar(Kind.Bool);
            case JsonTokenType.Null: return Kind.None;
            case JsonTokenType.StartObject: return WriteObject(ref r, depth, topLevel);
            case JsonTokenType.StartArray: return WriteArray(ref r, depth);
            default:
                r.Skip();
                return Scalar(Kind.Other);
        }
    }

    private Kind Scalar(Kind kind)
    {
        _out.Write(Token(kind));
        return kind;
    }

    private Kind WriteObject(ref Utf8JsonReader r, int depth, bool topLevel)
    {
        if (depth >= MaxDepth)
        {
            r.Skip();
            _out.Write("\"?deep\""u8);
            return Kind.Structured;
        }
        var start = _out.Length;
        var keysMark = _keys.Length;
        var members = _members[depth];
        members.Clear();
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var keyStart = _keys.Length;
            CopyKey(ref r);
            var keyLength = _keys.Length - keyStart;
            r.Read();
            if (topLevel && IsEnvelope(_keys.Span.Slice(keyStart, keyLength)))
            {
                r.Skip();
                _keys.Truncate(keyStart);
                continue;
            }
            var valueStart = _out.Length;
            var kind = WriteValue(ref r, depth + 1);
            if (kind == Kind.None)
            {
                // A null field: S3 drops it, so the shape does too.
                _out.Truncate(valueStart);
                _keys.Truncate(keyStart);
                continue;
            }
            members.Add(new Member { KeyStart = keyStart, KeyLength = keyLength, ValueStart = valueStart, ValueLength = _out.Length - valueStart, Kind = kind });
        }

        Kind result;
        if (members.Count == 0)
        {
            _out.Truncate(start);
            result = Scalar(Kind.Empty);
        }
        else if (!topLevel && WrapperKind(members) is var wrapped and not Kind.None)
        {
            _out.Truncate(start);
            result = Scalar(wrapped);
        }
        else if (!topLevel && IsIndexed(members))
        {
            result = EmitIndexed(members, start);
        }
        else
        {
            EmitSorted(members, start);
            result = Kind.Structured;
        }
        _keys.Truncate(keysMark);
        members.Clear();
        return result;
    }

    private Kind WriteArray(ref Utf8JsonReader r, int depth)
    {
        if (depth >= MaxDepth)
        {
            r.Skip();
            _out.Write("\"?deep\""u8);
            return Kind.Structured;
        }
        var start = _out.Length;
        var members = _members[depth];
        members.Clear();
        while (r.Read() && r.TokenType != JsonTokenType.EndArray)
        {
            var valueStart = _out.Length;
            var kind = WriteValue(ref r, depth + 1);
            if (kind == Kind.None)
            {
                // Null elements are not written: an array of nulls is an empty array.
                _out.Truncate(valueStart);
                continue;
            }
            members.Add(new Member { ValueStart = valueStart, ValueLength = _out.Length - valueStart, Kind = kind });
        }
        var result = members.Count == 0 ? ScalarAt(start, Kind.Empty) : EmitArray(members, start);
        members.Clear();
        return result;
    }

    private Kind ScalarAt(int start, Kind kind)
    {
        _out.Truncate(start);
        return Scalar(kind);
    }

    /// <summary>Members already written from <paramref name="start"/>, rewritten as {"key":value,...} in ordinal key order.</summary>
    private void EmitSorted(List<Member> members, int start)
    {
        members.Sort((a, b) => _keys.Span.Slice(a.KeyStart, a.KeyLength).SequenceCompareTo(_keys.Span.Slice(b.KeyStart, b.KeyLength)));
        Rewrite(members, start, (o, m, values) =>
        {
            o.WriteByte((byte)'"');
            o.Write(_keys.Span.Slice(m.KeyStart, m.KeyLength));
            o.Write("\":"u8);
            o.Write(values.Slice(m.ValueStart - start, m.ValueLength));
        }, (byte)'{', (byte)'}');
    }

    /// <summary>An S3 array: an object keyed exactly "1".."n". Written as the array it was, in index order.</summary>
    private Kind EmitIndexed(List<Member> members, int start)
    {
        members.Sort((a, b) => Index(_keys.Span.Slice(a.KeyStart, a.KeyLength)).CompareTo(Index(_keys.Span.Slice(b.KeyStart, b.KeyLength))));
        return EmitArray(members, start);
    }

    /// <summary>Elements already written from <paramref name="start"/>: one token when all are scalars, else [e1,e2] with runs written once.</summary>
    private Kind EmitArray(List<Member> members, int start)
    {
        var common = Kind.None;
        var allScalar = true;
        foreach (var m in members)
        {
            if (m.Kind is Kind.Structured)
            {
                allScalar = false;
                break;
            }
            common = common == Kind.None || common == m.Kind ? m.Kind : Kind.Other;
        }
        if (allScalar)
        {
            _out.Truncate(start);
            _out.Write(ArrayToken(common));
            return Kind.Structured;
        }
        Rewrite(members, start, null, (byte)'[', (byte)']', collapseRuns: true);
        return Kind.Structured;
    }

    private delegate void MemberWriter(Buffer output, Member member, ReadOnlySpan<byte> values);

    /// <summary>Moves the members' bytes aside, then writes them back from <paramref name="start"/> in list order.</summary>
    private void Rewrite(List<Member> members, int start, MemberWriter? write, byte open, byte close, bool collapseRuns = false)
    {
        var length = _out.Length - start;
        var copy = ArrayPool<byte>.Shared.Rent(Math.Max(1, length));
        try
        {
            _out.Span.Slice(start, length).CopyTo(copy);
            ReadOnlySpan<byte> values = copy.AsSpan(0, length);
            _out.Truncate(start);
            _out.WriteByte(open);
            var previous = -1;
            var written = 0;
            for (var i = 0; i < members.Count; i++)
            {
                var m = members[i];
                if (collapseRuns && previous >= 0)
                {
                    var p = members[previous];
                    if (values.Slice(p.ValueStart - start, p.ValueLength).SequenceEqual(values.Slice(m.ValueStart - start, m.ValueLength)))
                    {
                        continue;
                    }
                }
                if (written++ > 0)
                {
                    _out.WriteByte((byte)',');
                }
                if (write != null)
                {
                    write(_out, m, values);
                }
                else
                {
                    _out.Write(values.Slice(m.ValueStart - start, m.ValueLength));
                }
                previous = i;
            }
            _out.WriteByte(close);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(copy);
        }
    }

    /// <summary>Copies the property name the reader is on into the key buffer, unescaped.</summary>
    private void CopyKey(ref Utf8JsonReader r)
    {
        if (!r.ValueIsEscaped)
        {
            _keys.Write(r.ValueSpan);
            return;
        }
        var scratch = ArrayPool<byte>.Shared.Rent(r.ValueSpan.Length);
        try
        {
            var n = r.CopyString(scratch);
            _keys.Write(scratch.AsSpan(0, n));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(scratch);
        }
    }

    private bool IsIndexed(List<Member> members)
    {
        var keys = _keys.Span;
        Span<bool> seen = members.Count <= 64 ? stackalloc bool[members.Count + 1] : new bool[members.Count + 1];
        foreach (var m in members)
        {
            var index = Index(keys.Slice(m.KeyStart, m.KeyLength));
            if (index < 1 || index > members.Count || seen[index])
            {
                return false;
            }
            seen[index] = true;
        }
        return true;
    }

    /// <summary>"1" → 1; anything that is not a positive decimal without a leading zero → -1.</summary>
    private static int Index(ReadOnlySpan<byte> key)
    {
        if (key.IsEmpty || key.Length > 9 || key[0] == (byte)'0')
        {
            return -1;
        }
        var n = 0;
        foreach (var b in key)
        {
            var d = b - '0';
            if ((uint)d > 9)
            {
                return -1;
            }
            n = n * 10 + d;
        }
        return n;
    }

    private Kind WrapperKind(List<Member> members)
    {
        var keys = _keys.Span;
        var wrapped = Kind.None;
        foreach (var m in members)
        {
            var key = keys.Slice(m.KeyStart, m.KeyLength);
            if (key.Length < 2 || key[0] != (byte)'$')
            {
                return Kind.None;
            }
            var kind = WrapperKeyKind(key);
            if (kind != Kind.None)
            {
                if (wrapped != Kind.None)
                {
                    return Kind.None;
                }
                wrapped = kind;
            }
            else if (!IsCompanion(key))
            {
                return Kind.None;
            }
        }
        return wrapped;
    }

    private static bool IsCompanion(ReadOnlySpan<byte> key)
    {
        foreach (var c in WrapperCompanions)
        {
            if (key.SequenceEqual(c))
            {
                return true;
            }
        }
        return false;
    }

    private static Kind WrapperKeyKind(ReadOnlySpan<byte> name) => name switch
    {
        _ when name.SequenceEqual("$date"u8) => Kind.Date,
        _ when name.SequenceEqual("$oid"u8) => Kind.ObjectId,
        _ when name.SequenceEqual("$numberLong"u8) || name.SequenceEqual("$numberInt"u8) || name.SequenceEqual("$numberDouble"u8) || name.SequenceEqual("$numberDecimal"u8) => Kind.Number,
        _ when name.SequenceEqual("$binary"u8) => Kind.Binary,
        _ when name.SequenceEqual("$uuid"u8) => Kind.Uuid,
        _ when name.SequenceEqual("$timestamp"u8) => Kind.Timestamp,
        _ when name.SequenceEqual("$regularExpression"u8) => Kind.Regex,
        _ when name.SequenceEqual("$minKey"u8) => Kind.MinKey,
        _ when name.SequenceEqual("$maxKey"u8) => Kind.MaxKey,
        _ when name.SequenceEqual("$code"u8) || name.SequenceEqual("$symbol"u8) => Kind.Code,
        _ when name.SequenceEqual("$undefined"u8) || name.SequenceEqual("$dbPointer"u8) => Kind.Other,
        _ => Kind.None,
    };

    private static bool IsEnvelope(ReadOnlySpan<byte> name)
    {
        foreach (var e in Envelope)
        {
            if (name.SequenceEqual(e))
            {
                return true;
            }
        }
        return false;
    }

    private static ReadOnlySpan<byte> Token(Kind kind) => kind switch
    {
        Kind.String => "\"?string\""u8,
        Kind.Number => "\"?number\""u8,
        Kind.Bool => "\"?bool\""u8,
        Kind.Date => "\"?date\""u8,
        Kind.ObjectId => "\"?objectId\""u8,
        Kind.Binary => "\"?binary\""u8,
        Kind.Uuid => "\"?uuid\""u8,
        Kind.Timestamp => "\"?timestamp\""u8,
        Kind.Regex => "\"?regex\""u8,
        Kind.MinKey => "\"?minKey\""u8,
        Kind.MaxKey => "\"?maxKey\""u8,
        Kind.Code => "\"?code\""u8,
        Kind.Empty => "\"?empty\""u8,
        _ => "\"?other\""u8,
    };

    private static ReadOnlySpan<byte> ArrayToken(Kind kind) => kind switch
    {
        Kind.String => "\"?array<string>\""u8,
        Kind.Number => "\"?array<number>\""u8,
        Kind.Bool => "\"?array<bool>\""u8,
        Kind.Date => "\"?array<date>\""u8,
        Kind.ObjectId => "\"?array<objectId>\""u8,
        Kind.Uuid => "\"?array<uuid>\""u8,
        Kind.Empty => "\"?array<empty>\""u8,
        _ => "\"?array<mixed>\""u8,
    };

    /// <summary>A growable byte buffer that can be cut back.</summary>
    private sealed class Buffer
    {
        private byte[] _buffer;

        public Buffer(int capacity) => _buffer = new byte[capacity];

        public int Length { get; private set; }

        public ReadOnlySpan<byte> Span => _buffer.AsSpan(0, Length);

        public void Clear() => Length = 0;

        public void Truncate(int length) => Length = length;

        public void WriteByte(byte b)
        {
            Ensure(1);
            _buffer[Length++] = b;
        }

        public void Write(ReadOnlySpan<byte> bytes)
        {
            Ensure(bytes.Length);
            bytes.CopyTo(_buffer.AsSpan(Length));
            Length += bytes.Length;
        }

        private void Ensure(int more)
        {
            if (Length + more > _buffer.Length)
            {
                Array.Resize(ref _buffer, Math.Max(_buffer.Length * 2, Length + more));
            }
        }
    }
}
