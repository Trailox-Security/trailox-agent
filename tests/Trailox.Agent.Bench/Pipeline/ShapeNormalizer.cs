using System.IO.Hashing;
using System.Text.Json;

namespace Trailox.Agent.Bench.Pipeline;

/// <summary>
/// Replaces every value of a command document by its type, keeping the keys and their order, so the
/// shape and its hash can leave the customer's network and the literals cannot (PLAN §6.3). This is
/// the PII boundary: no value byte of the input is ever copied to the output, only key names and the
/// fixed tokens below.
/// <list type="bullet">
/// <item>scalars become "?string", "?number", "?bool", "?null";</item>
/// <item>extended-JSON wrappers ({"$date": ...}, {"$oid": ...}, ...) become one token for their type;</item>
/// <item>an array of scalars becomes "?array&lt;type&gt;" whatever its length (an $in list of 3 or 300 is one shape);</item>
/// <item>an array of documents keeps each element's shape, with consecutive equal shapes written once
///   (a 1-document and a 500-document insert are one shape; a pipeline keeps its stages);</item>
/// <item>the driver's envelope (session, cluster time, read/write concern, $db, comment) is dropped:
///   it is not part of what the query does, and a comment can carry anything.</item>
/// </list>
/// </summary>
internal sealed class ShapeNormalizer
{
    /// <summary>Travels with every shipped row, so a change to these rules is visible in the data.</summary>
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

    private enum Kind : byte { None, String, Number, Bool, Null, Date, ObjectId, Binary, Uuid, Timestamp, Regex, MinKey, MaxKey, Code, Other, Structured }

    private static ReadOnlySpan<byte> Token(Kind kind) => kind switch
    {
        Kind.String => "\"?string\""u8,
        Kind.Number => "\"?number\""u8,
        Kind.Bool => "\"?bool\""u8,
        Kind.Null => "\"?null\""u8,
        Kind.Date => "\"?date\""u8,
        Kind.ObjectId => "\"?objectId\""u8,
        Kind.Binary => "\"?binary\""u8,
        Kind.Uuid => "\"?uuid\""u8,
        Kind.Timestamp => "\"?timestamp\""u8,
        Kind.Regex => "\"?regex\""u8,
        Kind.MinKey => "\"?minKey\""u8,
        Kind.MaxKey => "\"?maxKey\""u8,
        Kind.Code => "\"?code\""u8,
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
        _ => "\"?array<mixed>\""u8,
    };

    private readonly ByteBuffer _out = new(4096);

    public ReadOnlySpan<byte> Shape => _out.Span;

    public ulong Hash { get; private set; }

    /// <summary>Normalises the value the reader is positioned on (normally the command's StartObject).</summary>
    public void Normalize(ref Utf8JsonReader reader)
    {
        _out.Clear();
        WriteValue(ref reader, 0);
        Hash = XxHash3.HashToUInt64(_out.Span);
    }

    private Kind WriteValue(ref Utf8JsonReader r, int depth)
    {
        switch (r.TokenType)
        {
            case JsonTokenType.String: return Scalar(Kind.String);
            case JsonTokenType.Number: return Scalar(Kind.Number);
            case JsonTokenType.True:
            case JsonTokenType.False: return Scalar(Kind.Bool);
            case JsonTokenType.Null: return Scalar(Kind.Null);
            case JsonTokenType.StartObject: return WriteObject(ref r, depth);
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

    private Kind WriteObject(ref Utf8JsonReader r, int depth)
    {
        if (depth >= MaxDepth)
        {
            r.Skip();
            _out.Write("\"?deep\""u8);
            return Kind.Structured;
        }
        var peek = r;
        if (peek.Read() && peek.TokenType == JsonTokenType.PropertyName && WrapperKind(peek.ValueSpan) is var wrapped and not Kind.None)
        {
            r.Skip();
            return Scalar(wrapped);
        }

        _out.WriteByte((byte)'{');
        var first = true;
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            var isEnvelope = depth == 0 && IsEnvelope(name);
            if (!isEnvelope)
            {
                if (!first)
                {
                    _out.WriteByte((byte)',');
                }
                first = false;
                _out.WriteByte((byte)'"');
                _out.Write(name);
                _out.Write("\":"u8);
            }
            r.Read();
            if (isEnvelope)
            {
                r.Skip();
                continue;
            }
            WriteValue(ref r, depth + 1);
        }
        _out.WriteByte((byte)'}');
        return Kind.Structured;
    }

    private Kind WriteArray(ref Utf8JsonReader r, int depth)
    {
        var start = _out.Length;
        _out.WriteByte((byte)'[');
        var count = 0;
        var allScalar = true;
        var common = Kind.None;
        var previousStart = -1;
        var previousLength = 0;
        while (r.Read() && r.TokenType != JsonTokenType.EndArray)
        {
            var beforeComma = _out.Length;
            if (count > 0)
            {
                _out.WriteByte((byte)',');
            }
            var elementStart = _out.Length;
            var kind = WriteValue(ref r, depth + 1);
            var elementLength = _out.Length - elementStart;
            if (kind == Kind.Structured)
            {
                allScalar = false;
            }
            else
            {
                common = common == Kind.None || common == kind ? kind : Kind.Other;
            }
            // Consecutive equal element shapes are written once.
            if (previousStart >= 0 && elementLength == previousLength &&
                _out.Span.Slice(elementStart, elementLength).SequenceEqual(_out.Span.Slice(previousStart, previousLength)))
            {
                _out.Truncate(beforeComma);
            }
            else
            {
                previousStart = elementStart;
                previousLength = elementLength;
            }
            count++;
        }
        if (count == 0)
        {
            _out.WriteByte((byte)']');
            return Kind.Structured;
        }
        if (allScalar)
        {
            _out.Truncate(start);
            _out.Write(ArrayToken(common));
            return Kind.Structured;
        }
        _out.WriteByte((byte)']');
        return Kind.Structured;
    }

    /// <summary>One length check first: this runs for every top-level key of every command.</summary>
    private static bool IsEnvelope(ReadOnlySpan<byte> name)
    {
        var length = name.Length;
        if (length < 3 || length > 22 || (EnvelopeLengths & (1 << length)) == 0)
        {
            return false;
        }
        foreach (var e in Envelope)
        {
            if (e.Length == length && name.SequenceEqual(e))
            {
                return true;
            }
        }
        return false;
    }

    private static readonly int EnvelopeLengths = Envelope.Aggregate(0, (mask, e) => mask | (1 << e.Length));

    private static readonly int WrapperLengths = new[]
    {
        "$date", "$oid", "$numberLong", "$numberInt", "$numberDouble", "$numberDecimal", "$binary", "$uuid", "$timestamp",
        "$regularExpression", "$minKey", "$maxKey", "$code", "$symbol", "$undefined", "$dbPointer",
    }.Aggregate(0, (mask, w) => mask | (1 << Math.Min(w.Length, 31)));

    private static Kind WrapperKind(ReadOnlySpan<byte> name)
    {
        // Query operators are the common case here; most ($in, $lt, $match, $group, $regex...) have a
        // length no wrapper has and leave at the length check.
        if (name.Length < 4 || name[0] != (byte)'$' || (WrapperLengths & (1 << Math.Min(name.Length, 31))) == 0)
        {
            return Kind.None;
        }
        return name switch
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
    }
}

/// <summary>A growable byte buffer that can be cut back, for writing a shape and undoing a duplicate element.</summary>
internal sealed class ByteBuffer
{
    private byte[] _buffer;

    public ByteBuffer(int capacity) => _buffer = new byte[capacity];

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
