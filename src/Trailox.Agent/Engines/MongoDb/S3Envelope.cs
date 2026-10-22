using System.Buffers.Text;
using System.Text;
using System.Text.Json;

namespace Trailox.Agent.Engines.MongoDb;

/// <summary>
/// One record of the Atlas S3 log export: an envelope whose <c>log</c> member holds the original line as a JSON
/// STRING (Phase 0 facts §18.3). <see cref="Inner"/> hands out that line, unescaped into a buffer this reuses.
/// </summary>
/// <remarks>
/// 🔴 THE ENVELOPE'S <c>timestamp</c> IS NEVER READ. On backfilled audit records it is the export time (an audit line
///    from 12:04 carried 13:30), so the engine reads each line's own <c>t</c> or <c>ts</c>.
/// ⚠ ONE PASS, NOT TWO. A JSON reader first scans the string to its end and then unescapes it in a second pass, and
///   in the S3 form that is a fifth of the cost of a line (bench, 2026-10-20). The common case is found and unescaped
///   in one pass here; anything else (spacing, a missing or non-string <c>log</c>) goes through the reader.
/// </remarks>
public sealed class S3Envelope
{
    private byte[] _inner = new byte[1 << 14];

    /// <summary>The record's inner line, valid until the next call; empty when the record has no string <c>log</c>.</summary>
    public ReadOnlySpan<byte> Inner(ReadOnlySpan<byte> record)
    {
        // An unescaped "log":" can only be the key itself: inside any string value a quote is written \".
        var at = record.IndexOf("\"log\":\""u8);
        if (at >= 0 && TryUnescape(record[(at + 7)..], out var length))
        {
            return _inner.AsSpan(0, length);
        }
        return ByReader(record);
    }

    /// <summary>
    /// The JSON string whose body starts <paramref name="s"/>, unescaped into the buffer, up to its closing quote.
    /// False for anything this does not decode exactly as a JSON reader would.
    /// </summary>
    private bool TryUnescape(ReadOnlySpan<byte> s, out int length)
    {
        length = 0;
        var i = 0;
        // 🔴 OUTSIDE THE LOOP. A stackalloc inside it takes more stack at every \u escape until the method returns: a
        //    record of 100,000 escapes overflowed a 1.5 MB thread, which no catch can stop, and the agent died on the
        //    same window at every retry (review of agent PR #14, 2026-10-22).
        Span<byte> utf8 = stackalloc byte[4];
        while (true)
        {
            var run = s[i..].IndexOfAny((byte)'"', (byte)'\\');
            if (run < 0)
            {
                return false;
            }
            Append(s.Slice(i, run), ref length);
            i += run;
            if (s[i] == (byte)'"')
            {
                return true;
            }
            if (i + 1 >= s.Length)
            {
                return false;
            }
            var escaped = s[i + 1];
            byte simple = escaped switch
            {
                (byte)'"' => (byte)'"',
                (byte)'\\' => (byte)'\\',
                (byte)'/' => (byte)'/',
                (byte)'b' => (byte)'\b',
                (byte)'f' => (byte)'\f',
                (byte)'n' => (byte)'\n',
                (byte)'r' => (byte)'\r',
                (byte)'t' => (byte)'\t',
                _ => 0,
            };
            if (simple != 0)
            {
                Append(new ReadOnlySpan<byte>(in simple), ref length);
                i += 2;
                continue;
            }
            if (escaped != (byte)'u' || !TryHex(s, i + 2, out var unit))
            {
                return false;
            }
            i += 6;
            var scalar = (int)unit;
            if (char.IsHighSurrogate((char)unit))
            {
                // A pair is two escapes in a row; anything else is a lone surrogate, which the reader refuses too.
                if (i + 1 >= s.Length || s[i] != (byte)'\\' || s[i + 1] != (byte)'u' || !TryHex(s, i + 2, out var low)
                    || !char.IsLowSurrogate((char)low))
                {
                    return false;
                }
                scalar = char.ConvertToUtf32((char)unit, (char)low);
                i += 6;
            }
            else if (char.IsLowSurrogate((char)unit))
            {
                return false;
            }
            var written = new Rune(scalar).EncodeToUtf8(utf8);
            Append(utf8[..written], ref length);
        }
    }

    private static bool TryHex(ReadOnlySpan<byte> s, int at, out ushort value)
    {
        value = 0;
        return at + 4 <= s.Length && Utf8Parser.TryParse(s.Slice(at, 4), out value, out var consumed, 'X') && consumed == 4;
    }

    private void Append(ReadOnlySpan<byte> bytes, ref int length)
    {
        if (_inner.Length - length < bytes.Length)
        {
            var bigger = new byte[Math.Max(_inner.Length * 2, length + bytes.Length)];
            _inner.AsSpan(0, length).CopyTo(bigger);
            _inner = bigger;
        }
        bytes.CopyTo(_inner.AsSpan(length));
        length += bytes.Length;
    }

    private ReadOnlySpan<byte> ByReader(ReadOnlySpan<byte> record)
    {
        try
        {
            var r = new Utf8JsonReader(record);
            if (!r.Read() || r.TokenType != JsonTokenType.StartObject)
            {
                return default;
            }
            while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
            {
                var isLog = r.ValueTextEquals("log"u8);
                r.Read();
                if (!isLog || r.TokenType != JsonTokenType.String)
                {
                    r.Skip();
                    continue;
                }
                if (!r.ValueIsEscaped)
                {
                    return r.ValueSpan;
                }
                if (_inner.Length < r.ValueSpan.Length)
                {
                    _inner = new byte[Math.Max(_inner.Length * 2, r.ValueSpan.Length)];
                }
                return _inner.AsSpan(0, r.CopyString(_inner));
            }
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException)
        {
        }
        return default;
    }
}
