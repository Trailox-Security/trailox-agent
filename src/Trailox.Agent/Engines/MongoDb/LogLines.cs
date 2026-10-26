using System.Buffers;
using System.Buffers.Text;
using System.IO.Compression;
using System.Text.Json;

namespace Trailox.Agent.Engines.MongoDb;

/// <summary>
/// The lines of a log file read from a stream as spans over one pooled buffer, with no string or array per line:
/// an Admin API download is gzip (any number of members: Atlas concatenates one per published chunk), an S3 export
/// object plain JSON lines.
/// <see cref="FillAsync"/> reads; <see cref="TryReadLine"/> only hands out lines already read, so a
/// caller can loop over spans between awaits. A line is valid until the next <see cref="FillAsync"/>.
/// </summary>
public sealed class LogLines : IDisposable
{
    private const int ReadSize = 1 << 18;

    private readonly Stream _source;
    private byte[] _buffer = ArrayPool<byte>.Shared.Rent(1 << 20);
    private int _start;
    private int _end;
    private bool _eof;

    public long Lines { get; private set; }

    /// <param name="source">The file; disposed with this reader.</param>
    /// <param name="gzip">True when <paramref name="source"/> is compressed: it is decompressed here.</param>
    public LogLines(Stream source, bool gzip = true)
    {
        _source = gzip ? new GZipStream(source, CompressionMode.Decompress) : source;
    }

    /// <summary>Reads more of the file. False once the file is exhausted and every line was handed out.</summary>
    public async ValueTask<bool> FillAsync(CancellationToken ct)
    {
        if (_eof)
        {
            return _start < _end;
        }
        var pending = _end - _start;
        if (_start > 0)
        {
            Buffer.BlockCopy(_buffer, _start, _buffer, 0, pending);
            _start = 0;
            _end = pending;
        }
        if (_buffer.Length - _end < ReadSize)
        {
            // Only a line longer than the buffer grows it; mongod truncates lines at 10 KB by default.
            var bigger = ArrayPool<byte>.Shared.Rent(_buffer.Length * 2);
            Buffer.BlockCopy(_buffer, 0, bigger, 0, _end);
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = bigger;
        }
        var read = await _source.ReadAsync(_buffer.AsMemory(_end), ct);
        if (read == 0)
        {
            _eof = true;
        }
        _end += read;
        return true;
    }

    /// <summary>The next complete line in what was read so far; at the end of the file, the last unterminated one.</summary>
    public bool TryReadLine(out ReadOnlySpan<byte> line)
    {
        var pending = _buffer.AsSpan(_start, _end - _start);
        var newline = pending.IndexOf((byte)'\n');
        if (newline >= 0)
        {
            line = pending[..newline];
            _start += newline + 1;
            Lines++;
            return true;
        }
        if (_eof && !pending.IsEmpty)
        {
            line = pending;
            _start = _end;
            Lines++;
            return true;
        }
        line = default;
        return false;
    }

    public void Dispose()
    {
        _source.Dispose();
        ArrayPool<byte>.Shared.Return(_buffer);
    }
}

/// <summary>How a line's keys are ordered.</summary>
public enum LineForm
{
    /// <summary>As mongod writes it (the Admin API's files): "t" or "atype" first, "id" in the first bytes. Read by position.</summary>
    Api,

    /// <summary>As the Atlas S3 export rewrites it: keys in any order at every depth. Read by a top-level JSON scan.</summary>
    Rewritten,
}

/// <summary>
/// The values the filter needs, read before the line is parsed: the line's own time, the server-log <c>id</c>,
/// the audit <c>atype</c>. An API line is read straight from its first bytes (the audit log is written spaced,
/// "atype" : "authCheck", the server log compact); a rewritten line by a scan of its top-level keys, because
/// a nested key of the same name can come first.
/// </summary>
public static class LineScan
{
    private const int HeadBytes = 160;

    /// <summary>The offset of the value of the top-level <paramref name="key"/>, found by skipping every other value; -1 if absent.</summary>
    public static int TopLevelValue(ReadOnlySpan<byte> line, ReadOnlySpan<byte> key)
    {
        try
        {
            var r = new Utf8JsonReader(line, new JsonReaderOptions { MaxDepth = 256 });
            if (!r.Read() || r.TokenType != JsonTokenType.StartObject)
            {
                return -1;
            }
            while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
            {
                var match = r.ValueTextEquals(key);
                r.Read();
                if (match)
                {
                    return (int)r.TokenStartIndex;
                }
                r.Skip();
            }
        }
        catch (JsonException)
        {
        }
        return -1;
    }

    /// <summary>
    /// The offsets of the values of two top-level keys of a rewritten line, found in ONE scan that stops once both are
    /// seen; -1 for one that is absent.
    /// </summary>
    /// <remarks>
    /// ⚠ One scan, not one per key: the S3 form gives no key a fixed place, so each lookup skips over the nested
    ///   values before it, and a line's time and its id (or atype) were two such scans before the parse (in the
    ///   bench, the S3 form cost 2.8 times the API form a line).
    /// </remarks>
    public static void TopLevelValues(ReadOnlySpan<byte> line, ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, out int firstAt, out int secondAt)
    {
        firstAt = secondAt = -1;
        try
        {
            var r = new Utf8JsonReader(line, new JsonReaderOptions { MaxDepth = 256 });
            if (!r.Read() || r.TokenType != JsonTokenType.StartObject)
            {
                return;
            }
            while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
            {
                var isFirst = firstAt < 0 && r.ValueTextEquals(first);
                var isSecond = !isFirst && secondAt < 0 && r.ValueTextEquals(second);
                r.Read();
                if (isFirst)
                {
                    firstAt = (int)r.TokenStartIndex;
                }
                else if (isSecond)
                {
                    secondAt = (int)r.TokenStartIndex;
                }
                if (firstAt >= 0 && secondAt >= 0)
                {
                    return;
                }
                r.Skip();
            }
        }
        catch (JsonException)
        {
        }
    }

    /// <summary>The offset of the value of <paramref name="quotedKey"/> in the first bytes of the line, or -1.</summary>
    public static int ValueOf(ReadOnlySpan<byte> line, ReadOnlySpan<byte> quotedKey, int from = 0)
    {
        if (from >= line.Length)
        {
            return -1;
        }
        var head = line[from..Math.Min(line.Length, from + HeadBytes)];
        var at = head.IndexOf(quotedKey);
        if (at < 0)
        {
            return -1;
        }
        var i = from + at + quotedKey.Length;
        while (i < line.Length && (line[i] == (byte)' ' || line[i] == (byte)':'))
        {
            i++;
        }
        return i < line.Length ? i : -1;
    }

    public static bool TryInt(ReadOnlySpan<byte> line, int at, out long value)
    {
        value = 0;
        return at >= 0 && Utf8Parser.TryParse(line[at..], out value, out _);
    }

    /// <summary>The quoted string at <paramref name="at"/>, without the quotes and without unescaping.</summary>
    public static ReadOnlySpan<byte> QuotedAt(ReadOnlySpan<byte> line, int at)
    {
        if (at < 0 || at >= line.Length || line[at] != (byte)'"')
        {
            return default;
        }
        var end = line[(at + 1)..].IndexOf((byte)'"');
        return end < 0 ? default : line.Slice(at + 1, end);
    }

    /// <summary>
    /// The line's own time in Unix microseconds. Server log: {"t":{"$date":"..."}}. Audit: "ts" : { "$date" : X }
    /// with X an ISO string, milliseconds, or {"$numberLong": "..."}.
    /// </summary>
    public static bool TryTimestampMicros(ReadOnlySpan<byte> line, bool audit, out long micros, LineForm form = LineForm.Api)
    {
        var key = form == LineForm.Rewritten ? TopLevelValue(line, audit ? "ts"u8 : "t"u8)
            : audit ? ValueOf(line, "\"ts\""u8) : 0;
        return TryTimestampAt(line, key, out micros);
    }

    /// <summary>The line's own time in Unix microseconds, from its <c>t</c>/<c>ts</c> value at <paramref name="key"/> (-1: none).</summary>
    public static bool TryTimestampAt(ReadOnlySpan<byte> line, int key, out long micros)
    {
        micros = 0;
        if (key < 0)
        {
            return false;
        }
        var at = ValueOf(line, "\"$date\""u8, key);
        if (at < 0)
        {
            return false;
        }
        long ms;
        if (line[at] == (byte)'"')
        {
            if (!TryIso(line[(at + 1)..], out ms))
            {
                return false;
            }
        }
        else if (line[at] == (byte)'{')
        {
            var inner = ValueOf(line, "\"$numberLong\""u8, at);
            if (inner < 0 || !Utf8Parser.TryParse(QuotedAt(line, inner), out ms, out _))
            {
                return false;
            }
        }
        else if (!Utf8Parser.TryParse(line[at..], out ms, out _))
        {
            return false;
        }
        micros = ms * 1000;
        return true;
    }

    /// <summary>yyyy-MM-ddTHH:mm:ss.fff then Z or ±hh:mm, at fixed positions, in Unix milliseconds.</summary>
    public static bool TryIso(ReadOnlySpan<byte> s, out long ms)
    {
        ms = 0;
        if (s.Length < 24 || s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':' || s[19] != '.')
        {
            return false;
        }
        int year = D(s, 0, 4), month = D(s, 5, 2), day = D(s, 8, 2), hour = D(s, 11, 2), minute = D(s, 14, 2), second = D(s, 17, 2), milli = D(s, 20, 3);
        if (year < 1970 || month is < 1 or > 12 || day is < 1 or > 31 || hour is < 0 or > 23 || minute is < 0 or > 59 || second is < 0 or > 60 || milli < 0)
        {
            return false;
        }
        var offsetMinutes = 0;
        var zone = s[23];
        if (zone is (byte)'+' or (byte)'-')
        {
            if (s.Length < 29 || D(s, 24, 2) < 0 || D(s, 27, 2) < 0)
            {
                return false;
            }
            offsetMinutes = (D(s, 24, 2) * 60 + D(s, 27, 2)) * (zone == '-' ? -1 : 1);
        }
        else if (zone != 'Z')
        {
            return false;
        }
        ms = ((DaysFromCivil(year, month, day) * 86400L + hour * 3600 + minute * 60 + second) * 1000 + milli) - offsetMinutes * 60_000L;
        return true;
    }

    private static int D(ReadOnlySpan<byte> s, int at, int n)
    {
        var v = 0;
        for (var i = at; i < at + n; i++)
        {
            var d = s[i] - '0';
            if ((uint)d > 9)
            {
                return -1;
            }
            v = v * 10 + d;
        }
        return v;
    }

    // Howard Hinnant's days_from_civil.
    private static long DaysFromCivil(int y, int m, int d)
    {
        y -= m <= 2 ? 1 : 0;
        var era = (y >= 0 ? y : y - 399) / 400;
        var yoe = y - era * 400;
        var doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
        var doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        return era * 146097L + doe - 719468;
    }
}
