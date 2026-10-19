using System.Buffers.Text;

namespace Trailox.Agent.Bench.Pipeline;

/// <summary>
/// Reads the few values the prefilter needs straight from a line's bytes, before any JSON parsing:
/// the timestamp (the window check), the server-log "id" and the audit "atype". Whitespace-agnostic,
/// because the audit format on Atlas is not measured yet.
/// </summary>
internal static class LineScan
{
    private const int HeadBytes = 160;

    /// <summary>The offset of the value of "key" in the first bytes of the line, or -1.</summary>
    public static int ValueOf(ReadOnlySpan<byte> line, ReadOnlySpan<byte> quotedKey, int from = 0)
    {
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

    public static bool TryInt(ReadOnlySpan<byte> line, int at, out long value) =>
        Utf8Parser.TryParse(line[at..], out value, out _);

    /// <summary>The quoted string at offset (without the quotes); no unescaping: atype values are plain.</summary>
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
    /// The line's own time in Unix milliseconds. Server log: {"t":{"$date":"..."}. Audit: "ts" : { "$date" : X }
    /// where X is an ISO string, a number of milliseconds, or {"$numberLong": "..."}.
    /// </summary>
    public static bool TryTimestamp(ReadOnlySpan<byte> line, bool audit, out long ms)
    {
        ms = 0;
        var key = audit ? ValueOf(line, "\"ts\""u8) : 0;
        if (key < 0)
        {
            return false;
        }
        var at = ValueOf(line, "\"$date\""u8, key);
        if (at < 0)
        {
            return false;
        }
        if (line[at] == (byte)'"')
        {
            return TryIso(line[(at + 1)..], out ms);
        }
        if (line[at] == (byte)'{')
        {
            var inner = ValueOf(line, "\"$numberLong\""u8, at);
            return inner >= 0 && Utf8Parser.TryParse(QuotedAt(line, inner), out ms, out _);
        }
        return Utf8Parser.TryParse(line[at..], out ms, out _);
    }

    /// <summary>yyyy-MM-ddTHH:mm:ss.fff followed by Z or ±hh:mm. Fixed positions, no allocation.</summary>
    public static bool TryIso(ReadOnlySpan<byte> s, out long ms)
    {
        ms = 0;
        if (s.Length < 24 || s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':' || s[19] != '.')
        {
            return false;
        }
        int year = D(s, 0, 4), month = D(s, 5, 2), day = D(s, 8, 2), hour = D(s, 11, 2), minute = D(s, 14, 2), second = D(s, 17, 2), milli = D(s, 20, 3);
        if (year < 0 || month is < 1 or > 12 || day < 1 || hour < 0 || minute < 0 || second < 0 || milli < 0)
        {
            return false;
        }
        var offsetMinutes = 0;
        var zone = s[23];
        if (zone is (byte)'+' or (byte)'-')
        {
            if (s.Length < 29)
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
