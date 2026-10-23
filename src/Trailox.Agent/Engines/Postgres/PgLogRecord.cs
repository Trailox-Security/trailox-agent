using System.Globalization;
using System.Text.RegularExpressions;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>The three forms a PostgreSQL server log takes (<c>log_destination</c>).</summary>
public enum PgLogFormat
{
    Stderr,
    Csv,
    Json,
}

/// <summary>What a record is, by its level and the start of its message. The agent ships only some kinds.</summary>
public enum PgRecordKind
{
    ConnectionReceived,
    ConnectionAuthenticated,
    ConnectionAuthorized,
    Disconnection,
    /// <summary>A FATAL or PANIC record: failed sign-ins and refused connections, with their DETAIL.</summary>
    Fatal,
    /// <summary><c>statement: ...</c>, written by <c>log_statement</c> for a statement sent in the simple protocol.</summary>
    Statement,
    /// <summary><c>execute &lt;name&gt;: ...</c>, written by <c>log_statement</c> for a statement sent in the extended protocol.</summary>
    Execute,
    /// <summary>A pgaudit <c>AUDIT:</c> line. Counted, not shipped.</summary>
    Audit,
    Other,
}

/// <summary>
/// One log record: a message and the parts PostgreSQL writes with it (its DETAIL, and the STATEMENT of an error).
/// Built by <see cref="PgLogParser"/> from any of the three formats.
/// </summary>
public sealed class PgLogRecord
{
    /// <summary>The record's own time, UTC, in microseconds; <see cref="long.MinValue"/> when it cannot be read.</summary>
    public long TsMicros { get; set; } = long.MinValue;
    public int Pid { get; set; }
    public string User { get; set; } = "";
    public string Database { get; set; } = "";
    public string RemoteHost { get; set; } = "";
    public string RemotePort { get; set; } = "";

    /// <summary>LOG, FATAL, ERROR... as the server wrote it.</summary>
    public string Level { get; set; } = "";

    /// <summary>The SQLSTATE: csvlog and jsonlog carry it; stderr with RDS's default prefix does not.</summary>
    public string SqlState { get; set; } = "";

    /// <summary>
    /// On every csvlog and jsonlog record (empty when the client set none). Null for stderr, which carries it only on the
    /// connection line.
    /// </summary>
    public string? ApplicationName { get; set; }

    public string SessionId { get; set; } = "";
    public string Message { get; set; } = "";
    public string Detail { get; set; } = "";

    /// <summary>The STATEMENT part of an error. Never shipped: it holds the application's values.</summary>
    public string Statement { get; set; } = "";

    public PgLogFormat Format { get; set; }

    public PgRecordKind Kind => PgRecordKinds.Of(Level, Message);
}

public static class PgRecordKinds
{
    // execute <unnamed>: CREATE ..., execute S_1: ..., execute fetch from S_1/C_2: ... (postgres.c, exec_execute_message).
    private static readonly Regex ExecutePrefix = new(@"^execute (?:fetch from )?[^\s:]+(?:/[^\s:]+)?: ",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The kind of a record. English text only: a server that writes another language reads as <see cref="PgRecordKind.Other"/>.
    /// </summary>
    public static PgRecordKind Of(string level, string message)
    {
        if (level is "FATAL" or "PANIC")
        {
            return PgRecordKind.Fatal;
        }
        if (level != "LOG")
        {
            return PgRecordKind.Other;
        }
        if (message.StartsWith("connection received: ", StringComparison.Ordinal))
        {
            return PgRecordKind.ConnectionReceived;
        }
        if (message.StartsWith("connection authenticated: ", StringComparison.Ordinal))
        {
            return PgRecordKind.ConnectionAuthenticated;
        }
        if (message.StartsWith("connection authorized: ", StringComparison.Ordinal)
            || message.StartsWith("replication connection authorized: ", StringComparison.Ordinal))
        {
            return PgRecordKind.ConnectionAuthorized;
        }
        if (message.StartsWith("disconnection: ", StringComparison.Ordinal))
        {
            return PgRecordKind.Disconnection;
        }
        if (message.StartsWith("statement: ", StringComparison.Ordinal))
        {
            return PgRecordKind.Statement;
        }
        if (message.StartsWith("execute ", StringComparison.Ordinal) && ExecutePrefix.IsMatch(message))
        {
            return PgRecordKind.Execute;
        }
        if (message.StartsWith("AUDIT: ", StringComparison.Ordinal))
        {
            return PgRecordKind.Audit;
        }
        return PgRecordKind.Other;
    }

    /// <summary>The kinds the agent ships at the Logs only level: sign-ins, failures and DDL. The rest is counted.</summary>
    public static bool Shipped(PgRecordKind kind) =>
        kind is PgRecordKind.ConnectionReceived or PgRecordKind.ConnectionAuthenticated or PgRecordKind.ConnectionAuthorized
            or PgRecordKind.Disconnection or PgRecordKind.Fatal or PgRecordKind.Statement or PgRecordKind.Execute;

    /// <summary>The statement text of a <c>statement:</c> or <c>execute ...:</c> record, without its prefix.</summary>
    public static string StatementText(string message)
    {
        if (message.StartsWith("statement: ", StringComparison.Ordinal))
        {
            return message["statement: ".Length..];
        }
        var m = ExecutePrefix.Match(message);
        return m.Success ? message[m.Length..] : message;
    }
}

/// <summary>Times as PostgreSQL writes them: <c>2026-10-22 13:45:01 UTC</c> or <c>2026-10-22 13:45:01.123 UTC</c>.</summary>
public static class PgTime
{
    /// <summary>
    /// The time in microseconds since the epoch, or null. Only a zero-offset zone is read (<c>log_timezone = 'UTC'</c>):
    /// a zone abbreviation such as <c>IDT</c> cannot be turned into an offset reliably.
    /// </summary>
    public static long? Micros(ReadOnlySpan<char> text)
    {
        if (text.Length < 23 || text[4] != '-' || text[7] != '-' || text[10] != ' ' || text[13] != ':' || text[16] != ':')
        {
            return null;
        }
        if (!int.TryParse(text[..4], NumberStyles.None, CultureInfo.InvariantCulture, out var year)
            || !int.TryParse(text[5..7], NumberStyles.None, CultureInfo.InvariantCulture, out var month)
            || !int.TryParse(text[8..10], NumberStyles.None, CultureInfo.InvariantCulture, out var day)
            || !int.TryParse(text[11..13], NumberStyles.None, CultureInfo.InvariantCulture, out var hour)
            || !int.TryParse(text[14..16], NumberStyles.None, CultureInfo.InvariantCulture, out var minute)
            || !int.TryParse(text[17..19], NumberStyles.None, CultureInfo.InvariantCulture, out var second))
        {
            return null;
        }
        var i = 19;
        long fraction = 0;
        if (text[i] == '.')
        {
            var digits = 0;
            i++;
            while (i < text.Length && char.IsAsciiDigit(text[i]))
            {
                if (digits < 6)
                {
                    fraction = fraction * 10 + (text[i] - '0');
                    digits++;
                }
                i++;
            }
            for (; digits < 6; digits++)
            {
                fraction *= 10;
            }
        }
        if (i >= text.Length || text[i] != ' ')
        {
            return null;
        }
        var zone = text[(i + 1)..];
        if (!(zone.SequenceEqual("UTC") || zone.SequenceEqual("GMT") || zone.SequenceEqual("+00") || zone.SequenceEqual("Z")))
        {
            return null;
        }
        try
        {
            var at = new DateTime(year, month, day, hour, minute, second, DateTimeKind.Utc);
            return (at - DateTime.UnixEpoch).Ticks / 10 + fraction;
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }
    }
}
