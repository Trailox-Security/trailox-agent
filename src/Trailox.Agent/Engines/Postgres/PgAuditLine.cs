using System.Globalization;
using System.Text;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// One pgaudit line: <c>AUDIT: AUDIT_TYPE,STATEMENT_ID,SUBSTATEMENT_ID,CLASS,COMMAND,OBJECT_TYPE,OBJECT_NAME,STATEMENT,PARAMETER</c>.
/// The parameter field is never kept: it holds the statement's bound values when <c>pgaudit.log_parameter</c> is on.
/// </summary>
/// <remarks>
/// pgaudit writes each field as CSV: a field that holds a comma, a double quote, a CR or an LF is quoted, its double
/// quotes doubled; any other field is written as it is. A session line with <c>pgaudit.log_relation</c> on names one
/// relation; a statement that reads three tables writes three lines with the same statement and substatement ids.
/// </remarks>
public sealed record PgAuditLine(
    string AuditType, long StatementId, long SubstatementId, string Class, string Command, string ObjectType,
    string ObjectName, string Statement, bool Truncated = false)
{
    public const string Prefix = "AUDIT: ";

    /// <summary>The fields before the statement.</summary>
    private const int Leading = 7;

    /// <summary>
    /// The line's fields, or null when the message is not a pgaudit line whose first seven fields can be read. A statement
    /// that does not end (a text cut by its reader) is kept as far as it goes, and marked <see cref="Truncated"/>.
    /// </summary>
    public static PgAuditLine? Parse(string message)
    {
        if (!message.StartsWith(Prefix, StringComparison.Ordinal))
        {
            return null;
        }
        var fields = new List<string>(Leading);
        var i = Prefix.Length;
        while (fields.Count < Leading)
        {
            if (Field(message, ref i) is { } field)
            {
                fields.Add(field);
            }
            else if (fields.Count == Leading - 1 && message.IndexOf(',', i) < 0 && (i >= message.Length || message[i] != '"'))
            {
                // pgaudit.log_statement off: the object name is the last field, and there is no statement.
                fields.Add(message[i..]);
                i = message.Length;
            }
            else
            {
                return null;
            }
        }
        if (i >= message.Length)
        {
            return Read(fields, "", truncated: false);
        }
        // The statement; the parameter after it is never read.
        var start = i;
        var statement = Field(message, ref i);
        var truncated = statement == null;
        if (truncated)
        {
            statement = message.Length > start && message[start] == '"'
                ? message[(start + 1)..].Replace("\"\"", "\"", StringComparison.Ordinal)
                : message[start..];
        }
        return Read(fields, statement!, truncated);
    }

    private static PgAuditLine? Read(List<string> fields, string statement, bool truncated) =>
        long.TryParse(fields[1], NumberStyles.None, CultureInfo.InvariantCulture, out var statementId)
        && long.TryParse(fields[2], NumberStyles.None, CultureInfo.InvariantCulture, out var substatementId)
        && fields[3].Length > 0
            ? new PgAuditLine(fields[0], statementId, substatementId, fields[3], fields[4], fields[5], fields[6], statement, truncated)
            : null;

    /// <summary>
    /// The field at <paramref name="i"/>, moving <paramref name="i"/> past its comma; null when it does not end with one
    /// (an unterminated quote, or a line with fewer fields).
    /// </summary>
    private static string? Field(string s, ref int i)
    {
        if (i < s.Length && s[i] == '"')
        {
            var value = new StringBuilder();
            var j = i + 1;
            while (j < s.Length)
            {
                if (s[j] == '"')
                {
                    if (j + 1 < s.Length && s[j + 1] == '"')
                    {
                        value.Append('"');
                        j += 2;
                        continue;
                    }
                    // The closing quote: a comma must follow.
                    if (j + 1 < s.Length && s[j + 1] == ',')
                    {
                        i = j + 2;
                        return value.ToString();
                    }
                    return null;
                }
                value.Append(s[j]);
                j++;
            }
            return null;
        }
        var comma = s.IndexOf(',', i);
        if (comma < 0)
        {
            return null;
        }
        var plain = s[i..comma];
        i = comma + 1;
        return plain;
    }
}
