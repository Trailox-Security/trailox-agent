using System.Text;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// A statement's shape: its text with every literal replaced by <c>?</c>, so that statements that differ only in their
/// values group together, and no value leaves the customer's network.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>Strings in any quoting (<c>'...'</c>, <c>E'...'</c>, <c>U&amp;'...'</c>, <c>$tag$...$tag$</c>) and numbers (hex,
///   octal and binary ones too) become <c>?</c>; a list of them, as in <c>IN (?, ?, ?)</c>, becomes one <c>?</c>, and so
///   does a list of rows, as in <c>VALUES (?), (?)</c>.</item>
/// <item>🔴 A plain string that holds a backslash ends at the farther of its two possible ends: with
///   <c>standard_conforming_strings</c> off the backslash escapes a quote, so the nearer end could leave a value outside.</item>
/// <item>Comments go, and each run of white space becomes one space. Words, quoted identifiers, operators and
///   <c>$1</c> placeholders stay.</item>
/// <item>🔴 It fails closed: a string that never ends (a cut text) ends the shape with <c>?</c>.</item>
/// </list>
/// The lexer is the password redactor's, so both read a statement the same way.
/// </remarks>
public static class PgStatementShape
{
    public static string Of(string sql)
    {
        var output = new StringBuilder(Math.Min(sql.Length, 4096));
        var i = 0;
        var space = false;
        while (i < sql.Length)
        {
            var c = sql[i];
            if (c == '$' && i + 1 < sql.Length && char.IsAsciiDigit(sql[i + 1]))
            {
                // A placeholder, not a value.
                var j = i + 1;
                while (j < sql.Length && char.IsAsciiDigit(sql[j]))
                {
                    j++;
                }
                Append(output, sql.AsSpan(i, j - i), ref space);
                i = j;
                continue;
            }
            if (c == '0' && i + 2 < sql.Length && sql[i + 1] is 'x' or 'X' or 'o' or 'O' or 'b' or 'B' && char.IsAsciiHexDigit(sql[i + 2]))
            {
                // PostgreSQL 16's 0x1F, 0o17 and 0b101: the digits are the value too.
                var j = i + 2;
                while (j < sql.Length && (char.IsAsciiHexDigit(sql[j]) || sql[j] == '_'))
                {
                    j++;
                }
                i = j;
                Value(output, ref space);
                continue;
            }
            if (char.IsAsciiDigit(c) || (c == '.' && i + 1 < sql.Length && char.IsAsciiDigit(sql[i + 1])))
            {
                i = NumberEnd(sql, i);
                Value(output, ref space);
                continue;
            }
            var token = PgPasswordRedactor.Lexer.Next(sql, i);
            switch (token.Kind)
            {
                case PgPasswordRedactor.TokenKind.Space:
                case PgPasswordRedactor.TokenKind.Comment:
                    space = output.Length > 0;
                    break;
                case PgPasswordRedactor.TokenKind.Literal:
                    Value(output, ref space);
                    if (!token.Terminated)
                    {
                        return Rows(output);
                    }
                    if (token.Plain && sql.AsSpan(i, token.End - i).Contains('\\'))
                    {
                        var escaped = PgPasswordRedactor.Lexer.Quoted(sql, i + 1, backslashEscapes: true);
                        if (!escaped.Terminated)
                        {
                            return Rows(output);
                        }
                        i = Math.Max(token.End, escaped.End);
                        continue;
                    }
                    break;
                default:
                    Append(output, sql.AsSpan(i, token.End - i), ref space);
                    break;
            }
            i = token.End;
        }
        return Rows(output);
    }

    /// <summary>A list of rows of values, as in <c>VALUES (?), (?)</c>, as one: the row count is no part of the shape.</summary>
    private static string Rows(StringBuilder output) =>
        System.Text.RegularExpressions.Regex.Replace(output.ToString(), @"\(\?\)(?:, ?\(\?\))+", "(?)");

    /// <summary>
    /// The direction of a COPY: "TO" (out of the database) or "FROM", the first of the two words outside parentheses;
    /// null when neither is there (a cut text).
    /// </summary>
    public static string? CopyDirection(string sql)
    {
        var depth = 0;
        var i = 0;
        while (i < sql.Length)
        {
            var token = PgPasswordRedactor.Lexer.Next(sql, i);
            if (token.Kind == PgPasswordRedactor.TokenKind.Word && depth == 0
                && (string.Equals(token.Word, "TO", StringComparison.OrdinalIgnoreCase) || string.Equals(token.Word, "FROM", StringComparison.OrdinalIgnoreCase)))
            {
                return token.Word!.ToUpperInvariant();
            }
            if (token.Kind == PgPasswordRedactor.TokenKind.Other)
            {
                depth += sql[i] == '(' ? 1 : sql[i] == ')' ? -1 : 0;
            }
            if (token.Kind == PgPasswordRedactor.TokenKind.Literal && !token.Terminated)
            {
                return null;
            }
            i = token.End;
        }
        return null;
    }

    /// <summary>A value: <c>?</c>, unless it continues a list of values (<c>?, ?</c>), which stays one <c>?</c>.</summary>
    private static void Value(StringBuilder output, ref bool space)
    {
        var n = output.Length;
        if (n >= 3 && output[n - 1] == ' ' && output[n - 2] == ',' && output[n - 3] == '?')
        {
            output.Length = n - 2;
            space = false;
            return;
        }
        if (n >= 2 && output[n - 1] == ',' && output[n - 2] == '?')
        {
            output.Length = n - 1;
            space = false;
            return;
        }
        Append(output, "?", ref space);
    }

    private static void Append(StringBuilder output, ReadOnlySpan<char> text, ref bool space)
    {
        if (space)
        {
            output.Append(' ');
            space = false;
        }
        output.Append(text);
    }

    private static int NumberEnd(string s, int i)
    {
        var j = i;
        while (j < s.Length && (char.IsAsciiDigit(s[j]) || s[j] == '.' || s[j] == '_'))
        {
            j++;
        }
        if (j < s.Length && s[j] is 'e' or 'E')
        {
            var k = j + 1;
            if (k < s.Length && s[k] is '+' or '-')
            {
                k++;
            }
            if (k < s.Length && char.IsAsciiDigit(s[k]))
            {
                j = k;
                while (j < s.Length && char.IsAsciiDigit(s[j]))
                {
                    j++;
                }
            }
        }
        return j;
    }
}
