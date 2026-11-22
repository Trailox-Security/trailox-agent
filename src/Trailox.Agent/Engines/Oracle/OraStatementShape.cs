using System.Text;
using System.Text.RegularExpressions;
using static Trailox.Agent.Engines.Oracle.OraPasswordRedactor;

namespace Trailox.Agent.Engines.Oracle;

/// <summary>
/// A statement's shape: its text, after <see cref="OraPasswordRedactor"/>, with every literal replaced by <c>?</c> and every
/// comment gone. It is the only text the agent ships, so no value leaves the customer's network, and statements that differ
/// only in their values group together.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>Strings in any quoting (<c>'...'</c>, <c>N'...'</c>, <c>q'[...]'</c> with any delimiter, <c>nq'...'</c>) and
///   numbers become <c>?</c>; a list of them, as in <c>IN (?, ?, ?)</c>, becomes one <c>?</c>, and so does a list of rows.
///   A number runs on through any letters joined to it, so <c>1e5</c>, <c>1f</c> and <c>0x1F</c> leave nothing behind.</item>
/// <item>Comments go, hints with them, and each run of white space becomes one space. Words, quoted identifiers, operators
///   and bind placeholders (<c>:1</c>, <c>:name</c>) stay. A double-quoted text compared with something is taken as a value.</item>
/// <item>🔴 It fails closed, ending the shape with <c>?</c>, wherever the text stops making sense as Oracle's quoting: a string or
///   quoted identifier that never ends (a cut text; the text after it would read as a name), and a word touching a quote
///   (<c>'O'Brien'</c>), which says the quotes were not where the lexer thinks.</item>
/// </list>
/// The lexer is the password redactor's, so both read a statement the same way.
/// </remarks>
public static class OraStatementShape
{
    private static readonly Regex RowLists = new(@"\(\?\)(?:, ?\(\?\))+", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public static string Of(string sql)
    {
        var output = new StringBuilder(Math.Min(sql.Length, 4096));
        var i = 0;
        var space = false;
        while (i < sql.Length)
        {
            var c = sql[i];
            if (c == ':' && i + 1 < sql.Length && (char.IsLetterOrDigit(sql[i + 1]) || sql[i + 1] == '_'))
            {
                // A placeholder, not a value.
                var j = i + 1;
                while (j < sql.Length && (char.IsLetterOrDigit(sql[j]) || sql[j] is '_' or '$' or '#'))
                {
                    j++;
                }
                Append(output, sql.AsSpan(i, j - i), ref space);
                i = j;
                continue;
            }
            if (char.IsAsciiDigit(c) || (c == '.' && i + 1 < sql.Length && char.IsAsciiDigit(sql[i + 1])))
            {
                i = NumberEnd(sql, i);
                Value(output, ref space);
                if (i < sql.Length && sql[i] == '\'')
                {
                    return Rows(output);
                }
                continue;
            }
            var token = Lexer.Next(sql, i);
            switch (token.Kind)
            {
                case TokenKind.Space:
                case TokenKind.Comment:
                    space = output.Length > 0;
                    break;
                case TokenKind.Literal:
                    Value(output, ref space);
                    if (!token.Terminated || Touches(sql, token.End))
                    {
                        return Rows(output);
                    }
                    break;
                case TokenKind.QuotedIdentifier when !token.Terminated || (i > 0 && (IsWordChar(sql[i - 1]) || sql[i - 1] is '"' or '\'')):
                    Value(output, ref space);
                    return Rows(output);
                case TokenKind.QuotedIdentifier when ComparedWith(output):
                    Value(output, ref space);
                    break;
                default:
                    Append(output, sql.AsSpan(i, token.End - i), ref space);
                    break;
            }
            i = token.End;
        }
        return Rows(output);
    }

    private static bool IsWordChar(char c) => char.IsLetterOrDigit(c) || c is '_' or '$' or '#';

    /// <summary>A word right after a closing quote: <c>'O'Brien'</c>.</summary>
    private static bool Touches(string sql, int end) => end < sql.Length && IsWordChar(sql[end]);

    /// <summary>The shape so far ends in a comparison, or the colon of a quoted bind: what follows is a value, not a name.</summary>
    private static bool ComparedWith(StringBuilder output) => output.Length > 0 && output[^1] is '=' or '<' or '>' or '!' or ':';

    /// <summary>A list of rows of values, as in <c>VALUES (?), (?)</c>, as one: the row count is no part of the shape.</summary>
    private static string Rows(StringBuilder output) => RowLists.Replace(output.ToString(), "(?)");

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
        while (j < s.Length && (char.IsAsciiDigit(s[j]) || s[j] == '.'))
        {
            j++;
        }
        if (j + 1 < s.Length && s[j] is 'e' or 'E' && s[j + 1] is '+' or '-')
        {
            j += 2;
        }
        while (j < s.Length && (char.IsLetterOrDigit(s[j]) || s[j] == '_'))
        {
            j++;
        }
        return j;
    }
}
