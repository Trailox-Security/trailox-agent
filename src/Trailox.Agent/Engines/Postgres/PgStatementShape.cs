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
/// <item>🔴 A plain string that holds a backslash has two possible ends: with <c>standard_conforming_strings</c> off the
///   backslash escapes a quote. Where they differ, the shape stops with <c>?</c>: lexing on from either end can read the
///   other reading's value as words.</item>
/// <item>Comments go, and each run of white space becomes one space. Words, quoted identifiers, operators and
///   <c>$1</c> placeholders stay.</item>
/// <item>🔴 It fails closed: a string that never ends (a cut text) ends the shape with <c>?</c>.</item>
/// <item>🔴 A SETTING'S VALUE IS A VALUE, WORDS TOO. In a statement that starts with <c>SET</c> or <c>ALTER</c>, after
///   a <c>SET</c> outside parentheses, what follows the first <c>=</c> or <c>TO</c> outside parentheses, up to the
///   statement's end, is one <c>?</c>: <c>SET app.tenant = acme</c> and <c>ALTER ROLE r SET search_path TO s1, s2</c>
///   keep no value as words. Whatever spells the name in between (<c>U&amp;"..." UESCAPE '!'</c>) is shaped as usual,
///   so no way of writing a name ends the rule early. A <c>SET</c> with no <c>=</c> or <c>TO</c> after it keeps its
///   words (<c>SET ROLE r</c>, <c>ALTER TABLE t SET SCHEMA s</c>), and so does a statement that starts otherwise
///   (<c>UPDATE t SET a = b</c>, a column). It fails closed on <c>ALTER TABLE</c>: in <c>SET DEFAULT ?, OWNER TO
///   bob</c> the owner is masked.</item>
/// </list>
/// The lexer is the password redactor's, so both read a statement the same way.
/// </remarks>
public static class PgStatementShape
{
    public static string Of(string sql) => new Shaper(sql).Run();

    /// <summary>Where a statement is in a <c>SET name = value</c> clause: after its SET (<see cref="Name"/>), or in its value.</summary>
    private enum Setting
    {
        None,
        Name,
        Value,
    }

    /// <summary>One statement text, lexed once into its shape.</summary>
    private sealed class Shaper
    {
        private readonly string _sql;
        private readonly StringBuilder _output;
        private bool _space;
        private string? _first;          // the statement's first word, upper-cased; "" when quoted
        private int _depth;              // parentheses
        private Setting _setting;
        private bool _pending;           // the setting's value is not written yet

        public Shaper(string sql)
        {
            _sql = sql;
            _output = new StringBuilder(Math.Min(sql.Length, 4096));
        }

        public string Run()
        {
            var i = 0;
            while (i < _sql.Length)
            {
                int? next = _setting == Setting.Value ? SettingValue(i) : Step(i);
                if (next is not { } end)
                {
                    break;
                }
                i = end;
            }
            return Rows(_output);
        }

        /// <summary>One token outside a setting's value; null when the shape stops here.</summary>
        private int? Step(int i)
        {
            var sql = _sql;
            var c = sql[i];
            if (c == '$' && i + 1 < sql.Length && char.IsAsciiDigit(sql[i + 1]))
            {
                // A placeholder, not a value.
                var j = i + 1;
                while (j < sql.Length && char.IsAsciiDigit(sql[j]))
                {
                    j++;
                }
                Append(_output, sql.AsSpan(i, j - i), ref _space);
                return j;
            }
            if (c == '0' && i + 2 < sql.Length && sql[i + 1] is 'x' or 'X' or 'o' or 'O' or 'b' or 'B' && char.IsAsciiHexDigit(sql[i + 2]))
            {
                // PostgreSQL 16's 0x1F, 0o17 and 0b101: the digits are the value too.
                var j = i + 2;
                while (j < sql.Length && (char.IsAsciiHexDigit(sql[j]) || sql[j] == '_'))
                {
                    j++;
                }
                Value(_output, ref _space);
                return j;
            }
            if (char.IsAsciiDigit(c) || (c == '.' && i + 1 < sql.Length && char.IsAsciiDigit(sql[i + 1])))
            {
                Value(_output, ref _space);
                return NumberEnd(sql, i);
            }
            var token = PgPasswordRedactor.Lexer.Next(sql, i);
            switch (token.Kind)
            {
                case PgPasswordRedactor.TokenKind.Space:
                case PgPasswordRedactor.TokenKind.Comment:
                    _space = _output.Length > 0;
                    break;
                case PgPasswordRedactor.TokenKind.Literal:
                    Value(_output, ref _space);
                    if (Ambiguous(sql, i, token))
                    {
                        return null;
                    }
                    break;
                default:
                    Append(_output, sql.AsSpan(i, token.End - i), ref _space);
                    Track(i, token);
                    break;
            }
            return token.End;
        }

        /// <summary>
        /// A word or a character kept as it is: the statement's first word, parentheses, the end of a statement, and the
        /// start of a setting's value (<c>SET name =</c>, <c>SET name TO</c>).
        /// </summary>
        private void Track(int i, PgPasswordRedactor.Token token)
        {
            var c = _sql[i];
            if (token.Kind == PgPasswordRedactor.TokenKind.Word)
            {
                // A quoted name ("set", "to") is never a keyword.
                var word = c == '"' ? "" : token.Word!.ToUpperInvariant();
                if (_first == null)
                {
                    _first = word;
                    if (word == "SET")
                    {
                        _setting = Setting.Name;
                    }
                }
                else if (_depth == 0 && _setting == Setting.Name && word == "TO")
                {
                    (_setting, _pending) = (Setting.Value, true);
                }
                else if (_depth == 0 && _setting == Setting.None && word == "SET" && _first == "ALTER")
                {
                    _setting = Setting.Name;
                }
                return;
            }
            if (c == '(')
            {
                _depth++;
            }
            else if (c == ')')
            {
                _depth--;
            }
            else if (c == ';' && _depth <= 0)
            {
                EndStatement();
            }
            else if (c == '=' && _depth == 0 && _setting == Setting.Name)
            {
                (_setting, _pending) = (Setting.Value, true);
            }
        }

        private void EndStatement() => (_setting, _first, _depth) = (Setting.None, null, 0);

        /// <summary>
        /// One token of a setting's value: the value is one <c>?</c>, written at its first token, and it runs to the
        /// statement's end. A string in it still fails closed. Null when the shape stops here.
        /// </summary>
        private int? SettingValue(int i)
        {
            var c = _sql[i];
            var token = PgPasswordRedactor.Lexer.Next(_sql, i);
            switch (token.Kind)
            {
                case PgPasswordRedactor.TokenKind.Space:
                case PgPasswordRedactor.TokenKind.Comment:
                    _space = _output.Length > 0;
                    break;
                case PgPasswordRedactor.TokenKind.Other when c == ';' && _depth <= 0:
                    Append(_output, ";", ref _space);
                    EndStatement();
                    break;
                default:
                    if (_pending)
                    {
                        Value(_output, ref _space);
                        _pending = false;
                    }
                    _space = false;
                    if (token.Kind == PgPasswordRedactor.TokenKind.Literal && Ambiguous(_sql, i, token))
                    {
                        return null;
                    }
                    if (token.Kind == PgPasswordRedactor.TokenKind.Other)
                    {
                        _depth += c == '(' ? 1 : c == ')' ? -1 : 0;
                    }
                    break;
            }
            return token.End;
        }
    }

    /// <summary>
    /// A string that never ends (a cut text), or a plain string holding a backslash whose two readings end it at different
    /// quotes: the text after either end can be a value under the other, as in <c>('C:\', 'x')</c>.
    /// </summary>
    private static bool Ambiguous(string sql, int i, PgPasswordRedactor.Token token)
    {
        if (!token.Terminated)
        {
            return true;
        }
        if (token.Plain && sql.AsSpan(i, token.End - i).Contains('\\'))
        {
            var escaped = PgPasswordRedactor.Lexer.Quoted(sql, i + 1, backslashEscapes: true);
            return !escaped.Terminated || escaped.End != token.End;
        }
        return false;
    }

    /// <summary>A list of rows of values, as in <c>VALUES (?), (?)</c>, as one: the row count is no part of the shape.</summary>
    private static string Rows(StringBuilder output) =>
        System.Text.RegularExpressions.Regex.Replace(output.ToString(), @"\(\?\)(?:, ?\(\?\))+", "(?)");

    /// <summary>
    /// The direction of a COPY: "TO" (out of the database) or "FROM", the first of the two unquoted words outside
    /// parentheses; null when neither is there (a cut text).
    /// </summary>
    public static string? CopyDirection(string sql)
    {
        var depth = 0;
        var i = 0;
        while (i < sql.Length)
        {
            var token = PgPasswordRedactor.Lexer.Next(sql, i);
            // TO and FROM are reserved words, so only a quoted name ("from", U&"to") can spell one: it is the table's.
            if (token.Kind == PgPasswordRedactor.TokenKind.Word && depth == 0 && sql[i] != '"'
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
