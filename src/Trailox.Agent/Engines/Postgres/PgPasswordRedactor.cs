using System.Text;
using System.Text.RegularExpressions;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// Removes passwords from SQL text before it leaves the customer's network. PostgreSQL writes
/// <c>CREATE ROLE ... PASSWORD '...'</c> into its log as typed when DDL is logged, and <c>pg_stat_statements</c> keeps
/// the literals of such a statement too.
/// </summary>
/// <remarks>
/// <para>What is redacted:</para>
/// <list type="bullet">
/// <item>after the word <c>PASSWORD</c> (also <c>passwd</c>, <c>pwd</c>, <c>odbc_pwd</c>), the first string within the
///   next four tokens, unless a clause starts first (<c>FROM</c>, <c>WHERE</c>, <c>NULL</c>...): role passwords, SCRAM
///   verifiers and md5 hashes (what <c>\password</c> sends), <c>OPTIONS (password '...')</c>, and an application's
///   <c>password = '...'</c>, <c>= N'...'</c>, <c>= crypt('...', ...)</c> or <c>&lt;&gt; '...'</c>. Any quoting:
///   <c>'...'</c>, <c>E'...'</c>, <c>U&amp;'...'</c>, <c>$tag$...$tag$</c>;</item>
/// <item>the strings that continue a redacted one (PostgreSQL joins two strings separated by a line break);</item>
/// <item>every string argument of <c>create_aws_credentials(...)</c> (an access key and its secret);</item>
/// <item>inside any string: the value of a connection-string <c>password=</c>, and the password of a
///   <c>postgresql://user:password@host</c> URI (subscriptions, dblink, foreign servers).</item>
/// </list>
/// <para>🔴 It fails closed: when the redacted string is not terminated (a cut text), everything from it on is
/// redacted; and a plain string that holds a backslash is redacted to the farther of its two possible ends, since
/// <c>standard_conforming_strings = off</c> makes the backslash an escape. It is not a general literal scrubber.</para>
/// </remarks>
public static class PgPasswordRedactor
{
    public const string Mask = "<redacted>";

    private const int Reach = 4;

    private static readonly HashSet<string> SecretWords = new(StringComparer.OrdinalIgnoreCase) { "password", "passwd", "pwd", "odbc_pwd" };

    // A clause that starts before any string: "SELECT password FROM t WHERE name = 'x'" holds no password.
    private static readonly HashSet<string> StopWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "FROM", "WHERE", "AND", "OR", "NULL", "VALID", "UNTIL", "IS", "AS", "SELECT", "INTO", "ORDER", "GROUP", "LIMIT",
        "RETURNING", "ON", "JOIN", "VALUES", "SET",
    };

    // password=value inside a connection string, in every quoting a value can have there: ''v'' (an SQL string's
    // doubled quotes), \'v\' (an E-string's), 'v', or unquoted with libpq's backslash escapes and doubled quotes.
    private static readonly Regex ConnInfoPassword = new(
        @"(?i)(\bpassword\s*=\s*)(''(?:\\''|[^'])*''|\\'.*?\\'|'(?:[^'\\]|\\.)*'|(?:\\.|''|[^\s'\\])+)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    // postgresql://user:secret@host and postgres://user:secret@host.
    private static readonly Regex UriPassword = new(@"(?i)(postgres(?:ql)?://[^:/@\s]*:)[^@\s]*(@)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The text with every password replaced by <see cref="Mask"/>. Text with no password comes back unchanged.</summary>
    public static string Redact(string sql)
    {
        if (string.IsNullOrEmpty(sql) || (sql.IndexOf("pass", StringComparison.OrdinalIgnoreCase) < 0
            && sql.IndexOf("pwd", StringComparison.OrdinalIgnoreCase) < 0
            && sql.IndexOf("://", StringComparison.Ordinal) < 0
            && sql.IndexOf("create_aws_credentials", StringComparison.OrdinalIgnoreCase) < 0))
        {
            return sql;
        }
        var output = new StringBuilder(sql.Length);
        var i = 0;
        var reach = 0;              // tokens left to find the string after a secret word
        var continues = false;      // the last string was redacted; a string after a line break continues it
        var broken = false;         // a line break since that string
        var awaitParen = false;     // create_aws_credentials, before its '('
        var argDepth = 0;           // inside create_aws_credentials(...)
        while (i < sql.Length)
        {
            var token = Lexer.Next(sql, i);
            var text = sql.AsSpan(i, token.End - i);
            switch (token.Kind)
            {
                case TokenKind.Space:
                case TokenKind.Comment:
                    output.Append(text);
                    broken |= text.Contains('\n');
                    i = token.End;
                    continue;
                case TokenKind.Literal when reach > 0 || argDepth > 0 || (continues && broken):
                {
                    var end = FailClosedEnd(sql, i, token);
                    output.Append('\'').Append(Mask).Append('\'');
                    if (end < 0)
                    {
                        return output.ToString();
                    }
                    reach = 0;
                    continues = true;
                    broken = false;
                    i = end;
                    continue;
                }
                case TokenKind.Literal:
                    output.Append(RedactInside(text.ToString()));
                    if (!token.Terminated)
                    {
                        return output.ToString();
                    }
                    reach = 0;
                    break;
                case TokenKind.Word:
                    output.Append(text);
                    if (SecretWords.Contains(token.Word!))
                    {
                        reach = Reach;
                    }
                    else if (reach > 0)
                    {
                        reach = StopWords.Contains(token.Word!) ? 0 : reach - 1;
                    }
                    awaitParen = string.Equals(token.Word, "create_aws_credentials", StringComparison.OrdinalIgnoreCase);
                    break;
                default:
                    output.Append(text);
                    var c = text[0];
                    if (c == '(' && (awaitParen || argDepth > 0))
                    {
                        argDepth++;
                    }
                    else if (c == ')' && argDepth > 0)
                    {
                        argDepth--;
                    }
                    // A ',' or ';' ends the secret word's clause; any other character uses up one token of its reach.
                    if (c is ',' or ';')
                    {
                        reach = 0;
                    }
                    else if (reach > 0)
                    {
                        reach--;
                    }
                    if (c != '.')
                    {
                        awaitParen = false;
                    }
                    break;
            }
            continues = false;
            broken = false;
            i = token.End;
        }
        return output.ToString();
    }

    /// <summary>
    /// Where a redacted string ends, or -1 when it never does (then everything after it is redacted too). A plain string
    /// holding a backslash ends at the farther of its standard end and its end under backslash escapes.
    /// </summary>
    private static int FailClosedEnd(string sql, int start, Token token)
    {
        if (!token.Terminated)
        {
            return -1;
        }
        if (token.Plain && sql.AsSpan(start, token.End - start).Contains('\\'))
        {
            var escaped = Lexer.Quoted(sql, start + 1, backslashEscapes: true);
            return escaped.Terminated ? Math.Max(token.End, escaped.End) : -1;
        }
        return token.End;
    }

    private static string RedactInside(string literal)
    {
        if (literal.IndexOf("password", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            literal = ConnInfoPassword.Replace(literal, m => m.Groups[1].Value + Mask);
        }
        if (literal.IndexOf("://", StringComparison.Ordinal) >= 0)
        {
            literal = UriPassword.Replace(literal, m => m.Groups[1].Value + Mask + m.Groups[2].Value);
        }
        return literal;
    }

    private enum TokenKind
    {
        Space,
        Comment,
        Literal,
        Word,
        Other,
    }

    /// <param name="Plain">A standard string, '...' with no E or U&amp; prefix.</param>
    private readonly record struct Token(TokenKind Kind, int End, string? Word = null, bool Terminated = true, bool Plain = false);

    /// <summary>Just enough of PostgreSQL's lexer to tell a password's string from the words and strings around it.</summary>
    private static class Lexer
    {
        public static Token Next(string s, int i)
        {
            var c = s[i];
            if (char.IsWhiteSpace(c))
            {
                var j = i;
                while (j < s.Length && char.IsWhiteSpace(s[j]))
                {
                    j++;
                }
                return new Token(TokenKind.Space, j);
            }
            if (c == '-' && At(s, i + 1, '-'))
            {
                var j = s.IndexOf('\n', i);
                return new Token(TokenKind.Comment, j < 0 ? s.Length : j);
            }
            if (c == '/' && At(s, i + 1, '*'))
            {
                return new Token(TokenKind.Comment, BlockCommentEnd(s, i));
            }
            if (c == '\'')
            {
                return Quoted(s, i + 1, backslashEscapes: false) with { Plain = true };
            }
            if ((c is 'E' or 'e') && At(s, i + 1, '\''))
            {
                return Quoted(s, i + 2, backslashEscapes: true);
            }
            if ((c is 'U' or 'u') && At(s, i + 1, '&') && At(s, i + 2, '\''))
            {
                return Quoted(s, i + 3, backslashEscapes: false);
            }
            if (c == '$' && DollarTag(s, i) is { } tag)
            {
                var close = s.IndexOf(tag, i + tag.Length, StringComparison.Ordinal);
                return close < 0
                    ? new Token(TokenKind.Literal, s.Length, Terminated: false)
                    : new Token(TokenKind.Literal, close + tag.Length);
            }
            if (c == '"')
            {
                return QuotedIdentifier(s, i);
            }
            if (char.IsLetter(c) || c == '_')
            {
                var j = i;
                while (j < s.Length && (char.IsLetterOrDigit(s[j]) || s[j] is '_' or '$'))
                {
                    j++;
                }
                return new Token(TokenKind.Word, j, s[i..j]);
            }
            return new Token(TokenKind.Other, i + 1);
        }

        private static bool At(string s, int i, char c) => i < s.Length && s[i] == c;

        /// <summary>A quoted identifier, as a word: "password" in OPTIONS ("password" '...') is still the password's name.</summary>
        private static Token QuotedIdentifier(string s, int i)
        {
            var j = i + 1;
            var name = new StringBuilder();
            while (j < s.Length)
            {
                if (s[j] == '"')
                {
                    if (At(s, j + 1, '"'))
                    {
                        name.Append('"');
                        j += 2;
                        continue;
                    }
                    j++;
                    break;
                }
                name.Append(s[j]);
                j++;
            }
            return new Token(TokenKind.Word, j, name.ToString());
        }

        /// <summary>
        /// A string from just after its opening quote: '' is a quote, and with backslash escapes a backslash escapes the next
        /// character.
        /// </summary>
        public static Token Quoted(string s, int i, bool backslashEscapes)
        {
            var j = i;
            while (j < s.Length)
            {
                if (backslashEscapes && s[j] == '\\')
                {
                    j += 2;
                    continue;
                }
                if (s[j] == '\'')
                {
                    if (At(s, j + 1, '\''))
                    {
                        j += 2;
                        continue;
                    }
                    return new Token(TokenKind.Literal, j + 1);
                }
                j++;
            }
            return new Token(TokenKind.Literal, s.Length, Terminated: false);
        }

        /// <summary>$$ or $tag$ at <paramref name="i"/>: the tag is letters, digits and '_', not starting with a digit.</summary>
        private static string? DollarTag(string s, int i)
        {
            var j = i + 1;
            if (j < s.Length && char.IsAsciiDigit(s[j]))
            {
                return null;
            }
            while (j < s.Length && (char.IsLetterOrDigit(s[j]) || s[j] == '_'))
            {
                j++;
            }
            return j < s.Length && s[j] == '$' ? s[i..(j + 1)] : null;
        }

        private static int BlockCommentEnd(string s, int i)
        {
            // PostgreSQL's block comments nest.
            var depth = 0;
            var j = i;
            while (j < s.Length - 1)
            {
                if (s[j] == '/' && s[j + 1] == '*')
                {
                    depth++;
                    j += 2;
                }
                else if (s[j] == '*' && s[j + 1] == '/')
                {
                    depth--;
                    j += 2;
                    if (depth == 0)
                    {
                        return j;
                    }
                }
                else
                {
                    j++;
                }
            }
            return s.Length;
        }
    }
}
