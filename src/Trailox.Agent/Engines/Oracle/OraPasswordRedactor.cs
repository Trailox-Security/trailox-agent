using System.Buffers;
using System.Text;
using System.Text.RegularExpressions;

namespace Trailox.Agent.Engines.Oracle;

/// <summary>
/// Removes passwords from statement text before it is shaped. Oracle masks some of them itself (<c>IDENTIFIED BY *</c>), but
/// not inside a PL/SQL block, and not in a statement it could not parse, and a failed statement is audited too. So this
/// runs on every text, whatever Oracle did.
/// </summary>
/// <remarks>
/// <para>🔴 THIS IS THE FIRST OF TWO STEPS. <see cref="OraStatementShape"/> then removes every string and number, and every
/// comment. No pattern can find a password that dynamic SQL spells in pieces, so strings are not left to this class. What
/// only this class can remove is a secret that is not a string: a password written as a bare word or a quoted name.</para>
/// <para>What is redacted:</para>
/// <list type="bullet">
/// <item>after <c>IDENTIFIED BY</c>: the next password, in any quoting or none (users, roles, database links with their
///   <c>AUTHENTICATED BY ... IDENTIFIED BY</c>, keystores); <c>IDENTIFIED BY VALUES '...'</c>, a hash; and the old or new
///   password after <c>REPLACE</c> or <c>SET</c>;</item>
/// <item>the next word, name or string after <c>SECRET</c> in <c>ADMINISTER KEY MANAGEMENT</c>, and after <c>ENCRYPT USING</c> or
///   <c>DECRYPT USING</c>;</item>
/// <item>the first string or quoted name within the next few tokens after a name that says it holds a secret:
///   <c>password => '...'</c>, <c>p_password => '...'</c>, <c>v_pwd VARCHAR2(30) := '...'</c>, and the strings joined to it;</item>
/// <item>every string in a call to a procedure that takes a secret by position (<see cref="SecretCalls"/>);</item>
/// <item>a whole string or quoted name that holds any of these itself, as dynamic SQL does, or a URL with a password in it.</item>
/// </list>
/// <para>🔴 It fails closed: any quote that never ends redacts the rest of the text, and so does <c>IDENTIFIED</c> followed by
/// anything Oracle does not take there. A bare password runs to the next ASCII white space, whatever it holds: one that is
/// no valid word fails the statement, whose text is still audited. A quoted identifier is never a keyword. It shares
/// nothing with PostgreSQL's redactor: the quoting rules differ.</para>
/// </remarks>
public static class OraPasswordRedactor
{
    public const string Mask = "<redacted>";

    private const int Reach = 12;

    /// <summary>Texts that hold none of these go out unchanged, without being lexed.</summary>
    private static readonly SearchValues<string> Hints = SearchValues.Create(
        new[] { "identified", "pass", "pwd", "pw", "secret", "credential", "token", "private_key", "api_key", "access_key", "crypt", "utl_http", "utl_smtp", "dbms_ldap", "dbms_datapump", "://" },
        StringComparison.OrdinalIgnoreCase);

    private static readonly SearchValues<string> SecretNames = SearchValues.Create(
        new[] { "pass", "pwd", "secret", "private_key", "api_key", "access_key", "token" },
        StringComparison.OrdinalIgnoreCase);

    /// <summary>Procedures with a secret among their arguments, by position: every string in the call goes.</summary>
    internal static readonly string[] SecretCalls =
    {
        "DBMS_CREDENTIAL.CREATE_CREDENTIAL", "DBMS_CREDENTIAL.UPDATE_CREDENTIAL",
        "DBMS_CLOUD.CREATE_CREDENTIAL", "DBMS_CLOUD.UPDATE_CREDENTIAL",
        "DBMS_SCHEDULER.CREATE_CREDENTIAL",
        "DBMS_VECTOR.CREATE_CREDENTIAL", "DBMS_VECTOR_CHAIN.CREATE_CREDENTIAL",
        "UTL_HTTP.SET_AUTHENTICATION", "UTL_HTTP.SET_WALLET", "UTL_HTTP.SET_PROXY",
        "UTL_HTTP.REQUEST", "UTL_HTTP.REQUEST_PIECES", "UTL_HTTP.CREATE_REQUEST_CONTEXT",
        "UTL_SMTP.AUTH",
        "DBMS_LDAP.SIMPLE_BIND_S", "DBMS_LDAP.BIND_S",
        "DBMS_DATAPUMP.SET_PARAMETER",
    };

    private static readonly HashSet<string> SecretCallSet = new(SecretCalls, StringComparer.OrdinalIgnoreCase);

    // A clause that starts before any string: "password FROM t WHERE name = 'x'" holds no password.
    private static readonly HashSet<string> StopWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "FROM", "WHERE", "AND", "OR", "IS", "SELECT", "INTO", "ORDER", "GROUP", "ON", "JOIN", "VALUES", "SET",
    };

    // What a statement goes on with after a password. Anything else there is no statement Oracle reads, and may be more of
    // the password: the rest goes. A valid clause missing from this list costs the end of a text, never a secret.
    private static readonly HashSet<string> AfterPassword = new(StringComparer.OrdinalIgnoreCase)
    {
        "DEFAULT", "LOCAL", "TEMPORARY", "QUOTA", "PROFILE", "PASSWORD", "ACCOUNT", "ENABLE", "DISABLE", "CONTAINER", "AUTHENTICATED", "USING", "WITH", "FORCE",
        "TO", "FROM", "FOR", "ROLES", "FILE_NAME_CONVERT", "PATH_PREFIX", "STORAGE", "CREATE_FILE_DEST", "TEMPFILE", "PARALLEL", "NOLOGGING", "EDITIONS",
    };

    // Inside a string: what makes the whole string go. Linear on any input, as the text comes from any user.
    private static readonly Regex InsideString = new(
        @"\bidentified\s+by\b"
        + @"|[a-z0-9_$#]*(password|passwd|pwd|secret|private_key|api_key|access_key|token)[a-z0-9_$#]*[""'\s]*(=>|:=|=)"
        + @"|[a-z][a-z0-9+.\-]*://[^:/@\s']*:[^@\s']*@"
        + "|" + string.Join("|", SecretCalls.Select(Regex.Escape)),
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.NonBacktracking);

    /// <summary>The text with every password replaced by <see cref="Mask"/>. Text with no password comes back unchanged.</summary>
    public static string Redact(string sql)
    {
        if (string.IsNullOrEmpty(sql) || !sql.AsSpan().ContainsAny(Hints))
        {
            return sql;
        }
        var keyManagement = sql.Contains("ADMINISTER", StringComparison.OrdinalIgnoreCase);
        var output = new StringBuilder(sql.Length);
        var i = 0;
        var reach = 0;              // tokens left to find the string after a name that says secret
        var joined = false;         // a string was redacted, and its argument or statement has not ended
        var awaitParen = false;     // a secret call's name, before its '('
        var argDepth = 0;           // inside a secret call
        string? before = null;      // the name before a '.'
        var dotted = false;
        while (i < sql.Length)
        {
            var token = Lexer.Next(sql, i);
            var text = sql.AsSpan(i, token.End - i);
            switch (token.Kind)
            {
                case TokenKind.Space:
                case TokenKind.Comment:
                    output.Append(text);
                    i = token.End;
                    continue;
                case TokenKind.Literal or TokenKind.QuotedIdentifier when !token.Terminated:
                    return output.Append(Mask).ToString();
                case TokenKind.Literal when joined || reach > 0 || argDepth > 0 || InsideString.IsMatch(text):
                case TokenKind.QuotedIdentifier when reach > 0 || InsideString.IsMatch(text):
                    output.Append('\'').Append(Mask).Append('\'');
                    // The next string may be joined to this one: 'ALTER USER a IDENTIFIED BY ' || CHR(34) || 'x'.
                    joined = true;
                    reach = 0;
                    break;
                case TokenKind.Literal:
                    output.Append(text);
                    reach = 0;
                    break;
                case TokenKind.Word when SecretClause(sql, i, token, keyManagement, output) is { } next:
                    if (next < 0)
                    {
                        return output.ToString();
                    }
                    i = next;
                    reach = 0;
                    joined = false;
                    continue;
                case TokenKind.Word:
                case TokenKind.QuotedIdentifier:
                    output.Append(text);
                    if (SecretCallSet.Contains(dotted && before != null ? before + "." + token.Word : token.Word ?? ""))
                    {
                        awaitParen = true;
                    }
                    if (IsSecretName(token.Word!))
                    {
                        reach = Reach;
                    }
                    else if (reach > 0)
                    {
                        reach = token.Kind == TokenKind.Word && StopWords.Contains(token.Word!) ? 0 : reach - 1;
                    }
                    before = token.Word;
                    dotted = false;
                    i = token.End;
                    continue;
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
                    // A ',' or ';' ends the name's clause; anything else uses up one token of its reach.
                    if (c is ',' or ';')
                    {
                        reach = 0;
                        joined = false;
                    }
                    else if (reach > 0)
                    {
                        reach--;
                    }
                    awaitParen = false;
                    if (c == '.')
                    {
                        dotted = true;
                        i = token.End;
                        continue;
                    }
                    break;
            }
            dotted = false;
            i = token.End;
        }
        return output.ToString();
    }

    /// <summary>
    /// The clause a keyword opens, its secret replaced, and the keyword itself written. Returns where the text goes on, -1 when
    /// nothing after it can be shipped, or null with nothing written when this word opens no such clause.
    /// </summary>
    private static int? SecretClause(string sql, int i, Token token, bool keyManagement, StringBuilder output)
    {
        if (Is(token, "IDENTIFIED"))
        {
            return Identified(sql, i, token, output);
        }
        if (keyManagement && Is(token, "SECRET"))
        {
            output.Append(sql, i, token.End - i);
            return Password(sql, token.End, output, mayContinue: false);
        }
        if ((Is(token, "ENCRYPT") || Is(token, "DECRYPT")) && After(sql, token.End, "USING") is { } usingEnd)
        {
            output.Append(sql, i, usingEnd - i);
            return Password(sql, usingEnd, output, mayContinue: false);
        }
        return null;
    }

    /// <summary>After <c>IDENTIFIED</c>: only BY opens a password, and only what Oracle takes there lets the text go on.</summary>
    private static int Identified(string sql, int i, Token token, StringBuilder output)
    {
        var at = Gap(sql, token.End);
        output.Append(sql, i, at - i);
        if (at >= sql.Length)
        {
            return at;
        }
        var next = Lexer.Next(sql, at);
        if (next.Kind == TokenKind.Word && Is(next, "BY"))
        {
            output.Append(sql, at, next.End - at);
            return Password(sql, next.End, output, mayContinue: true);
        }
        if (next.Kind == TokenKind.Word && (Is(next, "EXTERNALLY") || Is(next, "GLOBALLY") || Is(next, "USING") || Is(next, "WITH")))
        {
            return at;
        }
        output.Append(Mask);
        return -1;
    }

    private static bool Is(Token token, string keyword) => string.Equals(token.Word, keyword, StringComparison.OrdinalIgnoreCase);

    private static bool IsSecretName(string word) =>
        word.AsSpan().ContainsAny(SecretNames) || word.Equals("pw", StringComparison.OrdinalIgnoreCase) || word.EndsWith("_pw", StringComparison.OrdinalIgnoreCase);

    /// <summary>Where the unquoted <paramref name="keyword"/> after <paramref name="i"/> ends, past spaces and comments; null when the next word is not it.</summary>
    private static int? After(string sql, int i, string keyword)
    {
        var at = Gap(sql, i);
        if (at >= sql.Length)
        {
            return null;
        }
        var token = Lexer.Next(sql, at);
        return token.Kind == TokenKind.Word && Is(token, keyword) ? token.End : null;
    }

    /// <summary>Past white space and comments.</summary>
    private static int Gap(string sql, int i)
    {
        while (i < sql.Length)
        {
            var token = Lexer.Next(sql, i);
            if (token.Kind is not (TokenKind.Space or TokenKind.Comment))
            {
                break;
            }
            i = token.End;
        }
        return i;
    }

    /// <summary>
    /// Replaces the secret at <paramref name="start"/>, and with <paramref name="mayContinue"/> each one after REPLACE or SET.
    /// Returns where the text goes on, or -1 when nothing after it can be shipped.
    /// </summary>
    private static int Password(string sql, int start, StringBuilder output, bool mayContinue)
    {
        while (true)
        {
            var i = Gap(sql, start);
            output.Append(sql, start, i - start);
            if (i >= sql.Length)
            {
                return i;
            }
            var first = Lexer.Next(sql, i);
            if (first.Kind == TokenKind.Word && Is(first, "VALUES"))
            {
                var hash = Gap(sql, first.End);
                if (hash < sql.Length && Lexer.Next(sql, hash).Kind is TokenKind.Literal or TokenKind.QuotedIdentifier)
                {
                    output.Append(sql, i, hash - i);
                    i = hash;
                }
            }
            var end = SecretEnd(sql, i);
            output.Append(Mask);
            if (end < 0)
            {
                return -1;
            }
            if (!mayContinue)
            {
                return end;
            }
            if ((After(sql, end, "REPLACE") ?? After(sql, end, "SET")) is { } next)
            {
                output.Append(sql, end, next - end);
                start = next;
                continue;
            }
            return GoesOn(sql, end) ? end : RestGoes(output);
        }
    }

    /// <summary>Whether what follows a password is the statement going on, not more of what the user typed.</summary>
    private static bool GoesOn(string sql, int end)
    {
        var at = Gap(sql, end);
        if (at >= sql.Length)
        {
            return true;
        }
        var token = Lexer.Next(sql, at);
        return token.Kind == TokenKind.Other || (token.Kind == TokenKind.Word && AfterPassword.Contains(token.Word!));
    }

    private static int RestGoes(StringBuilder output)
    {
        output.Append(' ').Append(Mask);
        return -1;
    }

    /// <summary>
    /// Where the secret at <paramref name="i"/> ends: at ASCII white space, a comment, or a word that goes on with the
    /// statement. Whatever else follows it is part of it: <c>"pw"x</c> is no statement Oracle reads, and x may be part of the
    /// password. -1 when a quote in it never ends.
    /// </summary>
    private static int SecretEnd(string sql, int i)
    {
        var end = i;
        while (end < sql.Length)
        {
            var token = Lexer.Next(sql, end);
            if (!token.Terminated)
            {
                return -1;
            }
            if (token.Kind == TokenKind.Comment || (token.Kind == TokenKind.Space && char.IsAscii(sql[end])))
            {
                break;
            }
            if (end > i && token.Kind == TokenKind.Word && (Is(token, "REPLACE") || Is(token, "SET")))
            {
                break;
            }
            end = token.End;
        }
        return end;
    }

    internal enum TokenKind
    {
        Space,
        Comment,
        Literal,
        Word,
        QuotedIdentifier,
        Other,
    }

    /// <param name="Word">A word as written, or a quoted identifier without its quotes.</param>
    /// <param name="Terminated">A string or quoted identifier that has its closing quote.</param>
    internal readonly record struct Token(TokenKind Kind, int End, string? Word = null, bool Terminated = true);

    /// <summary>Just enough of Oracle's lexer to tell strings, names and comments apart. Both the redactor and the shape read a statement with it.</summary>
    internal static class Lexer
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
                // Oracle's block comments do not nest.
                var j = s.IndexOf("*/", i + 2, StringComparison.Ordinal);
                return new Token(TokenKind.Comment, j < 0 ? s.Length : j + 2);
            }
            if (c == '\'')
            {
                return Quoted(s, i + 1);
            }
            if (c is 'q' or 'Q' && At(s, i + 1, '\''))
            {
                return AlternativeQuote(s, i + 2);
            }
            if (c is 'n' or 'N' && At(s, i + 1, '\''))
            {
                return Quoted(s, i + 2);
            }
            if (c is 'n' or 'N' && i + 2 < s.Length && s[i + 1] is 'q' or 'Q' && s[i + 2] == '\'')
            {
                return AlternativeQuote(s, i + 3);
            }
            if (c == '"')
            {
                // No doubled quote: a quoted identifier cannot hold a double quote.
                var close = s.IndexOf('"', i + 1);
                return close < 0
                    ? new Token(TokenKind.QuotedIdentifier, s.Length, s[(i + 1)..], Terminated: false)
                    : new Token(TokenKind.QuotedIdentifier, close + 1, s[(i + 1)..close]);
            }
            if (char.IsLetter(c) || c == '_')
            {
                var j = i;
                while (j < s.Length && (char.IsLetterOrDigit(s[j]) || s[j] is '_' or '$' or '#'))
                {
                    j++;
                }
                return new Token(TokenKind.Word, j, s[i..j]);
            }
            if (char.IsAsciiDigit(c))
            {
                var j = i;
                while (j < s.Length && char.IsAsciiDigit(s[j]))
                {
                    j++;
                }
                return new Token(TokenKind.Other, j);
            }
            return new Token(TokenKind.Other, i + 1);
        }

        private static bool At(string s, int i, char c) => i < s.Length && s[i] == c;

        /// <summary>A string from just after its opening quote: '' is a quote.</summary>
        private static Token Quoted(string s, int i)
        {
            var j = i;
            while (j < s.Length)
            {
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

        /// <summary>
        /// <c>q'[...]'</c> from its delimiter: the string ends at the closing delimiter and a quote, so it can hold quotes.
        /// Any character may delimit; <c>[</c>, <c>{</c>, <c>(</c> and <c>&lt;</c> close with their pair.
        /// </summary>
        private static Token AlternativeQuote(string s, int i)
        {
            if (i >= s.Length)
            {
                return new Token(TokenKind.Literal, s.Length, Terminated: false);
            }
            var open = s[i];
            var close = open switch { '[' => ']', '{' => '}', '(' => ')', '<' => '>', _ => open };
            var end = s.IndexOf(close + "'", i + 1, StringComparison.Ordinal);
            return end < 0 ? new Token(TokenKind.Literal, s.Length, Terminated: false) : new Token(TokenKind.Literal, end + 2);
        }
    }
}
