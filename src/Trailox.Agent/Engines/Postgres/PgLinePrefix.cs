namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// A <c>log_line_prefix</c>, read as PostgreSQL reads it (elog.c, <c>log_line_prefix</c>): text written as it is, and
/// escapes. An escape is <c>%</c>, an optional padding (<c>-</c> and digits, as in <c>%-10u</c>), then one character;
/// <c>%%</c> writes a <c>%</c>, and a character PostgreSQL does not know writes nothing.
/// </summary>
public static class PgLinePrefix
{
    /// <summary>One part of a prefix: text (<see cref="Escape"/> is '\0'), or an escape and its padding.</summary>
    public readonly record struct Token(char Escape, string Text, int Padding);

    /// <summary>The parts of <paramref name="prefix"/> in order; neighbouring text is one part.</summary>
    public static IReadOnlyList<Token> Tokens(string prefix)
    {
        var tokens = new List<Token>();
        var text = new System.Text.StringBuilder();
        for (var i = 0; i < prefix.Length; i++)
        {
            if (prefix[i] != '%')
            {
                text.Append(prefix[i]);
                continue;
            }
            if (++i >= prefix.Length)
            {
                break;
            }
            var padding = 0;
            if (prefix[i] <= '9')
            {
                // PostgreSQL reads a padding before any character up to '9': '-', the digits, and also '%' and ' ',
                // which take none.
                var sign = 1;
                if (prefix[i] == '-')
                {
                    sign = -1;
                    if (++i >= prefix.Length)
                    {
                        break;
                    }
                }
                while (i < prefix.Length && char.IsAsciiDigit(prefix[i]))
                {
                    padding = padding * 10 + (prefix[i++] - '0');
                }
                if (i >= prefix.Length)
                {
                    break;
                }
                padding *= sign;
            }
            if (prefix[i] == '%')
            {
                text.Append('%');
                continue;
            }
            if (text.Length > 0)
            {
                tokens.Add(new Token('\0', text.ToString(), 0));
                text.Clear();
            }
            tokens.Add(new Token(prefix[i], "", padding));
        }
        if (text.Length > 0)
        {
            tokens.Add(new Token('\0', text.ToString(), 0));
        }
        return tokens;
    }

    /// <summary>The escape characters of <paramref name="prefix"/>, in order (<c>t</c> for <c>%t</c>).</summary>
    public static IReadOnlyList<char> Escapes(string prefix) =>
        Tokens(prefix).Where(t => t.Escape != '\0').Select(t => t.Escape).ToList();
}
