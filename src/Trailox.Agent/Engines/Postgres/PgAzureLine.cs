using System.Globalization;
using System.Text;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// One line of the server log download of Azure Database for PostgreSQL flexible server. Azure writes each record from
/// its JSON form: the line is the record's text as JSON string content (a quote as <c>\"</c>, a newline as <c>\n</c>),
/// and the record's other fields follow on the same line, each as <c>","name": "value</c> (an error's
/// <c>statement</c>, a refused sign-in's <c>detail_log</c>).
/// </summary>
/// <remarks>
/// 🔴 A SEPARATOR IS A QUOTE THAT IS NOT ESCAPED. Inside the content every quote of the text is <c>\"</c>, so the text
///    of a statement cannot forge a field: <c>","statement": "</c> written in a statement reaches the file as
///    <c>\",\"statement\": \"</c>.
/// </remarks>
public static class PgAzureLine
{
    public readonly record struct Field(string Name, string Value);

    /// <summary>The record's text and the fields after it, each unescaped.</summary>
    public static (string Text, List<Field> Fields) Split(string line)
    {
        var fields = new List<Field>();
        string? name = null;
        string? text = null;
        var start = 0;
        var escaped = false;
        for (var i = 0; i < line.Length; i++)
        {
            var c = line[i];
            if (escaped)
            {
                escaped = false;
                continue;
            }
            if (c == '\\')
            {
                escaped = true;
                continue;
            }
            if (c != '"' || SeparatorName(line, i) is not { } separator)
            {
                continue;
            }
            var (fieldName, valueStart) = separator;
            var value = Unescape(line.AsSpan(start, i - start));
            if (name == null)
            {
                text = value;
            }
            else
            {
                fields.Add(new Field(name, value));
            }
            name = fieldName;
            start = valueStart;
            i = valueStart - 1;
        }
        var last = Unescape(line.AsSpan(start));
        if (name == null)
        {
            return (last, fields);
        }
        fields.Add(new Field(name, last));
        return (text!, fields);
    }

    /// <summary>
    /// At an unescaped quote: <c>","name": "</c>, the field's name and where its value starts; null when the quote
    /// starts no separator.
    /// </summary>
    private static (string, int)? SeparatorName(string line, int quote)
    {
        if (!line.AsSpan(quote).StartsWith("\",\""))
        {
            return null;
        }
        var nameStart = quote + 3;
        var nameEnd = nameStart;
        while (nameEnd < line.Length && (char.IsAsciiLetterLower(line[nameEnd]) || line[nameEnd] == '_'))
        {
            nameEnd++;
        }
        return nameEnd > nameStart && line.AsSpan(nameEnd).StartsWith("\": \"")
            ? (line[nameStart..nameEnd], nameEnd + 4)
            : null;
    }

    /// <summary>
    /// A record as Azure's download writes it, from the record's text and fields as Log Analytics keeps them: the text,
    /// then each field that has a value as <c>","name": "value</c>, all escaped as JSON string content.
    /// </summary>
    /// <remarks>Every quote of a value is escaped, so no value can forge a field (<see cref="Split"/> reads it back whole).</remarks>
    public static string Join(string text, IEnumerable<Field> fields)
    {
        var b = new StringBuilder(text.Length + 16);
        Escape(text, b);
        foreach (var f in fields.Where(f => !string.IsNullOrEmpty(f.Value)))
        {
            b.Append("\",\"").Append(f.Name).Append("\": \"");
            Escape(f.Value, b);
        }
        return b.ToString();
    }

    /// <summary>Text as JSON string content: the escapes <see cref="Unescape"/> reads back.</summary>
    internal static void Escape(string s, StringBuilder b)
    {
        foreach (var c in s)
        {
            switch (c)
            {
                case '"':
                    b.Append("\\\"");
                    break;
                case '\\':
                    b.Append("\\\\");
                    break;
                case '\n':
                    b.Append("\\n");
                    break;
                case '\r':
                    b.Append("\\r");
                    break;
                case '\t':
                    b.Append("\\t");
                    break;
                case < ' ':
                    b.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                    break;
                default:
                    b.Append(c);
                    break;
            }
        }
    }

    /// <summary>JSON string content as text; an escape JSON does not know is kept as it is.</summary>
    internal static string Unescape(ReadOnlySpan<char> s)
    {
        if (s.IndexOf('\\') < 0)
        {
            return s.ToString();
        }
        var b = new StringBuilder(s.Length);
        for (var i = 0; i < s.Length; i++)
        {
            if (s[i] != '\\' || i + 1 >= s.Length)
            {
                b.Append(s[i]);
                continue;
            }
            var e = s[++i];
            switch (e)
            {
                case '"' or '\\' or '/':
                    b.Append(e);
                    break;
                case 'n':
                    b.Append('\n');
                    break;
                case 't':
                    b.Append('\t');
                    break;
                case 'r':
                    b.Append('\r');
                    break;
                case 'b':
                    b.Append('\b');
                    break;
                case 'f':
                    b.Append('\f');
                    break;
                case 'u' when i + 4 < s.Length
                    && int.TryParse(s.Slice(i + 1, 4), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out var code):
                    b.Append((char)code);
                    i += 4;
                    break;
                default:
                    b.Append('\\').Append(e);
                    break;
            }
        }
        return b.ToString();
    }
}
