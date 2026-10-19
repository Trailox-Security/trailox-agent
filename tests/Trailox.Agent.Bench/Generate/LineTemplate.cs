using System.Buffers;
using System.Text;

namespace Trailox.Agent.Bench.Generate;

/// <summary>The placeholders a template may use, written {{name}} in the .tmpl files.</summary>
internal enum Slot
{
    T, Conn, Cip, Cport, Nip, Host, Uuid, Cuuid, User, App, Drv, Drvver, Db, Ns, Cmd, Cmdname,
    Qsh, Keys, Docs, Nret, Reslen, Cpu, Dur, Pid, Result,
}

/// <summary>
/// One log line template compiled into literal byte segments and slots, so a generator can write
/// millions of lines without parsing the template again.
/// </summary>
internal sealed class LineTemplate
{
    private readonly (byte[]? Literal, Slot Slot)[] _segments;

    public string Name { get; }

    private LineTemplate(string name, (byte[]?, Slot)[] segments)
    {
        Name = name;
        _segments = segments;
    }

    public void Render(IBufferWriter<byte> output, LineValues values)
    {
        foreach (var (literal, slot) in _segments)
        {
            output.Write(literal ?? values[slot]);
        }
        output.Write("\n"u8);
    }

    /// <summary>Reads a .tmpl file: '#' lines are comments, every other line is name|template.</summary>
    public static IReadOnlyList<LineTemplate> Load(string path)
    {
        var templates = new List<LineTemplate>();
        foreach (var raw in File.ReadLines(path))
        {
            if (raw.Length == 0 || raw[0] == '#')
            {
                continue;
            }
            var bar = raw.IndexOf('|');
            if (bar <= 0)
            {
                throw new FormatException($"{path}: a template line must be name|line: {raw[..Math.Min(raw.Length, 60)]}");
            }
            templates.Add(Compile(raw[..bar], raw[(bar + 1)..]));
        }
        return templates;
    }

    private static LineTemplate Compile(string name, string line)
    {
        var segments = new List<(byte[]?, Slot)>();
        var at = 0;
        while (at < line.Length)
        {
            var open = line.IndexOf("{{", at, StringComparison.Ordinal);
            if (open < 0)
            {
                segments.Add((Encoding.UTF8.GetBytes(line[at..]), default));
                break;
            }
            var close = line.IndexOf("}}", open, StringComparison.Ordinal);
            if (close < 0)
            {
                throw new FormatException($"template {name}: unclosed placeholder");
            }
            if (open > at)
            {
                segments.Add((Encoding.UTF8.GetBytes(line[at..open]), default));
            }
            var slotName = line[(open + 2)..close];
            if (!Enum.TryParse<Slot>(slotName, ignoreCase: true, out var slot))
            {
                throw new FormatException($"template {name}: unknown placeholder {{{{{slotName}}}}}");
            }
            segments.Add((null, slot));
            at = close + 2;
        }
        return new LineTemplate(name, segments.ToArray());
    }
}

/// <summary>The values of one line, by slot, as UTF-8 bytes. Reused from line to line.</summary>
internal sealed class LineValues
{
    private readonly byte[][] _values = Enumerable.Range(0, Enum.GetValues<Slot>().Length).Select(_ => Array.Empty<byte>()).ToArray();

    public byte[] this[Slot slot]
    {
        get => _values[(int)slot];
        set => _values[(int)slot] = value;
    }

    public void Set(Slot slot, string value) => _values[(int)slot] = Encoding.UTF8.GetBytes(value);

    public void Set(Slot slot, long value) => _values[(int)slot] = Encoding.UTF8.GetBytes(value.ToString(System.Globalization.CultureInfo.InvariantCulture));
}
