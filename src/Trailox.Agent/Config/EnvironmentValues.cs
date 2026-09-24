using System.Text.RegularExpressions;
using YamlDotNet.Core;
using YamlDotNet.Core.Events;

namespace Trailox.Agent.Config;

/// <summary>
/// <c>${NAME}</c> in agent.yaml (1.5.0): a value, or part of one, taken from the agent's environment, so a
/// Helm chart or a compose file can supply what the file leaves open. <c>${NAME:-default}</c> falls back to
/// the default when NAME is not set or empty, <c>${NAME:-}</c> then leaves the key out, and <c>$${</c> is a
/// literal <c>${</c>.
/// </summary>
/// <remarks>
/// Values only: never keys, never comments, and never password_env or password_file, which already say
/// where a credential is. It works on the parser's events rather than on the text, so what a variable holds
/// is text - YAML inside it is not read, and a <c>${...}</c> inside it is not expanded again.
/// </remarks>
internal static class EnvironmentValues
{
    /// <summary>The agent key's variable. It, and every variable a password_env names, is never read as a value.</summary>
    public const string AgentKeyVariable = "TRAILOX_AGENT_KEY";

    /// <summary>Their values name where a credential is: never expanded, and <see cref="ConfigLoader"/> says so.</summary>
    private static readonly HashSet<string> CredentialKeys = new(StringComparer.Ordinal) { "password_env", "password_file" };

    // $${ is a literal ${; a reference is ${NAME} or ${NAME:-default}; any other ${ is refused.
    private static readonly Regex Reference = new(
        @"\$\$\{|\$\{(?<name>[A-Za-z_][A-Za-z0-9_]*)(?<default>:-[^}]*)?\}|\$\{", RegexOptions.Compiled);

    /// <summary>One reference that was read: where, which variable, and what it gave - null when its default applied.</summary>
    public sealed record Use(long Line, long Column, string Name, string? Value);

    /// <summary>The events to deserialize, every problem found, and every reference read.</summary>
    public sealed record Result(IReadOnlyList<ParsingEvent> Events, IReadOnlyList<string> Problems, IReadOnlyList<Use> Uses);

    /// <summary>
    /// Reads the file's events and expands every reference in its values. A syntax error throws
    /// <see cref="YamlException"/>; every other problem is returned, all of them, and the events are then
    /// not for use.
    /// </summary>
    public static Result Expand(string yamlText, Func<string, string?> environment)
    {
        var events = Read(yamlText);
        var (places, targets, aliasAt) = Locate(events);

        // Refused before anything is read: the agent key, and every password a password_env names, in any
        // endpoint, earlier or later in the file - a value is reported to Trailox or echoed by a problem. What
        // names a credential is never expanded: written at password_env or password_file, or anchored
        // elsewhere and aliased there. Ignoring case: Windows reads variables either way, and refusing too
        // much costs nothing.
        var refused = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { AgentKeyVariable };
        var credentialNames = new HashSet<int>();
        for (var i = 0; i < events.Count; i++)
        {
            if (places[i] is not { } place || !CredentialKeys.Contains(place.Key))
            {
                continue;
            }
            var at = events[i] is AnchorAlias ? targets[i] : i;
            if (at < 0 || events[at] is not Scalar named)
            {
                continue;
            }
            credentialNames.Add(at);
            if (place.Key == "password_env")
            {
                // password_env: ${CH_PW} is refused later, but CH_PW is still the password's variable.
                refused.Add(named.Value.Trim());
                refused.UnionWith(Reference.Matches(named.Value).Where(m => m.Groups["name"].Success).Select(m => m.Groups["name"].Value));
            }
        }

        var replaced = new ParsingEvent?[events.Count];
        var dropped = new bool[events.Count];
        var problems = new List<(int At, string What)>();
        var uses = new List<Use>();
        for (var i = 0; i < events.Count; i++)
        {
            if (places[i] is not { } place || events[i] is not Scalar scalar || !scalar.Value.Contains("${") || credentialNames.Contains(i))
            {
                continue;
            }
            var found = new List<string>();
            var value = Substitute(scalar, environment, refused, found, uses);
            if (found.Count > 0)
            {
                problems.AddRange(found.Select(what => (i, what)));
            }
            else if (value.Length > 0)
            {
                replaced[i] = Quoted(scalar, value);
            }
            else if (!scalar.Anchor.IsEmpty)
            {
                problems.Add((i, "a value with an anchor cannot be left out by ${NAME:-}"));
            }
            else if (place.InSequence)
            {
                dropped[i] = true;
            }
            else if (place.KeyIndex >= 0)
            {
                // ${NAME:-} gave nothing: the key goes too, as if the line were not there, so the field keeps
                // its documented default.
                dropped[i] = true;
                dropped[place.KeyIndex] = true;
            }
            else
            {
                replaced[i] = Quoted(scalar, "");
            }
        }

        // Problems name the endpoint by its alias, as ConfigLoader's own do - known only once it is read.
        string Alias(int endpoint)
        {
            var at = aliasAt[endpoint];
            return at < 0 || dropped[at] || problems.Any(p => p.At == at) ? "" : ((Scalar)(replaced[at] ?? events[at])).Value;
        }
        string Label(Place p) => p.Endpoint >= 0 ? $"endpoints[{Alias(p.Endpoint)}].{p.Field}" : p.Field.Length > 0 ? p.Field : "agent.yaml";

        var messages = problems
            .Select(p => $"line {events[p.At].Start.Line}, column {events[p.At].Start.Column}: {Label(places[p.At]!)}: {p.What}")
            .ToList();
        var output = new List<ParsingEvent>(events.Count);
        for (var i = 0; i < events.Count; i++)
        {
            if (!dropped[i])
            {
                output.Add(replaced[i] ?? events[i]);
            }
        }
        return new Result(output, messages, uses);
    }

    /// <summary>The scalar's text with every reference replaced. Problems go to <paramref name="problems"/>.</summary>
    private static string Substitute(Scalar scalar, Func<string, string?> environment, IReadOnlySet<string> refused, List<string> problems, List<Use> uses)
    {
        var text = scalar.Value;
        if (Reference.Matches(text).Any(m => m.Value != "$${" && (!m.Groups["name"].Success || m.Groups["default"].Value.Contains("${"))))
        {
            problems.Add($"'{text}' is not a valid reference - write ${{NAME}} or ${{NAME:-default}}, NAME being letters, digits and '_', not starting with a digit ($${{ is a literal ${{)");
            return text;
        }
        return Reference.Replace(text, m =>
        {
            if (m.Value == "$${")
            {
                return "${";
            }
            var name = m.Groups["name"].Value;
            if (refused.Contains(name))
            {
                problems.Add($"${{{name}}} holds a credential - the agent key, or a password that password_env names - and cannot be used as a value");
                return "";
            }
            // What a file-backed ConfigMap or Secret adds, and a password file loses too: .NET's $ matches just
            // before a final newline, so "hr_private\n" would pass the checks and then exclude nothing.
            var value = environment(name)?.TrimEnd('\r', '\n');
            var fallback = m.Groups["default"];
            if (string.IsNullOrEmpty(value))
            {
                if (!fallback.Success)
                {
                    problems.Add($"environment variable {name} is not set or empty");
                    return "";
                }
                uses.Add(new Use(scalar.Start.Line, scalar.Start.Column, name, null));
                return fallback.Value[2..];
            }
            if (value.Any(char.IsControl))
            {
                problems.Add($"environment variable {name} holds a control character");
                return "";
            }
            uses.Add(new Use(scalar.Start.Line, scalar.Start.Column, name, value));
            return value;
        });
    }

    /// <summary>
    /// The value handed on double-quoted: YamlDotNet reads a PLAIN empty, <c>~</c> or <c>null</c> as null, and
    /// a null true/false as false. The field's type still decides - a quoted "8443" reads as a number - and
    /// the marks stay, so a later type error still names the placeholder's line.
    /// </summary>
    private static Scalar Quoted(Scalar s, string value) =>
        new(s.Anchor, s.Tag, value, ScalarStyle.DoubleQuoted, false, s.Tag.IsEmpty, s.Start, s.End);

    private static List<ParsingEvent> Read(string yamlText)
    {
        var parser = new Parser(new StringReader(yamlText));
        var events = new List<ParsingEvent>();
        while (parser.MoveNext())
        {
            events.Add(parser.Current!);
        }
        return events;
    }

    /// <summary>Where a value sits: its key, its path for problems, and what goes with it when it is left out.</summary>
    /// <param name="Key">The key it is the value of; for a list item, the list's key.</param>
    /// <param name="Field">Its path from the endpoint inside an endpoint, from the top otherwise.</param>
    /// <param name="Endpoint">The endpoint it is in (0-based), or -1.</param>
    /// <param name="KeyIndex">The event index of its key, or -1.</param>
    /// <param name="InSequence">A list item.</param>
    private sealed record Place(string Key, string Field, int Endpoint, int KeyIndex, bool InSequence);

    /// <summary>A mapping or list being read, and where it sits.</summary>
    private sealed class Frame
    {
        public required bool IsMapping { get; init; }
        public required int Endpoint { get; init; }
        public required string Field { get; init; }
        public bool ExpectingKey { get; set; } = true;
        public string Key { get; set; } = "";
        public int KeyIndex { get; set; } = -1;
    }

    /// <summary>
    /// For every value - a scalar, or a YAML alias - where it sits; for every alias, the event index of the
    /// scalar its anchor names (-1: none, or a collection); for every endpoint, the event index of its alias
    /// field (-1: none). Keys and values are told apart by position - they alternate in a mapping - not by
    /// Scalar.IsKey.
    /// </summary>
    private static (Place?[] Places, int[] Targets, List<int> AliasAt) Locate(IReadOnlyList<ParsingEvent> events)
    {
        var places = new Place?[events.Count];
        var targets = new int[events.Count];
        var aliasAt = new List<int>();
        var anchors = new Dictionary<string, int>(StringComparer.Ordinal);
        var frames = new Stack<Frame>();
        for (var i = 0; i < events.Count; i++)
        {
            frames.TryPeek(out var top);
            if (events[i] is NodeEvent { Anchor.IsEmpty: false } anchored)
            {
                // The latest definition wins, as it does for the YAML alias that follows it.
                anchors[anchored.Anchor.Value] = anchored is Scalar ? i : -1;
            }
            switch (events[i])
            {
                case Scalar key when top is { IsMapping: true, ExpectingKey: true }:
                    top.Key = key.Value;
                    top.KeyIndex = i;
                    top.ExpectingKey = false;
                    break;
                case Scalar:
                    places[i] = PlaceIn(top);
                    if (top is { IsMapping: true, Endpoint: >= 0, Field: "", Key: "alias" })
                    {
                        aliasAt[top.Endpoint] = i;
                    }
                    Completed(top);
                    break;
                case AnchorAlias alias:
                    if (top is not { IsMapping: true, ExpectingKey: true })
                    {
                        places[i] = PlaceIn(top);
                        targets[i] = anchors.GetValueOrDefault(alias.Value.Value, -1);
                    }
                    Completed(top);
                    break;
                case MappingStart or SequenceStart:
                    frames.Push(Open(top, events[i] is MappingStart, aliasAt));
                    break;
                case MappingEnd or SequenceEnd:
                    frames.Pop();
                    frames.TryPeek(out var parent);
                    Completed(parent);
                    break;
            }
        }
        return (places, targets, aliasAt);
    }

    private static Frame Open(Frame? parent, bool isMapping, List<int> aliasAt)
    {
        if (parent == null)
        {
            return new Frame { IsMapping = isMapping, Endpoint = -1, Field = "" };
        }
        // A mapping directly in the top-level endpoints list is an endpoint; paths inside it start there.
        if (isMapping && parent is { IsMapping: false, Endpoint: -1, Field: "endpoints" })
        {
            aliasAt.Add(-1);
            return new Frame { IsMapping = true, Endpoint = aliasAt.Count - 1, Field = "" };
        }
        return new Frame
        {
            IsMapping = isMapping,
            Endpoint = parent.Endpoint,
            Field = parent.IsMapping ? Join(parent.Field, parent.Key) : parent.Field,
        };
    }

    private static Place PlaceIn(Frame? top) => top switch
    {
        null => new Place("", "", -1, -1, false),
        { IsMapping: true } => new Place(top.Key, Join(top.Field, top.Key), top.Endpoint, top.KeyIndex, false),
        _ => new Place(top.Field[(top.Field.LastIndexOf('.') + 1)..], top.Field, top.Endpoint, -1, true),
    };

    /// <summary>A whole node - scalar, alias or collection - has been read inside <paramref name="frame"/>.</summary>
    private static void Completed(Frame? frame)
    {
        if (frame is not { IsMapping: true })
        {
            return;
        }
        if (frame.ExpectingKey)
        {
            // A key that is not a scalar: an alias or a collection, which agent.yaml never uses.
            frame.Key = "";
            frame.KeyIndex = -1;
        }
        frame.ExpectingKey = !frame.ExpectingKey;
    }

    private static string Join(string path, string key) => path.Length == 0 ? key : path + "." + key;
}

/// <summary>Hands the deserializer events that were read once already.</summary>
internal sealed class EventReplay : IParser
{
    private readonly IReadOnlyList<ParsingEvent> _events;
    private int _next;

    public EventReplay(IReadOnlyList<ParsingEvent> events)
    {
        _events = events;
    }

    public ParsingEvent? Current { get; private set; }

    public bool MoveNext()
    {
        Current = _next < _events.Count ? _events[_next++] : null;
        return Current != null;
    }
}
