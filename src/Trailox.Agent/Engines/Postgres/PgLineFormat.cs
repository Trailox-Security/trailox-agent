using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// The stderr line of one <c>log_line_prefix</c>: a pattern built from its escapes (<see cref="PgLinePrefix"/>), so the
/// agent reads the prefix the server has, whatever it is. A line that starts a record is the prefix, then the level,
/// <c>:</c> and two spaces, then the message.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>Free text (<c>%u</c>, <c>%d</c>, <c>%a</c>...) is read up to the first place where the rest of the prefix fits:
///   a name that holds the text that follows it in the prefix is cut there.</item>
/// <item>A process that is no session (the checkpointer, a background worker) writes nothing after <c>%q</c>, and
///   nothing for the session's escapes.</item>
/// <item>🔴 THE PATTERN RUNS IN LINEAR TIME. A line can be megabytes long and its text is anyone's, so the pattern never
///   backtracks (<see cref="RegexOptions.NonBacktracking"/>), and only the head of a line is matched.</item>
/// </list>
/// </remarks>
public sealed class PgLineFormat
{
    private const string Levels = "DEBUG[1-5]|LOG|INFO|NOTICE|WARNING|ERROR|FATAL|PANIC|DETAIL|HINT|CONTEXT|STATEMENT|QUERY|LOCATION";

    /// <summary>How much of a line the pattern reads: a prefix and its level are far shorter.</summary>
    internal const int HeadChars = 4096;

    private readonly Regex _head;

    private PgLineFormat(string prefix, IReadOnlyList<PgLinePrefix.Token> tokens)
    {
        Prefix = prefix;
        var escapes = tokens.Where(t => t.Escape != '\0').Select(t => t.Escape).ToHashSet();
        HasUser = escapes.Contains('u');
        HasDatabase = escapes.Contains('d');
        HasSession = escapes.Contains('c');
        HasPid = escapes.Contains('p');
        HasRemote = escapes.Contains('r') || escapes.Contains('h');
        HasApplication = escapes.Contains('a');
        HasTime = escapes.Contains('t') || escapes.Contains('m') || escapes.Contains('n');
        _head = new Regex("^" + Pattern(tokens) + $"(?<level>{Levels}):  ?",
            RegexOptions.NonBacktracking | RegexOptions.CultureInvariant | RegexOptions.Singleline);
    }

    public string Prefix { get; }
    public bool HasUser { get; }
    public bool HasDatabase { get; }
    public bool HasSession { get; }
    public bool HasPid { get; }
    public bool HasRemote { get; }
    public bool HasApplication { get; }
    public bool HasTime { get; }

    /// <summary>A line's user can be known: on the line (<c>%u</c>), or from its session's or process's sign-in line.</summary>
    public bool KnowsUser => HasUser || HasSession || HasPid;

    /// <summary>
    /// The prefix writes a name a client chose: <c>%a</c>, which any client sets, or <c>%u</c> and <c>%d</c>, which a
    /// refused client asks for. Wherever it stands, the level follows it, so that client can write the rest of a line of
    /// its own inside the name.
    /// </summary>
    public bool ClientText => ClientEscape != '\0';

    /// <summary>The client-chosen escape: <c>a</c> when the prefix has it, else the first of <c>u</c> and <c>d</c>; '\0' for none.</summary>
    public char ClientEscape { get; private init; }

    private static readonly Regex LevelMarker = new($"(?:{Levels}):  ", RegexOptions.NonBacktracking | RegexOptions.CultureInvariant);

    /// <summary>
    /// What follows a refused client's own name on its line: the rest of the prefix, then the server's FATAL. Null when
    /// the prefix writes no user or database name.
    /// </summary>
    private Regex? _refusedTail;

    /// <summary>How far into a record's text a forgery's real rest of the line can start: the longest name, then the rest of the prefix.</summary>
    private int _forgeWindow;

    /// <summary>The line's identity (<c>%u</c>, <c>%d</c>, <c>%c</c>, <c>%p</c>) is written in or after the client's name, so a forgery can change it.</summary>
    private bool _identityAfterClient;

    /// <summary>The longest name PostgreSQL writes (NAMEDATALEN - 1 bytes): a user, a database, an application.</summary>
    private const int MaxName = 63;

    /// <summary>
    /// 🔴 A FORGED LINE. PostgreSQL writes a client's names into the prefix as they are, so a client can put a level and
    ///    a message, and the rest of a prefix, into its application name, or a refused client into the user or database
    ///    name it asked for; read from the left, the line then reads as that message. The real rest of the prefix and the
    ///    real level follow inside the text, at most a name and the rest of a prefix from its start: only there is a
    ///    level marker looked for. A record is taken as forged, counted and never read:
    ///    <list type="bullet">
    ///    <item>a connection line with a level marker there: its text is the server's, with only the names a client
    ///      chose;</item>
    ///    <item>where the line's own identity (<c>%u</c>, <c>%d</c>, <c>%c</c>, <c>%p</c>) is written in or after the
    ///      name, so a forgery could name another user: with <c>%a</c>, any record with a level marker there; with
    ///      <c>%u</c> or <c>%d</c>, any record with the rest of the prefix and <c>FATAL:  </c> there, as every refused
    ///      client's line ends.</item>
    ///    </list>
    ///    Where the identity comes before the name (Azure's <c>%t-%c-</c> then <c>%a</c>), a forgery rewrites only the
    ///    client's own lines, under its real user, so those are read. A statement's text past the start is never looked
    ///    at, so no one hides a statement by writing a level in it.
    /// </summary>
    /// <remarks>
    /// ⚠ NOT CAUGHT: a newline in a refused client's user name, which starts a line of its own with any prefix at all in
    ///   a real stderr file. csvlog and jsonlog quote the names, and Azure's download writes a newline as <c>\n</c>.
    /// </remarks>
    public bool Forged(PgLogRecord r)
    {
        if (ClientEscape == '\0')
        {
            return false;
        }
        var start = r.Message.Length > _forgeWindow ? r.Message[.._forgeWindow] : r.Message;
        if (r.Kind is PgRecordKind.ConnectionReceived or PgRecordKind.ConnectionAuthenticated or PgRecordKind.ConnectionAuthorized
            or PgRecordKind.Disconnection)
        {
            return LevelMarker.IsMatch(start);
        }
        return _identityAfterClient && (ClientEscape == 'a' ? LevelMarker.IsMatch(start) : _refusedTail!.IsMatch(start));
    }

    public static PgLineFormat Of(string prefix)
    {
        var tokens = PgLinePrefix.Tokens(prefix);
        var escapes = tokens.Select(t => t.Escape).ToList();
        var client = escapes.Contains('a') ? 'a' : escapes.FirstOrDefault(e => e is 'u' or 'd');
        if (client == '\0')
        {
            return new PgLineFormat(prefix, tokens);
        }
        var after = tokens.Skip(escapes.IndexOf(client) + 1).ToList();
        var refused = client is 'u' or 'd'
            ? new Regex(Pattern(after, capture: false) + "FATAL:  ", RegexOptions.NonBacktracking | RegexOptions.CultureInvariant | RegexOptions.Singleline)
            : null;
        return new PgLineFormat(prefix, tokens)
        {
            ClientEscape = client,
            _refusedTail = refused,
            _forgeWindow = MaxName + after.Sum(MaxLength) + 16,
            _identityAfterClient = client is 'u' or 'd' || after.Any(t => t.Escape is 'u' or 'd' or 'c' or 'p'),
        };
    }

    /// <summary>The most a part of a prefix writes.</summary>
    private static int MaxLength(PgLinePrefix.Token t) => Math.Max(Math.Abs(t.Padding), t.Escape switch
    {
        '\0' => t.Text.Length,
        'u' or 'd' or 'a' or 'i' or 'b' => MaxName,
        'r' or 'h' => 262,
        't' or 's' or 'm' => 40,
        _ => 24,
    });

    /// <summary>The fields of a line that starts a record; null for any other line (a continuation, or another prefix).</summary>
    public Fields? Match(string line)
    {
        var m = _head.Match(line.Length > HeadChars ? line[..HeadChars] : line);
        return m.Success ? new Fields(m, line) : null;
    }

    /// <summary>The time of a line that starts a record; null for any other line, or one whose time cannot be read.</summary>
    public long? TimeOf(string line) => Match(line)?.TsMicros;

    /// <summary>What one line's prefix says.</summary>
    public sealed class Fields
    {
        private readonly Match _m;

        internal Fields(Match m, string line)
        {
            _m = m;
            Level = m.Groups["level"].Value;
            Text = line[m.Length..];
        }

        public string Level { get; }

        /// <summary>The message: the rest of the line after the level.</summary>
        public string Text { get; }

        public long? TsMicros =>
            Group("m") is { } ms ? PgTime.Micros(ms)
            : Group("t") is { } t ? PgTime.Micros(t)
            : Group("n") is { } n ? EpochMicros(n)
            : null;

        /// <summary><c>%p</c>, else the process in <c>%c</c> (its part after the dot, in hex); 0 when neither.</summary>
        public int Pid =>
            Group("p") is { } p && int.TryParse(p, NumberStyles.None, CultureInfo.InvariantCulture, out var pid) ? pid
            : SessionId.Length > 0 && PgSessionId.Pid(SessionId) is { } fromSession ? fromSession
            : 0;

        public string SessionId => Group("c") ?? "";
        public string? User => Group("u")?.Trim();
        public string? Database => Group("d")?.Trim();
        public string? Application => Group("a")?.Trim();
        public string SqlState => Group("e") ?? "";
        public string BackendType => Group("b")?.Trim() ?? "";

        /// <summary><c>%r</c> (<c>host(port)</c> or <c>[local]</c>), else <c>%h</c>; null when the prefix has neither.</summary>
        public (string Host, string Port)? Remote
        {
            get
            {
                if (Group("r") is { } r)
                {
                    var open = r.LastIndexOf('(');
                    return open > 0 && r.EndsWith(')') ? (r[..open], r[(open + 1)..^1]) : (r, "");
                }
                return Group("h") is { } h ? (h, "") : null;
            }
        }

        private string? Group(string name)
        {
            var g = _m.Groups[name];
            return g.Success ? g.Value.Trim(' ') : null;
        }
    }

    /// <summary><c>%n</c>: seconds since the epoch, with milliseconds.</summary>
    private static long? EpochMicros(string text)
    {
        var dot = text.IndexOf('.');
        return dot > 0 && long.TryParse(text.AsSpan(0, dot), NumberStyles.None, CultureInfo.InvariantCulture, out var seconds)
            && int.TryParse(text.AsSpan(dot + 1), NumberStyles.None, CultureInfo.InvariantCulture, out var ms)
            ? seconds * 1_000_000 + ms * 1000L
            : null;
    }

    private static string Pattern(IReadOnlyList<PgLinePrefix.Token> tokens, bool capture = true)
    {
        var pattern = new StringBuilder();
        var captured = new HashSet<char>();
        var optional = 0;
        foreach (var token in tokens)
        {
            if (token.Escape == '\0')
            {
                pattern.Append(Regex.Escape(token.Text));
                continue;
            }
            if (token.Escape == 'q')
            {
                // Everything after %q is written by sessions only.
                pattern.Append("(?:");
                optional++;
                continue;
            }
            if (Value(token.Escape) is not { } value)
            {
                continue;   // an escape PostgreSQL does not know writes nothing
            }
            var group = capture && captured.Add(token.Escape) && Captured.Contains(token.Escape)
                ? $"(?<{token.Escape}>{value})"
                : $"(?:{value})";
            pattern.Append(token.Padding > 0 ? " *" + group : token.Padding < 0 ? group + " *" : group);
        }
        pattern.Append(')', optional);
        if (optional > 0)
        {
            pattern.Append('?');
        }
        return pattern.ToString();
    }

    private static readonly HashSet<char> Captured = new() { 't', 'm', 'n', 'p', 'c', 'u', 'd', 'a', 'r', 'h', 'e', 'b' };

    /// <summary>What an escape writes, as elog.c writes it; null for one it does not know.</summary>
    private static string? Value(char escape) => escape switch
    {
        't' or 's' => @"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d [A-Za-z0-9+\-]+",
        'm' => @"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3} [A-Za-z0-9+\-]+",
        'n' => @"\d+\.\d{3}",
        'p' or 'l' or 'x' => @"\d+",
        'P' => @"\d*",
        'c' => @"[0-9a-f]+\.[0-9a-f]+",
        'e' => @"[0-9A-Z]{5}",
        'v' => @"(?:-?\d+/\d+)?",
        'Q' => @"-?\d+",
        // A session's address: host(port), [local]; a process that is no session writes nothing.
        'r' => @"(?:\[local\]|[^\s()]+\(\d+\))?",
        'h' => @"\S*",
        'u' or 'd' or 'a' or 'i' or 'b' => @".*?",
        _ => null,
    };
}

/// <summary>A session id (<c>%c</c>): the session's start, in seconds since the epoch, and its process, both in hex.</summary>
public static class PgSessionId
{
    public static long? StartMicros(string sessionId)
    {
        var dot = sessionId.IndexOf('.');
        return dot > 0 && long.TryParse(sessionId.AsSpan(0, dot), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out var seconds)
            ? seconds * 1_000_000
            : null;
    }

    public static int? Pid(string sessionId)
    {
        var dot = sessionId.IndexOf('.');
        return dot > 0 && int.TryParse(sessionId.AsSpan(dot + 1), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out var pid)
            ? pid
            : null;
    }
}
