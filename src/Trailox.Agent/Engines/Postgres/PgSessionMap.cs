using System.Text.RegularExpressions;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// Who each session of one server is, from its sign-in lines, for a <c>log_line_prefix</c> that does not name the user
/// on every line (Azure's <c>%t-%c-</c>): a record takes its user, database, client address and application from its
/// session (<c>%c</c>), or, with no session id, from the newest sign-in of its process (<c>%p</c>) before it.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>It lives as long as the source, so a session that signed in during one window is known in the next. It also
///   remembers which stretches of log it has read: a session that started inside one signed in where its line was
///   seen, and is not looked for again.</item>
/// <item>A record of a session whose sign-in is before everything read asks for a look-back (<see cref="LookBackFor"/>):
///   with a session id, the minute after the session's start, which the id itself says; with a process only, the
///   <see cref="PidLookBackMicros"/> before the read.</item>
/// <item>🔴 A PROCESS ID IS REUSED. A record takes a process's sign-in only when that sign-in is the newest of the
///   process before the record, and no disconnection of it came between.</item>
/// <item>Bounded: past <see cref="MaxSessions"/> sessions the older half is dropped.</item>
/// </list>
/// </remarks>
public sealed class PgSessionMap
{
    public const int MaxSessions = 100_000;

    /// <summary>How far before a read a process's sign-in is looked for, when the prefix has no session id.</summary>
    public const long PidLookBackMicros = 2 * 3_600_000_000L;

    /// <summary>A session's sign-in comes within this of its start (a slow password check, an LDAP round trip).</summary>
    public const long SignInWithinMicros = 60_000_000;

    /// <summary>The user %u writes for a session that has not signed in yet.</summary>
    public const string Unknown = "[unknown]";

    private const long Second = 1_000_000;

    // connection authorized: user=U database=D application_name=A SSL enabled (...); a replication connection may name no
    // database; the application and the encryption are each optional.
    private static readonly Regex Authorized = new(
        @"^(?:replication )?connection authorized: user=(?<u>.*?)(?: database=(?<d>.*?))?(?: application_name=(?<a>.*?))?(?: (?:SSL enabled|GSS) \(.*)?$",
        RegexOptions.NonBacktracking | RegexOptions.CultureInvariant | RegexOptions.Singleline);

    private static readonly Regex Disconnected = new(
        @"^disconnection: session time: \S+ user=(?<u>.*?) database=(?<d>.*?) host=(?<h>\S*)(?: port=(?<p>\d+))?$",
        RegexOptions.NonBacktracking | RegexOptions.CultureInvariant | RegexOptions.Singleline);

    private static readonly Regex Received = new(@"^connection received: host=(?<h>\S*)(?: port=(?<p>\d+))?",
        RegexOptions.NonBacktracking | RegexOptions.CultureInvariant);

    // The user a refused sign-in asked for, as the FATAL text names it.
    private static readonly Regex RefusedUser = new(
        @"^(?:password authentication failed for user|role) ""(?<u>[^""]*)""|^no pg_hba\.conf entry for host ""[^""]*"", user ""(?<u>[^""]*)""|authentication failed for user ""(?<u>[^""]*)""",
        RegexOptions.NonBacktracking | RegexOptions.CultureInvariant);

    private readonly PgLineFormat _format;
    private readonly Dictionary<string, Session> _bySession = new(StringComparer.Ordinal);
    private readonly Dictionary<int, List<Session>> _byPid = new();
    private readonly HashSet<string> _lookedFor = new(StringComparer.Ordinal);
    private readonly List<(long From, long To)> _read = new();
    private (long From, long To)? _reading;
    private int _count;

    public PgSessionMap(PgLineFormat format)
    {
        _format = format;
    }

    /// <summary>What a record lacks after <see cref="Take"/>.</summary>
    public enum Outcome
    {
        /// <summary>It has its user, or needs none (a checkpoint, a sign-in line).</summary>
        Complete,

        /// <summary>No sign-in of its session or process was read: a look-back may find it.</summary>
        SessionUnknown,

        /// <summary>Its session is known but named no user (it never signed in).</summary>
        NoUser,
    }

    /// <summary>Whether records need it: the prefix names no user.</summary>
    public bool Needed => !_format.HasUser;

    public PgLineFormat Format => _format;

    private sealed class Session
    {
        public long StartMicros;
        public long EndMicros = long.MaxValue;
        public string Host = "";
        public string Port = "";
        public string? User;
        public string Database = "";
        public string? Application;
    }

    /// <summary>A read of the log starts: what it parses until <see cref="EndRead"/> is one stretch read.</summary>
    public void BeginRead() => _reading = null;

    public void EndRead()
    {
        if (_reading is not { } r)
        {
            return;
        }
        _reading = null;
        _read.Add(r);
        _read.Sort();
        for (var i = _read.Count - 1; i > 0; i--)
        {
            if (_read[i].From <= _read[i - 1].To + Second)
            {
                _read[i - 1] = (_read[i - 1].From, Math.Max(_read[i - 1].To, _read[i].To));
                _read.RemoveAt(i);
            }
        }
    }

    /// <summary>
    /// A look-back inside a read: it is a stretch of its own, which does not join the read in progress unless it
    /// reaches it. Returns what to give back to <see cref="EndLookBack"/>.
    /// </summary>
    public (long From, long To)? BeginLookBack()
    {
        var current = _reading;
        _reading = null;
        return current;
    }

    public void EndLookBack((long From, long To)? current)
    {
        EndRead();
        _reading = current;
    }

    /// <summary>The earliest time from which the log is read without a gap up to <paramref name="tsMicros"/>; null when it is not read.</summary>
    private long? ReadSince(long tsMicros)
    {
        var stretches = _reading is { } current ? _read.Append(current).OrderBy(s => s.From).ToList() : _read;
        long? since = null;
        for (var i = stretches.Count - 1; i >= 0; i--)
        {
            var s = stretches[i];
            if (since == null ? s.From - Second <= tsMicros && tsMicros <= s.To + Second : s.To + Second >= since.Value)
            {
                since = since is { } f ? Math.Min(f, s.From) : s.From;
            }
        }
        return since;
    }

    /// <summary>
    /// Takes one record, in the order of the log: a sign-in line names its session, and a record gets from its session
    /// what its prefix lacks.
    /// </summary>
    public Outcome Take(PgLogRecord r)
    {
        if (!Needed || r.UnreadLines > 0 || r.Forged || r.Format != PgLogFormat.Stderr)
        {
            return Outcome.Complete;
        }
        if (r.TsMicros != long.MinValue)
        {
            _reading = _reading is { } s ? (Math.Min(s.From, r.TsMicros), Math.Max(s.To, r.TsMicros)) : (r.TsMicros, r.TsMicros);
        }
        var kind = r.Kind;
        var session = kind == PgRecordKind.ConnectionReceived ? Open(r) : Find(r);
        Learn(session, r, kind);
        return Complete(r, kind, session);
    }

    /// <summary>After a look-back: the record takes its session again.</summary>
    public Outcome Retake(PgLogRecord r) => Complete(r, r.Kind, Find(r));

    private Outcome Complete(PgLogRecord r, PgRecordKind kind, Session? session)
    {
        if (session != null)
        {
            Fill(r, session);
        }
        if (kind == PgRecordKind.Fatal && r.User.Length == 0 && RefusedUser.Match(r.Message) is { Success: true } refused)
        {
            r.User = refused.Groups["u"].Value;
        }
        if (r.User.Length > 0 || kind is not (PgRecordKind.Statement or PgRecordKind.Execute or PgRecordKind.Audit or PgRecordKind.Fatal))
        {
            return Outcome.Complete;
        }
        return session == null ? Outcome.SessionUnknown : Outcome.NoUser;
    }

    /// <summary>
    /// The stretch of log to read for a record whose session is unknown, before <paramref name="readFromMicros"/> (where
    /// the read began); null when there is none: its sign-in should have been seen, or was looked for already.
    /// </summary>
    public (long From, long To)? LookBackFor(PgLogRecord r, long readFromMicros)
    {
        if (_lookedFor.Count >= MaxSessions)
        {
            // Bounded too: a session looked for long ago may be looked for once more.
            _lookedFor.Clear();
        }
        if (_format.HasSession && r.SessionId.Length > 0)
        {
            if (PgSessionId.StartMicros(r.SessionId) is not { } start || !_lookedFor.Add(r.SessionId))
            {
                return null;
            }
            var seen = ReadSince(start) is { } since && since <= start - Second;
            return seen || start - Second >= readFromMicros ? null : (start - Second, Math.Min(start + SignInWithinMicros, readFromMicros));
        }
        if (!_format.HasPid || r.Pid == 0 || r.TsMicros == long.MinValue)
        {
            return null;
        }
        var from = r.TsMicros - PidLookBackMicros;
        var readSince = ReadSince(readFromMicros) ?? readFromMicros;
        return from >= readSince || !_lookedFor.Add($"pid@{readSince}") ? null : (from, readSince);
    }

    private Session Open(PgLogRecord r)
    {
        var session = new Session { StartMicros = r.TsMicros };
        if (Received.Match(r.Message) is { Success: true } m)
        {
            session.Host = m.Groups["h"].Value;
            session.Port = m.Groups["p"].Value;
        }
        if (_format.HasSession && r.SessionId.Length > 0)
        {
            if (_bySession.TryAdd(r.SessionId, session))
            {
                _count++;
            }
            else
            {
                _bySession[r.SessionId] = session;
            }
        }
        else if (r.Pid != 0)
        {
            if (!_byPid.TryGetValue(r.Pid, out var list))
            {
                _byPid[r.Pid] = list = new List<Session>();
            }
            list.Add(session);
            _count++;
            list.Sort((a, b) => a.StartMicros.CompareTo(b.StartMicros));
            if (list.Count > 8)
            {
                list.RemoveAt(0);
                _count--;
            }
        }
        // A prefix with no session id and no process stores nothing: only what a line names of itself is read.
        if (_count > MaxSessions)
        {
            Trim();
        }
        return session;
    }

    private Session? Find(PgLogRecord r)
    {
        if (_format.HasSession && r.SessionId.Length > 0)
        {
            return _bySession.GetValueOrDefault(r.SessionId);
        }
        if (r.Pid == 0 || !_byPid.TryGetValue(r.Pid, out var list))
        {
            return null;
        }
        var newest = list.LastOrDefault(s => s.StartMicros <= r.TsMicros);
        return newest != null && r.TsMicros <= newest.EndMicros ? newest : null;
    }

    /// <summary>
    /// What a sign-in or disconnection line says of its session: the line names its own user, so it has one even when
    /// its session's start was never read.
    /// </summary>
    private void Learn(Session? s, PgLogRecord r, PgRecordKind kind)
    {
        string? user = null, database = null, host = null, port = null, app = null;
        if (kind == PgRecordKind.ConnectionAuthorized && Authorized.Match(r.Message) is { Success: true } a)
        {
            (user, database) = (a.Groups["u"].Value, a.Groups["d"].Value);
            app = a.Groups["a"].Success ? a.Groups["a"].Value : "";
        }
        else if (kind == PgRecordKind.Disconnection && Disconnected.Match(r.Message) is { Success: true } d)
        {
            (user, database, host, port) = (d.Groups["u"].Value, d.Groups["d"].Value, d.Groups["h"].Value, d.Groups["p"].Value);
        }
        if (user == null)
        {
            return;
        }
        if (s != null)
        {
            s.User ??= user;
            s.Database = s.Database.Length > 0 ? s.Database : database!;
            s.Application ??= app;
            if (kind == PgRecordKind.Disconnection)
            {
                s.EndMicros = r.TsMicros;
            }
        }
        Fill(r, new Session { User = user, Database = database!, Host = host ?? s?.Host ?? "", Port = port ?? s?.Port ?? "", Application = app ?? s?.Application });
    }

    /// <summary>What the prefix lacks, from the session. Before its sign-in a session's user is "[unknown]", as %u writes it.</summary>
    private void Fill(PgLogRecord r, Session s)
    {
        var early = s.User == null && r.Kind is PgRecordKind.ConnectionReceived or PgRecordKind.ConnectionAuthenticated;
        if (!_format.HasUser && r.User.Length == 0)
        {
            r.User = s.User ?? (early ? Unknown : "");
        }
        if (!_format.HasDatabase && r.Database.Length == 0)
        {
            r.Database = s.User != null ? s.Database : early ? Unknown : "";
        }
        if (!_format.HasRemote && r.RemoteHost.Length == 0)
        {
            r.RemoteHost = s.Host;
            r.RemotePort = s.Port;
        }
        if (!_format.HasApplication && r.ApplicationName == null && s.Application != null)
        {
            r.ApplicationName = s.Application;
        }
    }

    /// <summary>Drops the older half of the sessions, by their start.</summary>
    private void Trim()
    {
        var starts = _bySession.Values.Concat(_byPid.Values.SelectMany(l => l)).Select(s => s.StartMicros).Order().ToList();
        if (starts.Count == 0)
        {
            _count = 0;
            return;
        }
        var cut = starts[starts.Count / 2];
        foreach (var key in _bySession.Where(kv => kv.Value.StartMicros < cut).Select(kv => kv.Key).ToList())
        {
            _bySession.Remove(key);
        }
        foreach (var pid in _byPid.Keys.ToList())
        {
            _byPid[pid].RemoveAll(s => s.StartMicros < cut);
            if (_byPid[pid].Count == 0)
            {
                _byPid.Remove(pid);
            }
        }
        _lookedFor.Clear();
        _count = _bySession.Count + _byPid.Values.Sum(l => l.Count);
    }
}
