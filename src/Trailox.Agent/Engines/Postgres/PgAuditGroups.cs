using System.Globalization;
using System.IO.Hashing;
using System.Text;
using Trailox.Agent.Engines.MongoDb;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// The pgaudit READ and WRITE lines of one window, as statements, grouped per minute: one row per minute for each
/// (user, database, client address, application, class, command, objects, statement shape), with the count.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>🔴 ONE STATEMENT IS SEVERAL LINES. With <c>pgaudit.log_relation</c> on, a statement writes one line per relation,
///   and a relation under both object and session audit writes two; all of them carry the statement's id and
///   substatement id. The lines of one backend are joined into one statement whose objects are the distinct relations
///   named. The lines of other backends come in between, so a statement stays open per pid until its next line.</item>
/// <item>🔴 ONE STATEMENT CAN READ SOME RELATIONS AND WRITE OTHERS (INSERT ... SELECT, a data-modifying WITH). Its READ
///   lines and its WRITE lines stay apart: one row each, so a relation read is never filed as written.</item>
/// <item>🔴 A COPY IS NAMED BY ITS DIRECTION. With <c>pgaudit.log_relation</c> on, pgaudit names a COPY's lines by the
///   rights it uses (SELECT, INSERT), so the statement's own text says COPY TO or COPY FROM. COPY (query) TO adds a
///   line of its own after the query's (command COPY, no object, the next substatement id): it joins the statement, so
///   one export is one row.</item>
/// <item>Only READ and WRITE are grouped and shipped; lines of other classes are only counted.</item>
/// <item>On stderr a line carries no application name: only the session's connection line does, under the same pid.
///   There the pid is part of the key, so a group never spans two sessions.</item>
/// <item>The row's text is the shape (<see cref="PgStatementShape"/>): no value leaves the customer's network.</item>
/// <item>🔴 ONE USER CANNOT CROWD OUT THE OTHERS. Past <see cref="UserShapeCap"/> shapes of one user in one minute, that
///   user's further statements group without their shape: who, where, which class, command and objects stay.</item>
/// </list>
/// </remarks>
public sealed class PgAuditGroups
{
    /// <summary>Groups in one minute before new keys go into one <see cref="MinuteAggregator.Other"/> group.</summary>
    public const int GroupCap = 10_000;

    /// <summary>Shapes of one user in one minute before that user's further statements group without their shape.</summary>
    public const int UserShapeCap = 500;

    /// <summary>The longest shape kept as a row's text; the key hashes the whole shape.</summary>
    public const int MaxShapeChars = 16 * 1024;

    /// <summary>The text of a statement grouped without its shape.</summary>
    public const string ManyShapes = "<many shapes>";

    /// <summary>A statement still open this long behind the newest line in the window has had all its lines.</summary>
    private const long CloseMicros = 1_000_000;

    internal const int User = 0, Database = 1, RemoteHost = 2, RemotePort = 3, App = 4, SessionId = 5, Pid = 6, Class = 7,
        Command = 8, Objects = 9, Shape = 10, Format = 11, Truncated = 12, ShapeHash = 13, LabelCount = 14;

    /// <summary>The label of an application that the line does not carry (stderr): it is not the empty name.</summary>
    internal const string NoApp = "\u0000";

    internal const char ObjectSeparator = '\u001f';

    private static readonly string[] Grouped = { "READ", "WRITE" };

    private readonly MinuteAggregator _minutes;
    private readonly Dictionary<int, Open> _open = new();
    private readonly List<int> _closing = new();
    private readonly Dictionary<(long Minute, string User), HashSet<ulong>> _userShapes = new();
    private readonly StringBuilder _key = new();
    private long _newest = long.MinValue;
    private long _closedThrough = long.MinValue;

    public PgAuditGroups(Action<MinuteGroup> emit) =>
        _minutes = new MinuteAggregator(GroupCap, LabelCount, emit, MinuteAggregator.ApiGraceMicros);

    /// <summary>READ and WRITE lines taken.</summary>
    public long Lines { get; private set; }

    /// <summary>Lines of a minute already shipped: they open a second row for that minute.</summary>
    public long LateLines => _minutes.LateLines;

    /// <summary>Statements past <see cref="GroupCap"/> in their minute, counted in the minute's other row.</summary>
    public long OverflowLines => _minutes.OverflowLines;

    /// <summary>Statements grouped without their shape, past <see cref="UserShapeCap"/>.</summary>
    public long ShapelessLines { get; private set; }

    /// <summary>Takes a READ or WRITE line; false for a line of any other class, which the caller counts.</summary>
    public bool Add(PgLogRecord r, PgAuditLine line)
    {
        if (Array.IndexOf(Grouped, line.Class) < 0)
        {
            return false;
        }
        Lines++;
        if (_open.TryGetValue(r.Pid, out var open) && !open.Holds(line))
        {
            Close(open);
            open = null;
        }
        if (open == null)
        {
            open = new Open(r, line);
            _open[r.Pid] = open;
        }
        var part = open.Part(line.Class, line.Command);
        if (line.ObjectName.Length > 0)
        {
            part.Objects.Add(line.ObjectName);
        }
        open.Truncated |= line.Truncated;
        return true;
    }

    /// <summary>The window has reached <paramref name="tsMicros"/>: close the statements and ship the minutes behind it.</summary>
    public void Advance(long tsMicros)
    {
        if (tsMicros <= _newest)
        {
            return;
        }
        _newest = tsMicros;
        var closeBefore = tsMicros - CloseMicros;
        // At most once a second of log: the open statements are walked only then.
        if (_closedThrough != long.MinValue && closeBefore - _closedThrough < CloseMicros)
        {
            return;
        }
        _closedThrough = closeBefore;
        CloseWhere(o => o.First.TsMicros < closeBefore);
        _minutes.FlushBefore(tsMicros);
        // A minute that has shipped takes no new shape count: its late lines open a part of their own.
        var shipped = closeBefore - 2 * MinuteAggregator.MinuteMicros;
        foreach (var key in _userShapes.Keys.Where(k => k.Minute < shipped).ToList())
        {
            _userShapes.Remove(key);
        }
    }

    /// <summary>The window's end: every statement closes and every minute ships.</summary>
    public void Finish()
    {
        CloseWhere(_ => true);
        _minutes.FlushAll();
        _userShapes.Clear();
    }

    private void CloseWhere(Func<Open, bool> due)
    {
        _closing.Clear();
        foreach (var (pid, open) in _open)
        {
            if (due(open))
            {
                _closing.Add(pid);
            }
        }
        // In pid order, so a window read again groups the same way.
        _closing.Sort();
        foreach (var pid in _closing)
        {
            Close(_open[pid]);
            _open.Remove(pid);
        }
    }

    private void Close(Open open)
    {
        var full = PgStatementShape.Of(open.Statement);
        var direction = PgCommand.Of(open.Statement) == "COPY" ? PgStatementShape.CopyDirection(open.Statement) : null;
        foreach (var (cls, part) in open.Parts)
        {
            var command = direction switch
            {
                "TO" when cls == "READ" => "COPY TO",
                "FROM" when cls == "WRITE" => "COPY FROM",
                _ => part.Command,
            };
            Group(open, cls, command, string.Join(ObjectSeparator, part.Objects), full);
        }
    }

    private void Group(Open open, string cls, string command, string objects, string full)
    {
        var r = open.First;
        var minute = r.TsMicros - (r.TsMicros % MinuteAggregator.MinuteMicros + MinuteAggregator.MinuteMicros) % MinuteAggregator.MinuteMicros;
        var hash = XxHash64.HashToUInt64(Encoding.UTF8.GetBytes(full));
        if (!_userShapes.TryGetValue((minute, r.User), out var shapes))
        {
            shapes = new HashSet<ulong>();
            _userShapes[(minute, r.User)] = shapes;
        }
        var shapeless = !shapes.Contains(hash) && shapes.Count >= UserShapeCap;
        if (shapeless)
        {
            ShapelessLines++;
            hash = 0;
        }
        else
        {
            shapes.Add(hash);
        }
        var shape = shapeless ? ManyShapes : full.Length <= MaxShapeChars ? full : full[..MaxShapeChars];

        _key.Clear();
        _key.Append(r.User).Append('\u0000').Append(r.Database).Append('\u0000').Append(r.RemoteHost).Append('\u0000')
            .Append(r.ApplicationName ?? NoApp + r.Pid.ToString(CultureInfo.InvariantCulture)).Append('\u0000')
            .Append(cls).Append('\u0000').Append(command).Append('\u0000').Append(objects).Append('\u0000')
            .Append(hash.ToString("x16", CultureInfo.InvariantCulture));
        var key = MinuteAggregator.KeyOf(Encoding.UTF8.GetBytes(_key.ToString()));
        var group = _minutes.Group(r.TsMicros, key, out var created);
        if (created)
        {
            var labels = group.Labels;
            labels[User] = r.User;
            labels[Database] = r.Database;
            labels[RemoteHost] = r.RemoteHost;
            labels[RemotePort] = r.RemotePort;
            labels[App] = r.ApplicationName ?? NoApp;
            labels[SessionId] = r.SessionId;
            labels[Pid] = r.Pid.ToString(CultureInfo.InvariantCulture);
            labels[Class] = cls;
            labels[Command] = command;
            labels[Objects] = objects;
            labels[Shape] = shape;
            labels[Format] = PgWindowReader.FormatName(r.Format);
            labels[Truncated] = open.Truncated || (!shapeless && full.Length > MaxShapeChars) ? "1" : "";
            labels[ShapeHash] = hash.ToString("x16", CultureInfo.InvariantCulture);
        }
        group.Add(r.TsMicros);
    }

    private sealed class Open
    {
        private readonly SortedDictionary<string, ClassPart> _parts = new(StringComparer.Ordinal);

        public Open(PgLogRecord first, PgAuditLine line)
        {
            First = first;
            Statement = line.Statement;
            StatementId = line.StatementId;
            SubstatementId = line.SubstatementId;
        }

        public PgLogRecord First { get; }
        public string Statement { get; }
        public long StatementId { get; }
        public long SubstatementId { get; }
        public bool Truncated { get; set; }
        public IEnumerable<KeyValuePair<string, ClassPart>> Parts => _parts;

        /// <summary>
        /// Whether <paramref name="line"/> is one of this statement's lines: the same statement and substatement ids, or
        /// the COPY's own line of a COPY (query) TO, which comes after the query's lines with the next substatement id.
        /// </summary>
        public bool Holds(PgAuditLine line) =>
            line.StatementId == StatementId
            && (line.SubstatementId == SubstatementId || (line.Command == "COPY" && PgCommand.Of(Statement) == "COPY"));

        /// <summary>The statement's lines of one class; the command is the first line's of that class.</summary>
        public ClassPart Part(string cls, string command)
        {
            if (!_parts.TryGetValue(cls, out var part))
            {
                part = new ClassPart(command);
                _parts[cls] = part;
            }
            return part;
        }
    }

    private sealed class ClassPart(string command)
    {
        public string Command { get; } = command;
        public SortedSet<string> Objects { get; } = new(StringComparer.Ordinal);
    }
}
