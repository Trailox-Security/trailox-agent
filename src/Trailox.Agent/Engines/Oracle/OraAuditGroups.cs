using System.Globalization;
using System.Text;
using Trailox.Agent.Engines.MongoDb;

namespace Trailox.Agent.Engines.Oracle;

/// <summary>One record of the trail, as the poll read it. One instance is reused row after row.</summary>
public sealed class OraRecord
{
    public long Micros, Instance, ReturnCode, SessionId, EntryId, StatementId, TextLength;
    public string User = "", Proxy = "", Action = "", Schema = "", Object = "", Program = "", OsUser = "", UserHost = "", Terminal = "",
        Authentication = "", Policies = "", Privilege = "", AuditType = "", DataPump = "", Text = "";
    public bool Truncated;
}

/// <summary>
/// The trail's reads, writes and calls, grouped per minute: one row for each minute and (user, proxy, action, result, object,
/// program, client, policies, privilege, statement shape), with the count.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>Only the actions of <see cref="IsGrouped"/> are grouped: at the highest audit level every statement is a record.
///   Sign-ins, DDL, grants, and user, role and audit-setting changes stay one row each.</item>
/// <item>The result is part of the key, so a refused read never shares a row with one that worked; so are the client and the
///   policies, so a group never hides a second address or a policy that audited it.</item>
/// <item>One statement can write several records, one for each object: each object is a key of its own.</item>
/// <item>🔴 THE POLL HAS NO ORDER BY, so records of a minute come mixed with the next one's. A minute ships
///   <see cref="GraceMicros"/> after the newest record passes it, or sooner when <see cref="MaxOpenGroups"/> are open, so
///   what this holds does not grow with the window. A record later than that opens a second row for its minute, and the
///   counts still add up: each row's hash tells them apart.</item>
/// <item>🔴 ONE USER CANNOT CROWD OUT THE OTHERS. A user's records step down as they fill their caps, and each step still names
///   the user and the proxy. Past <see cref="UserShapeCap"/> keys in a minute they group without shape and client. Past
///   <see cref="UserObjectCap"/> objects, or <see cref="UserFailedObjectCap"/> for records that failed, which can carry any
///   invented name, they also group without the object. Past <see cref="GroupCap"/> named groups in a minute everyone's
///   further records take that last step, one row per user and action, so a burst of many accounts cannot put anyone in a
///   row that names nobody.</item>
/// <item>A record that worked keeps its object far longer than one that failed: it can only name something that exists.</item>
/// </list>
/// </remarks>
public sealed class OraAuditGroups
{
    /// <summary>Named groups in one minute, after which every further key takes the last step.</summary>
    public const int GroupCap = 10_000;

    /// <summary>Keys of one user in one minute before that user's further records group without their shape.</summary>
    public const int UserShapeCap = 500;

    /// <summary>Objects of one user in one minute, past <see cref="UserShapeCap"/>, before they group without their object.</summary>
    public const int UserObjectCap = 2000;

    /// <summary>The same for records that failed: a failed read can name anything.</summary>
    public const int UserFailedObjectCap = 500;

    /// <summary>Groups open at once before every open minute ships, wherever the newest record stands.</summary>
    public const int MaxOpenGroups = 2 * GroupCap;

    /// <summary>The text of records grouped without their shape.</summary>
    public const string ManyShapes = "<many shapes>";

    /// <summary>The object of records grouped without their object.</summary>
    public const string ManyObjects = "<many objects>";

    /// <summary>How far past a minute the newest record must be before it ships: how mixed the trail's order is.</summary>
    public const long GraceMicros = 120_000_000;

    private const long FlushEveryMicros = 1_000_000;

    internal const int Instance = 0, User = 1, Proxy = 2, Action = 3, ReturnCode = 4, Schema = 5, Object = 6, Program = 7, OsUser = 8, UserHost = 9,
        Terminal = 10, Authentication = 11, Policies = 12, Privilege = 13, AuditType = 14, SessionId = 15, EntryId = 16, StatementId = 17, Shape = 18,
        TextLength = 19, Truncated = 20, Hash = 21, LabelCount = 22;

    private static readonly HashSet<string> Grouped = new(StringComparer.Ordinal)
    {
        "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "EXECUTE", "PL/SQL EXECUTE",
    };

    private readonly MinuteAggregator _minutes;
    private readonly Dictionary<(long Minute, string User), HashSet<UInt128>> _userKeys = new();
    private readonly Dictionary<(long Minute, string User), HashSet<UInt128>> _userObjects = new();
    private readonly Dictionary<long, int> _named = new();
    private readonly StringBuilder _key = new();
    private int _open;
    private long _newest = long.MinValue;
    private long _flushedAt = long.MinValue;
    private long _sweptMinute = long.MinValue;

    public OraAuditGroups(Action<MinuteGroup> emit) =>
        _minutes = new MinuteAggregator(MaxOpenGroups, LabelCount, group =>
        {
            _open--;
            emit(group);
        }, GraceMicros);

    /// <summary>Records taken.</summary>
    public long Records { get; private set; }

    /// <summary>Records of a minute already shipped: they open a second row for it.</summary>
    public long LateRecords => _minutes.LateLines;

    /// <summary>Records grouped without their shape.</summary>
    public long ShapelessRecords { get; private set; }

    public static bool IsGrouped(string action) => Grouped.Contains(action);

    /// <param name="r">A record of an action that <see cref="IsGrouped"/> takes.</param>
    /// <param name="shape">Its statement's shape; empty when the source ships no text.</param>
    public void Add(OraRecord r, string shape)
    {
        Records++;
        Advance(r.Micros);
        var minute = r.Micros - (r.Micros % MinuteAggregator.MinuteMicros + MinuteAggregator.MinuteMicros) % MinuteAggregator.MinuteMicros;
        var crowded = _named.GetValueOrDefault(minute) >= GroupCap;
        var key = Key(r.Instance, r.User, r.Proxy, r.Action, r.ReturnCode.ToString(CultureInfo.InvariantCulture), r.Schema, r.Object, r.Program, r.OsUser,
            r.UserHost, ClientHost(r.Authentication), r.Policies, r.Privilege, r.AuditType, shape);
        var shapeless = !Admit(_userKeys, minute, r.User, key, UserShapeCap, crowded);
        var objectless = false;
        if (shapeless)
        {
            ShapelessRecords++;
            key = Key(r.Instance, r.User, r.Proxy, r.Action, r.ReturnCode.ToString(CultureInfo.InvariantCulture), r.Schema, r.Object);
            objectless = !Admit(_userObjects, minute, r.User, key, r.ReturnCode == 0 ? UserObjectCap : UserFailedObjectCap, crowded);
            if (objectless)
            {
                // Failed or not, not its code: a program can raise any number it likes.
                key = Key(r.Instance, r.User, r.Proxy, r.Action, r.ReturnCode == 0 ? "ok" : "failed");
            }
        }
        var group = _minutes.Group(r.Micros, key, out var created);
        if (created)
        {
            Fill(group, r, shape, key, shapeless, objectless);
            _named[minute] = _named.GetValueOrDefault(minute) + (objectless ? 0 : 1);
            if (++_open >= MaxOpenGroups)
            {
                group.Add(r.Micros);
                ShipOpenMinutes();
                return;
            }
        }
        group.Add(r.Micros);
    }

    /// <summary>The window's end: every minute ships.</summary>
    public void Finish() => ShipOpenMinutes();

    private void ShipOpenMinutes()
    {
        _minutes.FlushAll();
        _open = 0;
        _userKeys.Clear();
        _userObjects.Clear();
        _named.Clear();
    }

    private static void Fill(MinuteGroup group, OraRecord r, string shape, UInt128 key, bool shapeless, bool objectless)
    {
        var labels = group.Labels;
        labels[Instance] = r.Instance.ToString(CultureInfo.InvariantCulture);
        labels[User] = r.User;
        labels[Proxy] = r.Proxy;
        labels[Action] = r.Action;
        labels[ReturnCode] = r.ReturnCode.ToString(CultureInfo.InvariantCulture);
        labels[SessionId] = r.SessionId.ToString(CultureInfo.InvariantCulture);
        labels[EntryId] = r.EntryId.ToString(CultureInfo.InvariantCulture);
        labels[StatementId] = r.StatementId.ToString(CultureInfo.InvariantCulture);
        labels[Schema] = objectless ? "" : r.Schema;
        labels[Object] = objectless ? ManyObjects : r.Object;
        labels[Shape] = shapeless ? ManyShapes : shape;
        // The key's own hash, with this row's minute and part: no two rows share one, even when a late record opens a second.
        labels[Hash] = ((ulong)key ^ ((ulong)group.MinuteMicros << 8) ^ (ulong)group.Part).ToString("x16", CultureInfo.InvariantCulture);
        // A row without its shape stands for many clients: it names none of them.
        if (!shapeless)
        {
            labels[Program] = r.Program;
            labels[OsUser] = r.OsUser;
            labels[UserHost] = r.UserHost;
            labels[Terminal] = r.Terminal;
            labels[Authentication] = r.Authentication;
            labels[Policies] = r.Policies;
            labels[Privilege] = r.Privilege;
            labels[AuditType] = r.AuditType;
            labels[TextLength] = r.TextLength.ToString(CultureInfo.InvariantCulture);
            labels[Truncated] = r.Truncated ? "1" : "";
        }
    }

    private void Advance(long micros)
    {
        if (micros <= _newest)
        {
            return;
        }
        _newest = micros;
        // At most once a second of trail: the open groups are walked only then.
        if (_flushedAt != long.MinValue && micros - _flushedAt < FlushEveryMicros)
        {
            return;
        }
        _flushedAt = micros;
        _minutes.FlushBefore(micros);
        var minute = micros / MinuteAggregator.MinuteMicros;
        if (minute == _sweptMinute)
        {
            return;
        }
        _sweptMinute = minute;
        var shipped = micros - GraceMicros - (2 * MinuteAggregator.MinuteMicros);
        Sweep(_userKeys, shipped);
        Sweep(_userObjects, shipped);
        foreach (var old in _named.Keys.Where(m => m < shipped).ToList())
        {
            _named.Remove(old);
        }
    }

    /// <summary>A minute that has shipped counts no more keys: its late records open a part of their own.</summary>
    private static void Sweep(Dictionary<(long Minute, string User), HashSet<UInt128>> sets, long before)
    {
        foreach (var key in sets.Keys.Where(k => k.Minute < before).ToList())
        {
            sets.Remove(key);
        }
    }

    private static bool Admit(Dictionary<(long Minute, string User), HashSet<UInt128>> sets, long minute, string user, UInt128 key, int cap, bool crowded)
    {
        if (!sets.TryGetValue((minute, user), out var keys))
        {
            keys = new HashSet<UInt128>();
            sets[(minute, user)] = keys;
        }
        if (keys.Contains(key))
        {
            return true;
        }
        if (crowded || keys.Count >= cap)
        {
            return false;
        }
        keys.Add(key);
        return true;
    }

    private UInt128 Key(long instance, string user, string proxy, string action, params string[] rest)
    {
        _key.Clear();
        _key.Append(instance).Append('\u0000').Append(user).Append('\u0000').Append(proxy).Append('\u0000').Append(action);
        foreach (var part in rest)
        {
            _key.Append('\u0000').Append(part);
        }
        return MinuteAggregator.KeyOf(Encoding.UTF8.GetBytes(_key.ToString()));
    }

    /// <summary>
    /// The address in <c>AUTHENTICATION_TYPE</c>, for the key only: the text also holds the client's port, which is new each session.
    /// </summary>
    internal static string ClientHost(string authentication)
    {
        var at = authentication.IndexOf("HOST=", StringComparison.Ordinal);
        if (at < 0)
        {
            return "";
        }
        var end = authentication.IndexOf(')', at);
        return end < 0 ? authentication[(at + 5)..] : authentication[(at + 5)..end];
    }
}
