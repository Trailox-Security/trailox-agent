using System.Buffers;
using System.Buffers.Text;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Trailox.Agent.Engines.MongoDb;

/// <summary>The four streams built from Atlas log files.</summary>
public enum MongoLogStream
{
    /// <summary>mongo_ops: audit authCheck and DDL lines, per minute; the audit startup line as evidence of the active filter.</summary>
    Ops,

    /// <summary>mongo_slow_ops: server-log 51803 "Slow query", per minute.</summary>
    SlowOps,

    /// <summary>mongo_connections: server-log 22943, 22944, 5286306 and 4615611, one row a line.</summary>
    Connections,

    /// <summary>mongo_signin_failures: server-log 5286307, per minute and connection.</summary>
    SigninFailures,
}

/// <summary>What one file did with its lines: each line lands in exactly one count.</summary>
public sealed class LineCounts
{
    public long LinesIn, OutOfWindow, Filtered, Malformed, Kept;
    public long MaxTsMicros = long.MinValue;
}

/// <summary>Rows as NDJSON into a buffer the caller drains (the upload, or memory).</summary>
public sealed class RowSink
{
    private static readonly JsonWriterOptions Options = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping, SkipValidation = true };

    private readonly IBufferWriter<byte> _output;

    public Utf8JsonWriter Writer { get; }

    public long Rows { get; private set; }

    public RowSink(IBufferWriter<byte> output)
    {
        _output = output;
        Writer = new Utf8JsonWriter(output, Options);
    }

    public void EndRow()
    {
        Writer.Flush();
        _output.Write("\n"u8);
        Writer.Reset(_output);
        Rows++;
    }

    public static void Time(Utf8JsonWriter w, ReadOnlySpan<byte> name, long micros) =>
        w.WriteString(name, DateTime.UnixEpoch.AddTicks(micros * 10).ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", System.Globalization.CultureInfo.InvariantCulture));
}

/// <summary>Fields of one line, as spans into the line or into the processor's unescape scratch.</summary>
internal ref struct LineFields
{
    public ReadOnlySpan<byte> Ctx, Ns, App, Remote, Client, CommandName, User, Db, Mechanism, Uuid, Host, Driver, DriverVersion, Ip, DirectIp, Error, QueryShapeHash, AuditFilter;
    public long ConnectionId, Pid, Result, ErrCode, Nreturned, DocsExamined, KeysExamined, Reslen, DurationMs;
    public int AuditSuccesses;
    public bool IsClusterMember, IsSystemUser, HasShape, HasMetrics, IsFromUserConnection, IsFromUserConnectionSet;
}

/// <summary>
/// One Atlas log file of one process for one task window: the window check and the id/atype filter on
/// the raw bytes, then <see cref="Utf8JsonReader"/> over the lines kept, the shape, and the stream's rows.
/// Reads both the API's lines and the S3 export's rewritten ones (arrays as objects keyed "1".."n").
/// </summary>
public sealed partial class MongoLogProcessor
{
    public const int DefaultGroupCap = 20_000;

    /// <summary>The command name of an operation whose audit line could not be parsed: it is still counted, never dropped.</summary>
    public const string Unparsed = "?unparsed";

    /// <summary>
    /// Deeper than any line Atlas writes: MongoDB documents nest up to 100 levels, and the log adds a few. At the
    /// reader's default (64) a deliberately deep query would fail to parse and its operation would go unseen.
    /// </summary>
    internal static readonly JsonReaderOptions ReaderOptions = new() { MaxDepth = 256 };

    private static readonly byte[][] DdlTypes =
    [
        "createCollection"u8.ToArray(), "dropCollection"u8.ToArray(), "createDatabase"u8.ToArray(), "dropDatabase"u8.ToArray(),
        "createIndex"u8.ToArray(), "dropIndex"u8.ToArray(), "renameCollection"u8.ToArray(), "createUser"u8.ToArray(),
        "dropUser"u8.ToArray(), "updateUser"u8.ToArray(), "grantRolesToUser"u8.ToArray(), "revokeRolesFromUser"u8.ToArray(),
        "createRole"u8.ToArray(), "dropRole"u8.ToArray(), "updateRole"u8.ToArray(), "grantPrivilegesToRole"u8.ToArray(),
        "revokePrivilegesFromRole"u8.ToArray(), "grantRolesToRole"u8.ToArray(), "revokeRolesFromRole"u8.ToArray(),
        "dropAllUsersFromDatabase"u8.ToArray(), "dropAllRolesFromDatabase"u8.ToArray(), "shardCollection"u8.ToArray(),
        "enableSharding"u8.ToArray(), "addShard"u8.ToArray(), "removeShard"u8.ToArray(),
    ];

    // Command names a slow-query line's "command" can start with. The command is the first of these keys whose value
    // is a string or a number (the collection, or 1), never simply the first key: the S3 export reorders keys, and a
    // command's own fields can carry these names (findAndModify's "update", aggregate's "explain").
    private static readonly HashSet<string> KnownCommands = new(StringComparer.Ordinal)
    {
        "find", "aggregate", "count", "distinct", "getMore", "insert", "update", "delete", "findAndModify", "findandmodify",
        "bulkWrite", "mapReduce", "geoNear", "explain", "listCollections", "listIndexes", "listDatabases", "create", "drop",
        "dropDatabase", "createIndexes", "dropIndexes", "renameCollection", "collMod", "createUser", "updateUser", "dropUser",
        "grantRolesToUser", "revokeRolesFromUser", "createRole", "updateRole", "dropRole", "hello", "isMaster", "ismaster",
        "ping", "serverStatus", "replSetGetStatus", "killCursors", "endSessions", "dbStats", "collStats", "validate",
        "compact", "currentOp", "killOp", "shardCollection", "moveChunk", "balancerStart", "balancerStop", "replSetUpdatePosition",
        // Admin and sharding commands, so the S3 form names them as the API form does (moveRange and addShard are in a
        // sharded cluster's slow log). Only names no other command uses as a field.
        "moveRange", "addShard", "removeShard", "splitChunk", "mergeChunks", "refineCollectionShardKey", "reshardCollection",
        "enableSharding", "listShards", "balancerStatus", "configureCollectionBalancing", "cleanupOrphaned",
        "commitTransaction", "abortTransaction", "killSessions", "killAllSessions", "dropConnections",
        "createSearchIndexes", "updateSearchIndex", "dropSearchIndex", "listSearchIndexes", "buildInfo", "hostInfo",
        "getParameter", "setParameter", "setFeatureCompatibilityVersion", "connectionStatus", "usersInfo", "rolesInfo",
        "dropAllUsersFromDatabase", "dropAllRolesFromDatabase", "grantRolesToRole", "revokeRolesFromRole",
        "grantPrivilegesToRole", "revokePrivilegesFromRole", "replSetHeartbeat", "getLog", "fsync",
    };

    private readonly MongoLogStream _stream;
    private readonly bool _audit;
    private readonly LineForm _form;
    private readonly string _process;
    private readonly string _processType;
    private readonly long _windowStart;
    private readonly long _windowEnd;
    private readonly RowSink _sink;
    private readonly MinuteAggregator? _aggregator;
    private readonly MongoShape _shape = new();
    private readonly Dictionary<ulong, string> _shapes = new();
    private readonly ArrayBufferWriter<byte> _key = new(512);
    private byte[] _scratch = new byte[1 << 16];
    private int _scratchUsed;

    /// <summary>A rewritten line's top-level id (server log) or atype (audit) offset, found in the scan for its time.</summary>
    private int _keyAt = -1;

    public LineCounts Counts { get; } = new();

    public long OverflowLines => _aggregator?.OverflowLines ?? 0;

    public long LateLines => _aggregator?.LateLines ?? 0;

    /// <param name="windowStartMicros">Lines with start &lt;= t &lt; end are the window's; the others belong to another task.</param>
    /// <param name="form">How the file's lines order their keys: as mongod writes them, or rewritten by the S3 export.</param>
    /// <remarks>mongo_ops reads audit files; the three other streams read server logs.</remarks>
    public MongoLogProcessor(MongoLogStream stream, string process, string processType, long windowStartMicros, long windowEndMicros,
        RowSink sink, int groupCap = DefaultGroupCap, LineForm form = LineForm.Api)
    {
        _stream = stream;
        _audit = stream == MongoLogStream.Ops;
        _form = form;
        _process = process;
        _processType = processType;
        _windowStart = windowStartMicros;
        _windowEnd = windowEndMicros;
        _sink = sink;
        // The S3 export reorders records within a batch by up to 38 s: with the API's 2 s grace a busy
        // minute would ship in several parts.
        var grace = form == LineForm.Rewritten ? MinuteAggregator.S3GraceMicros : MinuteAggregator.ApiGraceMicros;
        _aggregator = stream switch
        {
            MongoLogStream.Ops => new MinuteAggregator(groupCap, OpsLabels, WriteOps, grace),
            MongoLogStream.SlowOps => new MinuteAggregator(groupCap, SlowLabels, WriteSlow, grace),
            MongoLogStream.SigninFailures => new MinuteAggregator(groupCap, FailureLabels, WriteFailure, grace),
            _ => null,
        };
    }

    /// <summary>Lets the aggregator ship every minute the file has moved past. Call every few thousand lines.</summary>
    public void FlushClosedMinutes() => _aggregator?.FlushBefore(Counts.MaxTsMicros);

    /// <summary>Ships what is left. The file is done after this.</summary>
    public void Finish() => _aggregator?.FlushAll();

    public void Process(ReadOnlySpan<byte> line)
    {
        Counts.LinesIn++;
        _scratchUsed = 0;
        if (line.IsEmpty)
        {
            Counts.Malformed++;
            return;
        }
        var audit = _audit;
        long ts;
        bool timed;
        if (_form == LineForm.Rewritten)
        {
            LineScan.TopLevelValues(line, audit ? "ts"u8 : "t"u8, audit ? "atype"u8 : "id"u8, out var timeAt, out _keyAt);
            timed = LineScan.TryTimestampAt(line, timeAt, out ts);
        }
        else
        {
            timed = LineScan.TryTimestampMicros(line, audit, out ts);
        }
        if (!timed)
        {
            Counts.Malformed++;
            return;
        }
        if (ts > Counts.MaxTsMicros)
        {
            Counts.MaxTsMicros = ts;
        }
        if (ts < _windowStart || ts >= _windowEnd)
        {
            Counts.OutOfWindow++;
            return;
        }
        var kept = Counts.Kept;
        try
        {
            if (audit)
            {
                ProcessAudit(line, ts);
            }
            else
            {
                ProcessServer(line, ts);
            }
        }
        catch (Exception ex) when (ex is not (OperationCanceledException or OutOfMemoryException))
        {
            // 🔴 ONE LINE NEVER FAILS A TASK, AND AN OPERATION IS NEVER DROPPED. Any failure on one line is that
            //    line's: a task that threw here would fail at every retry and its window would never land.
            // 🔴 BUT RUNNING OUT OF MEMORY IS NOT ONE LINE'S FAULT, and fails the task. Measured on agent 1.8.1:
            //    a 10-minute window whose held rows outgrew 512 MiB shipped exactly 128 MiB of them and
            //    succeeded, each later row's write counted here as a malformed line.
            //    Parsing comes before anything is counted, so the line is counted once, here: an audited
            //    operation that cannot be parsed is still shipped, counted, under the command "?unparsed".
            if (Counts.Kept != kept)
            {
                return;
            }
            if (audit && _stream == MongoLogStream.Ops && UnparsedOp(line, ts))
            {
                return;
            }
            Counts.Malformed++;
        }
    }

    /// <summary>An operation's audit line that would not parse: its type and time are known from the raw bytes, so it is counted.</summary>
    private bool UnparsedOp(ReadOnlySpan<byte> line, long ts)
    {
        var atype = LineScan.QuotedAt(line, KeyAt(line, "\"atype\""u8));
        if (!atype.SequenceEqual("authCheck"u8) && !IsDdl(atype))
        {
            return false;
        }
        var f = new LineFields();
        Counts.Kept++;
        AddOp(ts, "?unparsed"u8, atype, ref f);
        return true;
    }

    /// <summary>The offset of the line's id or atype value: from its first bytes, or for a rewritten line from the time scan.</summary>
    private int KeyAt(ReadOnlySpan<byte> line, ReadOnlySpan<byte> quotedKey) =>
        _form == LineForm.Rewritten ? _keyAt : LineScan.ValueOf(line, quotedKey);

    // ---- server log -------------------------------------------------------------------------------

    private void ProcessServer(ReadOnlySpan<byte> line, long ts)
    {
        var at = KeyAt(line, "\"id\""u8);
        if (!LineScan.TryInt(line, at, out var id))
        {
            Counts.Malformed++;
            return;
        }
        var wanted = _stream switch
        {
            MongoLogStream.SlowOps => id == 51803,
            MongoLogStream.Connections => id is 22943 or 22944 or 5286306 or 4615611,
            MongoLogStream.SigninFailures => id == 5286307,
            _ => false,
        };
        if (!wanted)
        {
            Counts.Filtered++;
            return;
        }
        var f = new LineFields();
        ParseServer(line, ref f);
        // Counted only once the line has parsed: see Process.
        Counts.Kept++;
        switch (id)
        {
            case 51803:
                AddSlow(ts, ref f);
                break;
            case 5286307:
                AddFailure(ts, ref f);
                break;
            default:
                WriteConnection(ts, id, ref f);
                break;
        }
    }

    private void ParseServer(ReadOnlySpan<byte> line, ref LineFields f)
    {
        var r = new Utf8JsonReader(line, ReaderOptions);
        r.Read();
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            if (name.SequenceEqual("ctx"u8))
            {
                f.Ctx = Str(ref r);
            }
            else if (name.SequenceEqual("attr"u8) && r.TokenType == JsonTokenType.StartObject)
            {
                ParseServerAttr(ref r, ref f);
            }
            else
            {
                r.Skip();
            }
        }
    }

    private void ParseServerAttr(scoped ref Utf8JsonReader r, ref LineFields f)
    {
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            switch (r.TokenType)
            {
                case JsonTokenType.Number:
                    var v = Long(ref r);
                    if (name.SequenceEqual("nreturned"u8))
                    {
                        f.Nreturned = v;
                        f.HasMetrics = true;
                    }
                    else if (name.SequenceEqual("docsExamined"u8))
                    {
                        f.DocsExamined = v;
                        f.HasMetrics = true;
                    }
                    else if (name.SequenceEqual("keysExamined"u8))
                    {
                        f.KeysExamined = v;
                        f.HasMetrics = true;
                    }
                    else if (name.SequenceEqual("reslen"u8))
                    {
                        f.Reslen = v;
                        f.HasMetrics = true;
                    }
                    else if (name.SequenceEqual("durationMillis"u8))
                    {
                        f.DurationMs = v;
                        f.HasMetrics = true;
                    }
                    else if (name.SequenceEqual("errCode"u8))
                    {
                        f.ErrCode = v;
                    }
                    else if (name.SequenceEqual("connectionId"u8))
                    {
                        f.ConnectionId = v;
                    }
                    else if (name.SequenceEqual("pid"u8))
                    {
                        f.Pid = v;
                    }
                    else if (name.SequenceEqual("result"u8))
                    {
                        f.Result = v;
                    }
                    break;
                case JsonTokenType.String:
                    if (name.SequenceEqual("ns"u8))
                    {
                        f.Ns = Str(ref r);
                    }
                    else if (name.SequenceEqual("appName"u8))
                    {
                        f.App = Str(ref r);
                    }
                    else if (name.SequenceEqual("remote"u8))
                    {
                        f.Remote = Str(ref r);
                    }
                    else if (name.SequenceEqual("client"u8))
                    {
                        f.Client = Str(ref r);
                    }
                    else if (name.SequenceEqual("user"u8))
                    {
                        f.User = Str(ref r);
                    }
                    else if (name.SequenceEqual("db"u8))
                    {
                        f.Db = Str(ref r);
                    }
                    else if (name.SequenceEqual("mechanism"u8))
                    {
                        f.Mechanism = Str(ref r);
                    }
                    else if (name.SequenceEqual("host"u8))
                    {
                        f.Host = Str(ref r);
                    }
                    else if (name.SequenceEqual("error"u8))
                    {
                        f.Error = Str(ref r);
                    }
                    else if (name.SequenceEqual("queryShapeHash"u8))
                    {
                        f.QueryShapeHash = Str(ref r);
                    }
                    break;
                case JsonTokenType.True:
                case JsonTokenType.False:
                    if (name.SequenceEqual("isClusterMember"u8))
                    {
                        f.IsClusterMember = r.TokenType == JsonTokenType.True;
                    }
                    else if (name.SequenceEqual("isFromUserConnection"u8))
                    {
                        f.IsFromUserConnection = r.TokenType == JsonTokenType.True;
                        f.IsFromUserConnectionSet = true;
                    }
                    break;
                case JsonTokenType.StartObject:
                    if (name.SequenceEqual("command"u8))
                    {
                        Command(ref r, ref f, fromSlowLog: true);
                    }
                    else if (name.SequenceEqual("doc"u8))
                    {
                        ClientDoc(ref r, ref f);
                    }
                    else if (name.SequenceEqual("uuid"u8))
                    {
                        f.Uuid = Nested(ref r, "uuid"u8, "$uuid"u8);
                    }
                    else
                    {
                        r.Skip();
                    }
                    break;
                default:
                    r.Skip();
                    break;
            }
        }
    }

    // ---- audit log ---------------------------------------------------------------------------------

    private void ProcessAudit(ReadOnlySpan<byte> line, long ts)
    {
        var atype = LineScan.QuotedAt(line, KeyAt(line, "\"atype\""u8));
        var isOp = atype.SequenceEqual("authCheck"u8);
        var isStartup = !isOp && atype.SequenceEqual("startup"u8);
        var isDdl = !isOp && !isStartup && IsDdl(atype);
        // authenticate, clientMetadata and logout duplicate the server log's sign-in and connection lines,
        // which mongo_connections reads whatever the audit level: dropped here, so nothing counts twice.
        if (!(isOp || isStartup || isDdl))
        {
            Counts.Filtered++;
            return;
        }
        var f = new LineFields { AuditSuccesses = -1 };
        ParseAudit(line, ref f, isStartup);
        Counts.Kept++;
        if (isStartup)
        {
            WriteStartup(ts, ref f);
            return;
        }
        AddOp(ts, isOp ? f.CommandName : atype, atype, ref f);
    }

    private void ParseAudit(ReadOnlySpan<byte> line, ref LineFields f, bool startup)
    {
        var r = new Utf8JsonReader(line, ReaderOptions);
        r.Read();
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            if (r.TokenType == JsonTokenType.StartObject && name.SequenceEqual("uuid"u8))
            {
                f.Uuid = Member(ref r, "$binary"u8);
            }
            else if (r.TokenType == JsonTokenType.StartObject && name.SequenceEqual("remote"u8))
            {
                f.Ip = Endpoint(ref r, out var system);
                f.IsSystemUser |= system;
            }
            else if (r.TokenType == JsonTokenType.StartObject && name.SequenceEqual("directRemote"u8))
            {
                f.DirectIp = Endpoint(ref r, out _);
            }
            else if (name.SequenceEqual("users"u8) && r.TokenType is JsonTokenType.StartArray or JsonTokenType.StartObject)
            {
                FirstUser(ref r, ref f);
            }
            else if (r.TokenType == JsonTokenType.StartObject && name.SequenceEqual("param"u8))
            {
                if (startup)
                {
                    ParseStartup(ref r, ref f);
                }
                else
                {
                    ParseParam(ref r, ref f);
                }
            }
            else if (r.TokenType == JsonTokenType.Number && name.SequenceEqual("result"u8))
            {
                f.Result = Long(ref r);
            }
            else
            {
                r.Skip();
            }
        }
    }

    /// <summary>{ "ip": .., "port": .. } or { "isSystemUser": true }.</summary>
    private ReadOnlySpan<byte> Endpoint(scoped ref Utf8JsonReader r, out bool isSystemUser)
    {
        ReadOnlySpan<byte> ip = default;
        isSystemUser = false;
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            if (r.TokenType == JsonTokenType.String && name.SequenceEqual("ip"u8))
            {
                ip = Str(ref r);
            }
            else if (r.TokenType == JsonTokenType.True && name.SequenceEqual("isSystemUser"u8))
            {
                isSystemUser = true;
            }
            else
            {
                r.Skip();
            }
        }
        return ip;
    }

    /// <summary>The first authenticated user: an array in the API's lines, an object keyed "1".."n" in the S3 export's.</summary>
    private void FirstUser(scoped ref Utf8JsonReader r, ref LineFields f)
    {
        var isArray = r.TokenType == JsonTokenType.StartArray;
        var depth = r.CurrentDepth;
        var taken = false;
        while (r.Read() && r.CurrentDepth > depth)
        {
            if (!isArray && r.TokenType == JsonTokenType.PropertyName)
            {
                var first = r.ValueSpan.SequenceEqual("1"u8);
                r.Read();
                if (first && r.TokenType == JsonTokenType.StartObject && !taken)
                {
                    UserAndDb(ref r, ref f);
                    taken = true;
                }
                else
                {
                    r.Skip();
                }
            }
            else if (isArray && r.TokenType == JsonTokenType.StartObject && !taken)
            {
                UserAndDb(ref r, ref f);
                taken = true;
            }
            else
            {
                r.Skip();
            }
        }
    }

    private void ParseParam(scoped ref Utf8JsonReader r, ref LineFields f)
    {
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            if (r.TokenType == JsonTokenType.StartObject && name.SequenceEqual("args"u8))
            {
                Command(ref r, ref f, fromSlowLog: false);
            }
            else if (r.TokenType == JsonTokenType.String && name.SequenceEqual("command"u8))
            {
                f.CommandName = Str(ref r);
            }
            else if (r.TokenType == JsonTokenType.String && name.SequenceEqual("ns"u8))
            {
                f.Ns = Str(ref r);
            }
            else
            {
                r.Skip();
            }
        }
    }

    /// <summary>param.startupOptions.auditLog.filter (JSON text) and setParameter.auditAuthorizationSuccess ("true"/"false").</summary>
    private void ParseStartup(scoped ref Utf8JsonReader r, ref LineFields f)
    {
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            if (r.TokenType != JsonTokenType.StartObject || !name.SequenceEqual("startupOptions"u8))
            {
                r.Skip();
                continue;
            }
            while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
            {
                var section = r.ValueSpan;
                r.Read();
                if (r.TokenType == JsonTokenType.StartObject && section.SequenceEqual("auditLog"u8))
                {
                    f.AuditFilter = Member(ref r, "filter"u8);
                }
                else if (r.TokenType == JsonTokenType.StartObject && section.SequenceEqual("setParameter"u8))
                {
                    while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
                    {
                        var key = r.ValueSpan;
                        r.Read();
                        if (key.SequenceEqual("auditAuthorizationSuccess"u8))
                        {
                            f.AuditSuccesses = r.TokenType switch
                            {
                                JsonTokenType.True => 1,
                                JsonTokenType.False => 0,
                                JsonTokenType.String => r.ValueSpan.SequenceEqual("true"u8) ? 1 : r.ValueSpan.SequenceEqual("false"u8) ? 0 : -1,
                                _ => -1,
                            };
                        }
                        r.Skip();
                    }
                }
                else
                {
                    r.Skip();
                }
            }
        }
    }

    // ---- shared parsing ----------------------------------------------------------------------------

    /// <summary>
    /// The command document: its shape, and for a slow line its name, the first known command key whose value is a
    /// string or a number.
    /// </summary>
    /// <remarks>
    /// 🔴 NOT JUST THE FIRST KNOWN KEY. A findAndModify carries an "update" field, and in the S3 export's key order it
    ///    can come first: the bench shipped 5 of 183 slow rows as update. The command's own key holds the
    ///    collection (or 1); "update" holds a document, as an aggregate's "explain" holds a boolean. Only when no known
    ///    key holds one (an explain, whose value is the explained command) is the first known key the name.
    /// 🔴 WITH NO KNOWN KEY, THE S3 FORM CANNOT USE THE FIRST KEY. A slow write statement is logged as
    ///    {"q":…,"u":…,"multi":…,"upsert":…}, and a command missing from the list by its own name, first in the API's
    ///    files. The export shuffles keys, so "the first key" named one statement q, upsert or multi at random.
    ///    The S3 form takes q when there is one, else the least key that holds a string or a number and
    ///    is not the driver's envelope, else the least key: the same name every time.
    /// </remarks>
    private void Command(scoped ref Utf8JsonReader r, ref LineFields f, bool fromSlowLog)
    {
        if (fromSlowLog && f.CommandName.IsEmpty)
        {
            var peek = r;
            ReadOnlySpan<byte> first = default, firstKnown = default, command = default, q = default, leastScalar = default, least = default;
            while (peek.Read() && peek.TokenType == JsonTokenType.PropertyName)
            {
                var key = peek.ValueSpan;
                if (first.IsEmpty)
                {
                    first = key;
                }
                var known = KnownCommands.Contains(Encoding.UTF8.GetString(key));
                peek.Read();
                var scalar = peek.TokenType is JsonTokenType.String or JsonTokenType.Number;
                if (known && scalar)
                {
                    command = key;
                    break;
                }
                if (known && firstKnown.IsEmpty)
                {
                    firstKnown = key;
                }
                if (key.SequenceEqual("q"u8))
                {
                    q = key;
                }
                if (least.IsEmpty || key.SequenceCompareTo(least) < 0)
                {
                    least = key;
                }
                if (scalar && key.Length > 0 && key[0] != (byte)'$' && !MongoShape.IsEnvelope(key)
                    && (leastScalar.IsEmpty || key.SequenceCompareTo(leastScalar) < 0))
                {
                    leastScalar = key;
                }
                peek.Skip();
            }
            // An explain's own value is the explained command, a document: with no better key it is still the name.
            f.CommandName = !command.IsEmpty ? command
                : !firstKnown.IsEmpty ? firstKnown
                : _form == LineForm.Api ? first
                : !q.IsEmpty ? q
                : !leastScalar.IsEmpty ? leastScalar
                : least;
        }
        _shape.Normalize(ref r);
        f.HasShape = true;
    }

    private void ClientDoc(scoped ref Utf8JsonReader r, ref LineFields f)
    {
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            if (r.TokenType != JsonTokenType.StartObject)
            {
                r.Skip();
                continue;
            }
            if (name.SequenceEqual("application"u8))
            {
                f.App = Member(ref r, "name"u8);
            }
            else if (name.SequenceEqual("driver"u8))
            {
                while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
                {
                    var key = r.ValueSpan;
                    r.Read();
                    if (r.TokenType == JsonTokenType.String && key.SequenceEqual("name"u8))
                    {
                        f.Driver = Str(ref r);
                    }
                    else if (r.TokenType == JsonTokenType.String && key.SequenceEqual("version"u8))
                    {
                        f.DriverVersion = Str(ref r);
                    }
                    else
                    {
                        r.Skip();
                    }
                }
            }
            else
            {
                r.Skip();
            }
        }
    }

    private void UserAndDb(scoped ref Utf8JsonReader r, ref LineFields f)
    {
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var key = r.ValueSpan;
            r.Read();
            if (r.TokenType == JsonTokenType.String && key.SequenceEqual("user"u8))
            {
                f.User = Str(ref r);
            }
            else if (r.TokenType == JsonTokenType.String && key.SequenceEqual("db"u8))
            {
                f.Db = Str(ref r);
            }
            else
            {
                r.Skip();
            }
        }
    }

    /// <summary>The string member <paramref name="key"/> of the object the reader is on; the rest of it is skipped.</summary>
    private ReadOnlySpan<byte> Member(scoped ref Utf8JsonReader r, ReadOnlySpan<byte> key)
    {
        ReadOnlySpan<byte> value = default;
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            if (r.TokenType == JsonTokenType.String && name.SequenceEqual(key))
            {
                value = Str(ref r);
            }
            else
            {
                r.Skip();
            }
        }
        return value;
    }

    /// <summary>{"outer": {"inner": "value"}} on the object the reader is on.</summary>
    private ReadOnlySpan<byte> Nested(scoped ref Utf8JsonReader r, ReadOnlySpan<byte> outer, ReadOnlySpan<byte> inner)
    {
        ReadOnlySpan<byte> value = default;
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            var name = r.ValueSpan;
            r.Read();
            if (r.TokenType == JsonTokenType.StartObject && name.SequenceEqual(outer))
            {
                value = Member(ref r, inner);
            }
            else
            {
                r.Skip();
            }
        }
        return value;
    }

    /// <summary>The string token's text: the raw span when it has no escapes, else unescaped into the line's scratch.</summary>
    private ReadOnlySpan<byte> Str(scoped ref Utf8JsonReader r)
    {
        if (!r.ValueIsEscaped)
        {
            return r.ValueSpan;
        }
        var need = r.ValueSpan.Length;
        if (_scratchUsed + need > _scratch.Length)
        {
            // Earlier spans of this line keep the old array alive through themselves.
            _scratch = new byte[Math.Max(_scratch.Length * 2, need)];
            _scratchUsed = 0;
        }
        var written = r.CopyString(_scratch.AsSpan(_scratchUsed));
        var text = _scratch.AsSpan(_scratchUsed, written);
        _scratchUsed += written;
        return text;
    }

    /// <summary>An integer; a double (the S3 export writes int64 as double) truncated.</summary>
    private static long Long(scoped ref Utf8JsonReader r) => r.TryGetInt64(out var v) ? v : (long)r.GetDouble();
}
