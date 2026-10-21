using System.Globalization;
using System.IO.Pipelines;
using System.Text.Json;
using Trailox.Agent.Config;
using Trailox.Agent.Protocol;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Engines.MongoDb;

/// <summary>
/// One MongoDB Atlas cluster, read through the Admin API as a service account: the probe checks the
/// account, the cluster and its tier, and each task downloads the cluster's log files for its window and
/// ships per-minute rows (agent 1.8). Only the optional catalog user (1.9.0) connects to the cluster itself,
/// for the collection catalog and the probe's catalog check (<see cref="MongoCatalog"/>).
/// </summary>
public sealed class MongoSource : IEngineSource
{
    /// <summary>Atlas keeps log files 30 days.</summary>
    internal static readonly TimeSpan Retention = TimeSpan.FromDays(30);

    /// <summary>Past this age a window is read as it is: a node that stopped writing must not hold its windows forever.</summary>
    internal static readonly TimeSpan CompletenessGiveUp = TimeSpan.FromHours(2);

    private static readonly TimeSpan ProcessCacheFor = TimeSpan.FromMinutes(15);
    private const int HeartbeatLines = 100_000;
    private const int FlushLines = 4096;

    private readonly AtlasApi _api;
    private readonly EndpointConfig _endpoint;
    private readonly AtlasCatalog _catalog;
    private readonly Func<DateTime> _utcNow;
    private (DateTime At, string Topology, IReadOnlyList<AtlasProcess> Processes)? _processes;

    public MongoSource(HttpClient http, EndpointConfig endpoint, string baseUrl = AtlasApi.DefaultBaseUrl, Func<DateTime>? utcNow = null)
    {
        _endpoint = endpoint;
        _api = new AtlasApi(http, endpoint.Host, endpoint.Username, endpoint.Password, baseUrl);
        _catalog = new AtlasCatalog(_api, endpoint.Username);
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        OpenCatalog = srv => MongoCatalogDriver.Open(srv, endpoint.ClusterName, endpoint.CatalogUsername, endpoint.CatalogPassword);
    }

    /// <summary>
    /// The catalog user's reads on the cluster at a <c>standardSrv</c> connection string: the driver, or a stand-in in
    /// tests. Each caller disposes what it opened, so no connection outlives its task or probe.
    /// </summary>
    internal Func<string, IMongoCatalogReads> OpenCatalog { get; init; }

    private bool HasCatalogUser => _endpoint.CatalogUsername.Length > 0;

    public async Task<EndpointCaps> ProbeAsync(CancellationToken ct)
    {
        var checks = new List<AtlasCheck>();
        await _api.TokenAsync(ct);
        checks.Add(Ok("authenticated", "the service account's token was issued"));

        string version, tier, topology, standardSrv;
        using (var cluster = await ClusterAsync(ct))
        {
            var root = cluster.RootElement;
            version = Str(root, "mongoDBVersion");
            topology = Str(root, "clusterType");
            standardSrv = StandardSrv(root);
            var provider = root.TryGetProperty("providerSettings", out var p) ? p : default;
            tier = provider.ValueKind == JsonValueKind.Object ? Str(provider, "instanceSizeName") : "";
            var providerName = provider.ValueKind == JsonValueKind.Object ? Str(provider, "providerName") : "";
            if (IsShared(tier, providerName))
            {
                // D2: a Free or Flex cluster answers the API and keeps no downloadable logs. Without this it would
                // "connect", collect nothing, and look like a fault of ours.
                throw NotDedicated(tier.Length > 0 ? tier : providerName);
            }
            if (root.TryGetProperty("paused", out var paused) && paused.ValueKind == JsonValueKind.True)
            {
                throw new SourceException($"cluster {_endpoint.ClusterName} is paused: it writes no logs until it is resumed");
            }
        }
        checks.Add(Ok("cluster", $"{_endpoint.ClusterName}: {tier}, {topology}, MongoDB {version}"));

        var (_, processes) = await ProcessesAsync(ct, refresh: true);
        checks.Add(Ok("processes", $"{processes.Count} process(es) whose logs are read"));

        // The log download is what every window needs: refuse the source now rather than fail every task.
        var now = new DateTimeOffset(_utcNow()).ToUnixTimeSeconds();
        using (await _api.DownloadLogAsync(processes[0].Host, ServerLog(processes[0]), now - 300, now - 240, ct))
        {
        }
        checks.Add(Ok("query_log", "the server log downloads (Project Cluster Log Viewer)"));

        checks.Add(await SoftCheckAsync("grants", "/databaseUsers?itemsPerPage=1", "database users and roles read (Project Read Only)", ct));
        checks.Add(await SoftCheckAsync("events", "/events?itemsPerPage=1", "project events read (Project Read Only)", ct));
        if (HasCatalogUser)
        {
            checks.Add(await CatalogCheckAsync(standardSrv, ct));
        }

        return new EndpointCaps
        {
            Version = version,
            Columns = new List<string>(),
            HasSessionLog = true,
            TextReadable = true,
            EarliestEventMicros = Micros(_utcNow() - Retention),
            EarliestSessionMicros = Micros(_utcNow() - Retention),
            Atlas = new AtlasCaps
            {
                ProjectId = _endpoint.Host,
                ClusterName = _endpoint.ClusterName,
                Tier = tier,
                Topology = topology,
                LogSource = MongoDbEngine.LogSourceApi,
                Processes = processes.ToList(),
                Checks = checks,
            },
        };
    }

    public async Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct)
    {
        switch (task.Stream)
        {
            case ProtocolInfo.Streams.MongoOps:
                return await OpsAsync(task, ct);
            case ProtocolInfo.Streams.MongoSlowOps:
                return await ServerLogAsync(MongoLogStream.SlowOps, task, ct);
            case ProtocolInfo.Streams.MongoConnections:
                return await ServerLogAsync(MongoLogStream.Connections, task, ct);
            case ProtocolInfo.Streams.MongoSigninFailures:
                return await ServerLogAsync(MongoLogStream.SigninFailures, task, ct);
            case ProtocolInfo.Streams.AtlasEvents:
                RequireWholeMinutes(task);
                return Buffered(await _catalog.EventsAsync(task.StartMicros, task.EndMicros, ct));
            case ProtocolInfo.Streams.CatalogUsers:
                return Buffered(await _catalog.UsersAsync(Today(), ct));
            case ProtocolInfo.Streams.CatalogRoles:
                return Buffered(await _catalog.RolesAsync(Today(), ct));
            case ProtocolInfo.Streams.CatalogAccessList:
                return Buffered(await _catalog.AccessListAsync(Today(), ct));
            case ProtocolInfo.Streams.CatalogDatabases:
                return Buffered(await _catalog.DatabasesAsync(Today(), await PrimariesAsync(ct), ct));
            case ProtocolInfo.Streams.CatalogCollections:
                return Buffered(await CollectionsAsync(ct));
            default:
                return null;
        }
    }

    /// <summary>
    /// The server-log streams (mongo_slow_ops, mongo_connections, mongo_signin_failures): each node's server log,
    /// read and streamed as mongo_ops reads its audit files.
    /// </summary>
    /// <remarks>
    /// 🔴 STREAMED, NOT HELD (from 1.8.2). They used to hold every row of the window until the upload, so that an
    ///    unpublished window was reported before any byte left. A cluster whose clients reconnect all the time then
    ///    outgrew the pod: each new connection is three rows, a 2-minute window at 1,000 new connections a second held
    ///    350 MiB (bench, 2026-10-20), and a 10-minute one about 630 MB, over the chart's 512 MiB. The completeness
    ///    check now runs first, on each node's last minutes of server log, as it always has for mongo_ops.
    /// </remarks>
    private Task<RowStream> ServerLogAsync(MongoLogStream stream, AgentTask task, CancellationToken ct) =>
        StreamedAsync(stream, ServerLog, task, ct);

    /// <summary>mongo_ops: the audit files, one line an operation at Full audit.</summary>
    private Task<RowStream> OpsAsync(AgentTask task, CancellationToken ct) =>
        StreamedAsync(MongoLogStream.Ops, AuditLog, task, ct);

    /// <summary>
    /// One stream's window, read from each node's <paramref name="logName"/> file: each minute is shipped as soon as
    /// the file is past it, through a pipe the upload drains, so what the task holds does not grow with the window.
    /// The window's completeness is checked first, on each node's server log, before any byte is uploaded.
    /// </summary>
    private async Task<RowStream> StreamedAsync(MongoLogStream stream, Func<AtlasProcess, string> logName, AgentTask task, CancellationToken ct)
    {
        RequireWholeMinutes(task);
        var (_, processes) = await ProcessesAsync(ct);
        foreach (var process in processes)
        {
            await RequirePublishedAsync(process, task, ct);
        }

        // The first download is opened here, so that a refusal surfaces as the task's failure before the upload.
        var first = await _api.DownloadLogAsync(processes[0].Host, logName(processes[0]), Seconds(task.StartMicros), SecondsUp(task.EndMicros), ct);
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: 8 << 20, resumeWriterThreshold: 4 << 20));
        var producer = Task.Run(async () =>
        {
            try
            {
                var sink = new RowSink(pipe.Writer);
                var opened = first;
                foreach (var process in processes)
                {
                    var processor = new MongoLogProcessor(stream, process.Host, process.Type, task.StartMicros, task.EndMicros, sink);
                    await ReadAsync(process, logName(process), task, processor, pipe.Writer, cts.Token, opened);
                    opened = null;
                    await pipe.Writer.FlushAsync(cts.Token);
                }
                await pipe.Writer.CompleteAsync();
            }
            catch (Exception ex)
            {
                first.Dispose();
                await pipe.Writer.CompleteAsync(ex);
            }
        }, CancellationToken.None);
        // Quiet by design: a busy cluster with few shapes, or a server log with few slow operations or failed sign-ins,
        // yields few rows while the files are read.
        return new RowStream(pipe.Reader.AsStream(), new Producer(cts, producer)) { KeepAlive = true };
    }

    /// <summary>Downloads one log file of one process for the window and feeds every line to the processor.</summary>
    private async Task ReadAsync(AtlasProcess process, string logName, AgentTask task, MongoLogProcessor processor, PipeWriter? pipe,
        CancellationToken ct, HttpResponseMessage? opened = null)
    {
        using var response = opened ?? await _api.DownloadLogAsync(process.Host, logName, Seconds(task.StartMicros), SecondsUp(task.EndMicros), ct);
        await using var body = await response.Content.ReadAsStreamAsync(ct);
        using var lines = new LogLines(body);
        var sinceFlush = 0;
        var sinceBeat = 0;
        while (await lines.FillAsync(ct))
        {
            while (lines.TryReadLine(out var line))
            {
                processor.Process(line);
                if (++sinceFlush == FlushLines)
                {
                    sinceFlush = 0;
                    processor.FlushClosedMinutes();
                }
                if (++sinceBeat == HeartbeatLines)
                {
                    sinceBeat = 0;
                    TaskHeartbeat.Beat();
                }
            }
            if (pipe != null && pipe.UnflushedBytes > 256 * 1024)
            {
                await pipe.FlushAsync(ct);
            }
        }
        processor.Finish();
        TaskHeartbeat.Beat();
    }

    /// <summary>A log stream's completeness: each node's server log must reach past the window's end.</summary>
    private async Task RequirePublishedAsync(AtlasProcess process, AgentTask task, CancellationToken ct)
    {
        if (_utcNow() - FromMicros(task.EndMicros) > CompletenessGiveUp)
        {
            return;
        }
        var end = Seconds(task.EndMicros);
        using var response = await _api.DownloadLogAsync(process.Host, ServerLog(process), end - 120, end + 60, ct);
        await using var body = await response.Content.ReadAsStreamAsync(ct);
        using var lines = new LogLines(body);
        var newest = long.MinValue;
        while (await lines.FillAsync(ct))
        {
            while (lines.TryReadLine(out var line))
            {
                if (LineScan.TryTimestampMicros(line, audit: false, out var ts) && ts > newest)
                {
                    newest = ts;
                }
            }
        }
        RequirePublished(process, newest, task);
    }

    /// <summary>
    /// Atlas publishes a node's log in chunks, minutes late (41 s to more than 4 minutes measured). A window read
    /// before its last chunk exists would lose those lines for good, because the next window drops them by their
    /// time: so it is reported as not yet published, and Trailox asks again at a later poll.
    /// </summary>
    /// <remarks>
    /// 🔴 A WINDOW BEFORE THE PROCESS EXISTED IS PUBLISHED, AND EMPTY. Its log has no line for that time and never
    ///    will, so without this the window waited out <see cref="CompletenessGiveUp"/>, and Trailox asked for it
    ///    again at every poll, with the windows after it (measured on a new M30, 2026-10-21: two hours, one window
    ///    accepted 9 times). Only when nothing at all is published around the window's end: a node replaced under
    ///    the same host still has the old process's lines, and those wait as before.
    /// ⚠ /processes <c>created</c> comes before the process's first log line (measured on an M10, 2026-10-18:
    ///   created 06:11:04Z, first lines 06:12:46Z and 06:13:09Z), so a window that ends before it has no line.
    /// </remarks>
    private void RequirePublished(AtlasProcess process, long newestMicros, AgentTask task)
    {
        if (newestMicros >= task.EndMicros || _utcNow() - FromMicros(task.EndMicros) > CompletenessGiveUp
            || (newestMicros == long.MinValue && process.CreatedUtc is { } created && FromMicros(task.EndMicros) <= created))
        {
            return;
        }
        var newest = newestMicros == long.MinValue ? "nothing yet" : $"up to {FromMicros(newestMicros):HH:mm:ss}Z";
        throw new SourceException($"the log of {process.Host} is published {newest}, before the window's end {FromMicros(task.EndMicros):HH:mm}Z")
        {
            NotYetPublished = true,
        };
    }

    /// <summary>The cluster's processes whose logs are read: the hosts of its standard connection string, matched to /processes.</summary>
    /// <remarks>
    /// 🔴 /processes lists every cluster of the project, Free ones included (measured). The connection string
    ///    names this cluster's replica-set members, or for a sharded cluster its mongos routers, whose files
    ///    count each operation once (D10).
    /// 🔴 ON A SHARDED CLUSTER, ONLY THE MONGOS PROCESSES. Every host of a sharded cluster runs a mongos
    ///    (SHARD_MONGOS) beside its mongod, under the same host name (measured on an M30, 2026-10-20), so the
    ///    connection string's hosts match both. The mongod audit files of the shards hold the operations again,
    ///    in lines that look exactly like a client's: reading both counted each operation about 2.7 times. A
    ///    replica set has no SHARD_MONGOS process and keeps its members.
    /// </remarks>
    internal async Task<(string Topology, IReadOnlyList<AtlasProcess> Processes)> ProcessesAsync(CancellationToken ct, bool refresh = false)
    {
        if (!refresh && _processes is { } cached && _utcNow() - cached.At < ProcessCacheFor)
        {
            return (cached.Topology, cached.Processes);
        }
        var (topology, processes) = await ClusterProcessesAsync(ct);
        if (processes.Any(p => p.Type == ShardMongos))
        {
            processes = processes.Where(p => p.Type == ShardMongos).ToList();
        }
        _processes = (_utcNow(), topology, processes);
        return (topology, processes);
    }

    /// <summary>
    /// The ids of the cluster's primaries, whose database measurements catalog_databases reads (1.9.0): REPLICA_PRIMARY,
    /// and on a sharded cluster each shard's (SHARD_PRIMARY, SHARD_CONFIG_PRIMARY). Never the routers.
    /// </summary>
    /// <remarks>
    /// 🔴 NOT <see cref="ProcessesAsync"/>: on a sharded cluster that keeps only the mongos routers (1.8.1), which hold no
    ///    data. The shards' mongod processes share the routers' host names, so the same match finds them.
    /// ⚠ Dedicated config servers run no router and are not in the connection string, so they are not found; they hold
    ///   only admin and config, which the catalog leaves out anyway. An embedded config shard is found.
    /// </remarks>
    internal async Task<IReadOnlyList<string>> PrimariesAsync(CancellationToken ct)
    {
        var (_, processes) = await ClusterProcessesAsync(ct);
        var primaries = processes
            .Where(p => p.Type.EndsWith("_PRIMARY", StringComparison.Ordinal) && p.Id.Length > 0)
            .Select(p => p.Id)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToList();
        if (primaries.Count == 0)
        {
            // An election in progress, most likely: Trailox asks for the day's catalog again later.
            throw new SourceException($"Atlas lists no primary of cluster {_endpoint.ClusterName} right now");
        }
        return primaries;
    }

    /// <summary>Every process of the cluster, its routers and its mongod processes alike, the primary first.</summary>
    private async Task<(string Topology, List<AtlasProcess> Processes)> ClusterProcessesAsync(CancellationToken ct)
    {
        string topology;
        HashSet<string> hosts;
        using (var cluster = await ClusterAsync(ct))
        {
            topology = Str(cluster.RootElement, "clusterType");
            var standard = cluster.RootElement.TryGetProperty("connectionStrings", out var cs) && cs.ValueKind == JsonValueKind.Object ? Str(cs, "standard") : "";
            hosts = HostsOf(standard);
        }
        var all = await _api.ListAsync("/processes", "the project's processes", ct);
        var processes = all
            .Select(p => new AtlasProcess { Host = Str(p, "userAlias"), Type = Str(p, "typeName"), Id = Str(p, "id"), CreatedUtc = Created(p) })
            .Where(p => p.Host.Length > 0 && hosts.Contains(p.Host))
            .OrderBy(p => p.Type == "REPLICA_PRIMARY" ? 0 : 1)
            .ThenBy(p => p.Host, StringComparer.Ordinal)
            .ToList();
        if (processes.Count == 0)
        {
            throw new SourceException($"Atlas lists no process of cluster {_endpoint.ClusterName} (its connection string names {hosts.Count} host(s))");
        }
        return (topology, processes);
    }

    /// <summary>
    /// catalog_collections (1.9.0), as the catalog user, on the cluster's SRV connection string from the same cluster
    /// read the probe makes.
    /// </summary>
    /// <remarks>
    /// 🔴 WITHOUT A CATALOG USER THE TASK FAILS AS AN OPTIONAL GRANT, so Trailox still counts the catalog day as swept and the
    ///    agent does not list it among its errors every day: the customer chose not to add one.
    /// </remarks>
    private async Task<ReadOnlyMemory<byte>> CollectionsAsync(CancellationToken ct)
    {
        if (!HasCatalogUser)
        {
            throw new SourceException("not configured: the collection catalog needs the optional catalog user (catalog_username and its "
                + "password in agent.yaml); database sizes still come from the Admin API") { MissingOptionalGrant = true };
        }
        var snapshotDate = Today();
        string standardSrv;
        using (var cluster = await ClusterAsync(ct))
        {
            standardSrv = RequireStandardSrv(StandardSrv(cluster.RootElement));
        }
        using var reads = OpenCatalog(standardSrv);
        return await new MongoCatalog(reads).CollectionsAsync(snapshotDate, ct);
    }

    /// <summary>
    /// The probe's catalog check (1.9.0): ok, warn when the user holds more than it needs (K4), fail when it cannot
    /// connect or sign in. Soft: the logs need no catalog user, so a failing check never fails the probe.
    /// </summary>
    private async Task<AtlasCheck> CatalogCheckAsync(string standardSrv, CancellationToken ct)
    {
        try
        {
            using var reads = OpenCatalog(RequireStandardSrv(standardSrv));
            return await new MongoCatalog(reads).CheckAsync(_endpoint.CatalogUsername, ct);
        }
        catch (Exception ex) when (!TaskExecutor.IsShutdown(ex, ct))
        {
            // The driver's failures arrive as SourceException, worded; anything else is named by its type.
            var message = ex is SourceException ? ex.Message : $"the catalog check failed unexpectedly ({ex.GetType().Name}: {ex.Message})";
            return new AtlasCheck { Name = "catalog", Status = "fail", Message = EndpointSession.Trim(message) };
        }
    }

    private static string StandardSrv(JsonElement cluster) =>
        cluster.TryGetProperty("connectionStrings", out var cs) && cs.ValueKind == JsonValueKind.Object ? Str(cs, "standardSrv") : "";

    private string RequireStandardSrv(string standardSrv) => standardSrv.StartsWith("mongodb+srv://", StringComparison.Ordinal)
        ? standardSrv
        : throw new SourceException($"Atlas gives cluster {_endpoint.ClusterName} no SRV connection string (connectionStrings.standardSrv)");

    /// <summary>"mongodb://h1:27017,h2:27017/?ssl=true..." → {h1, h2}, lower case.</summary>
    internal static HashSet<string> HostsOf(string standard)
    {
        var hosts = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var at = standard.IndexOf("://", StringComparison.Ordinal);
        if (at < 0)
        {
            return hosts;
        }
        var rest = standard[(at + 3)..];
        var end = rest.IndexOfAny(['/', '?']);
        foreach (var part in (end < 0 ? rest : rest[..end]).Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            var colon = part.LastIndexOf(':');
            hosts.Add((colon > 0 ? part[..colon] : part).Trim().ToLowerInvariant());
        }
        return hosts;
    }

    /// <remarks>
    /// 🔴 A FLEX CLUSTER NEVER REACHES <see cref="IsShared"/>: /clusters/{name} refuses it with 400
    ///    CANNOT_USE_FLEX_CLUSTER_IN_CLUSTER_API (measured 2026-10-21; only /flexClusters/{name}, on a newer API
    ///    version, describes it). The refusal is the same D2 sentence the other shared tiers get.
    /// </remarks>
    private async Task<JsonDocument> ClusterAsync(CancellationToken ct)
    {
        try
        {
            return await _api.GetJsonAsync($"/clusters/{Uri.EscapeDataString(_endpoint.ClusterName)}", $"cluster {_endpoint.ClusterName}", ct);
        }
        catch (AtlasException ex) when (ex.IsNotFound && ex.ErrorCode != "GROUP_NOT_FOUND")
        {
            throw new SourceException($"the project has no dedicated cluster named {_endpoint.ClusterName} (clusterName is the cluster's name in Atlas)");
        }
        catch (AtlasException ex) when (ex.ErrorCode == "CANNOT_USE_FLEX_CLUSTER_IN_CLUSTER_API")
        {
            throw NotDedicated("FLEX");
        }
    }

    /// <summary>D2's refusal of a cluster that keeps no downloadable logs; <paramref name="kind"/> is its tier or provider.</summary>
    private SourceException NotDedicated(string kind) =>
        new($"cluster {_endpoint.ClusterName} is {kind}: Trailox reads MongoDB Atlas through its log files, which Atlas keeps only for dedicated clusters (M10 or larger)");

    private async Task<AtlasCheck> SoftCheckAsync(string name, string path, string ok, CancellationToken ct)
    {
        try
        {
            using (await _api.GetJsonAsync(path, name, ct))
            {
            }
            return Ok(name, ok);
        }
        catch (SourceException ex)
        {
            return new AtlasCheck { Name = name, Status = "fail", Message = EndpointSession.Trim(ex.Message) };
        }
    }

    /// <summary>D2: Free (M0, and the retired M2/M5 shared tiers), Flex and Serverless keep no downloadable logs.</summary>
    internal static bool IsShared(string tier, string providerName) =>
        tier is "M0" or "M2" or "M5" || tier.StartsWith("FLEX", StringComparison.OrdinalIgnoreCase)
        || providerName is "TENANT" or "FLEX" or "SERVERLESS";

    /// <summary>A minute aggregate must be whole: a window that cut a minute in two would ship two halves under one key.</summary>
    internal static void RequireWholeMinutes(AgentTask task)
    {
        if (task.EndMicros <= task.StartMicros || task.StartMicros % MinuteAggregator.MinuteMicros != 0 || task.EndMicros % MinuteAggregator.MinuteMicros != 0)
        {
            throw new SourceException($"MongoDB windows must start and end on whole minutes (got {task.StartMicros}..{task.EndMicros} µs)");
        }
    }

    /// <summary>The typeName of a sharded cluster's router.</summary>
    private const string ShardMongos = "SHARD_MONGOS";

    private static string ServerLog(AtlasProcess p) => p.Type == ShardMongos ? "mongos" : "mongodb";

    private static string AuditLog(AtlasProcess p) => p.Type == ShardMongos ? "mongos-audit-log" : "mongodb-audit-log";

    private static AtlasCheck Ok(string name, string message) => new() { Name = name, Status = "ok", Message = message };

    private static string Str(JsonElement e, string name) =>
        e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() ?? "" : "";

    /// <summary>A process's <c>created</c> ("2026-10-21T06:55:48Z"), in UTC; null when Atlas leaves it out or it does not parse.</summary>
    private static DateTime? Created(JsonElement p) =>
        DateTime.TryParse(Str(p, "created"), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var created)
            ? created
            : null;

    private string Today() => _utcNow().ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);

    private static long Micros(DateTime utc) => new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).ToUnixTimeMilliseconds() * 1000;

    private static DateTime FromMicros(long micros) => DateTime.UnixEpoch.AddTicks(micros * 10);

    private static long Seconds(long micros) => micros / 1_000_000;

    private static long SecondsUp(long micros) => (micros + 999_999) / 1_000_000;

    private static RowStream Buffered(ReadOnlyMemory<byte> rows)
    {
        var body = new MemoryStream(rows.ToArray(), writable: false);
        return new RowStream(body, body);
    }

    /// <summary>Stops the producer when the upload is done or abandoned.</summary>
    private sealed class Producer : IDisposable
    {
        private readonly CancellationTokenSource _cts;
        private readonly Task _task;

        public Producer(CancellationTokenSource cts, Task task)
        {
            _cts = cts;
            _task = task;
        }

        public void Dispose()
        {
            _cts.Cancel();
            _ = _task.ContinueWith(t => _ = t.Exception, TaskScheduler.Default);
            _cts.Dispose();
        }
    }
}
