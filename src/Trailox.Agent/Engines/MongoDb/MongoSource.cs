using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;
using Trailox.Agent.Config;
using Trailox.Agent.Protocol;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Engines.MongoDb;

/// <summary>
/// One MongoDB Atlas cluster, read through the Admin API as a service account: the probe checks the
/// account, the cluster and its tier, and each task downloads the cluster's log files for its window and
/// ships per-minute rows (agent 1.8). Nothing here connects to the cluster itself.
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
    }

    public async Task<EndpointCaps> ProbeAsync(CancellationToken ct)
    {
        var checks = new List<AtlasCheck>();
        await _api.TokenAsync(ct);
        checks.Add(Ok("authenticated", "the service account's token was issued"));

        string version, tier, topology;
        using (var cluster = await ClusterAsync(ct))
        {
            var root = cluster.RootElement;
            version = Str(root, "mongoDBVersion");
            topology = Str(root, "clusterType");
            var provider = root.TryGetProperty("providerSettings", out var p) ? p : default;
            tier = provider.ValueKind == JsonValueKind.Object ? Str(provider, "instanceSizeName") : "";
            var providerName = provider.ValueKind == JsonValueKind.Object ? Str(provider, "providerName") : "";
            if (IsShared(tier, providerName))
            {
                // D2: a Free or Flex cluster answers the API and keeps no downloadable logs. Without this it would
                // "connect", collect nothing, and look like a fault of ours.
                throw new SourceException($"cluster {_endpoint.ClusterName} is {(tier.Length > 0 ? tier : providerName)}: Trailox reads MongoDB Atlas through its log files, which Atlas keeps only for dedicated clusters (M10 or larger)");
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
            default:
                return null;
        }
    }

    /// <summary>
    /// The server-log streams: small files (about 5 MB an hour a node under load), read whole into memory, so
    /// a window whose files are not published yet is reported as such before any byte is uploaded.
    /// </summary>
    private async Task<RowStream> ServerLogAsync(MongoLogStream stream, AgentTask task, CancellationToken ct)
    {
        RequireWholeMinutes(task);
        var (_, processes) = await ProcessesAsync(ct);
        var rows = new ArrayBufferWriter<byte>(64 * 1024);
        var sink = new RowSink(rows);
        foreach (var process in processes)
        {
            var processor = new MongoLogProcessor(stream, process.Host, process.Type, task.StartMicros, task.EndMicros, sink);
            await ReadAsync(process, ServerLog(process), task, processor, null, ct);
            RequirePublished(process, processor.Counts.MaxTsMicros, task);
        }
        return Buffered(rows.WrittenMemory);
    }

    /// <summary>
    /// mongo_ops: the audit files, one line an operation at Full audit, so this one streams: each minute is
    /// shipped as soon as the file is past it, through a pipe the upload drains. The windows' completeness is
    /// checked first, on each node's small server log.
    /// </summary>
    private async Task<RowStream> OpsAsync(AgentTask task, CancellationToken ct)
    {
        RequireWholeMinutes(task);
        var (_, processes) = await ProcessesAsync(ct);
        foreach (var process in processes)
        {
            await RequirePublishedAsync(process, task, ct);
        }

        // The first download is opened here, so that a refusal surfaces as the task's failure before the upload.
        var first = await _api.DownloadLogAsync(processes[0].Host, AuditLog(processes[0]), Seconds(task.StartMicros), SecondsUp(task.EndMicros), ct);
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
                    var processor = new MongoLogProcessor(MongoLogStream.Ops, process.Host, process.Type, task.StartMicros, task.EndMicros, sink);
                    await ReadAsync(process, AuditLog(process), task, processor, pipe.Writer, cts.Token, opened);
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
        return new RowStream(pipe.Reader.AsStream(), new Producer(cts, producer));
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

    /// <summary>The audit stream's completeness: each node's server log must reach past the window's end.</summary>
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
    private void RequirePublished(AtlasProcess process, long newestMicros, AgentTask task)
    {
        if (newestMicros >= task.EndMicros || _utcNow() - FromMicros(task.EndMicros) > CompletenessGiveUp)
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
    /// </remarks>
    internal async Task<(string Topology, IReadOnlyList<AtlasProcess> Processes)> ProcessesAsync(CancellationToken ct, bool refresh = false)
    {
        if (!refresh && _processes is { } cached && _utcNow() - cached.At < ProcessCacheFor)
        {
            return (cached.Topology, cached.Processes);
        }
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
            .Select(p => new AtlasProcess { Host = Str(p, "userAlias"), Type = Str(p, "typeName") })
            .Where(p => p.Host.Length > 0 && hosts.Contains(p.Host))
            .OrderBy(p => p.Type == "REPLICA_PRIMARY" ? 0 : 1)
            .ThenBy(p => p.Host, StringComparer.Ordinal)
            .ToList();
        if (processes.Count == 0)
        {
            throw new SourceException($"Atlas lists no process of cluster {_endpoint.ClusterName} (its connection string names {hosts.Count} host(s))");
        }
        _processes = (_utcNow(), topology, processes);
        return (topology, processes);
    }

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
    }

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

    private static string ServerLog(AtlasProcess p) => p.Type == "SHARD_MONGOS" ? "mongos" : "mongodb";

    private static string AuditLog(AtlasProcess p) => p.Type == "SHARD_MONGOS" ? "mongos-audit-log" : "mongodb-audit-log";

    private static AtlasCheck Ok(string name, string message) => new() { Name = name, Status = "ok", Message = message };

    private static string Str(JsonElement e, string name) =>
        e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() ?? "" : "";

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
