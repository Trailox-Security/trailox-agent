using System.IO.Compression;
using System.Net;
using System.Text;
using System.Text.Json;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.MongoDb;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests.MongoDb;

/// <summary>
/// The MongoDB source against a stand-in for the Atlas Admin API that answers as the real one did on
/// 2026-10-19: the cluster, the project's processes (another cluster's included), gzip log files that run
/// past the asked window, and the measured error answers.
/// </summary>
public class MongoSourceTests
{
    private const string Project = "0123456789abcdef01234567";
    private const string ClientId = "mdb_sa_id_00000000000000000000aaaa";
    private static readonly DateTime Now = new(2026, 10, 19, 12, 0, 0, DateTimeKind.Utc);
    private static readonly string[] Hosts = ["cluster0-shard-00-00.example.mongodb.net", "cluster0-shard-00-01.example.mongodb.net", "cluster0-shard-00-02.example.mongodb.net"];

    private sealed class FakeAtlas : HttpMessageHandler
    {
        public string Tier { get; set; } = "M10";
        public string ProviderName { get; set; } = "AWS";
        public Func<string, string, string[]> Log { get; set; } = (_, _) => Array.Empty<string>();
        public Func<HttpRequestMessage, HttpResponseMessage?> Override { get; set; } = _ => null;
        public List<string> Paths { get; } = new();
        public int TokenRequests { get; private set; }
        public string Events { get; set; } = """{"results":[],"totalCount":0}""";

        /// <summary>The members' <c>created</c> in /processes, as Atlas gives it ("2026-10-21T06:55:48Z"); null leaves it out.</summary>
        public string? Created { get; set; }

        /// <summary>
        /// A sharded cluster as Atlas runs one with its default embedded config servers (measured on an M30,
        /// 2026-10-20): every host runs a mongos (SHARD_MONGOS, port 27016) beside its mongod (27017), under the same
        /// host name, and the standard connection string names the routers.
        /// </summary>
        public bool Sharded { get; set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var path = request.RequestUri!.PathAndQuery;
            Paths.Add(path);
            if (Override(request) is { } overridden)
            {
                return overridden;
            }
            if (path == "/api/oauth/token")
            {
                TokenRequests++;
                Assert.Equal("Basic", request.Headers.Authorization!.Scheme);
                return Json("""{"access_token":"not-a-real-token","expires_in":3600,"token_type":"Bearer"}""");
            }
            Assert.Equal("not-a-real-token", request.Headers.Authorization!.Parameter);
            var prefix = $"/api/atlas/v2/groups/{Project}";
            Assert.StartsWith(prefix, path, StringComparison.Ordinal);
            var rest = path[prefix.Length..];
            if (Sharded && rest.StartsWith("/clusters/Cluster0", StringComparison.Ordinal) && !rest.Contains("/logs/", StringComparison.Ordinal))
            {
                return Json("""{"name":"Cluster0","clusterType":"SHARDED","paused":false,"mongoDBVersion":"8.0.34","connectionStrings":{"standard":"mongodb://@0@:27016,@1@:27016,@2@:27016/?ssl=true&authSource=admin"},"providerSettings":{"providerName":"AWS","instanceSizeName":"M30"}}""".Replace("@0@", Hosts[0]).Replace("@1@", Hosts[1]).Replace("@2@", Hosts[2]));
            }
            if (Sharded && rest.StartsWith("/processes", StringComparison.Ordinal))
            {
                var mongod = new[] { "SHARD_CONFIG_PRIMARY", "REPLICA_PRIMARY", "REPLICA_SECONDARY" };
                var routers = Hosts.Select(h => """{"userAlias":"@0@","port":27016,"typeName":"SHARD_MONGOS"}""".Replace("@0@", h));
                var shards = Hosts.Select((h, i) => """{"userAlias":"@0@","port":27017,"typeName":"@1@"}""".Replace("@0@", h).Replace("@1@", mongod[i]));
                return Json("""{"results":[@0@],"totalCount":6}""".Replace("@0@", string.Join(",", shards.Concat(routers))));
            }
            if (rest.StartsWith("/clusters/Cluster0", StringComparison.Ordinal) && !rest.Contains("/logs/", StringComparison.Ordinal))
            {
                return Json("""{"name":"Cluster0","clusterType":"REPLICASET","paused":false,"mongoDBVersion":"8.0.34","connectionStrings":{"standard":"mongodb://@0@:27017,@1@:27017,@2@:27017/?ssl=true&authSource=admin&replicaSet=atlas-abcdef-shard-0"},"providerSettings":{"providerName":"@3@","instanceSizeName":"@4@"}}""".Replace("@0@", (Hosts[0]).ToString()).Replace("@1@", (Hosts[1]).ToString()).Replace("@2@", (Hosts[2]).ToString()).Replace("@3@", (ProviderName).ToString()).Replace("@4@", (Tier).ToString()));
            }
            if (rest.StartsWith("/processes", StringComparison.Ordinal))
            {
                var created = Created is null ? "" : $",\"created\":\"{Created}\"";
                var mine = Hosts.Select((h, i) => """{"userAlias":"@0@","hostname":"atlas-abcdef-shard-00-0@1@.example.mongodb.net","typeName":"@2@"@3@}""".Replace("@0@", (h).ToString()).Replace("@1@", (i).ToString()).Replace("@2@", ((i == 2 ? "REPLICA_PRIMARY" : "REPLICA_SECONDARY")).ToString()).Replace("@3@", created));
                var other = """{"userAlias":"ac-zzzz-shard-00-00.other.mongodb.net","hostname":"atlas-zzzz-shard-00-00.other.mongodb.net","typeName":"REPLICA_PRIMARY"}""";
                return Json("""{"results":[@0@],"totalCount":4}""".Replace("@0@", (string.Join(",", mine.Append(other))).ToString()));
            }
            if (rest.Contains("/logs/", StringComparison.Ordinal))
            {
                Assert.Equal("application/vnd.atlas.2023-02-01+gzip", request.Headers.Accept.Single().MediaType);
                var host = rest.Split('/')[2];
                var name = rest.Split('/')[4].Split('.')[0];
                return new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(Gzip(Log(host, name))) };
            }
            if (rest.StartsWith("/events", StringComparison.Ordinal))
            {
                return Json(Events);
            }
            if (rest.StartsWith("/databaseUsers", StringComparison.Ordinal))
            {
                return Json("""{"results":[{"username":"app_orders","databaseName":"admin","roles":[{"databaseName":"shop","roleName":"readWrite"}],"scopes":[],"awsIAMType":"NONE","ldapAuthType":"NONE","oidcAuthType":"NONE","x509Type":"NONE"}],"totalCount":1}""");
            }
            await Task.CompletedTask;
            return new HttpResponseMessage(HttpStatusCode.NotFound) { Content = new StringContent("""{"errorCode":"RESOURCE_NOT_FOUND"}""") };
        }

        public static HttpResponseMessage Json(string body, HttpStatusCode status = HttpStatusCode.OK) =>
            new(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
    }

    private static byte[] Gzip(IEnumerable<string> lines)
    {
        using var buffer = new MemoryStream();
        using (var gz = new GZipStream(buffer, CompressionLevel.Fastest))
        {
            var text = Encoding.UTF8.GetBytes(string.Concat(lines.Select(l => l + "\n")));
            gz.Write(text);
        }
        return buffer.ToArray();
    }

    private static (MongoSource Source, FakeAtlas Atlas) Source(DateTime? now = null)
    {
        var atlas = new FakeAtlas();
        var endpoint = new EndpointConfig { Alias = "atlas", Engine = "mongodb", Host = Project, ClusterName = "Cluster0", Username = ClientId };
        endpoint.Password = "not-a-real-secret";
        return (new MongoSource(new HttpClient(atlas), endpoint, "https://atlas.example.test", () => now ?? Now), atlas);
    }

    private static long Micros(DateTime utc) => (utc - DateTime.UnixEpoch).Ticks / 10;

    private static string ServerLine(DateTime t) =>
        """{"t":{"$date":"@0@+00:00"},"s":"I",  "c":"NETWORK",  "id":6723804, "ctx":"conn1","msg":"Ingress TLS handshake complete","attr":{"durationMillis":27}}""".Replace("@0@", t.ToString("yyyy-MM-dd'T'HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture));

    private static string AuditLine(DateTime t, string user) =>
        """{ "atype" : "authCheck", "ts" : { "$date" : "@0@+00:00" }, "uuid" : { "$binary" : "zk70v4pUbutxtzTYyRHMog==", "$type" : "04" }, "remote" : { "ip" : "10.1.0.5", "port" : 1 }, "users" : [ { "user" : "@1@", "db" : "admin" } ], "param" : { "command" : "find", "ns" : "shop.orders", "args" : { "find" : "orders", "filter" : { "customer" : 1 } } }, "result" : 0 }""".Replace("@0@", t.ToString("yyyy-MM-dd'T'HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture)).Replace("@1@", (user).ToString());

    private static AgentTask Window(string stream, DateTime start, DateTime end) =>
        new() { ChunkId = "c1", EndpointId = 1, Stream = stream, StartMicros = Micros(start), EndMicros = Micros(end) };

    private static async Task<List<JsonElement>> RowsOf(RowStream rows)
    {
        await using (rows)
        {
            using var reader = new StreamReader(rows.Body);
            var text = await reader.ReadToEndAsync();
            return text.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(l => JsonDocument.Parse(l).RootElement.Clone()).ToList();
        }
    }

    [Fact]
    public async Task The_probe_reports_the_clusters_processes_and_reuses_one_token()
    {
        var (source, atlas) = Source();

        var caps = await source.ProbeAsync(CancellationToken.None);

        Assert.Equal("8.0.34", caps.Version);
        Assert.NotNull(caps.Atlas);
        Assert.Equal("M10", caps.Atlas!.Tier);
        Assert.Equal("REPLICASET", caps.Atlas.Topology);
        Assert.Equal(3, caps.Atlas.Processes.Count);
        Assert.Equal(Hosts[2], caps.Atlas.Processes[0].Host);
        Assert.DoesNotContain(caps.Atlas.Processes, p => p.Host.Contains("other", StringComparison.Ordinal));
        Assert.All(caps.Atlas.Checks, c => Assert.Equal("ok", c.Status));
        Assert.Equal(1, atlas.TokenRequests);
        Assert.Equal(Micros(Now - TimeSpan.FromDays(30)), caps.EarliestEventMicros);
    }

    [Theory]
    [InlineData("M0", "TENANT")]
    [InlineData("FLEX", "FLEX")]
    public async Task A_free_or_flex_cluster_is_refused_with_the_requirement(string tier, string provider)
    {
        var (source, atlas) = Source();
        atlas.Tier = tier;
        atlas.ProviderName = provider;

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.ProbeAsync(CancellationToken.None));

        Assert.Contains("M10 or larger", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// 🔴 Atlas's cluster read refuses a Flex cluster outright (measured 2026-10-21), so the probe never sees its
    ///    provider. The customer gets the same requirement, not Atlas's 400.
    /// </summary>
    [Fact]
    public async Task A_flex_cluster_refused_by_the_cluster_api_gets_the_requirement()
    {
        var (source, atlas) = Source();
        atlas.Override = r => r.RequestUri!.AbsolutePath == $"/api/atlas/v2/groups/{Project}/clusters/Cluster0"
            ? FakeAtlas.Json("""{"detail":"Flex cluster Cluster0 cannot be used in the Cluster API.","error":400,"errorCode":"CANNOT_USE_FLEX_CLUSTER_IN_CLUSTER_API","parameters":["Cluster0"],"reason":"Bad Request"}""", HttpStatusCode.BadRequest)
            : null;

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.ProbeAsync(CancellationToken.None));

        Assert.Equal("cluster Cluster0 is FLEX: Trailox reads MongoDB Atlas through its log files, which Atlas keeps only for dedicated clusters (M10 or larger)", ex.Message);
    }

    [Fact]
    public async Task Mongo_ops_reads_the_audit_file_of_each_member_and_keeps_only_its_window()
    {
        var start = new DateTime(2026, 10, 19, 9, 0, 0, DateTimeKind.Utc);
        var (source, atlas) = Source();
        atlas.Log = (host, name) => name == "mongodb"
            ? [ServerLine(start.AddMinutes(12))]
            : [AuditLine(start.AddMinutes(-3), "before"), AuditLine(start.AddSeconds(5), "app_orders"), AuditLine(start.AddSeconds(6), "app_orders"), AuditLine(start.AddMinutes(10), "after")];

        var rows = await RowsOf((await source.OpenAsync(Window(ProtocolInfo.Streams.MongoOps, start, start.AddMinutes(10)), CancellationToken.None))!);

        Assert.Equal(3, rows.Count);
        Assert.All(rows, r => Assert.Equal("app_orders", r.GetProperty("user").GetString()));
        Assert.All(rows, r => Assert.Equal(2, r.GetProperty("event_count").GetInt64()));
        Assert.Equal(Hosts.OrderBy(h => h == Hosts[2] ? 0 : 1).ThenBy(h => h, StringComparer.Ordinal), rows.Select(r => r.GetProperty("process").GetString()));
        Assert.DoesNotContain(atlas.Paths, p => p.Contains("other.mongodb.net", StringComparison.Ordinal));
    }

    /// <summary>
    /// 🔴 A sharded cluster is read from its mongos routers only. On an M30 (2026-10-20) the mongos audit files held each
    /// client operation once; the shard primaries' mongod audit files held them again, in lines that look exactly like a
    /// client's. Before 1.8.1 the agent matched processes by host name, which the mongos and the mongod share, so it read
    /// both and counted each operation about 2.7 times.
    /// </summary>
    [Fact]
    public async Task A_sharded_cluster_is_read_from_its_mongos_routers_only()
    {
        var start = new DateTime(2026, 10, 19, 9, 0, 0, DateTimeKind.Utc);
        var (source, atlas) = Source();
        atlas.Sharded = true;
        atlas.Log = (_, name) => name is "mongos" or "mongodb"
            ? [ServerLine(start.AddMinutes(12))]
            : [AuditLine(start.AddSeconds(5), "app_orders")];

        var caps = await source.ProbeAsync(CancellationToken.None);
        var rows = await RowsOf((await source.OpenAsync(Window(ProtocolInfo.Streams.MongoOps, start, start.AddMinutes(10)), CancellationToken.None))!);

        Assert.Equal(3, caps.Atlas!.Processes.Count);
        Assert.All(caps.Atlas.Processes, p => Assert.Equal("SHARD_MONGOS", p.Type));
        // One line in each router's file, each counted once.
        Assert.Equal(3, rows.Sum(r => r.GetProperty("event_count").GetInt64()));
        Assert.Equal(3, atlas.Paths.Count(p => p.Contains("/logs/mongos-audit-log", StringComparison.Ordinal)));
        Assert.DoesNotContain(atlas.Paths, p => p.Contains("/logs/mongodb-audit-log", StringComparison.Ordinal));
        Assert.DoesNotContain(atlas.Paths, p => p.Contains("/logs/mongodb.gz", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Only_the_streamed_log_uploads_ask_for_keep_alive()
    {
        var start = new DateTime(2026, 10, 19, 9, 0, 0, DateTimeKind.Utc);
        var (source, atlas) = Source();
        atlas.Log = (_, name) => name == "mongodb" ? [ServerLine(start.AddMinutes(12))] : [AuditLine(start.AddSeconds(5), "app_orders")];

        var ops = (await source.OpenAsync(Window(ProtocolInfo.Streams.MongoOps, start, start.AddMinutes(10)), CancellationToken.None))!;
        var connections = (await source.OpenAsync(Window(ProtocolInfo.Streams.MongoConnections, start, start.AddMinutes(10)), CancellationToken.None))!;
        var users = (await source.OpenAsync(new AgentTask { ChunkId = "c", Stream = ProtocolInfo.Streams.CatalogUsers }, CancellationToken.None))!;

        // The log rows stream while the files are read; the catalog is read whole first.
        Assert.True(ops.KeepAlive);
        Assert.True(connections.KeepAlive);
        Assert.False(users.KeepAlive);
        Assert.Equal(3, (await RowsOf(ops)).Count);
        Assert.Empty(await RowsOf(connections));
        Assert.Single(await RowsOf(users));
    }

    /// <summary>
    /// 🔴 A server-log stream checks every node's completeness before it opens a window's file (from 1.8.2), as
    ///    mongo_ops does: its rows now stream, so an unpublished window must be found before the upload starts.
    /// </summary>
    [Theory]
    [InlineData(ProtocolInfo.Streams.MongoSlowOps)]
    [InlineData(ProtocolInfo.Streams.MongoConnections)]
    [InlineData(ProtocolInfo.Streams.MongoSigninFailures)]
    public async Task A_server_log_stream_checks_completeness_before_it_reads_the_window(string stream)
    {
        var start = new DateTime(2026, 10, 19, 11, 40, 0, DateTimeKind.Utc);
        // A young window: past CompletenessGiveUp no check is made.
        var (source, atlas) = Source(now: start.AddMinutes(15));
        atlas.Log = (_, _) => [ServerLine(start.AddMinutes(12))];
        var end = new DateTimeOffset(start.AddMinutes(10)).ToUnixTimeSeconds();

        await RowsOf((await source.OpenAsync(Window(stream, start, start.AddMinutes(10)), CancellationToken.None))!);

        var logs = atlas.Paths.Where(p => p.Contains("/logs/mongodb.gz", StringComparison.Ordinal)).ToList();
        Assert.Equal(6, logs.Count);
        Assert.All(logs.Take(3), p => Assert.Contains($"startDate={end - 120}&", p, StringComparison.Ordinal));
        Assert.All(logs.Skip(3), p => Assert.Contains($"startDate={end - 600}&", p, StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_window_that_cuts_a_minute_is_refused()
    {
        var (source, _) = Source();
        var start = new DateTime(2026, 10, 19, 9, 0, 30, DateTimeKind.Utc);

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Window(ProtocolInfo.Streams.MongoSlowOps, start, start.AddMinutes(10)), CancellationToken.None));

        Assert.Contains("whole minutes", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_window_whose_logs_are_not_published_yet_is_reported_as_such()
    {
        var start = new DateTime(2026, 10, 19, 11, 40, 0, DateTimeKind.Utc);
        var (source, atlas) = Source(now: start.AddMinutes(12));
        // The newest published line is before the window's end.
        atlas.Log = (_, _) => [ServerLine(start.AddMinutes(8))];

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Window(ProtocolInfo.Streams.MongoConnections, start, start.AddMinutes(10)), CancellationToken.None));

        Assert.True(ex.NotYetPublished);
        Assert.Contains("before the window's end", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// 🔴 A window that ends before the process existed is published, and empty. Measured on a new M30 (2026-10-21): its
    /// source's backfill reached back before the cluster, those windows' logs had no line at all, and each one waited
    /// two hours as "not yet published" while Trailox asked for it, and the windows after it, at every poll.
    /// </summary>
    [Theory]
    [InlineData(ProtocolInfo.Streams.MongoConnections)]
    [InlineData(ProtocolInfo.Streams.MongoOps)]
    public async Task A_window_before_the_process_existed_is_published_and_empty(string stream)
    {
        var start = new DateTime(2026, 10, 19, 10, 0, 0, DateTimeKind.Utc);
        var (source, atlas) = Source(now: start.AddMinutes(60));
        atlas.Created = "2026-10-19T10:30:00Z";
        // Atlas answers a time before the process's first line with an empty file.
        atlas.Log = (_, _) => [];

        var rows = await RowsOf((await source.OpenAsync(Window(stream, start, start.AddMinutes(10)), CancellationToken.None))!);

        Assert.Empty(rows);
    }

    [Fact]
    public async Task A_live_process_whose_log_is_late_still_waits_whatever_its_creation()
    {
        var start = new DateTime(2026, 10, 19, 11, 40, 0, DateTimeKind.Utc);
        var (source, atlas) = Source(now: start.AddMinutes(12));
        atlas.Created = "2026-10-18T06:11:04Z";
        atlas.Log = (_, _) => [];

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Window(ProtocolInfo.Streams.MongoOps, start, start.AddMinutes(10)), CancellationToken.None));

        Assert.True(ex.NotYetPublished);
        Assert.Contains("nothing yet", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>A node replaced under the same host: the old process's lines are there, so the window waits for the rest of them.</summary>
    [Fact]
    public async Task A_window_before_the_creation_that_has_lines_still_waits_for_its_end()
    {
        var start = new DateTime(2026, 10, 19, 10, 0, 0, DateTimeKind.Utc);
        var (source, atlas) = Source(now: start.AddMinutes(14));
        atlas.Created = "2026-10-19T10:12:00Z";
        atlas.Log = (_, _) => [ServerLine(start.AddMinutes(8))];

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Window(ProtocolInfo.Streams.MongoSlowOps, start, start.AddMinutes(10)), CancellationToken.None));

        Assert.True(ex.NotYetPublished);
    }

    [Fact]
    public async Task Without_a_creation_time_an_empty_window_waits_as_before()
    {
        var start = new DateTime(2026, 10, 19, 10, 0, 0, DateTimeKind.Utc);
        var (source, atlas) = Source(now: start.AddMinutes(60));
        atlas.Log = (_, _) => [];

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Window(ProtocolInfo.Streams.MongoConnections, start, start.AddMinutes(10)), CancellationToken.None));

        Assert.True(ex.NotYetPublished);
    }

    [Fact]
    public async Task The_creation_time_is_not_sent_to_Trailox()
    {
        var (source, atlas) = Source();
        atlas.Created = "2026-10-18T06:11:04Z";

        var caps = await source.ProbeAsync(CancellationToken.None);
        var json = JsonSerializer.Serialize(caps, ProtocolInfo.Json);

        Assert.Equal(new DateTime(2026, 10, 18, 6, 11, 4, DateTimeKind.Utc), caps.Atlas!.Processes[0].CreatedUtc);
        Assert.Equal(DateTimeKind.Utc, caps.Atlas.Processes[0].CreatedUtc!.Value.Kind);
        Assert.DoesNotContain("created", json, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task An_old_window_is_read_as_it_is_even_if_a_node_stopped_writing()
    {
        var start = new DateTime(2026, 10, 19, 6, 0, 0, DateTimeKind.Utc);
        var (source, atlas) = Source(now: start.AddHours(5));
        atlas.Log = (_, _) => [ServerLine(start.AddMinutes(2))];

        var rows = await RowsOf((await source.OpenAsync(Window(ProtocolInfo.Streams.MongoConnections, start, start.AddMinutes(10)), CancellationToken.None))!);

        Assert.Empty(rows);
    }

    [Fact]
    public async Task The_agents_own_log_downloads_are_not_shipped_as_project_events()
    {
        var start = new DateTime(2026, 10, 19, 9, 0, 0, DateTimeKind.Utc);
        var (source, atlas) = Source();
        atlas.Events = """
            {"results":[
              {"id":"e1","created":"2026-10-19T09:01:00Z","eventTypeName":"MONGODB_LOGS_DOWNLOADED","username":"@0@","links":[]},
              {"id":"e2","created":"2026-10-19T09:02:00Z","eventTypeName":"SERVICE_ACCOUNT_ACCESS_LIST_ENTRY_ADDED","username":"admin@example.com","clientId":"@1@","remoteAddress":"203.0.113.9","links":[]},
              {"id":"e3","created":"2026-10-19T09:10:00Z","eventTypeName":"MONGODB_USER_ADDED","username":"admin@example.com","links":[]},
              {"id":"e4","created":"2026-10-19T09:03:00Z","eventTypeName":"MONGODB_USER_ADDED","username":"@0@","links":[]}
            ],"totalCount":4}
            """.Replace("@0@", (ClientId).ToString()).Replace("@1@", (ClientId).ToString());

        var rows = await RowsOf((await source.OpenAsync(Window(ProtocolInfo.Streams.AtlasEvents, start, start.AddMinutes(10)), CancellationToken.None))!);

        // e1 is the agent's own log download; e3 is outside the window. e4 is a change made WITH the agent's
        // service account, which must be shipped: a stolen or over-privileged secret is what Trailox shows.
        Assert.Equal(new[] { "e2", "e4" }, rows.Select(r => r.GetProperty("event_id").GetString()));
        Assert.Equal("admin@example.com", rows[0].GetProperty("username").GetString());
        Assert.Equal(ClientId, rows[1].GetProperty("username").GetString());
        Assert.DoesNotContain("\"links\"", rows[0].GetProperty("raw").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Every_catalog_row_carries_its_snapshot_date()
    {
        var (source, _) = Source();

        var rows = await RowsOf((await source.OpenAsync(new AgentTask { ChunkId = "c", Stream = ProtocolInfo.Streams.CatalogUsers }, CancellationToken.None))!);

        var row = Assert.Single(rows);
        Assert.Equal("2026-10-19", row.GetProperty("snapshot_date").GetString());
        Assert.Equal("app_orders", row.GetProperty("username").GetString());
        Assert.Contains("readWrite", row.GetProperty("roles").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_stream_the_engine_does_not_make_is_null()
    {
        var (source, _) = Source();

        Assert.Null(await source.OpenAsync(new AgentTask { Stream = ProtocolInfo.Streams.Events }, CancellationToken.None));
    }

    [Theory]
    [InlineData(401, """{"error":"invalid_client","error_description":"The provided credentials are not valid"}""", "rejected the service account's credentials")]
    [InlineData(401, """{"detail":"Current user is not authorized to perform this action.","error":401,"errorCode":"USER_UNAUTHORIZED"}""", "Project Read Only and Project Cluster Log Viewer")]
    [InlineData(404, """{"detail":"No group with ID exists.","error":404,"errorCode":"GROUP_NOT_FOUND"}""", "no project with the id in host")]
    // Measured 2026-10-21: a real project the service account is not a member of is this 401, not the 404.
    [InlineData(401, """{"detail":"User cannot access this group.","error":401,"errorCode":"USER_CANNOT_ACCESS_GROUP","parameters":[],"reason":"Unauthorized"}""", "the service account is not a member of the project in host: add it to that project with Project Read Only and Project Cluster Log Viewer")]
    // Measured 2026-10-20: Atlas checks the service account's own access list even when the organization does not
    // require one. The answer names the refused address, and the sentence says which list it belongs on.
    [InlineData(403, """{"detail":"IP address 203.0.113.7 is not allowed to access this resource.","error":403,"errorCode":"IP_ADDRESS_NOT_ON_ACCESS_LIST","parameters":["203.0.113.7"],"reason":"Forbidden"}""", "add the agent's outbound address to the service account's API access list")]
    [InlineData(403, """{"detail":"IP address 203.0.113.7 is not allowed to access this resource.","error":403,"errorCode":"IP_ADDRESS_NOT_ON_ACCESS_LIST","parameters":["203.0.113.7"],"reason":"Forbidden"}""", "203.0.113.7")]
    public async Task Atlas_errors_say_what_to_fix(int status, string body, string expected)
    {
        var (source, atlas) = Source();
        // A wrong secret is refused at the token step; a missing role or an unknown project at the first read.
        var refused = body.Contains("invalid_client", StringComparison.Ordinal) ? "/api/oauth/token" : $"/api/atlas/v2/groups/{Project}/clusters/Cluster0";
        atlas.Override = r => r.RequestUri!.AbsolutePath == refused ? FakeAtlas.Json(body, (HttpStatusCode)status) : null;

        var ex = await Assert.ThrowsAnyAsync<SourceException>(() => source.ProbeAsync(CancellationToken.None));

        Assert.Contains(expected, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("not-a-real-secret", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>An unknown project id says only that: not being a member is its own answer (USER_CANNOT_ACCESS_GROUP).</summary>
    [Fact]
    public async Task An_unknown_project_does_not_blame_membership()
    {
        var (source, atlas) = Source();
        atlas.Override = r => r.RequestUri!.AbsolutePath == $"/api/atlas/v2/groups/{Project}/clusters/Cluster0"
            ? FakeAtlas.Json("""{"detail":"No group with ID 000000000000000000000000 exists.","error":404,"errorCode":"GROUP_NOT_FOUND","parameters":["000000000000000000000000"],"reason":"Not Found"}""", HttpStatusCode.NotFound)
            : null;

        var ex = await Assert.ThrowsAnyAsync<SourceException>(() => source.ProbeAsync(CancellationToken.None));

        Assert.StartsWith("Atlas knows no project with the id in host:", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("member", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("not in it", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Too_many_requests_waits_for_retry_after_and_tries_again()
    {
        var (source, atlas) = Source();
        var refusals = 1;
        atlas.Override = r =>
        {
            if (r.RequestUri!.AbsolutePath.EndsWith("/clusters/Cluster0", StringComparison.Ordinal) && refusals-- > 0)
            {
                var tooMany = FakeAtlas.Json("""{"errorCode":"RATE_LIMITED"}""", (HttpStatusCode)429);
                tooMany.Headers.RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromMilliseconds(10));
                return tooMany;
            }
            return null;
        };

        var caps = await source.ProbeAsync(CancellationToken.None);

        Assert.Equal("M10", caps.Atlas!.Tier);
        // The refused read, its retry, and the processes lookup.
        Assert.Equal(3, atlas.Paths.Count(p => p.EndsWith("/clusters/Cluster0", StringComparison.Ordinal)));
    }

    [Fact]
    public void Hosts_come_from_the_standard_connection_string()
    {
        var hosts = MongoSource.HostsOf("mongodb://A.example.net:27017,b.example.net:27017/?ssl=true&replicaSet=x");

        Assert.Equal(new[] { "a.example.net", "b.example.net" }, hosts.OrderBy(h => h));
    }
}
