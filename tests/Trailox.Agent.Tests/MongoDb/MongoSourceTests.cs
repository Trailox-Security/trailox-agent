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
            if (rest.StartsWith("/clusters/Cluster0", StringComparison.Ordinal) && !rest.Contains("/logs/", StringComparison.Ordinal))
            {
                return Json("""{"name":"Cluster0","clusterType":"REPLICASET","paused":false,"mongoDBVersion":"8.0.34","connectionStrings":{"standard":"mongodb://@0@:27017,@1@:27017,@2@:27017/?ssl=true&authSource=admin&replicaSet=atlas-abcdef-shard-0"},"providerSettings":{"providerName":"@3@","instanceSizeName":"@4@"}}""".Replace("@0@", (Hosts[0]).ToString()).Replace("@1@", (Hosts[1]).ToString()).Replace("@2@", (Hosts[2]).ToString()).Replace("@3@", (ProviderName).ToString()).Replace("@4@", (Tier).ToString()));
            }
            if (rest.StartsWith("/processes", StringComparison.Ordinal))
            {
                var mine = Hosts.Select((h, i) => """{"userAlias":"@0@","hostname":"atlas-abcdef-shard-00-0@1@.example.mongodb.net","typeName":"@2@"}""".Replace("@0@", (h).ToString()).Replace("@1@", (i).ToString()).Replace("@2@", ((i == 2 ? "REPLICA_PRIMARY" : "REPLICA_SECONDARY")).ToString()));
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

    [Fact]
    public async Task Only_the_streamed_ops_upload_asks_for_keep_alive()
    {
        var start = new DateTime(2026, 10, 19, 9, 0, 0, DateTimeKind.Utc);
        var (source, atlas) = Source();
        atlas.Log = (_, name) => name == "mongodb" ? [ServerLine(start.AddMinutes(12))] : [AuditLine(start.AddSeconds(5), "app_orders")];

        var ops = (await source.OpenAsync(Window(ProtocolInfo.Streams.MongoOps, start, start.AddMinutes(10)), CancellationToken.None))!;
        var users = (await source.OpenAsync(new AgentTask { ChunkId = "c", Stream = ProtocolInfo.Streams.CatalogUsers }, CancellationToken.None))!;

        // The ops rows stream while gigabytes of audit log are read; every other stream is read whole first.
        Assert.True(ops.KeepAlive);
        Assert.False(users.KeepAlive);
        Assert.Equal(3, (await RowsOf(ops)).Count);
        Assert.Single(await RowsOf(users));
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
