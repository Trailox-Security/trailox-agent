using System.Text.Json;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.MongoDb;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests.MongoDb;

/// <summary>
/// The MongoDB source reading the customer's Atlas S3 log export (1.10.0) instead of the Admin API's files: the same
/// stand-in Atlas for the cluster and its processes, and a bucket in memory laid out as a real export is.
/// </summary>
public partial class MongoSourceTests
{
    private const string S3Root = "atlas-logs/Cluster0/";

    private static string S3Folder(int node, string dir) => $"{S3Root}atlas-abcdef-shard-00-0{node}.example.mongodb.net/{dir}/";

    private static string S3Key(int node, string dir, DateTime named) => $"{S3Folder(node, dir)}{named:yyyyMMdd'T'HHmmss.fff'Z'}-{dir}.log";

    private static (MongoSource Source, FakeAtlas Atlas, FakeS3 S3) S3Source(DateTime? now = null)
    {
        var atlas = new FakeAtlas();
        var s3 = new FakeS3();
        var endpoint = new EndpointConfig { Alias = "atlas", Engine = "mongodb", Host = Project, ClusterName = "Cluster0", Username = ClientId };
        endpoint.Password = "not-a-real-secret";
        endpoint.Options["log_source"] = "s3";
        endpoint.Options["s3_bucket"] = "my-atlas-logs";
        endpoint.Options["s3_region"] = "eu-west-1";
        endpoint.Options["s3_prefix"] = "atlas-logs";
        return (new MongoSource(new HttpClient(atlas), endpoint, "https://atlas.example.test", () => now ?? Now, s3), atlas, s3);
    }

    /// <summary>One object per node and log type, named <paramref name="named"/>, its lines rewritten as the export does.</summary>
    private static void PutS3(FakeS3 s3, int node, string dir, DateTime named, IEnumerable<string> lines, int seed = 1)
    {
        var rng = new Random(seed);
        s3.Put(S3Key(node, dir, named), lines.Select(l => S3Form.Record(l, rng, dir)));
    }

    /// <summary>Each node's server log a minute at a time from <paramref name="from"/> to <paramref name="to"/>: the export's live objects.</summary>
    private static void LiveServerLogs(FakeS3 s3, DateTime from, DateTime to, int nodes = 3, string dir = "mongod")
    {
        for (var node = 0; node < nodes; node++)
        {
            for (var t = from; t <= to; t = t.AddMinutes(1))
            {
                PutS3(s3, node, dir, t, [ServerLine(t.AddSeconds(-30)), ServerLine(t.AddSeconds(-10))]);
            }
        }
    }

    [Fact]
    public async Task The_probe_of_an_S3_source_checks_the_export_and_starts_at_its_live_objects()
    {
        var (source, atlas, s3) = S3Source();
        // The export was enabled at 10:00: its first objects hold each node's log from 03:00 (the backfill).
        for (var node = 0; node < 3; node++)
        {
            PutS3(s3, node, "mongod", new DateTime(2026, 10, 19, 10, 0, 20, DateTimeKind.Utc), [ServerLine(new DateTime(2026, 10, 19, 3, 0, 0, DateTimeKind.Utc))]);
        }
        LiveServerLogs(s3, new DateTime(2026, 10, 19, 10, 25, 30, DateTimeKind.Utc), Now.AddMinutes(-1));

        var caps = await source.ProbeAsync(CancellationToken.None);

        Assert.Equal("s3", caps.Atlas!.LogSource);
        var check = Assert.Single(caps.Atlas.Checks, c => c.Name == "s3");
        Assert.Equal("ok", check.Status);
        Assert.Contains("all 3 process(es)", check.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(caps.Atlas.Checks, c => c.Name == "query_log");
        Assert.DoesNotContain(atlas.Paths, p => p.Contains("/logs/", StringComparison.Ordinal));
        // The first live object is named 10:25:30: a whole minute, never earlier.
        Assert.Equal(Micros(new DateTime(2026, 10, 19, 10, 25, 0, DateTimeKind.Utc)), caps.EarliestEventMicros);
    }

    [Fact]
    public async Task A_node_the_export_does_not_write_fails_the_probe_with_what_to_check()
    {
        var (source, _, s3) = S3Source();
        LiveServerLogs(s3, Now.AddMinutes(-30), Now.AddMinutes(-1), nodes: 2);

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.ProbeAsync(CancellationToken.None));

        Assert.StartsWith("the S3 export does not write cluster0-shard-00-02.example.mongodb.net: s3://my-atlas-logs/atlas-logs/Cluster0/", ex.Message, StringComparison.Ordinal);
        Assert.Contains("log types", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task An_export_that_stopped_writing_is_a_warning_not_a_failure()
    {
        var (source, _, s3) = S3Source();
        LiveServerLogs(s3, Now.AddHours(-2), Now.AddMinutes(-40));

        var caps = await source.ProbeAsync(CancellationToken.None);

        var check = Assert.Single(caps.Atlas!.Checks, c => c.Name == "s3");
        Assert.Equal("warn", check.Status);
        Assert.Contains("wrote nothing in the last 10 minutes", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_bucket_the_agent_cannot_read_fails_the_probe_with_the_reason()
    {
        var (source, _, s3) = S3Source();
        s3.Refuse = new SourceException("list s3://my-atlas-logs/atlas-logs/Cluster0/: access denied. The agent's AWS identity needs s3:ListBucket");

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.ProbeAsync(CancellationToken.None));

        Assert.Contains("access denied", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// 🔴 THE API AND S3 READERS AGREE. The same lines, read from the Admin API's gzip files and
    ///    from the export's objects (keys reordered, arrays as objects, records shuffled within each object, lines
    ///    of the next window in the last objects), ship the same rows for every stream.
    /// </summary>
    [Theory]
    [InlineData(ProtocolInfo.Streams.MongoOps)]
    [InlineData(ProtocolInfo.Streams.MongoConnections)]
    public async Task The_API_and_S3_readers_ship_the_same_rows_for_one_window(string stream)
    {
        var start = new DateTime(2026, 10, 19, 9, 0, 0, DateTimeKind.Utc);
        var end = start.AddMinutes(10);
        var audit = Enumerable.Range(0, 60).Select(i => AuditLine(start.AddSeconds(i * 11 - 30), i % 3 == 0 ? "app_orders" : "app_billing")).ToList();
        var server = Enumerable.Range(0, 30).Select(i => SignInLine(start.AddSeconds(i * 23 - 20), 100 + i)).Append(ServerLine(start.AddMinutes(12))).ToList();

        var (api, atlas) = Source();
        atlas.Log = (_, name) => name == "mongodb" ? server.ToArray() : audit.ToArray();
        var fromApi = await RowsOf((await api.OpenAsync(Window(stream, start, end), CancellationToken.None))!);

        var (fromS3Source, _, s3) = S3Source();
        for (var node = 0; node < 3; node++)
        {
            // Batches of 20 lines, each named after its newest line, its records shuffled.
            foreach (var (dir, lines) in new[] { ("mongod_audit", audit), ("mongod", server) })
            {
                foreach (var batch in lines.Chunk(20))
                {
                    var newest = batch.Max(LineTime);
                    PutS3(s3, node, dir, newest.AddSeconds(5), batch.OrderBy(_ => Random.Shared.Next()), seed: node * 100 + Array.IndexOf(lines.ToArray(), batch[0]));
                }
            }
        }
        LiveServerLogs(s3, end.AddMinutes(5), end.AddMinutes(7));
        var fromS3 = await RowsOf((await fromS3Source.OpenAsync(Window(stream, start, end), CancellationToken.None))!);

        Assert.NotEmpty(fromApi);
        Assert.Equal(fromApi.Select(r => r.GetRawText()).Order(StringComparer.Ordinal), fromS3.Select(r => r.GetRawText()).Order(StringComparer.Ordinal));
        Assert.All(fromS3, r => Assert.Equal(0, r.TryGetProperty("part", out var part) ? part.GetInt32() : 0));
    }

    [Fact]
    public async Task An_S3_window_waits_until_every_node_has_an_object_five_minutes_past_its_end()
    {
        var start = new DateTime(2026, 10, 19, 9, 0, 0, DateTimeKind.Utc);
        var (source, _, s3) = S3Source(now: start.AddMinutes(16));
        LiveServerLogs(s3, start, start.AddMinutes(15));
        // Node 2's export is late: nothing after 09:12.
        foreach (var key in s3.Objects.Keys.Where(k => k.Contains("shard-00-02", StringComparison.Ordinal) && string.CompareOrdinal(k, S3Key(2, "mongod", start.AddMinutes(12))) > 0).ToList())
        {
            s3.Objects.Remove(key);
        }

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Window(ProtocolInfo.Streams.MongoOps, start, start.AddMinutes(10)), CancellationToken.None));

        Assert.True(ex.NotYetPublished);
        Assert.Contains("cluster0-shard-00-02.example.mongodb.net", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// 🔴 An export stall: the window's operations arrive in an object named 9 minutes after its end, past the margin, and
    ///    completeness passes on that same object. They used to be dropped without a word; they are now counted.
    /// </summary>
    [Fact]
    public async Task Operations_delivered_late_after_an_export_stall_are_counted()
    {
        var start = new DateTime(2026, 10, 19, 9, 0, 0, DateTimeKind.Utc);
        var end = start.AddMinutes(10);
        var (source, _, s3) = S3Source(now: end.AddMinutes(20));
        LiveServerLogs(s3, end.AddMinutes(9), end.AddMinutes(12));
        PutS3(s3, 0, "mongod_audit", end.AddMinutes(9), [AuditLine(end.AddMinutes(-1), "app_orders"), AuditLine(end.AddMinutes(-1).AddSeconds(5), "app_orders")]);
        PutS3(s3, 0, "mongod_audit", end.AddMinutes(10), [AuditLine(end.AddMinutes(4), "app_orders")]);

        var rows = await RowsOf((await source.OpenAsync(Window(ProtocolInfo.Streams.MongoOps, start, end), CancellationToken.None))!);

        Assert.Equal(2, rows.Sum(r => r.GetProperty("event_count").GetInt64()));
    }

    /// <summary>
    /// Right after the export is enabled, every object is its first backfill: the probe warns, and the start it reports is
    /// not kept, so the next probe, once live objects exist, reports the real one.
    /// </summary>
    [Fact]
    public async Task A_probe_while_the_export_is_still_backfilling_warns_and_looks_again_later()
    {
        var (source, _, s3) = S3Source();
        for (var node = 0; node < 3; node++)
        {
            PutS3(s3, node, "mongod", Now.AddMinutes(-5), [ServerLine(Now.AddHours(-6))]);
        }

        var first = await source.ProbeAsync(CancellationToken.None);
        LiveServerLogs(s3, Now.AddMinutes(-3), Now.AddMinutes(-1));
        var second = await source.ProbeAsync(CancellationToken.None);

        var check = Assert.Single(first.Atlas!.Checks, c => c.Name == "s3");
        Assert.Equal("warn", check.Status);
        Assert.Contains("still delivering", check.Message, StringComparison.Ordinal);
        Assert.Equal("ok", Assert.Single(second.Atlas!.Checks, c => c.Name == "s3").Status);
        Assert.Equal(Micros(Now.AddMinutes(-3).AddSeconds(-Now.AddMinutes(-3).Second)), second.EarliestEventMicros);
    }

    [Fact]
    public async Task A_window_past_the_give_up_age_is_read_as_it_is()
    {
        var start = new DateTime(2026, 10, 19, 6, 0, 0, DateTimeKind.Utc);
        var (source, _, s3) = S3Source();
        PutS3(s3, 0, "mongod_audit", start.AddMinutes(1), [AuditLine(start.AddSeconds(20), "app_orders")]);

        var rows = await RowsOf((await source.OpenAsync(Window(ProtocolInfo.Streams.MongoOps, start, start.AddMinutes(10)), CancellationToken.None))!);

        Assert.Equal(1, Assert.Single(rows).GetProperty("event_count").GetInt64());
    }

    /// <summary>In the S3 form too, a sharded cluster is read from its routers' mongos and mongos_audit folders only.</summary>
    [Fact]
    public async Task A_sharded_cluster_is_read_from_its_routers_folders_only()
    {
        var start = new DateTime(2026, 10, 19, 9, 0, 0, DateTimeKind.Utc);
        var (source, atlas, s3) = S3Source();
        atlas.Sharded = true;
        LiveServerLogs(s3, start.AddMinutes(15), start.AddMinutes(16), dir: "mongos");
        for (var node = 0; node < 3; node++)
        {
            PutS3(s3, node, "mongos_audit", start.AddMinutes(1), [AuditLine(start.AddSeconds(20), "app_orders")]);
            PutS3(s3, node, "mongod_audit", start.AddMinutes(1), [AuditLine(start.AddSeconds(20), "app_orders")]);
        }

        var rows = await RowsOf((await source.OpenAsync(Window(ProtocolInfo.Streams.MongoOps, start, start.AddMinutes(10)), CancellationToken.None))!);

        Assert.Equal(3, rows.Sum(r => r.GetProperty("event_count").GetInt64()));
        Assert.DoesNotContain(s3.Opened, k => k.Contains("/mongod_audit/", StringComparison.Ordinal));
    }

    private static DateTime LineTime(string line)
    {
        using var doc = JsonDocument.Parse(line);
        var t = doc.RootElement.TryGetProperty("ts", out var ts) ? ts : doc.RootElement.GetProperty("t");
        return DateTime.Parse(t.GetProperty("$date").GetString()!, null, System.Globalization.DateTimeStyles.AdjustToUniversal);
    }

    private static string SignInLine(DateTime t, int conn) =>
        """{"t":{"$date":"@0@+00:00"},"s":"I",  "c":"ACCESS",   "id":5286306, "ctx":"conn@1@","msg":"Successfully authenticated","attr":{"client":"10.1.0.5:58097","isSpeculative":true,"isClusterMember":false,"mechanism":"SCRAM-SHA-256","user":"app_orders","db":"admin","result":0,"doc":{"application":{"name":"orders-api"},"driver":{"name":"nodejs","version":"6.10.0"},"os":{"type":"Linux"},"platform":"Node.js v22"},"extraInfo":{}}}"""
            .Replace("@0@", t.ToString("yyyy-MM-dd'T'HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture)).Replace("@1@", conn.ToString(System.Globalization.CultureInfo.InvariantCulture));
}
