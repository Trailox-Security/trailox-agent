using System.Text.Json;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.MongoDb;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests.MongoDb;

/// <summary>
/// The collection catalog through the source (agent 1.9): catalog_databases from the Admin API's measurements of the
/// cluster's primaries, catalog_collections and the probe's catalog check through the optional catalog user.
/// </summary>
public partial class MongoSourceTests
{
    private const string CatalogUser = "trailox_catalog";
    private const string Srv = "mongodb+srv://cluster0.example.mongodb.net";

    // The fake's processes: the replica set's primary is its third member; on the sharded cluster the first host's mongod
    // is the config shard's primary and the second's the other shard's.
    private const string Primary = "atlas-abcdef-shard-00-02.example.mongodb.net:27017";
    private const string Secondary = "atlas-abcdef-shard-00-00.example.mongodb.net:27017";
    private const string ConfigShardPrimary = "atlas-abcdef-shard-00-00.example.mongodb.net:27017";
    private const string ShardPrimary = "atlas-abcdef-shard-00-01.example.mongodb.net:27017";
    private const string ShardSecondary = "atlas-abcdef-shard-00-02.example.mongodb.net:27017";

    /// <summary>A source with the catalog user, whose reads are <paramref name="reads"/>; every SRV string it opened is recorded.</summary>
    private static (MongoSource Source, FakeAtlas Atlas, List<string> Opened) CatalogSource(FakeCatalogReads reads)
    {
        var atlas = new FakeAtlas();
        var endpoint = new EndpointConfig
        {
            Alias = "atlas", Engine = "mongodb", Host = Project, ClusterName = "Cluster0", Username = ClientId, CatalogUsername = CatalogUser,
        };
        endpoint.Password = "not-a-real-secret";
        endpoint.CatalogPassword = "not-a-real-catalog-password";
        var opened = new List<string>();
        var source = new MongoSource(new HttpClient(atlas), endpoint, "https://atlas.example.test", () => Now)
        {
            OpenCatalog = srv =>
            {
                opened.Add(srv);
                return reads;
            },
        };
        return (source, atlas, opened);
    }

    private static AgentTask Catalog(string stream) => new() { ChunkId = "c", EndpointId = 1, Stream = stream };

    /// <summary>A measurements body: each metric's points as (timestamp, JSON value), oldest first as Atlas lists them.</summary>
    private static string Measurements(params (string Name, (string At, string Value)[] Points)[] series) =>
        """{"databaseName":"x","granularity":"PT1H","measurements":[@0@]}""".Replace("@0@", string.Join(",", series.Select(s =>
            """{"name":"@0@","units":"SCALAR","dataPoints":[@1@]}""".Replace("@0@", s.Name).Replace("@1@", string.Join(",", s.Points.Select(p =>
                """{"timestamp":"@0@","value":@1@}""".Replace("@0@", p.At).Replace("@1@", p.Value)))))));

    /// <summary>A test database on an M10 as its measurements read on 2026-10-21, with the newest data size not yet known.</summary>
    private static string LoadtestMeasurements() => Measurements(
        ("DATABASE_AVERAGE_OBJECT_SIZE", [("2026-10-21T15:35:10Z", "290.32013856506893")]),
        ("DATABASE_COLLECTION_COUNT", [("2026-10-21T14:24:08Z", "1"), ("2026-10-21T15:35:10Z", "1")]),
        ("DATABASE_DATA_SIZE", [("2026-10-21T14:24:08Z", "232230853"), ("2026-10-21T15:35:10Z", "null")]),
        ("DATABASE_STORAGE_SIZE", [("2026-10-21T14:24:08Z", "229568512"), ("2026-10-21T15:35:10Z", "232005632")]),
        ("DATABASE_INDEX_SIZE", [("2026-10-21T15:35:10Z", "2.7316224E7")]),
        ("DATABASE_INDEX_COUNT", [("2026-10-21T15:35:10Z", "2")]),
        ("DATABASE_OBJECT_COUNT", [("2026-10-21T14:24:08Z", "799256"), ("2026-10-21T15:35:10Z", "799913")]));

    [Fact]
    public async Task Catalog_databases_reads_each_database_of_the_primary_with_its_newest_values()
    {
        var (source, atlas) = Source();
        atlas.ProcessDatabases[Primary] = ["config", "loadtest", "local", "shop"];
        atlas.ProcessDatabases[Secondary] = ["loadtest"];
        atlas.Measurements = (_, db) => db == "loadtest" ? LoadtestMeasurements() : """{"measurements":[]}""";

        var rows = await RowsOf((await source.OpenAsync(Catalog(ProtocolInfo.Streams.CatalogDatabases), CancellationToken.None))!);

        Assert.Equal(["loadtest", "shop"], rows.Select(r => r.GetProperty("database").GetString()));
        var loadtest = rows[0];
        Assert.Equal(
            ["snapshot_date", "process", "database", "objects", "data_size", "storage_size", "index_size", "collections", "views", "indexes"],
            loadtest.EnumerateObject().Select(p => p.Name));
        Assert.Equal("2026-10-19", loadtest.GetProperty("snapshot_date").GetString());
        Assert.Equal(Primary, loadtest.GetProperty("process").GetString());
        Assert.Equal(799_913, loadtest.GetProperty("objects").GetInt64());
        // The newest point had no value: the one before it is the newest known.
        Assert.Equal(232_230_853, loadtest.GetProperty("data_size").GetInt64());
        Assert.Equal(232_005_632, loadtest.GetProperty("storage_size").GetInt64());
        Assert.Equal(27_316_224, loadtest.GetProperty("index_size").GetInt64());
        Assert.Equal(1, loadtest.GetProperty("collections").GetInt64());
        // Not measured at all: 0.
        Assert.Equal(0, loadtest.GetProperty("views").GetInt64());
        Assert.Equal(2, loadtest.GetProperty("indexes").GetInt64());
        // A database with no point yet is still listed, with zeros.
        Assert.All(["objects", "data_size", "storage_size", "index_size", "collections", "views", "indexes"], c => Assert.Equal(0, rows[1].GetProperty(c).GetInt64()));
    }

    /// <summary>🔴 The process id holds a colon: it goes as one escaped path segment, and only the primary's is read.</summary>
    [Fact]
    public async Task Catalog_databases_asks_by_the_escaped_process_id_for_a_days_hourly_points()
    {
        var (source, atlas) = Source();
        atlas.ProcessDatabases[Primary] = ["admin", "loadtest"];
        atlas.ProcessDatabases[Secondary] = ["loadtest"];

        await RowsOf((await source.OpenAsync(Catalog(ProtocolInfo.Streams.CatalogDatabases), CancellationToken.None))!);

        var escaped = "/processes/atlas-abcdef-shard-00-02.example.mongodb.net%3A27017/databases";
        Assert.Contains(atlas.Paths, p => p.Contains(escaped + "?", StringComparison.Ordinal));
        var measured = Assert.Single(atlas.Paths, p => p.Contains("/measurements", StringComparison.Ordinal));
        Assert.EndsWith(escaped + "/loadtest/measurements?granularity=PT1H&period=P1D", measured, StringComparison.Ordinal);
        Assert.DoesNotContain(atlas.Paths, p => p.Contains("shard-00-00.example.mongodb.net%3A", StringComparison.Ordinal));
        Assert.DoesNotContain(atlas.Paths, p => p.Contains("shard-00-01.example.mongodb.net%3A", StringComparison.Ordinal));
    }

    /// <summary>
    /// 🔴 A sharded cluster's data is on its shards' primaries: the config shard's (SHARD_CONFIG_PRIMARY) and every other
    ///    shard's, never a secondary and never a mongos router, which the log streams read instead.
    /// </summary>
    [Fact]
    public async Task Catalog_databases_on_a_sharded_cluster_reads_each_shards_primary_and_no_router()
    {
        var (source, atlas) = Source();
        atlas.Sharded = true;
        atlas.ProcessDatabases[ConfigShardPrimary] = ["config", "loadtest"];
        atlas.ProcessDatabases[ShardPrimary] = ["loadtest", "shop"];
        atlas.ProcessDatabases[ShardSecondary] = ["loadtest"];
        atlas.Measurements = (id, _) => Measurements(("DATABASE_OBJECT_COUNT", [("2026-10-19T11:24:00Z", id == ShardPrimary ? "399913" : "400000")]));

        var rows = await RowsOf((await source.OpenAsync(Catalog(ProtocolInfo.Streams.CatalogDatabases), CancellationToken.None))!);

        Assert.Equal(
            [(ConfigShardPrimary, "loadtest", 400_000L), (ShardPrimary, "loadtest", 399_913L), (ShardPrimary, "shop", 399_913L)],
            rows.Select(r => (r.GetProperty("process").GetString(), r.GetProperty("database").GetString(), r.GetProperty("objects").GetInt64())));
        Assert.DoesNotContain(atlas.Paths, p => p.Contains("%3A27016", StringComparison.Ordinal));
        Assert.DoesNotContain(atlas.Paths, p => p.Contains("shard-00-02.example.mongodb.net%3A", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Catalog_databases_without_a_primary_fails_rather_than_report_an_empty_day()
    {
        var (source, atlas) = Source();
        atlas.Override = r => r.RequestUri!.AbsolutePath.EndsWith("/processes", StringComparison.Ordinal)
            ? FakeAtlas.Json("""{"results":[{"id":"h:27017","userAlias":"@0@","typeName":"REPLICA_SECONDARY"}],"totalCount":1}""".Replace("@0@", Hosts[0]))
            : null;

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Catalog(ProtocolInfo.Streams.CatalogDatabases), CancellationToken.None));

        Assert.Contains("no primary of cluster Cluster0", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_newest_point_with_a_value_wins()
    {
        using var doc = JsonDocument.Parse(Measurements(
            ("DATABASE_OBJECT_COUNT", [("2026-10-21T15:35:10Z", "799913"), ("2026-10-21T14:24:08Z", "799256")]),
            ("DATABASE_DATA_SIZE", [("2026-10-21T14:24:08Z", "100"), ("2026-10-21T15:24:08Z", "null")]),
            ("DATABASE_VIEW_COUNT", [("2026-10-21T14:24:08Z", "null")]),
            ("DATABASE_STORAGE_SIZE", [("2026-10-21T15:35:10Z", "232005632.4")])));

        var values = AtlasCatalog.NewestValues(doc.RootElement);

        // Listed out of order: the time decides.
        Assert.Equal(799_913, values["DATABASE_OBJECT_COUNT"]);
        Assert.Equal(100, values["DATABASE_DATA_SIZE"]);
        Assert.False(values.ContainsKey("DATABASE_VIEW_COUNT"));
        Assert.Equal(232_005_632, values["DATABASE_STORAGE_SIZE"]);
    }

    // ------------------------------------------------------------ catalog_collections

    /// <summary>
    /// 🔴 No catalog user: the task fails as an optional grant ("not configured"), so Trailox counts the catalog day and the
    ///    agent lists no error, and nothing connects to the cluster or asks Atlas anything.
    /// </summary>
    [Fact]
    public async Task Without_a_catalog_user_catalog_collections_fails_as_an_optional_grant()
    {
        var (source, atlas) = Source();

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Catalog(ProtocolInfo.Streams.CatalogCollections), CancellationToken.None));

        Assert.True(ex.MissingOptionalGrant);
        Assert.StartsWith("not configured:", ex.Message, StringComparison.Ordinal);
        Assert.Contains("catalog_username", ex.Message, StringComparison.Ordinal);
        Assert.Empty(atlas.Paths);
    }

    [Fact]
    public async Task Catalog_collections_signs_in_on_the_clusters_srv_string_and_closes_the_connection()
    {
        var reads = new FakeCatalogReads { Databases = { "loadtest" } };
        reads.Collections["loadtest"] = [FakeCatalogReads.Collection("orders")];
        var (source, _, opened) = CatalogSource(reads);

        var rows = await RowsOf((await source.OpenAsync(Catalog(ProtocolInfo.Streams.CatalogCollections), CancellationToken.None))!);

        Assert.Equal([Srv], opened);
        Assert.True(reads.Disposed);
        var row = Assert.Single(rows);
        Assert.Equal("2026-10-19", row.GetProperty("snapshot_date").GetString());
        Assert.Equal("orders", row.GetProperty("collection").GetString());
    }

    [Fact]
    public async Task A_failure_on_the_cluster_fails_catalog_collections_as_an_error()
    {
        var reads = new FakeCatalogReads { Failure = new SourceException("the agent could not reach cluster Cluster0 as the catalog user") };
        var (source, _, _) = CatalogSource(reads);

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Catalog(ProtocolInfo.Streams.CatalogCollections), CancellationToken.None));

        Assert.False(ex.MissingOptionalGrant);
        Assert.True(reads.Disposed);
    }

    // ------------------------------------------------------------ the probe's catalog check

    [Fact]
    public async Task Without_a_catalog_user_the_probe_has_no_catalog_check_and_connects_nowhere()
    {
        var (source, _) = Source();

        var caps = await source.ProbeAsync(CancellationToken.None);

        Assert.DoesNotContain(caps.Atlas!.Checks, c => c.Name == "catalog");
    }

    [Fact]
    public async Task The_probe_checks_the_catalog_user_and_reports_ok()
    {
        var reads = new FakeCatalogReads();
        var (source, _, opened) = CatalogSource(reads);

        var caps = await source.ProbeAsync(CancellationToken.None);

        var check = Assert.Single(caps.Atlas!.Checks, c => c.Name == "catalog");
        Assert.Equal("ok", check.Status);
        Assert.Equal([Srv], opened);
        Assert.Equal(["connectionStatus"], reads.Calls);
        Assert.True(reads.Disposed);
    }

    /// <summary>K4: a catalog user that can read documents is a warn on the source page, and the source stays up.</summary>
    [Fact]
    public async Task A_catalog_user_that_can_read_documents_is_a_warn()
    {
        var reads = new FakeCatalogReads { Status = FakeCatalogReads.ConnectionStatus(CatalogUser, "find", "listDatabases", "listCollections", "collStats", "dbStats") };
        var (source, _, _) = CatalogSource(reads);

        var caps = await source.ProbeAsync(CancellationToken.None);

        var check = Assert.Single(caps.Atlas!.Checks, c => c.Name == "catalog");
        Assert.Equal("warn", check.Status);
        Assert.Contains("read documents (find)", check.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// 🔴 A catalog user that cannot connect or sign in fails its own check, never the probe: the logs, events and database
    ///    sizes need no catalog user.
    /// </summary>
    [Theory]
    [InlineData("the agent could not reach cluster Cluster0 as the catalog user: add the agent's outbound address to the project's IP access list (Network Access), or reach the cluster over PrivateLink")]
    [InlineData("cluster Cluster0 refused the catalog user's sign-in: check catalog_username (trailox_catalog) and its password")]
    public async Task A_catalog_user_that_cannot_connect_or_sign_in_fails_its_check_and_not_the_probe(string failure)
    {
        var (source, _, _) = CatalogSource(new FakeCatalogReads { Failure = new SourceException(failure) });

        var caps = await source.ProbeAsync(CancellationToken.None);

        var check = Assert.Single(caps.Atlas!.Checks, c => c.Name == "catalog");
        Assert.Equal("fail", check.Status);
        Assert.Equal(failure, check.Message);
        Assert.All(caps.Atlas.Checks.Where(c => c.Name != "catalog"), c => Assert.Equal("ok", c.Status));
        Assert.DoesNotContain("not-a-real-catalog-password", JsonSerializer.Serialize(caps, ProtocolInfo.Json), StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_connection_string_that_cannot_be_opened_fails_the_check_too()
    {
        var endpoint = new EndpointConfig { Alias = "atlas", Engine = "mongodb", Host = Project, ClusterName = "Cluster0", Username = ClientId, CatalogUsername = CatalogUser };
        var source = new MongoSource(new HttpClient(new FakeAtlas()), endpoint, "https://atlas.example.test", () => Now)
        {
            OpenCatalog = _ => throw new SourceException("cluster Cluster0's connection string could not be used (DNS)"),
        };

        var caps = await source.ProbeAsync(CancellationToken.None);

        var check = Assert.Single(caps.Atlas!.Checks, c => c.Name == "catalog");
        Assert.Equal("fail", check.Status);
        Assert.Contains("connection string could not be used", check.Message, StringComparison.Ordinal);
    }

    /// <summary>Even a failure nobody worded stays inside the check: the probe still registers the source.</summary>
    [Fact]
    public async Task An_unexpected_failure_of_the_catalog_check_does_not_fail_the_probe()
    {
        var endpoint = new EndpointConfig { Alias = "atlas", Engine = "mongodb", Host = Project, ClusterName = "Cluster0", Username = ClientId, CatalogUsername = CatalogUser };
        var source = new MongoSource(new HttpClient(new FakeAtlas()), endpoint, "https://atlas.example.test", () => Now)
        {
            OpenCatalog = _ => throw new InvalidOperationException("the driver did something nobody expected"),
        };

        var caps = await source.ProbeAsync(CancellationToken.None);

        var check = Assert.Single(caps.Atlas!.Checks, c => c.Name == "catalog");
        Assert.Equal("fail", check.Status);
        Assert.Contains("InvalidOperationException: the driver did something nobody expected", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_processes_id_is_not_sent_to_Trailox()
    {
        var json = JsonSerializer.Serialize(new AtlasProcess { Host = Hosts[0], Type = "REPLICA_PRIMARY", Id = Primary }, ProtocolInfo.Json);

        Assert.DoesNotContain("27017", json, StringComparison.Ordinal);
    }
}
