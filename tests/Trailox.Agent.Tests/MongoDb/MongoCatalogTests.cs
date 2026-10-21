using System.Text;
using System.Text.Json;
using MongoDB.Bson;
using Trailox.Agent.Engines.MongoDb;

namespace Trailox.Agent.Tests.MongoDb;

/// <summary>
/// The collection catalog (agent 1.9) as rows, over a stand-in for the catalog user's four reads: what is listed,
/// what is skipped, how shards add up, the cap, and the probe's check of the user's privileges.
/// </summary>
public class MongoCatalogTests
{
    private const string Today = "2026-10-21";

    private static async Task<(List<JsonElement> Rows, string Text)> RowsOf(FakeCatalogReads reads)
    {
        var bytes = await new MongoCatalog(reads).CollectionsAsync(Today, CancellationToken.None);
        var text = Encoding.UTF8.GetString(bytes.Span);
        var rows = text.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(l => JsonDocument.Parse(l).RootElement.Clone()).ToList();
        return (rows, text);
    }

    /// <summary>The live row of 2026-10-21: a test collection on an M10 as $collStats answered it.</summary>
    [Fact]
    public async Task A_collection_is_one_row_with_its_count_and_sizes_and_the_snapshot_date_first()
    {
        var reads = new FakeCatalogReads { Databases = { "admin", "loadtest" } };
        reads.Collections["loadtest"] = [FakeCatalogReads.Collection("orders")];
        reads.Stats = (_, _) => [FakeCatalogReads.CollStats(count: 799_913, size: 232_230_853, storageSize: 232_005_632, indexSize: 27_316_224, indexes: 2)];

        var (rows, text) = await RowsOf(reads);

        var row = Assert.Single(rows);
        Assert.StartsWith("{\"snapshot_date\":\"2026-10-21\",", text, StringComparison.Ordinal);
        Assert.Equal(
            ["snapshot_date", "database", "collection", "type", "count", "size", "storage_size", "index_size", "indexes", "capped", "sharded"],
            row.EnumerateObject().Select(p => p.Name));
        Assert.Equal("loadtest", row.GetProperty("database").GetString());
        Assert.Equal("orders", row.GetProperty("collection").GetString());
        Assert.Equal("collection", row.GetProperty("type").GetString());
        Assert.Equal(799_913, row.GetProperty("count").GetInt64());
        Assert.Equal(232_230_853, row.GetProperty("size").GetInt64());
        Assert.Equal(232_005_632, row.GetProperty("storage_size").GetInt64());
        Assert.Equal(27_316_224, row.GetProperty("index_size").GetInt64());
        Assert.Equal(2, row.GetProperty("indexes").GetInt64());
        Assert.False(row.GetProperty("capped").GetBoolean());
        Assert.False(row.GetProperty("sharded").GetBoolean());
    }

    [Fact]
    public async Task MongoDBs_own_databases_and_system_collections_are_never_listed()
    {
        var reads = new FakeCatalogReads { Databases = { "admin", "config", "local", "shop" } };
        reads.Collections["shop"] = [FakeCatalogReads.Collection("system.views"), FakeCatalogReads.Collection("system.buckets.metrics"), FakeCatalogReads.Collection("orders")];

        var (rows, _) = await RowsOf(reads);

        Assert.Equal(["orders"], rows.Select(r => r.GetProperty("collection").GetString()));
        Assert.Equal(["listDatabases", "listCollections shop", "$collStats shop.orders"], reads.Calls);
    }

    /// <summary>A view holds nothing: one row with zeros, and no $collStats (it would be refused on a view anyway).</summary>
    [Fact]
    public async Task A_view_is_a_row_of_zeros_and_is_not_asked_for_stats()
    {
        var reads = new FakeCatalogReads { Databases = { "shop" } };
        reads.Collections["shop"] = [FakeCatalogReads.Collection("big_orders", "view"), FakeCatalogReads.Collection("metrics", "timeseries")];

        var (rows, _) = await RowsOf(reads);

        var view = rows.Single(r => r.GetProperty("type").GetString() == "view");
        Assert.Equal("big_orders", view.GetProperty("collection").GetString());
        Assert.All(["count", "size", "storage_size", "index_size", "indexes"], c => Assert.Equal(0, view.GetProperty(c).GetInt64()));
        Assert.Equal("timeseries", rows.Single(r => r.GetProperty("collection").GetString() == "metrics").GetProperty("type").GetString());
        Assert.DoesNotContain(reads.Calls, c => c.Contains("big_orders", StringComparison.Ordinal) && c.StartsWith("$collStats", StringComparison.Ordinal));
    }

    /// <summary>A collection's name is sent as it is, its own dots included; the collector encodes them.</summary>
    [Fact]
    public async Task Names_are_sent_as_they_are_in_name_order()
    {
        var reads = new FakeCatalogReads { Databases = { "zeta", "alpha" } };
        reads.Collections["zeta"] = [FakeCatalogReads.Collection("b")];
        reads.Collections["alpha"] = [FakeCatalogReads.Collection("orders.2026"), FakeCatalogReads.Collection("Orders"), FakeCatalogReads.Collection("archive")];

        var (rows, _) = await RowsOf(reads);

        Assert.Equal(["alpha.Orders", "alpha.archive", "alpha.orders.2026", "zeta.b"],
            rows.Select(r => r.GetProperty("database").GetString() + "." + r.GetProperty("collection").GetString()));
    }

    /// <summary>
    /// A sharded collection answers once per shard holding it: counts and sizes add up, the index count does not (each
    /// shard holds the same indexes), and the row says it is sharded.
    /// </summary>
    [Fact]
    public async Task A_sharded_collections_answers_are_summed_and_it_says_sharded()
    {
        var reads = new FakeCatalogReads { Databases = { "loadtest" } };
        reads.Collections["loadtest"] = [FakeCatalogReads.Collection("orders")];
        reads.Stats = (_, _) =>
        [
            FakeCatalogReads.CollStats(count: 400_000, size: 116_000_000, storageSize: 115_000_000, indexSize: 13_000_000, indexes: 2, shard: "atlas-abcdef-config-0"),
            FakeCatalogReads.CollStats(count: 399_913, size: 116_230_853, storageSize: 117_005_632, indexSize: 14_316_224, indexes: 2, shard: "atlas-abcdef-shard-0"),
        ];

        var (rows, _) = await RowsOf(reads);

        var row = Assert.Single(rows);
        Assert.Equal(799_913, row.GetProperty("count").GetInt64());
        Assert.Equal(232_230_853, row.GetProperty("size").GetInt64());
        Assert.Equal(232_005_632, row.GetProperty("storage_size").GetInt64());
        Assert.Equal(27_316_224, row.GetProperty("index_size").GetInt64());
        Assert.Equal(2, row.GetProperty("indexes").GetInt64());
        Assert.True(row.GetProperty("sharded").GetBoolean());
    }

    [Fact]
    public async Task A_capped_collection_says_so()
    {
        var reads = new FakeCatalogReads { Databases = { "logs" } };
        reads.Collections["logs"] = [FakeCatalogReads.Collection("recent")];
        reads.Stats = (_, _) => [FakeCatalogReads.CollStats(count: 10, size: 1000, storageSize: 4096, indexSize: 4096, indexes: 1, capped: true)];

        var (rows, _) = await RowsOf(reads);

        Assert.True(Assert.Single(rows).GetProperty("capped").GetBoolean());
    }

    [Fact]
    public async Task A_collection_dropped_after_it_was_listed_has_no_row()
    {
        var reads = new FakeCatalogReads { Databases = { "shop" } };
        reads.Collections["shop"] = [FakeCatalogReads.Collection("gone"), FakeCatalogReads.Collection("orders")];
        var stats = reads.Stats;
        reads.Stats = (d, c) => c == "gone" ? [] : stats(d, c);

        var (rows, _) = await RowsOf(reads);

        Assert.Equal(["orders"], rows.Select(r => r.GetProperty("collection").GetString()));
    }

    /// <summary>
    /// 🔴 At most 10,000 collection rows. Past them the listing stops, the next database is not even listed, and one
    ///    row with no names and type truncated says the list is incomplete.
    /// </summary>
    [Fact]
    public async Task Past_the_cap_the_listing_stops_and_a_truncated_row_says_so()
    {
        var reads = new FakeCatalogReads { Databases = { "big", "later" } };
        reads.Collections["big"] = Enumerable.Range(0, MongoCatalog.MaxCollections + 5).Select(i => FakeCatalogReads.Collection($"c{i:D5}")).ToList();
        reads.Collections["later"] = [FakeCatalogReads.Collection("orders")];

        var (rows, _) = await RowsOf(reads);

        Assert.Equal(MongoCatalog.MaxCollections + 1, rows.Count);
        Assert.Equal(MongoCatalog.MaxCollections, reads.Calls.Count(c => c.StartsWith("$collStats", StringComparison.Ordinal)));
        Assert.DoesNotContain("listCollections later", reads.Calls);
        var last = rows[^1];
        Assert.Equal("truncated", last.GetProperty("type").GetString());
        Assert.Equal("", last.GetProperty("database").GetString());
        Assert.Equal("", last.GetProperty("collection").GetString());
        Assert.Equal(Today, last.GetProperty("snapshot_date").GetString());
        Assert.All(["count", "size", "storage_size", "index_size", "indexes"], c => Assert.Equal(0, last.GetProperty(c).GetInt64()));
        Assert.False(last.GetProperty("capped").GetBoolean());
        Assert.False(last.GetProperty("sharded").GetBoolean());
    }

    [Fact]
    public async Task Exactly_the_cap_is_not_truncated()
    {
        var reads = new FakeCatalogReads { Databases = { "big" } };
        reads.Collections["big"] = Enumerable.Range(0, MongoCatalog.MaxCollections).Select(i => FakeCatalogReads.Collection($"c{i:D5}")).ToList();

        var (rows, _) = await RowsOf(reads);

        Assert.Equal(MongoCatalog.MaxCollections, rows.Count);
        Assert.DoesNotContain(rows, r => r.GetProperty("type").GetString() == "truncated");
    }

    [Fact]
    public async Task Eight_collections_are_asked_at_a_time()
    {
        var reads = new FakeCatalogReads { Databases = { "shop" }, StatsDelay = TimeSpan.FromMilliseconds(20) };
        reads.Collections["shop"] = Enumerable.Range(0, 40).Select(i => FakeCatalogReads.Collection($"c{i:D2}")).ToList();

        var (rows, _) = await RowsOf(reads);

        Assert.Equal(40, rows.Count);
        Assert.InRange(reads.MaxInFlight, 2, 8);
    }

    [Fact]
    public async Task A_failed_read_fails_the_catalog()
    {
        var reads = new FakeCatalogReads { Databases = { "shop" }, Failure = new Trailox.Agent.Engines.SourceException("cluster Cluster0 refused listDatabases") };

        var ex = await Assert.ThrowsAsync<Trailox.Agent.Engines.SourceException>(() => RowsOf(reads));

        Assert.Contains("refused listDatabases", ex.Message, StringComparison.Ordinal);
    }

    // ------------------------------------------------------------ the probe's check of the user (K4)

    [Fact]
    public async Task The_measured_role_is_ok()
    {
        var check = await new MongoCatalog(new FakeCatalogReads()).CheckAsync("trailox_catalog", CancellationToken.None);

        Assert.Equal("catalog", check.Name);
        Assert.Equal("ok", check.Status);
        Assert.Contains("collStats, dbStats, listCollections, listDatabases", check.Message, StringComparison.Ordinal);
    }

    /// <summary>K4: a user that can read documents still runs, and the source page says so.</summary>
    [Theory]
    [InlineData("find", "read documents (find)")]
    [InlineData("changeStream", "read documents (changeStream)")]
    [InlineData("insert", "change them (insert)")]
    [InlineData("dropCollection", "change them (dropCollection)")]
    public async Task A_user_that_can_read_or_change_documents_is_a_warn_naming_what(string extra, string expected)
    {
        var reads = new FakeCatalogReads { Status = FakeCatalogReads.ConnectionStatus("trailox_catalog", "listDatabases", "listCollections", "collStats", "dbStats", extra) };

        var check = await new MongoCatalog(reads).CheckAsync("trailox_catalog", CancellationToken.None);

        Assert.Equal("warn", check.Status);
        Assert.Contains(expected, check.Message, StringComparison.Ordinal);
        Assert.Contains("listDatabases, listCollections, collStats and dbStats", check.Message, StringComparison.Ordinal);
    }

    /// <summary>A built-in role such as readAnyDatabase holds find among many: both kinds are named.</summary>
    [Fact]
    public async Task Reading_and_changing_are_both_named()
    {
        var reads = new FakeCatalogReads { Status = FakeCatalogReads.ConnectionStatus("trailox_catalog", "find", "insert", "update", "remove", "listCollections") };

        var check = await new MongoCatalog(reads).CheckAsync("trailox_catalog", CancellationToken.None);

        Assert.Equal("warn", check.Status);
        Assert.Contains("may also read documents (find) and change them (insert, remove, update)", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_connection_with_no_user_signed_in_fails_the_check()
    {
        var status = FakeCatalogReads.ConnectionStatus("x");
        status["authInfo"]["authenticatedUsers"] = new BsonArray();

        var check = await new MongoCatalog(new FakeCatalogReads { Status = status }).CheckAsync("trailox_catalog", CancellationToken.None);

        Assert.Equal("fail", check.Status);
    }

    [Fact]
    public void Actions_are_gathered_from_every_privilege()
    {
        var info = new BsonDocument("authenticatedUserPrivileges", new BsonArray
        {
            new BsonDocument { { "resource", new BsonDocument("cluster", true) }, { "actions", new BsonArray { "listDatabases" } } },
            new BsonDocument { { "resource", new BsonDocument { { "db", "" }, { "collection", "" } } }, { "actions", new BsonArray { "listCollections", "collStats", "dbStats" } } },
        });

        Assert.Equal(["collStats", "dbStats", "listCollections", "listDatabases"], MongoCatalog.Actions(info));
    }
}
