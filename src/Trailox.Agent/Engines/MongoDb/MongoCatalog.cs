using System.Buffers;
using MongoDB.Bson;
using Trailox.Agent.Protocol;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Engines.MongoDb;

/// <summary>
/// The reads <see cref="MongoCatalog"/> makes on a cluster, and nothing else: MongoDB.Driver behind it in production
/// (<see cref="MongoCatalogDriver"/>), a stand-in in tests. None of them reads a document.
/// </summary>
internal interface IMongoCatalogReads : IDisposable
{
    /// <summary>listDatabases with nameOnly and authorizedDatabases.</summary>
    Task<IReadOnlyList<string>> DatabaseNamesAsync(CancellationToken ct);

    /// <summary>listCollections with nameOnly and authorizedCollections: each collection's name and type.</summary>
    Task<IReadOnlyList<BsonDocument>> CollectionsAsync(string database, CancellationToken ct);

    /// <summary>
    /// <c>$collStats</c> with storageStats and count: one answer on a replica set, one per shard on a sharded cluster,
    /// none for a collection dropped since it was listed.
    /// </summary>
    Task<IReadOnlyList<BsonDocument>> CollStatsAsync(string database, string collection, CancellationToken ct);

    /// <summary>connectionStatus with showPrivileges: who signed in, and every action its roles hold.</summary>
    Task<BsonDocument> ConnectionStatusAsync(CancellationToken ct);
}

/// <summary>
/// The collection catalog (agent 1.9), read on the cluster itself as the optional catalog user: every collection and
/// view with its document count and sizes from metadata (catalog_collections), and the probe's check of what that
/// user may do. Only listDatabases, listCollections, <c>$collStats</c> and connectionStatus are ever sent.
/// </summary>
/// <remarks>
/// 🔴 NEVER A DOCUMENT. No find, and no aggregation but <c>$collStats</c>, which reads the storage engine's metadata. The
///    role the README asks for (listDatabases, listCollections, collStats, dbStats) cannot read one either: find on a test
///    collection was refused with code 13, and $collStats gave the same counts and sizes as the Admin API's
///    database measurements (measured on an M10).
/// </remarks>
public sealed class MongoCatalog
{
    /// <summary>At most this many collection rows a day; past it the listing stops and one <c>truncated</c> row says so.</summary>
    public const int MaxCollections = 10_000;

    /// <summary>
    /// <c>$collStats</c> calls in flight. One takes about 81 ms one region away (measured), so the cap's 10,000 take about
    /// 2 minutes rather than 13.5 in sequence. Also the connection pool's size: the catalog user signs in at most this often.
    /// </summary>
    internal const int Parallelism = 8;

    /// <summary>MongoDB's own databases: never in the catalog, from the cluster or from the Admin API.</summary>
    internal static readonly IReadOnlySet<string> SkippedDatabases = new HashSet<string>(StringComparer.Ordinal) { "admin", "local", "config" };

    // What a catalog user should not hold. It is shown as a warn, and the catalog still runs: the agent sends only
    // the commands above whatever the role allows. Reading documents: find, and a change stream, which carries them.
    private static readonly IReadOnlySet<string> ReadsDocuments = new HashSet<string>(StringComparer.Ordinal) { "find", "changeStream" };

    // Changing data, collections, indexes, users or roles.
    private static readonly IReadOnlySet<string> Changes = new HashSet<string>(StringComparer.Ordinal)
    {
        "insert", "update", "remove", "bypassDocumentValidation", "createCollection", "dropCollection", "createIndex",
        "dropIndex", "dropDatabase", "renameCollectionSameDB", "convertToCapped", "collMod", "compact", "reIndex",
        "createSearchIndexes", "updateSearchIndex", "dropSearchIndex", "enableSharding", "createUser", "dropUser",
        "changePassword", "changeCustomData", "grantRole", "revokeRole", "createRole", "dropRole",
    };

    private readonly IMongoCatalogReads _reads;

    internal MongoCatalog(IMongoCatalogReads reads)
    {
        _reads = reads;
    }

    /// <summary>
    /// catalog_collections: one row per collection or view of every database the user may list, but MongoDB's own
    /// (<see cref="SkippedDatabases"/>) and <c>system.*</c> collections. A view is listed with zeros: it holds nothing.
    /// </summary>
    /// <remarks>
    /// Databases and collections are taken in name order, so the cap keeps the same ones from day to day.
    /// ⚠ A sharded collection answers once per shard that holds it: counts and sizes are summed, the index count is
    ///   the largest (each shard holds the same indexes), and <c>sharded</c> is true. One whose data sits on a single
    ///   shard answers once and reads as not sharded: the catalog user cannot read config.collections to tell.
    /// </remarks>
    public async Task<ReadOnlyMemory<byte>> CollectionsAsync(string snapshotDate, CancellationToken ct)
    {
        var (listed, truncated) = await ListAsync(ct);
        var stats = new CollectionStats?[listed.Count];
        var counted = Enumerable.Range(0, listed.Count).Where(i => listed[i].Type != "view");
        await Parallel.ForEachAsync(counted, new ParallelOptions { MaxDegreeOfParallelism = Parallelism, CancellationToken = ct }, async (i, token) =>
        {
            stats[i] = Sum(await _reads.CollStatsAsync(listed[i].Database, listed[i].Name, token));
            TaskHeartbeat.Beat();
        });

        var rows = new ArrayBufferWriter<byte>(16 * 1024);
        var sink = new RowSink(rows);
        for (var i = 0; i < listed.Count; i++)
        {
            var s = listed[i].Type == "view" ? CollectionStats.None : stats[i];
            if (s == null)
            {
                // Dropped between the listing and its $collStats.
                continue;
            }
            Row(sink, snapshotDate, listed[i].Database, listed[i].Name, listed[i].Type, s);
        }
        if (truncated)
        {
            Row(sink, snapshotDate, "", "", "truncated", CollectionStats.None);
        }
        return rows.WrittenMemory;
    }

    /// <summary>
    /// The probe's <c>catalog</c> check (1.9.0): the user signs in, and holds nothing that reads a document or changes
    /// anything (warn, never refuse). A failure to connect or sign in is the caller's to report.
    /// </summary>
    internal async Task<AtlasCheck> CheckAsync(string user, CancellationToken ct)
    {
        var status = await _reads.ConnectionStatusAsync(ct);
        var info = Doc(status, "authInfo");
        if (!info.TryGetValue("authenticatedUsers", out var users) || !users.IsBsonArray || users.AsBsonArray.Count == 0)
        {
            return new AtlasCheck { Name = "catalog", Status = "fail", Message = $"the cluster accepted the connection but no sign-in as {user}" };
        }
        var actions = Actions(info);
        var reads = actions.Where(ReadsDocuments.Contains).ToList();
        var changes = actions.Where(Changes.Contains).ToList();
        if (reads.Count == 0 && changes.Count == 0)
        {
            return new AtlasCheck
            {
                Name = "catalog",
                Status = "ok",
                Message = $"{user} signs in and may list databases and collections and read their sizes, not their documents ({string.Join(", ", actions)})",
            };
        }
        var what = new List<string>();
        if (reads.Count > 0)
        {
            what.Add($"read documents ({string.Join(", ", reads)})");
        }
        if (changes.Count > 0)
        {
            what.Add($"change them ({string.Join(", ", changes.Take(8))}{(changes.Count > 8 ? ", ..." : "")})");
        }
        return new AtlasCheck
        {
            Name = "catalog",
            Status = "warn",
            Message = $"{user} may also {string.Join(" and ", what)}. The agent never does, but a catalog user needs only "
                + "listDatabases, listCollections, collStats and dbStats: give it a role with those four",
        };
    }

    /// <summary>Every action the user's privileges hold, in name order, from connectionStatus's authInfo.</summary>
    internal static IReadOnlyList<string> Actions(BsonDocument authInfo)
    {
        var actions = new SortedSet<string>(StringComparer.Ordinal);
        if (authInfo.TryGetValue("authenticatedUserPrivileges", out var privileges) && privileges.IsBsonArray)
        {
            foreach (var privilege in privileges.AsBsonArray.Where(p => p.IsBsonDocument).Select(p => p.AsBsonDocument))
            {
                if (privilege.TryGetValue("actions", out var list) && list.IsBsonArray)
                {
                    actions.UnionWith(list.AsBsonArray.Where(a => a.IsString).Select(a => a.AsString));
                }
            }
        }
        return actions.ToList();
    }

    /// <summary>
    /// Every (database, collection, type) to report, up to <see cref="MaxCollections"/>, and whether there were more.
    /// The listing stops at the first one past the cap: what follows it is not even listed.
    /// </summary>
    private async Task<(List<Listed> Listed, bool Truncated)> ListAsync(CancellationToken ct)
    {
        var listed = new List<Listed>();
        var databases = (await _reads.DatabaseNamesAsync(ct)).Where(d => !SkippedDatabases.Contains(d)).Order(StringComparer.Ordinal);
        foreach (var database in databases)
        {
            var collections = (await _reads.CollectionsAsync(database, ct))
                .Select(c => new Listed(database, Str(c, "name"), Str(c, "type") is { Length: > 0 } type ? type : "collection"))
                .Where(c => c.Name.Length > 0 && !c.Name.StartsWith("system.", StringComparison.Ordinal))
                .OrderBy(c => c.Name, StringComparer.Ordinal);
            foreach (var collection in collections)
            {
                if (listed.Count == MaxCollections)
                {
                    return (listed, true);
                }
                listed.Add(collection);
            }
            TaskHeartbeat.Beat();
        }
        return (listed, false);
    }

    /// <summary>The answers of one collection's <c>$collStats</c>, summed over shards; null when there was none.</summary>
    internal static CollectionStats? Sum(IReadOnlyList<BsonDocument> answers)
    {
        if (answers.Count == 0)
        {
            return null;
        }
        long count = 0, size = 0, storageSize = 0, indexSize = 0, indexes = 0;
        var capped = false;
        foreach (var answer in answers)
        {
            var storage = Doc(answer, "storageStats");
            // The count stage's own figure; storageStats has the same one, read if the stage's is missing.
            count += answer.TryGetValue("count", out var c) && c.IsNumeric ? c.ToInt64() : Long(storage, "count");
            size += Long(storage, "size");
            storageSize += Long(storage, "storageSize");
            indexSize += Long(storage, "totalIndexSize");
            indexes = Math.Max(indexes, Long(storage, "nindexes"));
            capped |= storage.TryGetValue("capped", out var cap) && cap.IsBoolean && cap.AsBoolean;
        }
        return new CollectionStats(count, size, storageSize, indexSize, indexes, capped, Sharded: answers.Count > 1);
    }

    private static void Row(RowSink sink, string snapshotDate, string database, string collection, string type, CollectionStats s)
    {
        var w = sink.Writer;
        w.WriteStartObject();
        // 🔴 snapshot_date first on every catalog row, as docs/PROTOCOL.md says (AtlasCatalog.UsersAsync).
        w.WriteString("snapshot_date"u8, snapshotDate);
        w.WriteString("database"u8, database);
        // As it is, its own dots included.
        w.WriteString("collection"u8, collection);
        w.WriteString("type"u8, type);
        w.WriteNumber("count"u8, s.Count);
        w.WriteNumber("size"u8, s.Size);
        w.WriteNumber("storage_size"u8, s.StorageSize);
        w.WriteNumber("index_size"u8, s.IndexSize);
        w.WriteNumber("indexes"u8, s.Indexes);
        w.WriteBoolean("capped"u8, s.Capped);
        w.WriteBoolean("sharded"u8, s.Sharded);
        w.WriteEndObject();
        sink.EndRow();
    }

    private static BsonDocument Doc(BsonDocument d, string name) =>
        d.TryGetValue(name, out var v) && v.IsBsonDocument ? v.AsBsonDocument : new BsonDocument();

    private static long Long(BsonDocument d, string name) => d.TryGetValue(name, out var v) && v.IsNumeric ? v.ToInt64() : 0;

    private static string Str(BsonDocument d, string name) => d.TryGetValue(name, out var v) && v.IsString ? v.AsString : "";

    private sealed record Listed(string Database, string Name, string Type);

    internal sealed record CollectionStats(long Count, long Size, long StorageSize, long IndexSize, long Indexes, bool Capped, bool Sharded)
    {
        /// <summary>A view's, and the truncated row's: zeros and false.</summary>
        public static readonly CollectionStats None = new(0, 0, 0, 0, 0, false, false);
    }
}
