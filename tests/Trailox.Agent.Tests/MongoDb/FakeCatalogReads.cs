using System.Collections.Concurrent;
using MongoDB.Bson;
using Trailox.Agent.Engines.MongoDb;

namespace Trailox.Agent.Tests.MongoDb;

/// <summary>
/// The catalog user's reads without a cluster: databases, their collections and each collection's <c>$collStats</c>
/// answers as a test sets them, shaped as an M10 answered on 2026-10-21. Records every call.
/// </summary>
internal sealed class FakeCatalogReads : IMongoCatalogReads
{
    private int _inFlight;

    public List<string> Databases { get; } = new();

    /// <summary>Each database's listCollections answer: {name, type}, as nameOnly gives it.</summary>
    public Dictionary<string, List<BsonDocument>> Collections { get; } = new(StringComparer.Ordinal);

    /// <summary>One collection's $collStats answers; by default one replica-set answer with its name's length as the count.</summary>
    public Func<string, string, IReadOnlyList<BsonDocument>> Stats { get; set; } = (_, c) => [CollStats(count: c.Length, size: 100, storageSize: 4096, indexSize: 4096, indexes: 1)];

    public BsonDocument Status { get; set; } = ConnectionStatus("trailox_catalog", "listDatabases", "listCollections", "collStats", "dbStats");

    /// <summary>Thrown by every read, as the driver's failures reach the agent: already a SourceException.</summary>
    public Exception? Failure { get; set; }

    /// <summary>A delay inside each $collStats, so calls in flight overlap.</summary>
    public TimeSpan StatsDelay { get; set; }

    public ConcurrentQueue<string> Calls { get; } = new();
    public int MaxInFlight { get; private set; }
    public bool Disposed { get; private set; }

    public Task<IReadOnlyList<string>> DatabaseNamesAsync(CancellationToken ct)
    {
        Record("listDatabases");
        return Task.FromResult<IReadOnlyList<string>>(Databases.ToList());
    }

    public Task<IReadOnlyList<BsonDocument>> CollectionsAsync(string database, CancellationToken ct)
    {
        Record($"listCollections {database}");
        return Task.FromResult<IReadOnlyList<BsonDocument>>(Collections.GetValueOrDefault(database) ?? new List<BsonDocument>());
    }

    public async Task<IReadOnlyList<BsonDocument>> CollStatsAsync(string database, string collection, CancellationToken ct)
    {
        Record($"$collStats {database}.{collection}");
        var now = Interlocked.Increment(ref _inFlight);
        lock (Calls)
        {
            MaxInFlight = Math.Max(MaxInFlight, now);
        }
        try
        {
            if (StatsDelay > TimeSpan.Zero)
            {
                await Task.Delay(StatsDelay, ct);
            }
            return Stats(database, collection);
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
        }
    }

    public Task<BsonDocument> ConnectionStatusAsync(CancellationToken ct)
    {
        Record("connectionStatus");
        return Task.FromResult(Status);
    }

    public void Dispose() => Disposed = true;

    private void Record(string call)
    {
        Calls.Enqueue(call);
        if (Failure != null)
        {
            throw Failure;
        }
    }

    public static BsonDocument Collection(string name, string type = "collection") => new() { { "name", name }, { "type", type } };

    /// <summary>One $collStats answer with storageStats and count, as a replica set (no shard) or one shard gives it.</summary>
    public static BsonDocument CollStats(long count, long size, long storageSize, long indexSize, int indexes, bool capped = false, string? shard = null)
    {
        var answer = new BsonDocument { { "ns", "db.coll" }, { "host", "atlas-abcdef-shard-00-02.example.mongodb.net:27017" } };
        if (shard != null)
        {
            answer["shard"] = shard;
        }
        answer["storageStats"] = new BsonDocument
        {
            // Atlas answers sizes as 32-bit or 64-bit integers, whichever fits; the count here as a 64-bit one.
            { "size", size }, { "count", count }, { "storageSize", storageSize }, { "freeStorageSize", 0 }, { "capped", capped },
            { "wiredTiger", new BsonDocument("type", "file") }, { "nindexes", indexes }, { "totalIndexSize", indexSize },
            { "indexSizes", new BsonDocument("_id_", indexSize) },
        };
        answer["count"] = count;
        return answer;
    }

    /// <summary>connectionStatus with showPrivileges for a user whose one privilege holds <paramref name="actions"/> on all databases.</summary>
    public static BsonDocument ConnectionStatus(string user, params string[] actions) => new()
    {
        {
            "authInfo", new BsonDocument
            {
                { "authenticatedUsers", new BsonArray { new BsonDocument { { "user", user }, { "db", "admin" } } } },
                { "authenticatedUserRoles", new BsonArray { new BsonDocument { { "role", "trailoxCatalog" }, { "db", "admin" } } } },
                {
                    "authenticatedUserPrivileges", new BsonArray
                    {
                        new BsonDocument
                        {
                            { "resource", new BsonDocument { { "db", "" }, { "collection", "" } } },
                            { "actions", new BsonArray(actions) },
                        },
                    }
                },
            }
        },
        { "ok", 1.0 },
    };
}
