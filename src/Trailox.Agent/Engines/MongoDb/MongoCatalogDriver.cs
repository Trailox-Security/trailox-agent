using MongoDB.Bson;
using MongoDB.Driver;

namespace Trailox.Agent.Engines.MongoDb;

/// <summary>
/// <see cref="IMongoCatalogReads"/> over MongoDB.Driver, as the optional catalog user. One client serves one task or
/// one probe and is closed after it, so the agent holds no connection to the cluster in between.
/// </summary>
/// <remarks>
/// TLS, SCRAM against <c>admin</c>, and the application name <c>trailox-agent/&lt;version&gt; catalog</c>, which marks its
/// sessions as the agent's. No compression is asked for, and the driver's logging is off. The password is handed to the
/// driver as a credential of its own, never inside a connection string that an error could echo.
/// </remarks>
internal sealed class MongoCatalogDriver : IMongoCatalogReads
{
    /// <summary>How long to look for a server: past it, the agent's address is most likely not let in.</summary>
    internal static readonly TimeSpan ServerSelectionTimeout = TimeSpan.FromSeconds(20);

    private readonly MongoClient _client;
    private readonly string _cluster;
    private readonly string _user;

    private MongoCatalogDriver(MongoClient client, string cluster, string user)
    {
        _client = client;
        _cluster = cluster;
        _user = user;
    }

    /// <summary>A client for the cluster's <c>connectionStrings.standardSrv</c> (mongodb+srv://...), as the catalog user.</summary>
    public static MongoCatalogDriver Open(string standardSrv, string cluster, string user, string password)
    {
        MongoClientSettings settings;
        try
        {
            settings = MongoClientSettings.FromConnectionString(standardSrv);
        }
        catch (MongoException ex)
        {
            throw new SourceException($"cluster {cluster}'s connection string could not be used ({Short(ex)})");
        }
        settings.Credential = MongoCredential.CreateCredential("admin", user, password);
        settings.UseTls = true;
        settings.ApplicationName = $"trailox-agent/{Program.Version} catalog";
        settings.ServerSelectionTimeout = ServerSelectionTimeout;
        settings.ConnectTimeout = ServerSelectionTimeout;
        settings.MaxConnectionPoolSize = MongoCatalog.Parallelism;
        settings.RetryWrites = false;
        return new MongoCatalogDriver(new MongoClient(settings), cluster, user);
    }

    public Task<IReadOnlyList<string>> DatabaseNamesAsync(CancellationToken ct) =>
        GuardAsync("listDatabases", async () =>
        {
            using var cursor = await _client.ListDatabaseNamesAsync(new ListDatabaseNamesOptions { AuthorizedDatabases = true }, ct);
            return (IReadOnlyList<string>)await cursor.ToListAsync(ct);
        });

    /// <remarks>
    /// The command itself: the driver's own listing offers neither nameOnly nor authorizedCollections, and nameOnly
    /// still answers each collection's type. Only the first batch is read, asked for without a count limit: it then
    /// stops only at 16 MB, tens of thousands of names, far past <see cref="MongoCatalog.MaxCollections"/>. A getMore
    /// could also reach another mongos than the one holding the cursor.
    /// </remarks>
    public Task<IReadOnlyList<BsonDocument>> CollectionsAsync(string database, CancellationToken ct) =>
        GuardAsync("listCollections", async () =>
        {
            var command = new BsonDocument
            {
                { "listCollections", 1 }, { "nameOnly", true }, { "authorizedCollections", true },
                { "cursor", new BsonDocument("batchSize", int.MaxValue) },
            };
            var answer = await _client.GetDatabase(database).RunCommandAsync<BsonDocument>(command, cancellationToken: ct);
            var cursor = answer.TryGetValue("cursor", out var c) && c.IsBsonDocument ? c.AsBsonDocument : new BsonDocument();
            var batch = cursor.TryGetValue("firstBatch", out var b) && b.IsBsonArray ? b.AsBsonArray : new BsonArray();
            return (IReadOnlyList<BsonDocument>)batch.Where(v => v.IsBsonDocument).Select(v => v.AsBsonDocument).ToList();
        });

    public Task<IReadOnlyList<BsonDocument>> CollStatsAsync(string database, string collection, CancellationToken ct) =>
        GuardAsync("$collStats", async () =>
        {
            var stage = new BsonDocument("$collStats", new BsonDocument { { "storageStats", new BsonDocument() }, { "count", new BsonDocument() } });
            // ⚠ ONLY THE FIGURES A ROW USES. storageStats carries the storage engine's own statistics too: measured on
            //   an Atlas cluster, 56,553 bytes an answer, against 159 with this projection, so about 565 MB a day at
            //   the 10,000-collection cap. Trimmed on the server, before it is sent.
            var keep = new BsonDocument("$project", new BsonDocument
            {
                { "_id", 0 }, { "shard", 1 }, { "count", 1 }, { "storageStats.count", 1 }, { "storageStats.size", 1 },
                { "storageStats.storageSize", 1 }, { "storageStats.totalIndexSize", 1 }, { "storageStats.nindexes", 1 },
                { "storageStats.capped", 1 },
            });
            try
            {
                var target = _client.GetDatabase(database).GetCollection<BsonDocument>(collection);
                using var cursor = await target.AggregateAsync(PipelineDefinition<BsonDocument, BsonDocument>.Create(stage, keep), cancellationToken: ct);
                return (IReadOnlyList<BsonDocument>)await cursor.ToListAsync(ct);
            }
            catch (MongoCommandException ex) when (ex.Code == NamespaceNotFound)
            {
                // Dropped since it was listed.
                return Array.Empty<BsonDocument>();
            }
        });

    public Task<BsonDocument> ConnectionStatusAsync(CancellationToken ct) =>
        GuardAsync("connectionStatus", () => _client.GetDatabase("admin")
            .RunCommandAsync<BsonDocument>(new BsonDocument { { "connectionStatus", 1 }, { "showPrivileges", true } }, cancellationToken: ct));

    public void Dispose() => _client.Dispose();

    private const int Unauthorized = 13;
    private const int NamespaceNotFound = 26;

    /// <summary>The driver's failures, each turned into what the customer has to fix.</summary>
    private async Task<T> GuardAsync<T>(string command, Func<Task<T>> read)
    {
        try
        {
            return await read();
        }
        catch (MongoAuthenticationException)
        {
            throw new SourceException($"cluster {_cluster} refused the catalog user's sign-in: check catalog_username ({_user}) and its password, "
                + "and that the database user exists in the project (Database Access) with access to this cluster");
        }
        catch (Exception ex) when (ex is TimeoutException or MongoConnectionException)
        {
            // Atlas's IP access list refuses the connection itself, so it surfaces as no server found in time. The
            // timeout's own text only describes the driver's server selector: it is left out.
            var cause = ex is TimeoutException ? $"no server answered in {ServerSelectionTimeout.TotalSeconds:0} s" : Short(Innermost(ex));
            throw new SourceException($"the agent could not reach cluster {_cluster} as the catalog user ({cause}): add the agent's "
                + "outbound address to the project's IP access list (Network Access), or reach the cluster over PrivateLink");
        }
        catch (MongoCommandException ex) when (ex.Code == Unauthorized)
        {
            throw new SourceException($"cluster {_cluster} refused {command} to the catalog user {_user}: its role needs listDatabases, "
                + "listCollections, collStats and dbStats on all databases");
        }
        catch (MongoException ex)
        {
            throw new SourceException($"cluster {_cluster} answered {command} with an error ({Short(ex)})");
        }
    }

    private static string Short(Exception ex) => ex.Message.Length > 200 ? ex.Message[..200] + "..." : ex.Message;

    private static Exception Innermost(Exception ex) => ex.InnerException is { } inner ? Innermost(inner) : ex;
}
