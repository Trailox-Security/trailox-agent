namespace Trailox.Agent.Bench.Generate;

/// <summary>One client connection: who it is, where from, and with which app and driver.</summary>
internal sealed class Connection
{
    public required long Number { get; init; }
    public required string Ip { get; init; }
    public required int Port { get; init; }
    public required Guid Uuid { get; init; }
    public required string User { get; init; }
    public required string App { get; init; }
    public required string Driver { get; init; }
    public required string DriverVersion { get; init; }
    public required string Db { get; init; }

    /// <summary>The driver's session on this connection; reused by every operation, as drivers pool sessions.</summary>
    public Guid Lsid { get; } = System.Guid.NewGuid();

    public long TxnNumber { get; set; }

    public void Fill(LineValues v)
    {
        v.Set(Slot.Conn, Number);
        v.Set(Slot.Cip, Ip);
        v.Set(Slot.Cport, Port);
        v.Set(Slot.Uuid, Uuid.ToString());
        v.Set(Slot.Cuuid, Commands.Base64Of(Uuid));
        v.Set(Slot.User, User);
        v.Set(Slot.App, App);
        v.Set(Slot.Drv, Driver);
        v.Set(Slot.Drvver, DriverVersion);
        v.Set(Slot.Db, Db);
    }
}

/// <summary>
/// The clients of one node. "typical": a few applications on pooled connections from 20 addresses,
/// 50 query shapes, 16 namespaces. "hostile": every operation a new shape, address, connection and
/// one of 1,000 users, a 1,000-a-second connection churn and a sign-in spray.
/// </summary>
internal sealed class Population
{
    private static readonly (string User, string App, string Driver, string Version, int Weight)[] Apps =
    [
        ("app_orders", "orders-api", "nodejs", "6.10.0", 40),
        ("app_billing", "billing-worker", "PyMongo|c", "4.10.1", 20),
        ("app_search", "search-indexer", "mongo-java-driver|sync", "5.2.1", 20),
        ("app_reports", "reports", "mongo-csharp-driver", "3.0.0", 10),
        ("etl_nightly", "etl", "PyMongo|c", "4.10.1", 8),
        ("analyst_jane", "mongosh 2.3.2", "nodejs|mongosh", "6.10.0", 2),
    ];

    private static readonly string[] Databases = ["shop", "billing", "search", "reports", "analytics"];
    private static readonly string[] Collections = ["orders", "customers", "events"];

    internal readonly record struct Query(string Template, string F1, string F2, string Db, string Coll);

    // 50 queries in all, fixed per application (the same for every seed, so every corpus has the same shapes).
    private static readonly Dictionary<string, Query[]> Queries = BuildQueries();

    private static Dictionary<string, Query[]> BuildQueries()
    {
        var rng = new Random(50);
        var perApp = new[] { 12, 10, 10, 8, 6, 4 };
        var queries = new Dictionary<string, Query[]>();
        for (var a = 0; a < Apps.Length; a++)
        {
            var db = Databases[a % Databases.Length];
            queries[Apps[a].User] = Enumerable.Range(0, perApp[a]).Select(i =>
            {
                var set = rng.Next(5);
                // The orders application also stores files in GridFS, whose collection name has its own dot.
                var (qdb, coll) = a == 0 && i == 0 ? ("media", "fs.files") : (db, Collections[rng.Next(Collections.Length)]);
                return new Query(Commands.Pick(rng), Commands.FieldPool[set * 2], Commands.FieldPool[set * 2 + 1], qdb, coll);
            }).ToArray();
        }
        return queries;
    }

    private readonly CorpusSpec _spec;
    private Random _rng;
    private readonly Connection[] _pool;
    private long _nextConn;

    /// <param name="slice">The slices of a node are generated in parallel: the pool is the same in every
    /// slice of a node (seeded by the node alone), and short-lived connections get numbers unique to the slice.</param>
    public Population(CorpusSpec spec, int node, int slice, Random rng)
    {
        _spec = spec;
        _rng = new Random(spec.Seed * 1_000_003 + node * 7919);
        _nextConn = 1000;
        var poolSize = node == 0 ? 200 : 20;
        _pool = Enumerable.Range(0, poolSize).Select(_ => NewConnection()).ToArray();
        _rng = rng;
        _nextConn = 100_000 + slice * 1_000_000L;
    }

    public Connection ConnectionFor(Random rng) => _spec.Hostile ? NewConnection() : _pool[rng.Next(_pool.Length)];

    /// <summary>
    /// What the connection's application runs next. "typical": each application has its own fixed
    /// queries (command, fields, namespace), 50 shapes over the six of them, as an application's code
    /// does. "hostile": a new shape and a random namespace every time.
    /// </summary>
    public Query QueryFor(Connection connection, Random rng)
    {
        if (_spec.Hostile)
        {
            var (db, coll) = Namespace(rng);
            return new Query(Commands.Pick(rng), "f" + Commands.Hex(rng, 4), "g" + Commands.Hex(rng, 4), db, coll);
        }
        var queries = Queries[connection.User];
        return queries[rng.Next(queries.Length)];
    }

    public (string Db, string Coll) Namespace(Random rng)
    {
        // One GridFS collection, whose own dot must not be read as the database separator.
        if (rng.Next(16) == 0)
        {
            return ("media", "fs.files");
        }
        return (Databases[rng.Next(Databases.Length)], Collections[rng.Next(Collections.Length)]);
    }

    // Measured on an Atlas cluster under load: about 0.8 accepted connections a second per node.
    public double ChurnPerSecond(double share) => (_spec.Hostile ? 1000 : 1) * Math.Max(share, 0.05);

    public double FailuresPerSecond(int node) => node != 0 ? 0 : _spec.Hostile ? 100 : 0.1;

    /// <summary>A short-lived client (a script, a job, a serverless function): it connects, signs in and leaves.
    /// The pooled connections stay open for the whole corpus.</summary>
    public Connection Churn() => NewConnection();

    public Connection Attacker(Random rng) => new()
    {
        Number = _nextConn++,
        Ip = $"198.51.100.{rng.Next(1, 255)}",
        Port = rng.Next(30000, 65000),
        Uuid = new Guid(Commands.Hex(rng, 16)),
        User = "admin" + rng.Next(100),
        App = "",
        Driver = "PyMongo|c",
        DriverVersion = "4.10.1",
        Db = "admin",
    };

    private Connection NewConnection()
    {
        var app = PickApp();
        return new Connection
        {
            Number = _nextConn++,
            Ip = _spec.Hostile ? $"10.{_rng.Next(1, 255)}.{_rng.Next(256)}.{_rng.Next(1, 255)}" : $"10.1.0.{_rng.Next(1, 21)}",
            Port = _rng.Next(30000, 65000),
            Uuid = new Guid(Commands.Hex(_rng, 16)),
            User = _spec.Hostile ? "user" + _rng.Next(1000) : app.User,
            App = app.App,
            Driver = app.Driver,
            DriverVersion = app.Version,
            Db = Databases[Array.IndexOf(Apps, app) % Databases.Length],
        };
    }

    private (string User, string App, string Driver, string Version, int Weight) PickApp()
    {
        var at = _rng.Next(Apps.Sum(a => a.Weight));
        foreach (var a in Apps)
        {
            if (at < a.Weight)
            {
                return a;
            }
            at -= a.Weight;
        }
        return Apps[0];
    }
}
