using Trailox.Agent.Config;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Engines;

/// <summary>
/// One database engine the agent can read. Adding an engine is one class implementing this
/// plus one line in <see cref="EngineRegistry"/>; nothing in the runner knows which engine it
/// is driving.
/// </summary>
public interface IEngine
{
    /// <summary>Lower-case name as written in agent.yaml and reported to Trailox ('clickhouse').</summary>
    string Name { get; }

    /// <summary>
    /// Engine-specific validation of an endpoint's config: which fields it needs and what they
    /// must look like. Returns problems as "field: message"; empty means valid. May fill in
    /// engine defaults (a port, a username) on the config it is given.
    /// </summary>
    IReadOnlyList<string> Validate(EndpointConfig endpoint);

    /// <summary>
    /// Whether an endpoint may name the optional catalog user (catalog_username and its password reference,
    /// resolved by <see cref="ConfigLoader"/>). MongoDB Atlas only (1.9.0); every other engine refuses the keys.
    /// </summary>
    bool TakesCatalogUser => false;

    /// <summary>A live connection to one endpoint of this engine.</summary>
    IEngineSource CreateSource(EndpointConfig endpoint, IHttpClientFactory httpClientFactory);
}

/// <summary>
/// A connection to one configured endpoint: what it is (probe) and the raw rows for one task.
/// </summary>
public interface IEngineSource
{
    /// <summary>What the server is and holds. Throws on failure; the session records it as ProbeError.</summary>
    Task<EndpointCaps> ProbeAsync(CancellationToken ct);

    /// <summary>
    /// The rows for one task as a stream the caller forwards to Trailox, or null when the
    /// stream named by the task is not one this engine produces. The caller disposes it.
    /// </summary>
    Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct);
}

/// <summary>Rows in flight from the database to the gateway; disposing releases the connection.</summary>
public sealed class RowStream : IAsyncDisposable
{
    private readonly IDisposable _owner;

    public Stream Body { get; }

    /// <summary>
    /// The body can go quiet for minutes while the source works (agent 1.8: MongoDB's audit files, read while
    /// the rows stream). The upload then writes blank lines (never inside a row) and flushes, so the connection
    /// is never idle long enough for a proxy or firewall on the path to close it. Set only for a stream whose
    /// body may contain blank lines (PROTOCOL.md §4: MongoDB's log streams, and every PostgreSQL stream).
    /// </summary>
    public bool KeepAlive { get; init; }

    public RowStream(Stream body, IDisposable owner)
    {
        Body = body;
        _owner = owner;
    }

    public async ValueTask DisposeAsync()
    {
        await Body.DisposeAsync();
        _owner.Dispose();
    }
}

/// <summary>The database refused or failed a statement. Engine-neutral; engines derive from it.</summary>
public class SourceException : Exception
{
    public SourceException(string message) : base(message)
    {
    }

    /// <summary>
    /// The database refused a stream whose grant the customer may choose not to give (agent 1.7: catalog_settings).
    /// Reported as the task's failure, but not listed among the agent's errors: a grant the customer chose not to
    /// give is not a fault of the agent, and it would be listed again with every day's catalog.
    /// </summary>
    public bool MissingOptionalGrant { get; init; }

    /// <summary>
    /// The source has not published the whole window yet (agent 1.8: MongoDB Atlas, whose log files arrive
    /// minutes late). Reported with reason "not_yet_published", so Trailox plans the window again at a later
    /// poll; not listed among the agent's errors, because waiting for the source is not a fault.
    /// </summary>
    public bool NotYetPublished { get; init; }

    /// <summary>
    /// The database stopped the window's read at its time limit (agent 1.15: Snowflake's statement timeout). Reported
    /// with reason "window_too_long", so Trailox asks for shorter windows; the same window would fail the same way.
    /// Still listed among the agent's errors: a limit that also stops the shortest window needs the customer.
    /// </summary>
    public bool WindowTooLong { get; init; }

    /// <summary>
    /// The database rejected the password (agent 1.12: PostgreSQL's SQLSTATE 28P01). The agent stops signing in to the
    /// source until it restarts: it reads its secrets only at start, so a fixed password needs a restart anyway, and each
    /// attempt would write one more failed sign-in into the database's own log.
    /// </summary>
    public bool AuthenticationRejected { get; init; }
}
