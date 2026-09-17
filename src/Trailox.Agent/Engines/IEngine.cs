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
}
