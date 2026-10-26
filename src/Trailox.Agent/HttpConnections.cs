namespace Trailox.Agent;

/// <summary>
/// What every HTTP client of the agent does with a pooled connection.
/// </summary>
internal static class HttpConnections
{
    /// <summary>
    /// A connection in constant use is retired after this long, so a change of address behind a host is
    /// picked up in the middle of a long backfill and not only once the agent goes quiet. A request
    /// already in flight finishes on its connection; only reuse stops.
    /// </summary>
    /// <remarks>
    /// The HTTP client factory sets this, to the same two minutes, on a handler it builds itself - and
    /// not on one it is handed. So every client that supplies a handler of its own has to set it too,
    /// or loses it without a word: 1.3.3 did that to the gateway client, and the Snowflake client had
    /// been that way since 1.3.0. HttpClientWiringTests asks the factory what each client really gets.
    /// </remarks>
    internal static readonly TimeSpan Lifetime = TimeSpan.FromMinutes(2);
}
