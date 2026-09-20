namespace Trailox.Agent.Gateway;

/// <summary>
/// How the agent holds its connection to Trailox.
/// </summary>
/// <remarks>
/// A pooled connection is reused until it has sat idle for this long, and no longer. Whatever stands in
/// front of the gateway drops an idle connection on a timer of its own - measured on 2026-09-20, a
/// kept-alive connection was answered after 58 seconds idle and found closed after 62 - and a connection
/// reused at that moment fails. An agent checking in every 60 seconds did exactly that and logged
/// "check-in failed ... retrying" about once a minute, every retry succeeding.
/// The check-in interval is the server's to set and it now stays clear of that timer. This is the agent's
/// own guarantee, whatever the interval: it never reuses a connection old enough to have been dropped.
/// The cost is one TLS handshake per idle check-in, which is nothing.
/// </remarks>
internal static class GatewayHttp
{
    internal static readonly TimeSpan IdleConnectionLifetime = TimeSpan.FromSeconds(30);

    /// <summary>
    /// A connection in constant use is retired after this long, so a change of address behind the
    /// gateway is picked up in the middle of a long backfill. A request already in flight finishes on
    /// its connection; only reuse stops.
    /// </summary>
    /// <remarks>
    /// The HTTP client factory sets this, to the same two minutes, on the handler it builds itself - and
    /// not on one it is handed. Supplying a handler to set the idle timeout therefore drops it unless it
    /// is set here as well, which is what 1.3.3 did.
    /// </remarks>
    internal static readonly TimeSpan ConnectionLifetime = TimeSpan.FromMinutes(2);

    internal static SocketsHttpHandler NewHandler() => new()
    {
        PooledConnectionIdleTimeout = IdleConnectionLifetime,
        PooledConnectionLifetime = ConnectionLifetime,
    };
}
