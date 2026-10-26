namespace Trailox.Agent.Gateway;

/// <summary>
/// How the agent holds its connection to Trailox.
/// </summary>
/// <remarks>
/// A pooled connection is reused until it has sat idle for this long, and no longer. A proxy or load balancer
/// on the way may drop an idle connection on a timer of its own, and a connection reused at that moment fails.
/// This is the agent's own guarantee, whatever the check-in interval: it never reuses a connection old enough
/// to have been dropped.
/// The cost is one TLS handshake per idle check-in, which is nothing.
/// </remarks>
internal static class GatewayHttp
{
    internal static readonly TimeSpan IdleConnectionLifetime = TimeSpan.FromSeconds(30);

    internal static SocketsHttpHandler NewHandler() => new()
    {
        PooledConnectionIdleTimeout = IdleConnectionLifetime,
        PooledConnectionLifetime = HttpConnections.Lifetime,
    };
}
