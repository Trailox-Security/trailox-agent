using Trailox.Agent.Gateway;

namespace Trailox.Agent.Tests;

/// <summary>
/// The connection to Trailox must never be reused after the far side has dropped it.
/// </summary>
/// <remarks>
/// Measured on 2026-09-20: a kept-alive connection was answered after 58 seconds idle and found closed
/// after 62, and an agent checking in every 60 seconds logged "check-in failed ... retrying" about once a
/// minute, every retry succeeding. The check-in interval is the server's to set; this is the agent's own
/// guarantee, whatever that interval is.
/// </remarks>
public class GatewayConnectionTests
{
    [Fact]
    public void The_connection_to_Trailox_is_dropped_well_before_a_load_balancer_would_drop_it()
    {
        using var handler = GatewayHttp.NewHandler();

        Assert.InRange(handler.PooledConnectionIdleTimeout, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(30));
    }
}
