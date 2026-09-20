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

    /// <summary>
    /// A connection in constant use is still retired every two minutes, so a change of address behind
    /// the gateway is picked up in the middle of a long backfill and not only once the agent goes quiet.
    /// </summary>
    /// <remarks>
    /// Every agent up to 1.3.2 had this without asking: it is what the HTTP client factory gives the
    /// handler it builds itself. 1.3.3 supplied its own handler, to set the idle timeout, and lost it.
    /// Measured on 2026-09-20 with both published images checking in every 10 seconds: 1.3.2 replaced
    /// its connection at 120 and 241 seconds, 1.3.3 was still on its first after 240.
    /// </remarks>
    [Fact]
    public void A_connection_in_constant_use_is_still_retired_every_two_minutes()
    {
        using var handler = GatewayHttp.NewHandler();

        Assert.Equal(TimeSpan.FromMinutes(2), handler.PooledConnectionLifetime);
    }
}
