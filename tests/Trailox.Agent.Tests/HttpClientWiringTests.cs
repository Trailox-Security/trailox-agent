using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.Databricks;
using Trailox.Agent.Engines.Snowflake;
using Trailox.Agent.Gateway;

namespace Trailox.Agent.Tests;

/// <summary>
/// What each of the agent's HTTP clients really gets, asked of the factory that builds them.
/// </summary>
/// <remarks>
/// The HTTP client factory retires a connection after two minutes of constant use - on a handler it
/// builds itself, and NOT on one it is handed. A client configured with a handler of its own loses that
/// without a word: 1.3.3 did it to the gateway client, and the Snowflake client had been that way since
/// 1.3.0. Testing the handler a client is given would miss exactly this, because the mistake is in what
/// the factory then does with it. So these ask the factory for the finished handler of every named
/// client and walk down to the one that owns the sockets.
/// </remarks>
public class HttpClientWiringTests
{
    public static TheoryData<string> EveryClient()
    {
        var names = new TheoryData<string> { Program.GatewayClientName, DatabricksEngine.LinksClientName };
        foreach (var engine in EngineRegistry.Names)
        {
            names.Add(engine);
        }
        return names;
    }

    private static SocketsHttpHandler SocketsOf(string clientName)
    {
        var services = new ServiceCollection();
        Program.AddHttpClients(services);
        var provider = services.BuildServiceProvider();

        HttpMessageHandler handler = provider.GetRequiredService<IHttpMessageHandlerFactory>().CreateHandler(clientName);
        while (handler is DelegatingHandler { InnerHandler: not null } wrapper)
        {
            handler = wrapper.InnerHandler;
        }
        return Assert.IsType<SocketsHttpHandler>(handler);
    }

    /// <summary>
    /// So a change of address behind a host is picked up in the middle of a long backfill, and not only
    /// once the agent goes quiet. A request in flight finishes on its connection; only reuse stops.
    /// </summary>
    [Theory]
    [MemberData(nameof(EveryClient))]
    public void Every_client_retires_a_connection_after_two_minutes_of_constant_use(string clientName)
    {
        Assert.Equal(TimeSpan.FromMinutes(2), SocketsOf(clientName).PooledConnectionLifetime);
    }

    [Fact]
    public void The_gateway_client_never_reuses_a_connection_that_sat_idle_long_enough_to_be_dropped()
    {
        Assert.Equal(GatewayHttp.IdleConnectionLifetime, SocketsOf(Program.GatewayClientName).PooledConnectionIdleTimeout);
    }

    /// <summary>Snowflake serves every result partition after the first gzip-compressed.</summary>
    [Fact]
    public void The_Snowflake_client_still_decompresses_what_it_is_sent()
    {
        Assert.Equal(DecompressionMethods.All, SocketsOf(new SnowflakeEngine().Name).AutomaticDecompression);
    }
}
