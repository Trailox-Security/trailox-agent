using System.Net;
using System.Text;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.ClickHouse;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests;

/// <summary>
/// Agent 1.7 reads system.settings_profile_elements (catalog_settings), whose grant older setup scripts did not ask for.
/// A server that refuses it answers 497 ACCESS_DENIED: that one refusal is a missing optional grant, reported to Trailox
/// and kept off the agent's error list. The same answer on any other stream is an ordinary source failure.
/// </summary>
public class SettingsGrantTests
{
    private const string Refused =
        "Code: 497. DB::Exception: trailox_monitor: Not enough privileges. To execute this query, it's necessary to have the grant " +
        "SELECT(profile_name, user_name, role_name, index, setting_name, value, writability, inherit_profile) ON system.settings_profile_elements. " +
        "(ACCESS_DENIED) (version 25.3.6.10034.altinitystable)";

    private sealed class RefusingHost : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.InternalServerError) { Content = new StringContent(Refused, Encoding.UTF8, "text/plain") });
    }

    private static ChSource Source() => new(new HttpClient(new RefusingHost()), new EndpointConfig
    {
        Alias = "qa-trd-snaps", Engine = "clickhouse", Host = "ch.example.test", Port = 8443, Username = "trailox_monitor", Password = "not-a-real-password",
    });

    [Fact]
    public async Task A_refused_settings_stream_is_a_missing_optional_grant()
    {
        var ex = await Assert.ThrowsAsync<SourceException>(() =>
            Source().OpenAsync(new AgentTask { ChunkId = "c", Stream = ProtocolInfo.Streams.CatalogSettings }, CancellationToken.None));

        Assert.True(ex.MissingOptionalGrant);
        Assert.StartsWith("ClickHouse answered HTTP 500: Code: 497.", ex.Message);   // Trailox recognises the refusal by this text
    }

    [Fact]
    public async Task The_same_refusal_on_another_stream_is_an_ordinary_failure()
    {
        var ex = await Assert.ThrowsAsync<ChException>(() =>
            Source().OpenAsync(new AgentTask { ChunkId = "c", Stream = ProtocolInfo.Streams.CatalogGrants }, CancellationToken.None));

        Assert.False(ex.MissingOptionalGrant);
        Assert.True(ex.IsAccessDenied);
    }

    [Theory]
    [InlineData("ClickHouse answered HTTP 500: " + Refused, true)]
    [InlineData("ClickHouse answered HTTP 403: Code: 497. DB::Exception: Not enough privileges.", true)]
    [InlineData("ClickHouse answered HTTP 404: Code: 47. DB::Exception: Missing columns: 'writability'. (UNKNOWN_IDENTIFIER)", false)]
    public void Only_ClickHouse_code_497_is_access_denied(string message, bool denied)
    {
        Assert.Equal(denied, new ChException(500, message).IsAccessDenied);
    }
}
