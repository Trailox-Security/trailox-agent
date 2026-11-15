using System.Net;
using System.Text;
using System.Text.Json;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.ClickHouse;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests;

/// <summary>
/// Agent 1.17: catalog_show_grants sends every grant as SHOW GRANTS prints it, so that a wildcard grant (ON db*.*) can be
/// told from an exact one. SHOW GRANTS names its one column after itself and cannot carry a date, so the agent reads the
/// server's date first and sends each statement as {"snapshot_date", "statement"}.
/// </summary>
public class ShowGrantsTests
{
    private sealed class Host : HttpMessageHandler
    {
        public List<string> Sent { get; } = new();
        public List<string> Urls { get; } = new();
        public bool Refuse { get; init; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var sql = await request.Content!.ReadAsStringAsync(ct);
            Sent.Add(sql);
            Urls.Add(request.RequestUri!.Query);
            if (Refuse && sql.StartsWith("SHOW GRANTS", StringComparison.Ordinal))
            {
                return new HttpResponseMessage(HttpStatusCode.InternalServerError)
                {
                    Content = new StringContent("Code: 497. DB::Exception: trailox_monitor: Not enough privileges. (ACCESS_DENIED)"),
                };
            }
            var json = sql == RawSelects.ServerToday
                ? "{\"data\":[[\"2026-11-15\"]]}"
                : "{\"data\":[[\"GRANT SELECT ON db*.* TO w1\"],[\"GRANT SELECT ON `my-db`*.* TO `odd \\\"user` WITH GRANT OPTION\"],[\"GRANT wr TO w10\"]]}";
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(json, Encoding.UTF8, "application/json") };
        }
    }

    private static ChSource Source(Host host) => new(new HttpClient(host), new EndpointConfig
    {
        Alias = "src", Engine = "clickhouse", Host = "ch.example.test", Port = 8443, Username = "trailox_monitor", Password = "not-a-real-password",
    });

    private static AgentTask Task() => new() { ChunkId = "c", Stream = ProtocolInfo.Streams.CatalogShowGrants };

    [Fact]
    public async Task Each_statement_is_sent_as_printed_with_the_servers_date()
    {
        var host = new Host();

        await using var rows = (await Source(host).OpenAsync(Task(), CancellationToken.None))!;
        var lines = (await new StreamReader(rows.Body).ReadToEndAsync()).Split('\n', StringSplitOptions.RemoveEmptyEntries);

        Assert.Equal(new[] { RawSelects.ServerToday, RawSelects.ShowGrants }, host.Sent);
        Assert.All(host.Urls, u => Assert.Contains("log_comment=trailox-agent", u));
        var parsed = lines.Select(l => JsonSerializer.Deserialize<Dictionary<string, string>>(l)!).ToList();
        Assert.All(parsed, p => Assert.Equal(new[] { "snapshot_date", "statement" }, p.Keys));
        Assert.All(parsed, p => Assert.Equal("2026-11-15", p["snapshot_date"]));
        Assert.Equal(new[]
        {
            "GRANT SELECT ON db*.* TO w1",
            "GRANT SELECT ON `my-db`*.* TO `odd \"user` WITH GRANT OPTION",
            "GRANT wr TO w10",
        }, parsed.Select(p => p["statement"]));
    }

    /// <summary>Every setup script grants SHOW USERS and SHOW ROLES: a refusal is an ordinary failure, reported as such.</summary>
    [Fact]
    public async Task A_refusal_is_an_ordinary_failure()
    {
        var ex = await Assert.ThrowsAsync<ChException>(() => Source(new Host { Refuse = true }).OpenAsync(Task(), CancellationToken.None));

        Assert.True(ex.IsAccessDenied);
        Assert.False(ex.MissingOptionalGrant);
    }

    [Fact]
    public void The_statements_read_and_write_nothing_else()
    {
        Assert.Equal("SELECT toString(today())", RawSelects.ServerToday);
        Assert.Equal("SHOW GRANTS FOR ALL", RawSelects.ShowGrants);
        Assert.Equal("catalog_show_grants", ProtocolInfo.Streams.CatalogShowGrants);
    }
}
