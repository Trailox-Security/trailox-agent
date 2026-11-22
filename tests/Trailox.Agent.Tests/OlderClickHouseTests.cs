using System.Net;
using System.Text;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.ClickHouse;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests;

/// <summary>
/// ClickHouse 23.10 is the oldest release the agent reads. Its system logs have no hostname column (every log gained one
/// in 23.11), so the agent reads the server's FQDN() in its place - the value the column holds where it exists.
/// </summary>
public class OlderClickHouseTests
{
    private static EndpointConfig Endpoint(string cluster = "") => new()
    {
        Alias = "dev", Engine = "clickhouse", Host = "ch.example.test", Port = 8443, Username = "trailox_monitor",
        Password = "not-a-real-password", ClusterName = cluster,
    };

    private static readonly IReadOnlySet<string> AllColumns = RawSelects.QueryLogColumns.ToHashSet(StringComparer.OrdinalIgnoreCase);

    /// <summary>What system.query_log has on 23.10.3.5 of the columns the agent reads.</summary>
    private static readonly IReadOnlySet<string> Columns2310 = AllColumns
        .Where(c => c is not ("hostname" or "authenticated_user" or "used_privileges" or "missing_privileges" or "is_internal"))
        .ToHashSet(StringComparer.OrdinalIgnoreCase);

    [Theory]
    [InlineData("23.9.6.20", true)]
    [InlineData("22.8.21.38", true)]
    [InlineData("23.10.3.5", false)]
    [InlineData("23.11.1.2711", false)]
    [InlineData("25.3.6.10034.altinitystable", false)]
    [InlineData("26.6.1.2455", false)]
    [InlineData("", false)]
    [InlineData("unknown", false)]
    public void The_floor_is_23_10_and_an_unreadable_version_is_left_to_the_column_probe(string version, bool older)
    {
        Assert.Equal(older, RawSelects.IsOlderThanMinimum(version));
    }

    [Fact]
    public void Hostname_is_not_a_required_column()
    {
        Assert.DoesNotContain("hostname", RawSelects.RequiredColumns);
        Assert.Contains("hostname", RawSelects.ProbedColumns);
    }

    [Fact]
    public void Without_the_column_the_statement_read_selects_the_server_s_fqdn_in_its_place()
    {
        var sql = RawSelects.Events(Endpoint(), Columns2310, 1, 2);

        Assert.StartsWith("SELECT FQDN() AS hostname, query_id, toString(type) AS type, event_time_microseconds,", sql);
        Assert.DoesNotContain("hostName()", sql);
        Assert.DoesNotContain("is_internal", sql);
        Assert.StartsWith("SELECT hostname, query_id,", RawSelects.Events(Endpoint(), AllColumns, 1, 2));
    }

    [Fact]
    public void The_sign_in_read_follows_the_query_log_s_column()
    {
        var task = new AgentTask { ChunkId = "s", Stream = ProtocolInfo.Streams.Sessions, StartMicros = 1, EndMicros = 2 };

        Assert.StartsWith("SELECT FQDN() AS hostname, toString(type) AS type,", RawSelects.ForTask(Endpoint(), Columns2310, task));
        Assert.StartsWith("SELECT hostname, toString(type) AS type,", RawSelects.ForTask(Endpoint(), AllColumns, task));
        Assert.Contains("FROM clusterAllReplicas('default', system.session_log)", RawSelects.ForTask(Endpoint("default"), Columns2310, task));
    }

    /// <summary>A server of the given version with the given query_log columns; every other probe answers as a healthy one.</summary>
    private sealed class Server : HttpMessageHandler
    {
        public string Version { get; init; } = "23.10.3.5";
        public IReadOnlySet<string> Columns { get; init; } = Columns2310;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var sql = await request.Content!.ReadAsStringAsync(ct);
            var json = sql switch
            {
                _ when sql == RawSelects.ProbeVersion => $"[[\"{Version}\"]]",
                _ when sql == RawSelects.ProbeColumns => "[[[" + string.Join(", ", Columns.Select(c => "\"" + c + "\"")) + "]]]",
                _ when sql == RawSelects.ProbeSessionLog => "[[1]]",
                _ when sql.Contains("FROM system.tables WHERE database = 'system' AND match(name") => "[[\"1760000000000000\", \"0\", \"0\"]]",
                _ => "[[\"0\"]]",
            };
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{\"data\": " + json + "}", Encoding.UTF8, "application/json") };
        }
    }

    [Fact]
    public async Task A_23_10_server_passes_the_probe_and_reports_the_columns_it_has()
    {
        var caps = await new ChSource(new HttpClient(new Server()), Endpoint()).ProbeAsync(CancellationToken.None);

        Assert.Equal("23.10.3.5", caps.Version);
        Assert.DoesNotContain("hostname", caps.Columns);
        Assert.Contains("normalized_query_hash", caps.Columns);
    }

    [Fact]
    public async Task An_older_server_is_refused_with_the_release_it_needs()
    {
        var ex = await Assert.ThrowsAsync<SourceException>(() =>
            new ChSource(new HttpClient(new Server { Version = "23.8.16.40" }), Endpoint()).ProbeAsync(CancellationToken.None));

        Assert.Equal("server 23.8.16.40 is too old; ClickHouse 23.10 or newer is required", ex.Message);
    }

    [Fact]
    public async Task A_server_missing_a_required_column_names_it_and_the_release_it_needs()
    {
        var ex = await Assert.ThrowsAsync<SourceException>(() =>
            new ChSource(new HttpClient(new Server { Version = "unknown", Columns = Columns2310.Where(c => c != "normalized_query_hash").ToHashSet() }), Endpoint())
                .ProbeAsync(CancellationToken.None));

        Assert.Equal("server unknown lacks system.query_log columns normalized_query_hash; ClickHouse 23.10 or newer is required", ex.Message);
    }
}
