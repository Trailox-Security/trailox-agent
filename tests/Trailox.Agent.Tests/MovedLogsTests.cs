using System.Net;
using System.Text;
using Trailox.Agent.Config;
using Trailox.Agent.Engines.ClickHouse;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests;

/// <summary>
/// A ClickHouse server can move its query log aside (query_log_1, ...): ClickHouse Cloud does at every server
/// replacement. The agent reads the moved copies only when its probe can see one, and says in its caps when the log was
/// replaced and when a copy was attached.
/// </summary>
public class MovedLogsTests
{
    private static EndpointConfig Endpoint(string cluster = "") => new()
    {
        Alias = "prod", Engine = "clickhouse", Host = "ch.example.test", Port = 8443, Username = "trailox_monitor",
        Password = "not-a-real-password", ClusterName = cluster,
    };

    private static readonly IReadOnlySet<string> AllColumns = RawSelects.QueryLogColumns.ToHashSet(StringComparer.OrdinalIgnoreCase);

    [Fact]
    public void The_moved_sources_read_the_log_and_every_numbered_copy()
    {
        Assert.Equal("merge('system', '^query_log(_[0-9]+)?$')", RawSelects.QueryLogSource(Endpoint(), moved: true));
        Assert.Equal("clusterAllReplicas('default', merge('system', '^session_log(_[0-9]+)?$'))",
            RawSelects.SessionLogSource(Endpoint("default"), moved: true));
        Assert.Equal("clusterAllReplicas('default', system.query_log)", RawSelects.QueryLogSource(Endpoint("default")));
    }

    [Fact]
    public void A_window_reads_the_copies_only_when_told_to()
    {
        var events = new AgentTask { ChunkId = "e", Stream = ProtocolInfo.Streams.Events, StartMicros = 1, EndMicros = 2 };
        var sessions = new AgentTask { ChunkId = "s", Stream = ProtocolInfo.Streams.Sessions, StartMicros = 1, EndMicros = 2 };

        Assert.Contains("\nFROM merge('system', '^query_log(_[0-9]+)?$')\n", RawSelects.ForTask(Endpoint(), AllColumns, events, queryLogMoved: true));
        Assert.Contains("\nFROM merge('system', '^session_log(_[0-9]+)?$')\n", RawSelects.ForTask(Endpoint(), AllColumns, sessions, sessionLogMoved: true));
        Assert.Contains("\nFROM system.query_log\n", RawSelects.ForTask(Endpoint(), AllColumns, events, sessionLogMoved: true));
        Assert.Contains("\nFROM system.session_log\n", RawSelects.ForTask(Endpoint(), AllColumns, sessions, queryLogMoved: true));

        // A moved read is bounded by its days, so each copy is pruned by partition; the live log's read is as it was.
        const string days = " AND event_date BETWEEN toDate(fromUnixTimestamp64Micro(1)) AND toDate(fromUnixTimestamp64Micro(2))";
        Assert.Contains(days, RawSelects.ForTask(Endpoint(), AllColumns, events, queryLogMoved: true));
        Assert.Contains(days, RawSelects.ForTask(Endpoint(), AllColumns, sessions, sessionLogMoved: true));
        Assert.DoesNotContain("event_date", RawSelects.ForTask(Endpoint(), AllColumns, events));
        Assert.DoesNotContain("event_date", RawSelects.ForTask(Endpoint(), AllColumns, sessions));
    }

    [Fact]
    public void The_log_table_probe_reads_system_tables_on_every_replica()
    {
        Assert.Equal(
            "SELECT toInt64(toUnixTimestamp(maxIf(metadata_modification_time, name = 'query_log'))) * 1000000, " +
            "toInt64(toUnixTimestamp(maxIf(metadata_modification_time, match(name, '^query_log_[0-9]+$')))) * 1000000, " +
            "countIf(match(name, '^session_log_[0-9]+$')) FROM clusterAllReplicas('default', system.tables) " +
            "WHERE database = 'system' AND match(name, '^(query|session)_log(_[0-9]+)?$')",
            RawSelects.ProbeLogTables(Endpoint("default")));
    }

    /// <summary>A server that answers each probe statement, and records every statement it was sent.</summary>
    private sealed class Server : HttpMessageHandler
    {
        public long Created { get; init; }
        public long Attached { get; init; }
        public long SessionCopies { get; init; }
        public bool LogTablesFail { get; set; }
        public List<string> Sent { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var sql = await request.Content!.ReadAsStringAsync(ct);
            Sent.Add(sql);
            string? json = sql switch
            {
                _ when sql == RawSelects.ProbeVersion => "[[\"26.6.1.2455\"]]",
                _ when sql == RawSelects.ProbeColumns => "[[" + "[" + string.Join(", ", RawSelects.QueryLogColumns.Select(c => "\"" + c + "\"")) + "]" + "]]",
                _ when sql == RawSelects.ProbeSessionLog => "[[1]]",
                _ when sql.Contains("FROM system.tables WHERE database = 'system' AND match(name") =>
                    LogTablesFail ? null : $"[[\"{Created}\", \"{Attached}\", \"{SessionCopies}\"]]",
                _ when sql.Contains("min(event_time_microseconds)") => "[[\"1761955200000000\"]]",
                _ => "",
            };
            if (json == null)
            {
                return new HttpResponseMessage(HttpStatusCode.InternalServerError) { Content = new StringContent("Code: 497. Not enough privileges", Encoding.UTF8) };
            }
            var body = json.Length == 0 ? "" : "{\"data\": " + json + "}";
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
        }
    }

    [Fact]
    public async Task A_visible_copy_is_reported_in_the_caps_and_read_by_the_next_windows()
    {
        var server = new Server { Created = 1760000000000000, Attached = 1760000600000000, SessionCopies = 1 };
        var source = new ChSource(new HttpClient(server), Endpoint());

        var caps = await source.ProbeAsync(CancellationToken.None);
        await using (await source.OpenAsync(new AgentTask { ChunkId = "e", Stream = ProtocolInfo.Streams.Events, StartMicros = 1, EndMicros = 2 }, CancellationToken.None)) { }
        await using (await source.OpenAsync(new AgentTask { ChunkId = "s", Stream = ProtocolInfo.Streams.Sessions, StartMicros = 1, EndMicros = 2 }, CancellationToken.None)) { }

        Assert.Equal(1760000000000000, caps.QueryLogCreatedMicros);
        Assert.Equal(1760000600000000, caps.MovedLogAttachedMicros);
        // The oldest-row probe stays on the live log: it reads every row it covers.
        Assert.Contains(server.Sent, s => s.Contains("min(event_time_microseconds)), 0)) FROM system.query_log"));
        Assert.DoesNotContain(server.Sent, s => s.Contains("min(event_time_microseconds)), 0)) FROM merge("));
        Assert.Contains("\nFROM merge('system', '^query_log(_[0-9]+)?$')\n", server.Sent[^2]);
        Assert.Contains("\nFROM merge('system', '^session_log(_[0-9]+)?$')\n", server.Sent[^1]);
    }

    [Fact]
    public async Task A_log_table_probe_that_fails_later_keeps_reading_the_copies()
    {
        var server = new Server { Created = 1760000000000000, Attached = 1760000600000000, SessionCopies = 1 };
        var source = new ChSource(new HttpClient(server), Endpoint());
        await source.ProbeAsync(CancellationToken.None);

        server.LogTablesFail = true;                      // the hourly re-probe, while the replicas churn
        var caps = await source.ProbeAsync(CancellationToken.None);
        await using (await source.OpenAsync(new AgentTask { ChunkId = "e", Stream = ProtocolInfo.Streams.Events, StartMicros = 1, EndMicros = 2 }, CancellationToken.None)) { }

        Assert.Equal(0, caps.MovedLogAttachedMicros);     // no times to report
        Assert.Contains("\nFROM merge('system', '^query_log(_[0-9]+)?$')\n", server.Sent[^1]);
    }

    [Fact]
    public async Task No_visible_copy_or_a_failed_probe_leaves_every_statement_as_it_was()
    {
        foreach (var server in new[] { new Server { Created = 1760000000000000 }, new Server { LogTablesFail = true } })
        {
            var source = new ChSource(new HttpClient(server), Endpoint());

            var caps = await source.ProbeAsync(CancellationToken.None);
            await using (await source.OpenAsync(new AgentTask { ChunkId = "e", Stream = ProtocolInfo.Streams.Events, StartMicros = 1, EndMicros = 2 }, CancellationToken.None)) { }

            Assert.Equal(0, caps.MovedLogAttachedMicros);
            Assert.DoesNotContain(server.Sent, s => s.Contains("merge("));
            Assert.Contains("\nFROM system.query_log\n", server.Sent[^1]);
        }
    }
}
