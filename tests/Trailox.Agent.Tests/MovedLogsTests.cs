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

        /// <summary>Refuse the read across the copies, as ClickHouse 23.10 does for a user without CREATE TEMPORARY TABLE.</summary>
        public bool MovedReadRefused { get; set; }

        /// <summary>Refuse it inside a 200, as 23.10 answers a JSON request that started before the refusal.</summary>
        public bool RefusalInsideOk { get; set; }

        /// <summary>Answer the proof with a server error that is not a refusal (a busy server, a replica that does not answer).</summary>
        public bool ProofServerError { get; set; }

        /// <summary>Let the proof time out, as the HTTP client reports it.</summary>
        public bool ProofTimesOut { get; set; }

        /// <summary>A copy written by an older server lacks a column the window reads (a server upgraded from 23.10).</summary>
        public bool CopyLacksAColumn { get; set; }

        public List<string> Sent { get; } = new();

        private static bool IsProof(string sql) => sql.EndsWith("\nLIMIT 0\nFORMAT JSONEachRow", StringComparison.Ordinal);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var sql = await request.Content!.ReadAsStringAsync(ct);
            Sent.Add(sql);
            if (IsProof(sql))
            {
                if (CopyLacksAColumn)
                {
                    return new HttpResponseMessage(HttpStatusCode.NotFound)
                    {
                        Content = new StringContent("Code: 16. DB::Exception: There is no column hostname in table.: Child table: system.query_log_0. (NO_SUCH_COLUMN_IN_TABLE)", Encoding.UTF8),
                    };
                }
                // The proof of the read across the copies waits for the end of the statement, and asks for no rows.
                Assert.Contains("wait_end_of_query=1", request.RequestUri!.Query);
                if (ProofTimesOut)
                {
                    throw new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout");
                }
                if (ProofServerError)
                {
                    return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                    {
                        Content = new StringContent("Code: 202. DB::Exception: Too many simultaneous queries. (TOO_MANY_SIMULTANEOUS_QUERIES)", Encoding.UTF8),
                    };
                }
                if (RefusalInsideOk)
                {
                    return new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent("{\"meta\": [], \"data\": [], \"rows\": 0, \"exception\": \"Code: 497. DB::Exception: Not enough privileges\"}", Encoding.UTF8),
                    };
                }
            }
            string? json = sql switch
            {
                _ when sql == RawSelects.ProbeVersion => "[[\"26.6.1.2455\"]]",
                _ when sql == RawSelects.ProbeColumns => "[[" + "[" + string.Join(", ", RawSelects.QueryLogColumns.Select(c => "\"" + c + "\"")) + "]" + "]]",
                _ when sql == RawSelects.ProbeSessionLog => "[[1]]",
                _ when sql.Contains("system.tables") && sql.Contains("WHERE database = 'system' AND match(name") =>
                    LogTablesFail ? null : $"[[\"{Created}\", \"{Attached}\", \"{SessionCopies}\"]]",
                _ when sql.Contains("min(event_time_microseconds)") => "[[\"1761955200000000\"]]",
                _ when IsProof(sql) => MovedReadRefused ? null : "[]",
                _ => "",
            };
            if (json == null)
            {
                return new HttpResponseMessage(HttpStatusCode.InternalServerError)
                {
                    Content = new StringContent("Code: 497. DB::Exception: trailox_monitor: Not enough privileges. To execute this query, it's necessary to have the grant CREATE TEMPORARY TABLE ON *.*. (ACCESS_DENIED)", Encoding.UTF8),
                };
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
    public async Task The_read_across_the_copies_is_proved_before_any_window_uses_it()
    {
        var server = new Server { Created = 1760000000000000, Attached = 1760000600000000, SessionCopies = 1 };
        await new ChSource(new HttpClient(server), Endpoint("default")).ProbeAsync(CancellationToken.None);

        // The proof is each window's own SELECT, columns and all, reading no row.
        var proofs = server.Sent.Where(s => s.EndsWith("\nLIMIT 0\nFORMAT JSONEachRow", StringComparison.Ordinal)).ToList();
        Assert.Equal(2, proofs.Count);
        Assert.StartsWith("SELECT hostname, query_id,", proofs[0]);
        Assert.Contains("\nFROM clusterAllReplicas('default', merge('system', '^query_log(_[0-9]+)?$'))\n", proofs[0]);
        Assert.StartsWith("SELECT hostname, toString(type) AS type,", proofs[1]);
        Assert.Contains("\nFROM clusterAllReplicas('default', merge('system', '^session_log(_[0-9]+)?$'))\n", proofs[1]);
    }

    /// <summary>
    /// A server upgraded across a new column keeps copies without it (a 23.10 copy on a 25.7 server has no hostname, and
    /// a read naming it fails with Code 16). Reading through them would fail every window, so the live log is read.
    /// </summary>
    [Fact]
    public async Task A_copy_that_lacks_a_column_the_window_reads_is_not_read()
    {
        var server = new Server { Created = 1760000000000000, Attached = 1760000600000000, SessionCopies = 1, CopyLacksAColumn = true };
        var source = new ChSource(new HttpClient(server), Endpoint());

        var caps = await source.ProbeAsync(CancellationToken.None);
        await using (await source.OpenAsync(new AgentTask { ChunkId = "e", Stream = ProtocolInfo.Streams.Events, StartMicros = 1, EndMicros = 2 }, CancellationToken.None)) { }
        await using (await source.OpenAsync(new AgentTask { ChunkId = "s", Stream = ProtocolInfo.Streams.Sessions, StartMicros = 1, EndMicros = 2 }, CancellationToken.None)) { }

        Assert.Equal(0, caps.MovedLogAttachedMicros);
        Assert.Contains("\nFROM system.query_log\n", server.Sent[^2]);
        Assert.Contains("\nFROM system.session_log\n", server.Sent[^1]);
    }

    [Fact]
    public void The_proof_of_a_window_is_the_window_without_rows()
    {
        var events = RawSelects.Events(Endpoint(), AllColumns, 0, 0, moved: true);

        Assert.Equal(events[..^"\nFORMAT JSONEachRow".Length] + "\nLIMIT 0\nFORMAT JSONEachRow", RawSelects.ProbeRead(events));
        Assert.Throws<ArgumentException>(() => RawSelects.ProbeRead(RawSelects.CatalogTables + " LIMIT 1"));
    }

    /// <summary>
    /// ClickHouse 23.10 lists the copies (the setup script's SHOW TABLES) but refuses merge() to a user without CREATE
    /// TEMPORARY TABLE: reading through it would fail every window, so the live log is read and no copy is reported.
    /// </summary>
    [Fact]
    public async Task A_server_that_refuses_the_read_across_the_copies_keeps_reading_the_live_log()
    {
        var server = new Server { Created = 1760000000000000, Attached = 1760000600000000, SessionCopies = 1, MovedReadRefused = true };
        var source = new ChSource(new HttpClient(server), Endpoint());

        var caps = await source.ProbeAsync(CancellationToken.None);
        await using (await source.OpenAsync(new AgentTask { ChunkId = "e", Stream = ProtocolInfo.Streams.Events, StartMicros = 1, EndMicros = 2 }, CancellationToken.None)) { }
        await using (await source.OpenAsync(new AgentTask { ChunkId = "s", Stream = ProtocolInfo.Streams.Sessions, StartMicros = 1, EndMicros = 2 }, CancellationToken.None)) { }

        Assert.Equal(0, caps.MovedLogAttachedMicros);
        Assert.Equal(1760000000000000, caps.QueryLogCreatedMicros);
        Assert.Contains("\nFROM system.query_log\n", server.Sent[^2]);
        Assert.Contains("\nFROM system.session_log\n", server.Sent[^1]);
    }

    /// <summary>A copy the user can see but not read (SHOW TABLES without the wildcard grant) is refused the same way.</summary>
    [Fact]
    public async Task A_refusal_inside_a_200_is_still_a_refusal()
    {
        var server = new Server { Created = 1760000000000000, Attached = 1760000600000000, SessionCopies = 1, RefusalInsideOk = true };
        var source = new ChSource(new HttpClient(server), Endpoint());

        var caps = await source.ProbeAsync(CancellationToken.None);
        await using (await source.OpenAsync(new AgentTask { ChunkId = "e", Stream = ProtocolInfo.Streams.Events, StartMicros = 1, EndMicros = 2 }, CancellationToken.None)) { }

        Assert.Equal(0, caps.MovedLogAttachedMicros);
        Assert.Contains("\nFROM system.query_log\n", server.Sent[^1]);
    }

    /// <summary>
    /// Only a refusal turns the copies off. A busy server or a timeout on a later probe is not an answer: the copies stay on
    /// and are reported, and the probe itself does not fail.
    /// </summary>
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task A_failure_that_is_not_a_refusal_keeps_the_earlier_decision(bool serverError, bool timeout)
    {
        var server = new Server { Created = 1760000000000000, Attached = 1760000600000000 };
        var source = new ChSource(new HttpClient(server), Endpoint());
        await source.ProbeAsync(CancellationToken.None);

        server.ProofServerError = serverError;
        server.ProofTimesOut = timeout;
        var caps = await source.ProbeAsync(CancellationToken.None);
        await using (await source.OpenAsync(new AgentTask { ChunkId = "e", Stream = ProtocolInfo.Streams.Events, StartMicros = 1, EndMicros = 2 }, CancellationToken.None)) { }

        Assert.Equal(1760000600000000, caps.MovedLogAttachedMicros);
        Assert.Contains("\nFROM merge('system', '^query_log(_[0-9]+)?$')\n", server.Sent[^1]);
    }

    /// <summary>On the first probe there is no earlier decision: a copy not yet proved is not read.</summary>
    [Fact]
    public async Task A_first_probe_that_cannot_prove_the_read_reads_the_live_log()
    {
        var server = new Server { Created = 1760000000000000, Attached = 1760000600000000, ProofServerError = true };
        var source = new ChSource(new HttpClient(server), Endpoint());

        var caps = await source.ProbeAsync(CancellationToken.None);
        await using (await source.OpenAsync(new AgentTask { ChunkId = "e", Stream = ProtocolInfo.Streams.Events, StartMicros = 1, EndMicros = 2 }, CancellationToken.None)) { }

        Assert.Equal(0, caps.MovedLogAttachedMicros);
        Assert.Contains("\nFROM system.query_log\n", server.Sent[^1]);
    }

    [Fact]
    public async Task A_refusal_on_a_later_probe_turns_the_copies_off()
    {
        var server = new Server { Created = 1760000000000000, Attached = 1760000600000000 };
        var source = new ChSource(new HttpClient(server), Endpoint());
        await source.ProbeAsync(CancellationToken.None);

        server.MovedReadRefused = true;
        var caps = await source.ProbeAsync(CancellationToken.None);
        await using (await source.OpenAsync(new AgentTask { ChunkId = "e", Stream = ProtocolInfo.Streams.Events, StartMicros = 1, EndMicros = 2 }, CancellationToken.None)) { }

        Assert.Equal(0, caps.MovedLogAttachedMicros);
        Assert.Contains("\nFROM system.query_log\n", server.Sent[^1]);
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
