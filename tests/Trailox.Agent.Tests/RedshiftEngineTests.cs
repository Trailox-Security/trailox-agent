using System.Data;
using System.Text;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.Redshift;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests;

public class RedshiftEngineTests
{
    private static EndpointConfig Endpoint(params string[] excluded) => new()
    {
        Alias = "warehouse", Engine = "redshift", Host = "my-wg.123456789012.eu-west-1.redshift-serverless.amazonaws.com",
        ClusterName = "my-wg", Username = "trailox_agent", ExcludedDatabases = excluded.ToList(),
    };

    [Fact]
    public void Registry_knows_redshift_and_validation_fills_the_defaults()
    {
        Assert.Equal("redshift", EngineRegistry.Require("Redshift").Name);
        var e = Endpoint();
        Assert.Empty(new RedshiftEngine().Validate(e));
        Assert.Equal(5439, e.Port);
        Assert.True(e.Tls);
        Assert.Equal("dev", e.Options["database"]);
    }

    [Theory]
    [InlineData("host", "localhost")]
    [InlineData("username", "Trailox-Agent")]
    [InlineData("clusterName", "a b")]
    public void Validate_refuses_a_wrong_shape_by_field(string field, string value)
    {
        var e = Endpoint();
        switch (field)
        {
            case "host": e.Host = value; break;
            case "username": e.Username = value; break;
            default: e.ClusterName = value; break;
        }
        var problems = new RedshiftEngine().Validate(e);
        Assert.Single(problems);
        Assert.StartsWith(field + ":", problems[0]);
    }

    [Fact]
    public void Events_lists_every_history_column_except_the_text_and_windows_on_end_time()
    {
        var sql = RsRawSelects.Events(Endpoint(), 1000, 2000);
        Assert.StartsWith("SELECT user_id, query_id, query_label,", sql);
        Assert.DoesNotContain("query_text", sql);
        Assert.EndsWith("FROM sys_query_history WHERE end_time > DATEADD(microsecond, 1000::BIGINT, '1970-01-01'::timestamp) AND end_time <= DATEADD(microsecond, 2000::BIGINT, '1970-01-01'::timestamp)", sql);
        Assert.Equal(34, RsRawSelects.QueryHistoryColumns.Length);
    }

    [Fact]
    public void Every_stream_has_a_select_and_nothing_is_joined_or_classified()
    {
        var streams = new[]
        {
            ProtocolInfo.Streams.Events, ProtocolInfo.Streams.QueryText, ProtocolInfo.Streams.QueryDetail, ProtocolInfo.Streams.Unloads, ProtocolInfo.Streams.Sessions,
            ProtocolInfo.Streams.CatalogTables, ProtocolInfo.Streams.CatalogColumns, ProtocolInfo.Streams.CatalogUsers, ProtocolInfo.Streams.CatalogGrants,
        };
        foreach (var stream in streams)
        {
            var sql = RsRawSelects.ForTask(Endpoint(), new AgentTask { Stream = stream, StartMicros = 1, EndMicros = 2 });
            Assert.NotNull(sql);
            Assert.DoesNotContain("JOIN", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("CASE", sql);
            Assert.DoesNotContain("LISTAGG", sql, StringComparison.OrdinalIgnoreCase);
        }
        Assert.Null(RsRawSelects.ForTask(Endpoint(), new AgentTask { Stream = ProtocolInfo.Streams.LineageTables }));
        Assert.Contains("TRIM(step_name) IN ('scan', 'insert', 'update', 'delete')", RsRawSelects.QueryDetail(1, 2));
        Assert.DoesNotContain("passwd", RsRawSelects.CatalogUsers);
        Assert.DoesNotContain("valuntil", RsRawSelects.CatalogUsers);
    }

    [Fact]
    public void Excluded_databases_filter_the_statement_streams_with_postgres_escaping()
    {
        var sql = RsRawSelects.Events(Endpoint("hr", "it's"), 1, 2);
        Assert.EndsWith("AND TRIM(database_name) NOT IN ('hr', 'it''s')", sql);
        Assert.Contains("query_id IN (SELECT query_id FROM sys_query_history", RsRawSelects.QueryText(Endpoint("hr"), 1, 2));
        Assert.DoesNotContain("NOT IN", RsRawSelects.Sessions(1, 2));
    }

    [Fact]
    public void Statement_text_never_leaves_when_the_customer_keeps_it()
    {
        var kept = Endpoint();
        kept.StoreRawQueryText = false;
        var sql = RsRawSelects.QueryText(kept, 1, 2);
        Assert.StartsWith("SELECT user_id, query_id, start_time, sequence FROM sys_query_text", sql);
        Assert.DoesNotContain("*", sql);
        Assert.DoesNotContain("query_text", RsRawSelects.Events(kept, 1, 2));
        Assert.StartsWith("SELECT * FROM sys_query_text", RsRawSelects.QueryText(Endpoint(), 1, 2));
    }

    [Fact]
    public async Task Reader_rows_become_typed_ndjson_lines()
    {
        var table = new DataTable();
        table.Columns.Add("query_id", typeof(long));
        table.Columns.Add("status", typeof(string));
        table.Columns.Add("result_cache_hit", typeof(bool));
        table.Columns.Add("end_time", typeof(DateTime));
        table.Columns.Add("pct_used", typeof(decimal));
        table.Columns.Add("table_id", typeof(uint));
        table.Columns.Add("error_message", typeof(string));
        table.Rows.Add(174442961L, "success   ", false, new DateTime(2026, 9, 16, 16, 11, 29, 383, DateTimeKind.Unspecified).AddTicks(2720), 0.0006m, 127503u, DBNull.Value);
        table.Rows.Add(1L, "failed    ", true, new DateTime(2026, 1, 1), 50000m, 1u, "it \"quoted\" that");

        await using var stream = new ReaderNdjsonStream(new DataTableReader(table));
        using var reader = new StreamReader(stream, Encoding.UTF8);
        var lines = (await reader.ReadToEndAsync()).Split('\n', StringSplitOptions.RemoveEmptyEntries);

        Assert.Equal(2, lines.Length);
        Assert.Equal("{\"query_id\":174442961,\"status\":\"success   \",\"result_cache_hit\":false,\"end_time\":\"2026-09-16T16:11:29.383272Z\",\"pct_used\":0.0006,\"table_id\":127503,\"error_message\":null}", lines[0]);
        Assert.Equal("{\"query_id\":1,\"status\":\"failed    \",\"result_cache_hit\":true,\"end_time\":\"2026-01-01T00:00:00.000000Z\",\"pct_used\":50000,\"table_id\":1,\"error_message\":\"it \\\"quoted\\\" that\"}", lines[1]);
        Assert.Equal(2, stream.RowsEmitted);
    }
}
