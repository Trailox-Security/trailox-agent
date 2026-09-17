using System.Text;
using System.Text.Json;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.Databricks;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests;

/// <summary>
/// The SQL the agent runs on a workspace is the whole of what it does there, so its shape is
/// pinned; so is the re-emission of JSON_ARRAY pages as NDJSON, which is the one transformation
/// the agent performs.
/// </summary>
public class DatabricksEngineTests
{
    private static EndpointConfig Endpoint(bool text = true, params string[] excluded) => new()
    {
        Alias = "lakehouse", Engine = "databricks", Host = "dbc-12345678-abcd.cloud.databricks.com",
        ClusterName = "0123456789abcdef", Username = "00000000-1111-2222-3333-444444444444",
        StoreRawQueryText = text, ExcludedDatabases = excluded.ToList(),
    };

    [Fact]
    public void Registry_knows_databricks()
    {
        Assert.Equal("databricks", EngineRegistry.Require("Databricks").Name);
        Assert.Contains("databricks", EngineRegistry.Names);
    }

    [Fact]
    public void Validate_accepts_a_workspace_and_pins_https()
    {
        var e = Endpoint();
        e.Port = 8443;
        e.Tls = false;
        var problems = new DatabricksEngine().Validate(e);
        Assert.Empty(problems);
        Assert.Equal(443, e.Port);
        Assert.True(e.Tls);
    }

    [Theory]
    [InlineData("host", "https://dbc-1.cloud.databricks.com")]
    [InlineData("host", "localhost")]
    [InlineData("clusterName", "0123456789abcde")]
    [InlineData("clusterName", "default")]
    [InlineData("username", "trailox_monitor")]
    public void Validate_refuses_a_wrong_shape_by_field(string field, string value)
    {
        var e = Endpoint();
        switch (field)
        {
            case "host": e.Host = value; break;
            case "clusterName": e.ClusterName = value; break;
            default: e.Username = value; break;
        }
        var problems = new DatabricksEngine().Validate(e);
        Assert.Single(problems);
        Assert.StartsWith(field + ":", problems[0]);
    }

    [Fact]
    public void Events_is_select_star_on_end_time_and_drops_text_when_the_customer_said_so()
    {
        Assert.Equal("SELECT * FROM system.query.history WHERE end_time > timestamp_micros(1000L) AND end_time <= timestamp_micros(2000L)",
            DbxRawSelects.Events(Endpoint(), 1000, 2000));
        Assert.StartsWith("SELECT * EXCEPT (statement_text) FROM system.query.history", DbxRawSelects.Events(Endpoint(text: false), 1000, 2000));
    }

    [Fact]
    public void Every_stream_the_profile_names_has_a_select_and_nothing_is_joined_or_classified()
    {
        var streams = new[]
        {
            ProtocolInfo.Streams.Events, ProtocolInfo.Streams.LineageTables, ProtocolInfo.Streams.LineageColumns, ProtocolInfo.Streams.Sessions,
            ProtocolInfo.Streams.CatalogTables, ProtocolInfo.Streams.CatalogColumns, ProtocolInfo.Streams.CatalogTablePrivileges, ProtocolInfo.Streams.CatalogSchemaPrivileges,
        };
        foreach (var stream in streams)
        {
            var sql = DbxRawSelects.ForTask(Endpoint(), new AgentTask { Stream = stream, StartMicros = 1, EndMicros = 2 });
            Assert.NotNull(sql);
            Assert.DoesNotContain("JOIN", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("CASE", sql);
            Assert.DoesNotContain("GROUP BY", sql, StringComparison.OrdinalIgnoreCase);
        }
        Assert.Null(DbxRawSelects.ForTask(Endpoint(), new AgentTask { Stream = ProtocolInfo.Streams.CatalogUsers }));
        Assert.Null(DbxRawSelects.ForTask(Endpoint(), new AgentTask { Stream = ProtocolInfo.Streams.CatalogGrants }));
    }

    [Fact]
    public void Sessions_keeps_the_accounts_service_and_lineage_needs_a_statement_id()
    {
        Assert.EndsWith("AND service_name = 'accounts'", DbxRawSelects.Sessions(1, 2));
        Assert.EndsWith("AND statement_id IS NOT NULL", DbxRawSelects.TableLineage(1, 2));
        Assert.EndsWith("AND statement_id IS NOT NULL", DbxRawSelects.ColumnLineage(1, 2));
    }

    [Fact]
    public void Catalog_excludes_the_system_catalogs_and_the_customer_list_with_spark_escaping()
    {
        var sql = DbxRawSelects.CatalogTables(Endpoint(true, "hr_private", "it's"));
        Assert.StartsWith("SELECT current_date() AS snapshot_date, * FROM system.information_schema.tables WHERE table_catalog NOT IN (", sql);
        Assert.Contains("'system', 'samples', '__databricks_internal', 'hr_private', 'it\\'s'", sql);
        Assert.Contains("catalog_name NOT IN (", DbxRawSelects.CatalogSchemaPrivileges(Endpoint()));
    }

    [Fact]
    public void Spark_literal_escapes_with_a_backslash_never_by_doubling()
    {
        Assert.Equal("'it\\'s'", DbxRawSelects.Lit("it's"));
        Assert.Equal("'a\\\\b'", DbxRawSelects.Lit("a\\b"));
    }

    [Fact]
    public async Task Pages_become_one_object_per_line_keyed_by_column_and_values_are_forwarded_untouched()
    {
        var columns = new[] { "statement_id", "total_duration_ms", "compute", "error_message" };
        var rows = Rows(
            "[\"01f1\", \"6892\", \"{\\\"warehouse_id\\\":\\\"cd83\\\"}\", null]",
            "[\"01f2\", \"0\"]");

        await using var stream = new NdjsonRowStream(columns, rows, CancellationToken.None);
        using var reader = new StreamReader(stream, Encoding.UTF8);
        var text = await reader.ReadToEndAsync();

        var lines = text.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(2, lines.Length);
        Assert.Equal("{\"statement_id\":\"01f1\",\"total_duration_ms\":\"6892\",\"compute\":\"{\\\"warehouse_id\\\":\\\"cd83\\\"}\",\"error_message\":null}", lines[0]);
        Assert.Equal("{\"statement_id\":\"01f2\",\"total_duration_ms\":\"0\",\"compute\":null,\"error_message\":null}", lines[1]);
        Assert.Equal(2, stream.RowsEmitted);
    }

    [Fact]
    public async Task An_empty_page_set_is_an_empty_body()
    {
        await using var stream = new NdjsonRowStream(new[] { "a" }, Rows(), CancellationToken.None);
        using var reader = new StreamReader(stream);
        Assert.Equal("", await reader.ReadToEndAsync());
    }

    [Fact]
    public void Statement_manifest_parses_columns_chunks_and_first_links()
    {
        const string json = """
            {"statement_id":"s1","status":{"state":"SUCCEEDED"},
             "manifest":{"format":"JSON_ARRAY","schema":{"column_count":2,"columns":[{"name":"a","type_name":"STRING"},{"name":"b","type_name":"LONG"}]},
                         "total_chunk_count":2,"total_row_count":300},
             "result":{"external_links":[{"chunk_index":0,"row_count":200,"external_link":"https://storage/x","expiration":"2026-09-16T12:55:29Z"}]}}
            """;
        using var doc = JsonDocument.Parse(json);
        var result = DbxStatements.Parse(doc.RootElement, "s1");
        Assert.Equal(new[] { "a", "b" }, result.Columns);
        Assert.Equal(2, result.TotalChunks);
        Assert.Equal(300, result.TotalRows);
        Assert.Single(result.FirstLinks);
        Assert.Equal("https://storage/x", result.FirstLinks[0].Url);
    }

    private static async IAsyncEnumerable<JsonElement> Rows(params string[] arrays)
    {
        foreach (var a in arrays)
        {
            using var doc = JsonDocument.Parse(a);
            yield return doc.RootElement.Clone();
        }
        await Task.CompletedTask;
    }
}
