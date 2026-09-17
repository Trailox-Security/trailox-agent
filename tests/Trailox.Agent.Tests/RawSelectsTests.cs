using Trailox.Agent.Engines.ClickHouse;
using Trailox.Agent.Config;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests;

/// <summary>
/// The SQL the agent runs is the whole of what it does on a customer's server, so its shape is
/// pinned: which columns, which filters, what is computed server-side, and what is never
/// selected when the customer said so.
/// </summary>
public class RawSelectsTests
{
    private static EndpointConfig Endpoint(bool text = true, string cluster = "", params string[] excluded) => new()
    {
        Alias = "prod", Engine = "clickhouse", Host = "ch.internal", Port = 8443, Username = "trailox_monitor", ClusterName = cluster, StoreRawQueryText = text, ExcludedDatabases = excluded.ToList(),
    };

    private static readonly IReadOnlySet<string> AllColumns = RawSelects.QueryLogColumns.ToHashSet(StringComparer.OrdinalIgnoreCase);

    [Fact]
    public void Events_selects_raw_columns_and_the_two_server_computed_values()
    {
        var sql = RawSelects.Events(Endpoint(), AllColumns, 1_000, 2_000);

        Assert.StartsWith("SELECT hostname, query_id, toString(type) AS type, event_time_microseconds,", sql);
        Assert.Contains("toString(address) AS address", sql);
        Assert.Contains("normalizeQuery(query) AS normalized_query", sql);
        Assert.Contains("sipHash64(query) AS raw_text_hash", sql);
        Assert.Contains(", query\nFROM system.query_log", sql);
        Assert.Contains("event_time_microseconds > fromUnixTimestamp64Micro(1000) AND event_time_microseconds <= fromUnixTimestamp64Micro(2000)", sql);
        Assert.Contains("type != 'QueryStart' AND is_initial_query = 1", sql);
        // The agent filters its own tag and nothing else; other monitoring traffic is Trailox's to exclude.
        Assert.Contains("log_comment != 'trailox-agent'", sql);
        Assert.DoesNotContain("log_comment NOT IN", sql);   // one tag, its own; log_comment is also a selected column
        Assert.Contains("AND is_internal = 0", sql);
        Assert.EndsWith("FORMAT JSONEachRow", sql);
    }

    [Fact]
    public void With_storeRawQueryText_off_the_text_column_is_never_selected()
    {
        var sql = RawSelects.Events(Endpoint(text: false), AllColumns, 1, 2);
        Assert.DoesNotContain(", query\n", sql);
        Assert.DoesNotContain(" query,", sql);
        // but the server-computed shape and hash still are, so Trailox can build its dictionaries
        Assert.Contains("normalizeQuery(query) AS normalized_query", sql);
        Assert.Contains("sipHash64(query) AS raw_text_hash", sql);
    }

    [Fact]
    public void Columns_the_server_lacks_are_omitted_and_their_filters_too()
    {
        var older = AllColumns.Where(c => c is not ("is_internal" or "used_privileges" or "missing_privileges" or "authenticated_user")).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var sql = RawSelects.Events(Endpoint(), older, 1, 2);

        Assert.DoesNotContain("is_internal", sql);
        Assert.DoesNotContain("used_privileges", sql);
        Assert.DoesNotContain("authenticated_user", sql);
        Assert.Contains("used_table_functions", sql);
    }

    [Fact]
    public void Excluded_databases_become_a_where_clause_with_escaped_literals()
    {
        var sql = RawSelects.Events(Endpoint(excluded: new[] { "hr_private", "o'brien" }), AllColumns, 1, 2);
        Assert.Contains("AND NOT hasAny(databases, ['hr_private', 'o\\'brien'])", sql);
    }

    [Fact]
    public void A_cluster_name_reads_every_replica()
    {
        Assert.Equal("clusterAllReplicas('default', system.query_log)", RawSelects.QueryLogSource(Endpoint(cluster: "default")));
        Assert.Equal("system.query_log", RawSelects.QueryLogSource(Endpoint()));
        Assert.Contains("FROM clusterAllReplicas('default', system.session_log)", RawSelects.Sessions(Endpoint(cluster: "default"), 1, 2));
    }

    [Fact]
    public void Catalog_selects_read_only_system_tables_and_include_the_two_retention_rows()
    {
        Assert.Contains("FROM system.tables", RawSelects.CatalogTables);
        Assert.Contains("(database = 'system' AND name IN ('query_log', 'session_log'))", RawSelects.CatalogTables);
        Assert.Contains("FROM system.users", RawSelects.CatalogUsersArray);
        Assert.Contains("FROM system.users", RawSelects.CatalogUsersScalar);
        Assert.Contains("FROM system.grants", RawSelects.CatalogGrants);
    }

    [Fact]
    public void Every_protocol_stream_maps_to_a_select_and_unknown_ones_to_null()
    {
        foreach (var stream in new[] { ProtocolInfo.Streams.Events, ProtocolInfo.Streams.Sessions, ProtocolInfo.Streams.CatalogTables, ProtocolInfo.Streams.CatalogUsers, ProtocolInfo.Streams.CatalogGrants })
        {
            Assert.NotNull(RawSelects.ForTask(Endpoint(), AllColumns, new AgentTask { Stream = stream, StartMicros = 1, EndMicros = 2 }));
        }
        Assert.Null(RawSelects.ForTask(Endpoint(), AllColumns, new AgentTask { Stream = "audit" }));
        Assert.Contains("[toString(auth_type)]", RawSelects.ForTask(Endpoint(), AllColumns, new AgentTask { Stream = ProtocolInfo.Streams.CatalogUsers }, usersAuthTypeIsArray: false));
    }

    /// <summary>No statement the agent can ever send modifies anything.</summary>
    [Fact]
    public void Nothing_here_writes()
    {
        var all = new[]
        {
            RawSelects.Events(Endpoint(), AllColumns, 1, 2), RawSelects.Sessions(Endpoint(), 1, 2), RawSelects.CatalogTables,
            RawSelects.CatalogUsersArray, RawSelects.CatalogUsersScalar, RawSelects.CatalogGrants, RawSelects.ProbeVersion,
            RawSelects.ProbeColumns, RawSelects.ProbeSessionLog, RawSelects.ProbeEarliestEvent(Endpoint()), RawSelects.ProbeEarliestSession(Endpoint()),
        };
        foreach (var sql in all)
        {
            Assert.StartsWith("SELECT", sql.TrimStart());
            // Whole words only: `create_table_query` is a column we read, not a statement we run.
            foreach (var verb in new[] { "INSERT", "ALTER", "DROP", "CREATE", "DELETE", "UPDATE", "GRANT", "TRUNCATE", "KILL", "OPTIMIZE" })
            {
                Assert.DoesNotMatch(@"(?i)(^|[^A-Za-z0-9_])" + verb + @"([^A-Za-z0-9_]|$)", sql);
            }
        }
    }
}
