using Trailox.Agent.Config;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Engines.ClickHouse;

/// <summary>
/// The whole of the SQL this agent runs on a ClickHouse server. Every statement here reads a
/// system table as-is; nothing is transformed, classified or joined - that happens on the
/// Trailox side. docs/PROTOCOL.md §6 is the human-readable copy of this file.
/// </summary>
public static class RawSelects
{
    /// <summary>Tag on every request, so Trailox can exclude the agent's own reads from what it collects.</summary>
    public const string LogComment = "trailox-agent";

    /// <summary>The system.query_log columns Trailox reads. Absent ones (older servers) are simply not selected.</summary>
    public static readonly string[] QueryLogColumns =
    {
        "hostname", "query_id", "type", "event_time_microseconds", "user", "authenticated_user", "os_user",
        "initial_user", "query_kind", "normalized_query_hash", "databases", "tables", "views", "columns",
        "used_table_functions", "used_privileges", "missing_privileges", "exception_code",
        "query_duration_ms", "result_rows", "result_bytes", "read_rows", "read_bytes", "written_rows",
        "written_bytes", "memory_usage", "address", "forwarded_for", "interface", "is_secure", "client_name",
        "http_user_agent", "client_version_major", "client_version_minor", "client_version_patch",
        "log_comment", "is_initial_query", "distributed_depth", "is_internal",
    };

    /// <summary>The columns that must exist for collection to make sense at all (ClickHouse >= 23.8).</summary>
    public static readonly string[] RequiredColumns = { "hostname", "query_id", "event_time_microseconds", "normalized_query_hash", "query_kind" };

    /// <summary>Columns whose presence is probed and reported (they vary by server version).</summary>
    public static readonly string[] ProbedColumns =
    {
        "is_internal", "authenticated_user", "hostname", "normalized_query_hash", "used_table_functions", "used_privileges", "missing_privileges",
    };

    public static string QueryLogSource(EndpointConfig endpoint) =>
        string.IsNullOrEmpty(endpoint.ClusterName) ? "system.query_log" : $"clusterAllReplicas('{endpoint.ClusterName}', system.query_log)";

    public static string SessionLogSource(EndpointConfig endpoint) =>
        string.IsNullOrEmpty(endpoint.ClusterName) ? "system.session_log" : $"clusterAllReplicas('{endpoint.ClusterName}', system.session_log)";

    private static string Window(long startMicros, long endMicros) =>
        $"event_time_microseconds > fromUnixTimestamp64Micro({startMicros}) AND event_time_microseconds <= fromUnixTimestamp64Micro({endMicros})";

    private static string Lit(string value) => "'" + value.Replace("\\", "\\\\").Replace("'", "\\'") + "'";

    /// <summary>
    /// The events SELECT for one window. <paramref name="presentColumns"/> is what the probe found
    /// in system.columns; columns not present are omitted and Trailox fills their defaults.
    /// </summary>
    public static string Events(EndpointConfig endpoint, IReadOnlySet<string> presentColumns, long startMicros, long endMicros)
    {
        var columns = QueryLogColumns.Where(presentColumns.Contains).Select(c => c switch
        {
            "type" => "toString(type) AS type",
            "address" => "toString(address) AS address",
            _ => c,
        }).ToList();

        // Two values the SERVER computes so the text itself can stay behind when storeRawQueryText is off.
        columns.Add("normalizeQuery(query) AS normalized_query");
        columns.Add("sipHash64(query) AS raw_text_hash");
        if (endpoint.StoreRawQueryText)
        {
            columns.Add("query");
        }

        // The agent's own reads are tagged (see LogComment) and excluded here. Any other
        // monitoring traffic on the server is Trailox's to filter on its side, not the agent's.
        var where = Window(startMicros, endMicros)
                    + " AND type != 'QueryStart' AND is_initial_query = 1"
                    + $" AND log_comment != '{LogComment}'";
        if (presentColumns.Contains("is_internal"))
        {
            where += " AND is_internal = 0";
        }
        if (endpoint.ExcludedDatabases.Count > 0)
        {
            where += " AND NOT hasAny(databases, [" + string.Join(", ", endpoint.ExcludedDatabases.Select(Lit)) + "])";
        }

        return $"SELECT {string.Join(", ", columns)}\nFROM {QueryLogSource(endpoint)}\nWHERE {where}\nFORMAT JSONEachRow";
    }

    public static string Sessions(EndpointConfig endpoint, long startMicros, long endMicros) =>
        "SELECT hostname, toString(type) AS type, toString(auth_id) AS auth_id, session_id, event_time_microseconds, user, " +
        "toString(auth_type) AS auth_type, toString(client_address) AS client_address, toString(interface) AS interface, " +
        "client_name, client_version_major, client_version_minor, client_version_patch, failure_reason\n" +
        $"FROM {SessionLogSource(endpoint)}\nWHERE {Window(startMicros, endMicros)}\nFORMAT JSONEachRow";

    /// <summary>The customer's tables plus the two system rows Trailox reads retention settings from.</summary>
    public const string CatalogTables =
        "SELECT today() AS snapshot_date, database, name, engine, engine_full, total_rows, total_bytes, " +
        "metadata_modification_time, comment, create_table_query\n" +
        "FROM system.tables\n" +
        "WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA', '_table_function', '_temporary_and_external_tables')\n" +
        "   OR (database = 'system' AND name IN ('query_log', 'session_log'))\n" +
        "FORMAT JSONEachRow";

    /// <summary>system.users where auth_type is an array (24.x and newer).</summary>
    public const string CatalogUsersArray =
        "SELECT today() AS snapshot_date, name, arrayMap(t -> toString(t), auth_type) AS auth_types, default_roles_list AS default_roles\n" +
        "FROM system.users\nFORMAT JSONEachRow";

    /// <summary>The fallback for servers where auth_type is a scalar.</summary>
    public const string CatalogUsersScalar =
        "SELECT today() AS snapshot_date, name, [toString(auth_type)] AS auth_types, CAST([] AS Array(String)) AS default_roles\n" +
        "FROM system.users\nFORMAT JSONEachRow";

    public const string CatalogGrants =
        "SELECT today() AS snapshot_date, user_name, role_name, toString(access_type) AS access_type, database, table, column, " +
        "is_partial_revoke, grant_option\nFROM system.grants\nFORMAT JSONEachRow";

    // ---- probe (once per endpoint per probe interval) ----

    public const string ProbeVersion = "SELECT version()";

    public static readonly string ProbeColumns =
        "SELECT groupArray(name) FROM system.columns WHERE database = 'system' AND table = 'query_log' AND name IN (" +
        string.Join(", ", QueryLogColumns.Select(c => "'" + c + "'")) + ")";

    public const string ProbeSessionLog = "SELECT count() FROM system.tables WHERE database = 'system' AND name = 'session_log'";

    public static string ProbeEarliestEvent(EndpointConfig endpoint) =>
        $"SELECT toInt64(ifNull(toUnixTimestamp64Micro(min(event_time_microseconds)), 0)) FROM {QueryLogSource(endpoint)}";

    public static string ProbeEarliestSession(EndpointConfig endpoint) =>
        $"SELECT toInt64(ifNull(toUnixTimestamp64Micro(min(event_time_microseconds)), 0)) FROM {SessionLogSource(endpoint)}";

    /// <summary>The SELECT for a task, or null when the stream is not one this engine knows.</summary>
    public static string? ForTask(EndpointConfig endpoint, IReadOnlySet<string> presentColumns, AgentTask task, bool usersAuthTypeIsArray = true) =>
        task.Stream switch
        {
            ProtocolInfo.Streams.Events => Events(endpoint, presentColumns, task.StartMicros, task.EndMicros),
            ProtocolInfo.Streams.Sessions => Sessions(endpoint, task.StartMicros, task.EndMicros),
            ProtocolInfo.Streams.CatalogTables => CatalogTables,
            ProtocolInfo.Streams.CatalogUsers => usersAuthTypeIsArray ? CatalogUsersArray : CatalogUsersScalar,
            ProtocolInfo.Streams.CatalogGrants => CatalogGrants,
            _ => null,
        };
}
