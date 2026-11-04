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

    /// <summary>The query kinds of a read or a listing: the only statements the agent runs.</summary>
    private const string ReadKinds = "('Select', 'Explain', 'Show', 'Describe', 'Exists', 'Check')";

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

    /// <param name="moved">Also every copy of the log the server moved aside (<see cref="MovedQueryLogs"/>).</param>
    public static string QueryLogSource(EndpointConfig endpoint, bool moved = false) =>
        OnEveryReplica(endpoint, moved ? MovedQueryLogs : "system.query_log");

    /// <param name="moved">Also every copy of the log the server moved aside (<see cref="MovedSessionLogs"/>).</param>
    public static string SessionLogSource(EndpointConfig endpoint, bool moved = false) =>
        OnEveryReplica(endpoint, moved ? MovedSessionLogs : "system.session_log");

    private static string OnEveryReplica(EndpointConfig endpoint, string source) =>
        string.IsNullOrEmpty(endpoint.ClusterName) ? source : $"clusterAllReplicas('{endpoint.ClusterName}', {source})";

    /// <summary>
    /// system.query_log and every copy of it the server moved aside: query_log_1, query_log_2 and so on.
    /// </summary>
    /// <remarks>
    /// When ClickHouse Cloud replaces a service's servers, the new servers start with an empty system.query_log and,
    /// minutes later, attach the old servers' tables as query_log_N; a self-hosted server renames its log the same way
    /// when an upgrade changes the log's columns. A window read from system.query_log alone then misses the old
    /// servers' statements. The copies need their own grant (`GRANT SELECT ON system.query_log*`, ClickHouse 24.10 or
    /// later), and the agent reads them only when the probe could see one (<see cref="ProbeLogTables"/>): otherwise
    /// every statement is exactly what it was.
    /// </remarks>
    public const string MovedQueryLogs = "merge('system', '^query_log(_[0-9]+)?$')";

    /// <summary>system.session_log and every copy of it the server moved aside, at the same replacements.</summary>
    public const string MovedSessionLogs = "merge('system', '^session_log(_[0-9]+)?$')";

    private static string Window(long startMicros, long endMicros) =>
        $"event_time_microseconds > fromUnixTimestamp64Micro({startMicros}) AND event_time_microseconds <= fromUnixTimestamp64Micro({endMicros})";

    private static string Lit(string value) => "'" + value.Replace("\\", "\\\\").Replace("'", "\\'") + "'";

    /// <summary>
    /// The events SELECT for one window. <paramref name="presentColumns"/> is what the probe found
    /// in system.columns; columns not present are omitted.
    /// </summary>
    /// <param name="moved">Read the moved copies of the log too (<see cref="MovedQueryLogs"/>).</param>
    public static string Events(EndpointConfig endpoint, IReadOnlySet<string> presentColumns, long startMicros, long endMicros, bool moved = false)
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

        // The agent's own reads are tagged (see LogComment) and excluded here: a read by the user the agent signs in
        // as. The tag is a setting any user may send, so a tagged statement that is not one of those reads - a GRANT
        // sent with the tag, or anything another user tags - is reported like any other.
        var where = Window(startMicros, endMicros)
                    + " AND type != 'QueryStart' AND is_initial_query = 1"
                    + $" AND NOT (log_comment = '{LogComment}' AND query_kind IN {ReadKinds} AND user = {Lit(endpoint.Username)})";
        if (presentColumns.Contains("is_internal"))
        {
            where += " AND is_internal = 0";
        }
        if (endpoint.ExcludedDatabases.Count > 0)
        {
            where += " AND NOT hasAny(databases, [" + string.Join(", ", endpoint.ExcludedDatabases.Select(Lit)) + "])";
        }

        return $"SELECT {string.Join(", ", columns)}\nFROM {QueryLogSource(endpoint, moved)}\nWHERE {where}\nFORMAT JSONEachRow";
    }

    /// <param name="moved">Read the moved copies of the log too (<see cref="MovedSessionLogs"/>).</param>
    public static string Sessions(EndpointConfig endpoint, long startMicros, long endMicros, bool moved = false) =>
        "SELECT hostname, toString(type) AS type, toString(auth_id) AS auth_id, session_id, event_time_microseconds, user, " +
        "toString(auth_type) AS auth_type, toString(client_address) AS client_address, toString(interface) AS interface, " +
        "client_name, client_version_major, client_version_minor, client_version_patch, failure_reason\n" +
        $"FROM {SessionLogSource(endpoint, moved)}\nWHERE {Window(startMicros, endMicros)}\nFORMAT JSONEachRow";

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

    /// <summary>
    /// The settings that decide whether a user is read-only (agent 1.7): readonly, allow_ddl, and every element that
    /// names a profile, so Trailox can follow a user's chain of profiles. Also the six settings that can keep a
    /// statement out of system.query_log or cut what it records, and, on each profile's element, whom the profile
    /// applies to (system.settings_profiles). Only these rows - a profile can hold any value a customer put in it, and
    /// nothing else is used. `index` is returned as element_index.
    /// </summary>
    /// <remarks>
    /// Needs its own grant, which older setup scripts did not ask for: SELECT ON system.settings_profile_elements (or
    /// SHOW SETTINGS PROFILES on a server with select_from_system_db_requires_grant = false). SHOW SETTINGS PROFILES
    /// also reads system.settings_profiles, with that setting true or false. A refusal is reported as the task's
    /// failure but kept off the agent's own error list (TaskExecutor, MissingOptionalGrant).
    /// A user's or a role's element finds no profile in the join: it is sent with apply_to_all 0 and two empty lists, also
    /// on a server whose join_use_nulls = 1 (which would give NULL without the ifNull).
    /// Every column that crosses the join is named with AS, so each keeps its own name in the output.
    /// </remarks>
    public const string CatalogSettings =
        "SELECT today() AS snapshot_date, e.profile_name AS profile_name, e.user_name AS user_name, e.role_name AS role_name, " +
        "e.index AS element_index, e.setting_name AS setting_name, e.value AS value, toString(e.writability) AS writability, " +
        "e.inherit_profile AS inherit_profile, ifNull(p.apply_to_all, 0) AS apply_to_all, p.apply_to_list AS apply_to_list, " +
        "p.apply_to_except AS apply_to_except\n" +
        "FROM system.settings_profile_elements AS e\nLEFT JOIN system.settings_profiles AS p ON p.name = e.profile_name\n" +
        "WHERE setting_name IN ('readonly', 'allow_ddl', 'log_queries', 'log_queries_min_type', 'log_queries_min_query_duration_ms', " +
        "'log_queries_probability', 'log_query_settings', 'log_queries_cut_to_length') OR inherit_profile IS NOT NULL\n" +
        "FORMAT JSONEachRow";

    // ---- probe (once per endpoint per probe interval) ----

    public const string ProbeVersion = "SELECT version()";

    public static readonly string ProbeColumns =
        "SELECT groupArray(name) FROM system.columns WHERE database = 'system' AND table = 'query_log' AND name IN (" +
        string.Join(", ", QueryLogColumns.Select(c => "'" + c + "'")) + ")";

    public const string ProbeSessionLog = "SELECT count() FROM system.tables WHERE database = 'system' AND name = 'session_log'";

    public static string ProbeEarliestEvent(EndpointConfig endpoint, bool moved = false) =>
        $"SELECT toInt64(ifNull(toUnixTimestamp64Micro(min(event_time_microseconds)), 0)) FROM {QueryLogSource(endpoint, moved)}";

    public static string ProbeEarliestSession(EndpointConfig endpoint, bool moved = false) =>
        $"SELECT toInt64(ifNull(toUnixTimestamp64Micro(min(event_time_microseconds)), 0)) FROM {SessionLogSource(endpoint, moved)}";

    /// <summary>
    /// The log tables the monitoring user can see, in one row: when system.query_log was created and when the newest
    /// moved copy of it was attached (epoch micros, 0 = none), and how many moved copies of system.session_log there are.
    /// </summary>
    /// <remarks>
    /// system.tables lists only the tables the user may read, so a copy shows here exactly when the agent can read it.
    /// A newer creation time means the server replaced its log; the attach time says when the old one could be read
    /// again. Both go to Trailox in the probe's caps.
    /// </remarks>
    public static string ProbeLogTables(EndpointConfig endpoint) =>
        "SELECT toInt64(toUnixTimestamp(maxIf(metadata_modification_time, name = 'query_log'))) * 1000000, " +
        "toInt64(toUnixTimestamp(maxIf(metadata_modification_time, match(name, '^query_log_[0-9]+$')))) * 1000000, " +
        "countIf(match(name, '^session_log_[0-9]+$'))" +
        $" FROM {OnEveryReplica(endpoint, "system.tables")} WHERE database = 'system' AND match(name, '^(query|session)_log(_[0-9]+)?$')";

    /// <summary>The SELECT for a task, or null when the stream is not one this engine knows.</summary>
    /// <param name="queryLogMoved">The probe saw a moved copy of system.query_log: the statement windows read it too.</param>
    /// <param name="sessionLogMoved">The same for system.session_log and the sign-in windows.</param>
    public static string? ForTask(EndpointConfig endpoint, IReadOnlySet<string> presentColumns, AgentTask task, bool usersAuthTypeIsArray = true,
        bool queryLogMoved = false, bool sessionLogMoved = false) =>
        task.Stream switch
        {
            ProtocolInfo.Streams.Events => Events(endpoint, presentColumns, task.StartMicros, task.EndMicros, queryLogMoved),
            ProtocolInfo.Streams.Sessions => Sessions(endpoint, task.StartMicros, task.EndMicros, sessionLogMoved),
            ProtocolInfo.Streams.CatalogTables => CatalogTables,
            ProtocolInfo.Streams.CatalogUsers => usersAuthTypeIsArray ? CatalogUsersArray : CatalogUsersScalar,
            ProtocolInfo.Streams.CatalogGrants => CatalogGrants,
            ProtocolInfo.Streams.CatalogSettings => CatalogSettings,
            _ => null,
        };
}
