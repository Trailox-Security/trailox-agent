using Trailox.Agent.Config;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Engines.Redshift;

/// <summary>
/// The whole of the SQL this agent runs on a Redshift cluster. Every statement reads a SYS_,
/// SVV_ or pg view as-is; nothing is joined, classified or reassembled - that happens on the
/// Trailox side. docs/PROTOCOL.md 6c is the human-readable copy of this file.
/// </summary>
public static class RsRawSelects
{
    /// <summary>Turned off per connection so the agent's own reads are recorded (RS facts §16).</summary>
    public const string DisableResultCache = "SET enable_result_cache_for_session TO off";

    /// <summary>
    /// sys_query_history without query_text: the text travels as sys_query_text sequence rows
    /// (no 4,000-character truncation to reassemble around on the agent side), and shipping it
    /// twice would be waste. Listed explicitly because Redshift has no `* EXCEPT`.
    /// </summary>
    public static readonly string[] QueryHistoryColumns =
    {
        "user_id", "query_id", "query_label", "transaction_id", "session_id", "database_name", "query_type", "status",
        "result_cache_hit", "start_time", "end_time", "elapsed_time", "queue_time", "execution_time", "error_message",
        "returned_rows", "returned_bytes", "redshift_version", "usage_limit", "compute_type", "compile_time",
        "planning_time", "lock_wait_time", "service_class_id", "service_class_name", "query_priority",
        "short_query_accelerated", "user_query_hash", "generic_query_hash", "query_hash_version",
        "result_cache_query_id", "username", "result_offloaded", "query_uuid",
    };

    /// <summary>The plan steps the Trailox side reads; the rest of a plan is never shipped.</summary>
    public const string DetailSteps = "('scan', 'insert', 'update', 'delete')";

    /// <summary>pg_user without passwd (always masked), useconfig (an array) and valuntil (abstime, unreadable by the driver).</summary>
    public const string UserColumns = "usename, usesysid, usecreatedb, usesuper, usecatupd";

    /// <summary>A Redshift string literal: quotes are doubled (Postgres rules, unlike Spark).</summary>
    public static string Lit(string value) => "'" + value.Replace("'", "''") + "'";

    private static string Ts(long micros) => $"DATEADD(microsecond, {micros}::BIGINT, '1970-01-01'::timestamp)";

    private static string Window(string column, long startMicros, long endMicros) =>
        $"{column} > {Ts(startMicros)} AND {column} <= {Ts(endMicros)}";

    private static string ExcludedDatabases(EndpointConfig endpoint) =>
        endpoint.ExcludedDatabases.Count == 0
            ? ""
            : $" AND TRIM(database_name) NOT IN ({string.Join(", ", endpoint.ExcludedDatabases.Select(Lit))})";

    // ---- probe ----

    public const string ProbeVersion = "SELECT version()";

    /// <summary>Statements and distinct users in 24 h: more than one user means sys:monitor works (RS facts §1).</summary>
    public const string ProbeVisibility =
        "SELECT count(*), count(DISTINCT TRIM(username)) FROM sys_query_history WHERE start_time > DATEADD(hour, -24, GETDATE())";

    public const string ProbeConnectionLog =
        "SELECT count(*) FROM sys_connection_log WHERE record_time > DATEADD(hour, -24, GETDATE())";

    public const string ProbeEarliestEvent =
        "SELECT COALESCE(DATEDIFF(microsecond, '1970-01-01'::timestamp, MIN(end_time)), 0) FROM sys_query_history";

    public const string ProbeEarliestSession =
        "SELECT COALESCE(DATEDIFF(microsecond, '1970-01-01'::timestamp, MIN(record_time)), 0) FROM sys_connection_log";

    // ---- streams ----

    public static string Events(EndpointConfig endpoint, long startMicros, long endMicros) =>
        $"SELECT {string.Join(", ", QueryHistoryColumns)} FROM sys_query_history WHERE {Window("end_time", startMicros, endMicros)}{ExcludedDatabases(endpoint)}";

    /// <summary>
    /// Statement text as Redshift stores it, in sequence rows. When the customer keeps statement
    /// text at home the rows are still sent WITHOUT the text column, so the setting holds here in
    /// the agent whatever it is asked to read.
    /// </summary>
    public static string QueryText(EndpointConfig endpoint, long startMicros, long endMicros) =>
        $"SELECT {(endpoint.StoreRawQueryText ? "*" : "user_id, query_id, start_time, sequence")} FROM sys_query_text WHERE {Window("start_time", startMicros, endMicros)}"
        + (endpoint.ExcludedDatabases.Count == 0 ? "" : $" AND query_id IN (SELECT query_id FROM sys_query_history WHERE {Window("start_time", startMicros - 3_600_000_000L, endMicros)}{ExcludedDatabases(endpoint)})");

    public static string QueryDetail(long startMicros, long endMicros) =>
        $"SELECT * FROM sys_query_detail WHERE {Window("start_time", startMicros, endMicros)} AND TRIM(step_name) IN {DetailSteps}";

    public static string Unloads(EndpointConfig endpoint, long startMicros, long endMicros) =>
        $"SELECT * FROM sys_unload_history WHERE {Window("end_time", startMicros, endMicros)}{ExcludedDatabases(endpoint)}";

    public static string Sessions(long startMicros, long endMicros) =>
        $"SELECT * FROM sys_connection_log WHERE {Window("record_time", startMicros, endMicros)}";

    // ---- catalog (daily) ----

    public const string CatalogTables = "SELECT CURRENT_DATE AS snapshot_date, * FROM svv_table_info";
    public const string CatalogColumns = "SELECT CURRENT_DATE AS snapshot_date, * FROM svv_all_columns";
    public static readonly string CatalogUsers = $"SELECT CURRENT_DATE AS snapshot_date, {UserColumns} FROM pg_user";
    public const string CatalogGrants = "SELECT CURRENT_DATE AS snapshot_date, * FROM svv_relation_privileges";

    /// <summary>The SELECT for a task, or null when the stream is not one this engine knows.</summary>
    public static string? ForTask(EndpointConfig endpoint, AgentTask task) => task.Stream switch
    {
        ProtocolInfo.Streams.Events => Events(endpoint, task.StartMicros, task.EndMicros),
        ProtocolInfo.Streams.QueryText => QueryText(endpoint, task.StartMicros, task.EndMicros),
        ProtocolInfo.Streams.QueryDetail => QueryDetail(task.StartMicros, task.EndMicros),
        ProtocolInfo.Streams.Unloads => Unloads(endpoint, task.StartMicros, task.EndMicros),
        ProtocolInfo.Streams.Sessions => Sessions(task.StartMicros, task.EndMicros),
        ProtocolInfo.Streams.CatalogTables => CatalogTables,
        ProtocolInfo.Streams.CatalogColumns => CatalogColumns,
        ProtocolInfo.Streams.CatalogUsers => CatalogUsers,
        ProtocolInfo.Streams.CatalogGrants => CatalogGrants,
        _ => null,
    };
}
