using Trailox.Agent.Config;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Engines.Databricks;

/// <summary>
/// The whole of the SQL this agent runs on a Databricks workspace. Every statement reads a
/// system table or information_schema view as-is (SELECT *); nothing is transformed, classified
/// or joined - that happens on the Trailox side. docs/PROTOCOL.md 6b is the human-readable copy.
/// </summary>
public static class DbxRawSelects
{
    /// <summary>The catalogs Databricks itself owns; never inventoried, never part of a customer's estate.</summary>
    public static readonly string[] SystemCatalogs = { "system", "samples", "__databricks_internal" };

    /// <summary>The marker Databricks substitutes for statement text the principal may not read.</summary>
    public const string RedactedMarker = "<REDACTED>";

    /// <summary>
    /// A Spark SQL string literal. Spark escapes with a backslash and treats '' as two adjacent
    /// literals (silently concatenated), so the ClickHouse-style doubling must not be used here.
    /// </summary>
    public static string Lit(string value) => "'" + value.Replace("\\", "\\\\").Replace("'", "\\'") + "'";

    private static string Ts(long micros) => $"timestamp_micros({micros}L)";

    private static string Window(string column, long startMicros, long endMicros) =>
        $"{column} > {Ts(startMicros)} AND {column} <= {Ts(endMicros)}";

    private static string ExcludedCatalogs(EndpointConfig endpoint, string column)
    {
        var all = SystemCatalogs.Concat(endpoint.ExcludedDatabases).Distinct(StringComparer.OrdinalIgnoreCase).Select(Lit);
        return $"{column} NOT IN ({string.Join(", ", all)})";
    }

    // ---- probe (once per endpoint per probe interval) ----

    public const string ProbeVersion = "SELECT current_version().dbsql_version";

    /// <summary>Fails toward readable: only an all-redacted sample of at least one row is evidence of masking.</summary>
    public const string ProbeTextRedaction =
        "SELECT count(*) AS sampled, sum(CASE WHEN statement_text = '" + RedactedMarker + "' THEN 1 ELSE 0 END) AS redacted " +
        "FROM (SELECT statement_text FROM system.query.history WHERE statement_text IS NOT NULL ORDER BY end_time DESC LIMIT 200)";

    public const string ProbeAudit =
        "SELECT count(*) FROM system.access.audit WHERE event_time > current_timestamp() - INTERVAL 24 HOURS AND service_name = 'accounts'";

    public const string ProbeEarliestEvent = "SELECT COALESCE(min(unix_micros(end_time)), 0) FROM system.query.history";

    public const string ProbeEarliestSession =
        "SELECT COALESCE(min(unix_micros(event_time)), 0) FROM system.access.audit WHERE service_name = 'accounts'";

    // ---- streams ----

    /// <summary>
    /// One window of statements. end_time, not start_time: a statement has its metrics, its error
    /// and its lineage only once it has finished, and the window is half-open on the column it is
    /// ordered by. When the customer keeps statement text at home it is simply not selected.
    /// </summary>
    public static string Events(EndpointConfig endpoint, long startMicros, long endMicros)
    {
        var columns = endpoint.StoreRawQueryText ? "*" : "* EXCEPT (statement_text)";
        return $"SELECT {columns} FROM system.query.history WHERE {Window("end_time", startMicros, endMicros)}";
    }

    public static string TableLineage(long startMicros, long endMicros) =>
        $"SELECT * FROM system.access.table_lineage WHERE {Window("event_time", startMicros, endMicros)} AND statement_id IS NOT NULL";

    public static string ColumnLineage(long startMicros, long endMicros) =>
        $"SELECT * FROM system.access.column_lineage WHERE {Window("event_time", startMicros, endMicros)} AND statement_id IS NOT NULL";

    /// <summary>The sign-in feed is the accounts service; which of its actions are sign-ins is Trailox's to decide.</summary>
    public static string Sessions(long startMicros, long endMicros) =>
        $"SELECT * FROM system.access.audit WHERE {Window("event_time", startMicros, endMicros)} AND service_name = 'accounts'";

    // ---- catalog (daily) ----

    public static string CatalogTables(EndpointConfig endpoint) =>
        $"SELECT current_date() AS snapshot_date, * FROM system.information_schema.tables WHERE {ExcludedCatalogs(endpoint, "table_catalog")}";

    public static string CatalogColumns(EndpointConfig endpoint) =>
        $"SELECT current_date() AS snapshot_date, * FROM system.information_schema.columns WHERE {ExcludedCatalogs(endpoint, "table_catalog")}";

    public static string CatalogTablePrivileges(EndpointConfig endpoint) =>
        $"SELECT current_date() AS snapshot_date, * FROM system.information_schema.table_privileges WHERE {ExcludedCatalogs(endpoint, "table_catalog")}";

    public static string CatalogSchemaPrivileges(EndpointConfig endpoint) =>
        $"SELECT current_date() AS snapshot_date, * FROM system.information_schema.schema_privileges WHERE {ExcludedCatalogs(endpoint, "catalog_name")}";

    /// <summary>The SELECT for a task, or null when the stream is not one this engine knows.</summary>
    public static string? ForTask(EndpointConfig endpoint, AgentTask task) => task.Stream switch
    {
        ProtocolInfo.Streams.Events => Events(endpoint, task.StartMicros, task.EndMicros),
        ProtocolInfo.Streams.LineageTables => TableLineage(task.StartMicros, task.EndMicros),
        ProtocolInfo.Streams.LineageColumns => ColumnLineage(task.StartMicros, task.EndMicros),
        ProtocolInfo.Streams.Sessions => Sessions(task.StartMicros, task.EndMicros),
        ProtocolInfo.Streams.CatalogTables => CatalogTables(endpoint),
        ProtocolInfo.Streams.CatalogColumns => CatalogColumns(endpoint),
        ProtocolInfo.Streams.CatalogTablePrivileges => CatalogTablePrivileges(endpoint),
        ProtocolInfo.Streams.CatalogSchemaPrivileges => CatalogSchemaPrivileges(endpoint),
        _ => null,
    };
}
