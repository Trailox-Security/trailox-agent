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

    /// <summary>
    /// Agent 1.20.0: the grants that reach tables beyond the table and schema privileges, as shipped: each catalog
    /// privilege, and the owner of each catalog, schema and table. An owner holds every privilege on its object without
    /// a grant, and the privilege views never list it. <c>kind</c> says which row is which.
    /// </summary>
    public static string CatalogOtherGrants(EndpointConfig endpoint) => $"""
        SELECT current_date() AS snapshot_date, 'catalog_privilege' AS kind, catalog_name,
               CAST(NULL AS STRING) AS schema_name, CAST(NULL AS STRING) AS table_name,
               grantee, privilege_type, is_grantable, inherited_from
        FROM system.information_schema.catalog_privileges WHERE {ExcludedCatalogs(endpoint, "catalog_name")}
        UNION ALL
        SELECT current_date(), 'catalog_owner', catalog_name, CAST(NULL AS STRING), CAST(NULL AS STRING),
               catalog_owner, CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING)
        FROM system.information_schema.catalogs WHERE {ExcludedCatalogs(endpoint, "catalog_name")}
        UNION ALL
        SELECT current_date(), 'schema_owner', catalog_name, schema_name, CAST(NULL AS STRING),
               schema_owner, CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING)
        FROM system.information_schema.schemata WHERE {ExcludedCatalogs(endpoint, "catalog_name")}
        UNION ALL
        SELECT current_date(), 'table_owner', table_catalog, table_schema, table_name,
               table_owner, CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING)
        FROM system.information_schema.tables WHERE {ExcludedCatalogs(endpoint, "table_catalog")}
        """;

    // ---- the grant read (agent 1.20.0, DbxGrantRead) ----
    //
    // These two are the exception to "as-is": they ask about this principal's own groups, which only the workspace can
    // answer (current_user(), is_account_group_member(), is_member()).

    /// <summary>A grant of SELECT, MODIFY or ALL PRIVILEGES made on the object itself, not inherited from above it.</summary>
    private const string DirectCountedGrant =
        "upper(COALESCE(inherited_from, 'NONE')) = 'NONE' AND replace(upper(privilege_type), ' ', '_') IN ('SELECT', 'MODIFY', 'ALL_PRIVILEGES')";

    /// <summary>The customer's tables: not in an excluded catalog, and not an information_schema view.</summary>
    private static string NotOurTables(EndpointConfig endpoint) =>
        $"{ExcludedCatalogs(endpoint, "table_catalog")} AND table_schema <> 'information_schema'";

    private static string Holds(string column) =>
        $"{column} = current_user() OR is_account_group_member({column}) OR is_member({column})";

    /// <summary>A name that is neither an email (a user) nor a uuid (a service principal): a group.</summary>
    private static string IsGroupName(string column) =>
        $"COALESCE({column}, '') <> '' AND {column} NOT LIKE '%@%' AND NOT (length({column}) = 36 AND {column} RLIKE '^[0-9a-fA-F-]+$')";

    /// <summary>
    /// Each inventoried catalog with whether this principal may read every grant on it (READ METADATA or MANAGE on the
    /// catalog or the metastore, held itself or through its groups, or owning the catalog), one row with only the
    /// workspace's date, and one row for each group that holds SELECT, MODIFY or ALL PRIVILEGES or owns a table.
    /// Columns: day, catalog, visible, granted_group.
    /// </summary>
    /// <remarks>
    /// Part of the protocol, like the requests of the read: the text changes only with a new version, and
    /// DbxGrantReadTests pins it. information_schema views are left out: they are no customer table, and their owner is
    /// a system name, not a group.
    /// </remarks>
    public static string GrantVisibility(EndpointConfig endpoint) => $"""
        WITH cats AS (
            SELECT DISTINCT table_catalog AS catalog
            FROM system.information_schema.tables
            WHERE {NotOurTables(endpoint)}
        ),
        mine AS (
            SELECT DISTINCT catalog_name AS catalog
            FROM system.information_schema.catalog_privileges
            WHERE replace(upper(privilege_type), ' ', '_') IN ('READ_METADATA', 'MANAGE')
              AND ({Holds("grantee")})
            UNION
            SELECT catalog_name
            FROM system.information_schema.catalogs
            WHERE {Holds("catalog_owner")}
        ),
        metastore AS (
            SELECT count(*) AS n
            FROM system.information_schema.metastore_privileges
            WHERE replace(upper(privilege_type), ' ', '_') IN ('READ_METADATA', 'MANAGE')
              AND ({Holds("grantee")})
        )
        SELECT date_format(current_date(), 'yyyy-MM-dd') AS `day`,
               c.catalog AS `catalog`,
               CASE WHEN m.catalog IS NOT NULL OR (SELECT n FROM metastore) > 0 THEN 1 ELSE 0 END AS `visible`,
               CAST(NULL AS STRING) AS `granted_group`
        FROM cats c
        LEFT JOIN mine m ON m.catalog = c.catalog
        UNION ALL
        SELECT date_format(current_date(), 'yyyy-MM-dd'), CAST(NULL AS STRING), 1, CAST(NULL AS STRING)
        UNION ALL
        SELECT DISTINCT date_format(current_date(), 'yyyy-MM-dd'), CAST(NULL AS STRING), 1, name
        FROM (
            SELECT grantee AS name FROM system.information_schema.table_privileges
            WHERE {NotOurTables(endpoint)} AND {DirectCountedGrant}
            UNION ALL
            SELECT grantee FROM system.information_schema.schema_privileges
            WHERE {ExcludedCatalogs(endpoint, "catalog_name")} AND schema_name <> 'information_schema' AND {DirectCountedGrant}
            UNION ALL
            SELECT grantee FROM system.information_schema.catalog_privileges
            WHERE {ExcludedCatalogs(endpoint, "catalog_name")} AND {DirectCountedGrant}
            UNION ALL
            SELECT table_owner FROM system.information_schema.tables
            WHERE {NotOurTables(endpoint)}
        ) p
        WHERE {IsGroupName("name")}
        ORDER BY `catalog`
        """;

    /// <summary>Tables listed per group as candidates for the effective-permissions read.</summary>
    public const int MaxCandidatesPerGroup = 20;

    /// <summary>
    /// Each group and up to <see cref="MaxCandidatesPerGroup"/> tables (catalog.schema.table) on which it holds SELECT
    /// or MODIFY, granted on the table or reaching it from its schema or catalog. Columns: group, table. No name is
    /// spliced in: every group is listed and the ones to ask about are picked by the agent.
    /// </summary>
    public static string GrantCandidates(EndpointConfig endpoint) => $"""
        SELECT grantee AS `group`, full_name AS `table`
        FROM (
            SELECT grantee, full_name, row_number() OVER (PARTITION BY grantee ORDER BY full_name) AS rn
            FROM (
                SELECT DISTINCT grantee, concat_ws('.', table_catalog, table_schema, table_name) AS full_name
                FROM system.information_schema.table_privileges
                WHERE {NotOurTables(endpoint)}
                  AND replace(upper(privilege_type), ' ', '_') IN ('SELECT', 'MODIFY')
                  AND {IsGroupName("grantee")}
            ) d
        ) r
        WHERE rn <= {MaxCandidatesPerGroup}
        ORDER BY `group`, `table`
        """;

    /// <summary>The SELECT for a task, or null when the stream is not one this engine knows.</summary>
    public static string? ForTask(EndpointConfig endpoint, AgentTask task) => task.Stream switch
    {
        ProtocolInfo.Streams.CatalogOtherGrants => CatalogOtherGrants(endpoint),
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
