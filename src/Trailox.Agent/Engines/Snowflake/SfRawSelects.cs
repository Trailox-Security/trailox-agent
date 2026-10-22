using Trailox.Agent.Config;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Engines.Snowflake;

/// <summary>
/// The whole of the SQL this agent runs on a Snowflake account. Every statement reads one
/// SNOWFLAKE.ACCOUNT_USAGE view; nothing is joined, flattened or classified - ACCESS_HISTORY
/// attribution and the login/session join happen on the Trailox side. docs/PROTOCOL.md 6d is the
/// human-readable copy.
/// </summary>
public static class SfRawSelects
{
    public const string AccountUsage = "SNOWFLAKE.ACCOUNT_USAGE";

    /// <summary>A Snowflake string literal: single quotes doubled, backslashes escaped.</summary>
    public static string Lit(string value) => "'" + value.Replace("\\", "\\\\").Replace("'", "''") + "'";

    private static string Ts(long micros) => $"TO_TIMESTAMP_LTZ({micros}, 6)";

    private static string Window(string column, long startMicros, long endMicros) =>
        $"{column} > {Ts(startMicros)} AND {column} <= {Ts(endMicros)}";

    // The SQL API returns DATE as a day count; the raw tables want the calendar date as text.
    private const string SnapshotDate = "TO_VARCHAR(CURRENT_DATE(), 'YYYY-MM-DD') AS SNAPSHOT_DATE";

    private static string ExcludedDatabases(EndpointConfig endpoint, string column)
    {
        var all = new[] { "SNOWFLAKE" }.Concat(endpoint.ExcludedDatabases.Select(d => d.Trim()))
            .Where(d => d.Length > 0).Distinct(StringComparer.OrdinalIgnoreCase).Select(Lit);
        return $"{column} NOT IN ({string.Join(", ", all)})";
    }

    // ---- probe ----

    public const string ProbeVersion = "SELECT CURRENT_VERSION()";

    public const string ProbeLogins =
        "SELECT COUNT(*) FROM " + AccountUsage + ".LOGIN_HISTORY WHERE EVENT_TIMESTAMP > DATEADD(hour, -24, CURRENT_TIMESTAMP())";

    public const string ProbeEarliestEvent =
        "SELECT COALESCE(DATE_PART(epoch_microsecond, MIN(END_TIME)), 0) FROM " + AccountUsage + ".QUERY_HISTORY";

    public const string ProbeEarliestSession =
        "SELECT COALESCE(DATE_PART(epoch_microsecond, MIN(EVENT_TIMESTAMP)), 0) FROM " + AccountUsage + ".LOGIN_HISTORY";

    // ---- streams ----

    /// <summary>
    /// One window of statements on END_TIME (a statement is complete only once finished). When
    /// the customer keeps statement text at home, the text AND the bind values - which carry the
    /// same literals - are not selected.
    /// </summary>
    public static string Events(EndpointConfig endpoint, long startMicros, long endMicros)
    {
        var columns = endpoint.StoreRawQueryText ? "*" : "* EXCLUDE (QUERY_TEXT, BIND_VALUES)";
        return $"SELECT {columns} FROM {AccountUsage}.QUERY_HISTORY WHERE {Window("END_TIME", startMicros, endMicros)}";
    }

    public static string AccessHistory(long startMicros, long endMicros) =>
        $"SELECT * FROM {AccountUsage}.ACCESS_HISTORY WHERE {Window("QUERY_START_TIME", startMicros, endMicros)}";

    public static string Logins(long startMicros, long endMicros) =>
        $"SELECT * FROM {AccountUsage}.LOGIN_HISTORY WHERE {Window("EVENT_TIMESTAMP", startMicros, endMicros)}";

    public static string Sessions(long startMicros, long endMicros) =>
        $"SELECT * FROM {AccountUsage}.SESSIONS WHERE {Window("CREATED_ON", startMicros, endMicros)}";

    // ---- catalog (daily): the columns the raw tables keep, live objects only ----

    public static string CatalogTables(EndpointConfig endpoint) =>
        $"SELECT {SnapshotDate}, TABLE_ID, TABLE_NAME, TABLE_SCHEMA, TABLE_CATALOG, TABLE_OWNER, TABLE_TYPE, IS_TRANSIENT, ROW_COUNT, BYTES, " +
        $"RETENTION_TIME, CREATED, LAST_ALTERED, LAST_DDL, LAST_DDL_BY, DELETED, COMMENT FROM {AccountUsage}.TABLES " +
        $"WHERE DELETED IS NULL AND {ExcludedDatabases(endpoint, "TABLE_CATALOG")}";

    public static string CatalogColumns(EndpointConfig endpoint) =>
        $"SELECT {SnapshotDate}, COLUMN_ID, COLUMN_NAME, TABLE_ID, TABLE_NAME, TABLE_SCHEMA, TABLE_CATALOG, ORDINAL_POSITION, IS_NULLABLE, " +
        $"DATA_TYPE, COMMENT, DELETED FROM {AccountUsage}.COLUMNS " +
        $"WHERE DELETED IS NULL AND {ExcludedDatabases(endpoint, "TABLE_CATALOG")}";

    /// <remarks>
    /// From 1.11: HAS_WORKLOAD_IDENTITY, PASSWORD_LAST_SET_TIME, DISPLAY_NAME and EMAIL, so Trailox can say which users
    /// are people and which are services, how each signs in, and whose each one is. Still no password, key or
    /// first and last name: the view holds no secret, and the two name columns are not read.
    /// </remarks>
    public const string CatalogUsers =
        "SELECT " + SnapshotDate + ", USER_ID, NAME, CREATED_ON, DELETED_ON, LOGIN_NAME, HAS_PASSWORD, HAS_MFA, HAS_RSA_PUBLIC_KEY, HAS_PAT, " +
        "DISABLED, DEFAULT_ROLE, DEFAULT_WAREHOUSE, LAST_SUCCESS_LOGIN, TYPE, OWNER, " +
        "HAS_WORKLOAD_IDENTITY, PASSWORD_LAST_SET_TIME, DISPLAY_NAME, EMAIL FROM " + AccountUsage + ".USERS WHERE DELETED_ON IS NULL";

    /// <summary>
    /// From 1.11: every credential of the account - programmatic access tokens, key pairs, MFA methods and workload
    /// identities - with its owner, status, role restriction and dates. The view holds no secret, and expired
    /// tokens drop out of it 7 days after they expire.
    /// </summary>
    public const string CatalogCredentials =
        "SELECT " + SnapshotDate + ", CREDENTIAL_ID, NAME, USER_NAME, TYPE, DOMAIN, COMMENT, STATUS, ADDITIONAL_DETAILS, " +
        "CREATED_BY, LAST_ALTERED_BY, CREATED_ON, LAST_USED_ON, LAST_ALTERED, EXPIRATION_DATE FROM " + AccountUsage + ".CREDENTIALS";

    public const string CatalogGrants =
        "SELECT " + SnapshotDate + ", CREATED_ON, MODIFIED_ON, PRIVILEGE, GRANTED_ON, NAME, TABLE_CATALOG, TABLE_SCHEMA, GRANTED_TO, " +
        "GRANTEE_NAME, GRANT_OPTION, GRANTED_BY, DELETED_ON, GRANTED_BY_ROLE_TYPE, OBJECT_INSTANCE FROM " + AccountUsage + ".GRANTS_TO_ROLES " +
        "WHERE DELETED_ON IS NULL AND GRANTED_ON IN ('TABLE', 'VIEW', 'DATABASE', 'SCHEMA')";

    /// <summary>The SELECT for a task, or null when the stream is not one this engine knows.</summary>
    public static string? ForTask(EndpointConfig endpoint, AgentTask task) => task.Stream switch
    {
        ProtocolInfo.Streams.Events => Events(endpoint, task.StartMicros, task.EndMicros),
        ProtocolInfo.Streams.AccessHistory => AccessHistory(task.StartMicros, task.EndMicros),
        ProtocolInfo.Streams.Logins => Logins(task.StartMicros, task.EndMicros),
        ProtocolInfo.Streams.Sessions => Sessions(task.StartMicros, task.EndMicros),
        ProtocolInfo.Streams.CatalogTables => CatalogTables(endpoint),
        ProtocolInfo.Streams.CatalogColumns => CatalogColumns(endpoint),
        ProtocolInfo.Streams.CatalogUsers => CatalogUsers,
        ProtocolInfo.Streams.CatalogGrants => CatalogGrants,
        ProtocolInfo.Streams.CatalogCredentials => CatalogCredentials,
        _ => null,
    };
}
