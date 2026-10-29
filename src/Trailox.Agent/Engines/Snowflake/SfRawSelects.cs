using Trailox.Agent.Config;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Engines.Snowflake;

/// <summary>
/// The whole of the SQL this agent runs on a Snowflake account. Each statement reads SNOWFLAKE.ACCOUNT_USAGE
/// views and sends their rows as they are: nothing is flattened or classified. Two reads (access history and
/// sessions) also name the window's statements in QUERY_HISTORY, to keep to their rows. docs/PROTOCOL.md 6d is
/// the human-readable copy.
/// </summary>
public static class SfRawSelects
{
    public const string AccountUsage = "SNOWFLAKE.ACCOUNT_USAGE";

    /// <summary>A Snowflake string literal: single quotes doubled, backslashes escaped.</summary>
    public static string Lit(string value) => "'" + value.Replace("\\", "\\\\").Replace("'", "''") + "'";

    private static string Ts(long micros) => $"TO_TIMESTAMP_LTZ({micros}, 6)";

    private static string Window(string column, long startMicros, long endMicros) =>
        $"{column} > {Ts(startMicros)} AND {column} <= {Ts(endMicros)}";

    // The SQL API returns DATE as a day count; the calendar date is sent as text.
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
    /// One window of statements on END_TIME (a statement is complete only once finished). BIND_VALUES -
    /// the values a statement was run with - is never selected. When the customer keeps statement text
    /// at home, the text is not selected either.
    /// </summary>
    /// <remarks>
    /// ⚠ Not even with storeRawQueryText on, as it was before 1.15.0: bound values are the customer's data,
    ///   not the statement's text.
    /// </remarks>
    public static string Events(EndpointConfig endpoint, long startMicros, long endMicros)
    {
        var excluded = endpoint.StoreRawQueryText ? "BIND_VALUES" : "QUERY_TEXT, BIND_VALUES";
        return $"SELECT * EXCLUDE ({excluded}) FROM {AccountUsage}.QUERY_HISTORY WHERE {Window("END_TIME", startMicros, endMicros)}";
    }

    /// <summary>How far before a window a statement that ended in it may have started, and still be attributed.</summary>
    internal const long StatementLookbackMicros = 24L * 3600 * 1_000_000;

    /// <summary>The statements that ended in a window: the rows that the access history and session reads keep to.</summary>
    private static string WindowStatements(string columns, long startMicros, long endMicros) =>
        $"WITH wq AS (SELECT {columns} FROM {AccountUsage}.QUERY_HISTORY WHERE {Window("END_TIME", startMicros, endMicros)}) ";

    /// <summary>
    /// The ACCESS_HISTORY rows of the statements that ENDED in the window (from 1.15.0), as the events stream reads them.
    /// </summary>
    /// <remarks>
    /// ⚠ BY THE STATEMENT'S END, NOT ITS START. ACCESS_HISTORY has only QUERY_START_TIME, and Snowflake writes a row
    ///   once its statement is complete. Read by start time, a window was read before its long statements ended, so a
    ///   statement that ran over about 3 hours never had its tables. A statement may have started up to a day before
    ///   the window, and the read goes back no further than the window's first statement started, less a minute.
    /// </remarks>
    public static string AccessHistory(long startMicros, long endMicros) =>
        WindowStatements("QUERY_ID, START_TIME", startMicros, endMicros) +
        $"SELECT a.* FROM {AccountUsage}.ACCESS_HISTORY a WHERE {Window("a.QUERY_START_TIME", startMicros - StatementLookbackMicros, endMicros)} " +
        "AND a.QUERY_START_TIME >= (SELECT DATEADD('minute', -1, MIN(wq.START_TIME)) FROM wq) " +
        "AND a.QUERY_ID IN (SELECT wq.QUERY_ID FROM wq)";

    public static string Logins(long startMicros, long endMicros) =>
        $"SELECT * FROM {AccountUsage}.LOGIN_HISTORY WHERE {Window("EVENT_TIMESTAMP", startMicros, endMicros)}";

    /// <summary>
    /// The sessions created in the window, and (from 1.15.0) the older sessions of the statements that ended in it.
    /// </summary>
    /// <remarks>
    /// ⚠ A STATEMENT NEEDS ITS SESSION, HOWEVER OLD. A pooled connection can keep one session for weeks, and a session
    ///   opened before the first window was never read: its statements had no client application. The second half reads
    ///   each such session by its id, every time a window has its statements, so it costs one scan of SESSIONS per window.
    /// </remarks>
    public static string Sessions(long startMicros, long endMicros) =>
        WindowStatements("DISTINCT SESSION_ID", startMicros, endMicros) +
        $"SELECT * FROM {AccountUsage}.SESSIONS WHERE {Window("CREATED_ON", startMicros, endMicros)} " +
        $"UNION ALL SELECT s.* FROM {AccountUsage}.SESSIONS s WHERE s.CREATED_ON <= {Ts(startMicros)} " +
        "AND s.SESSION_ID IN (SELECT wq.SESSION_ID FROM wq)";

    // ---- catalog (daily): the columns sent, live objects only ----

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
