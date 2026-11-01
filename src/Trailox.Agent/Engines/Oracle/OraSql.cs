namespace Trailox.Agent.Engines.Oracle;

/// <summary>
/// Every statement the agent runs on an Oracle server: <c>SELECT</c>s, and the session's own settings. It calls no
/// procedure, and of the functions in packages only <c>SYS.DBMS_LOB</c>'s, for a text's length and first characters.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>🔴 EVERY OBJECT IS NAMED WITH ITS OWNER, so no public synonym that a user created can stand in for it.</item>
/// <item>🔴 THE TRAIL IS NEVER COUNTED. A count reads the whole partition; the trail's size comes from the dictionary's
///   statistics and its tablespace's room. The spillover files are slower still, so they are read to their first row.</item>
/// <item>Never <c>DBA_SEGMENTS</c> (slow), and never <c>V$ACTIVE_SESSION_HISTORY</c>, <c>DBA_HIST_*</c> or
///   <c>V$SQL_MONITOR</c> (they need a licence the customer may not have).</item>
/// <item>No <c>EXPLAIN PLAN</c>: as this user it fails, because the grant is on the view and not on what is under it.</item>
/// </list>
/// </remarks>
public static class OraSql
{
    /// <summary>The longest any one statement may run. Past it the driver cancels the call.</summary>
    public const int CommandTimeoutSeconds = 60;

    /// <summary>The longest <c>SQL_TEXT</c> that ships, in characters; a longer one is marked <c>truncated</c>.</summary>
    public const int MaxTextChars = 4000;

    private const int TextPieceChars = 1000;

    private const string TrailTable = "AUD$UNIFIED";

    /// <summary>
    /// Run on every connection. Times are UTC whatever the server's zone, names compare byte by byte, which the column pages
    /// rely on, and no parallel query servers work for the agent.
    /// </summary>
    public static readonly IReadOnlyList<string> SessionSettings = new[]
    {
        "ALTER SESSION SET TIME_ZONE = 'UTC' NLS_COMP = 'BINARY' NLS_SORT = 'BINARY'",
        "ALTER SESSION DISABLE PARALLEL QUERY",
    };

    /// <summary>Where the columns of <see cref="Poll"/> stand in its rows.</summary>
    public const int ColumnTime = 0, ColumnInstance = 1, ColumnUser = 2, ColumnProxy = 3, ColumnAction = 4, ColumnReturnCode = 5, ColumnSchema = 6,
        ColumnObject = 7, ColumnProgram = 8, ColumnOsUser = 9, ColumnUserHost = 10, ColumnTerminal = 11, ColumnAuthentication = 12, ColumnPolicies = 13,
        ColumnPrivilege = 14, ColumnAuditType = 15, ColumnSessionId = 16, ColumnEntryId = 17, ColumnStatementId = 18, ColumnDataPump = 19, ColumnTextLength = 20,
        ColumnText = 21, TextPieces = MaxTextChars / TextPieceChars, ColumnSource = ColumnText + TextPieces;

    private const string PollColumns = """
        event_timestamp_utc, instance_id, dbusername, dbproxy_username, action_name, return_code, object_schema, object_name,
                   client_program_name, os_username, userhost, terminal, authentication_type, unified_audit_policies,
                   system_privilege_used, audit_type, sessionid, entry_id, statement_id, dp_text_parameters1
        """;

    /// <summary>
    /// One window of the audit trail, <c>:lo &lt;= t &lt; :hi</c> by <c>EVENT_TIMESTAMP_UTC</c>, without the agent's own
    /// successful reads, sign-ins and sign-offs. <c>:me</c> is its user in upper case.
    /// </summary>
    /// <remarks>
    /// <list type="bullet">
    /// <item>🔴 ONLY <c>EVENT_TIMESTAMP_UTC</c> PRUNES THE TRAIL'S PARTITIONS; <c>EVENT_TIMESTAMP</c> reads every one. The
    ///   table has no index on time, so a poll reads its whole day's partition, whatever the window.</item>
    /// <item>No <c>ORDER BY</c>: a sort of a busy window costs the customer, and nothing needs the order.</item>
    /// <item>🔴 THE AGENT'S OWN CHANGES AND FAILED SIGN-INS ARE SHIPPED, and so is a record with no user: the filter is
    ///   written so that a null keeps the row. Someone signed in as the agent's user through a proxy is not the agent.</item>
    /// <item>🔴 <c>SQL_TEXT</c> IS A CLOB, AND NO LOB ENTERS THE RESULT. A locator would cost a round trip a row. Only the
    ///   length and the first characters are selected, in pieces that fit a <c>VARCHAR2(4000)</c> in any character set: one
    ///   <c>DBMS_LOB.SUBSTR</c> of 4,000 characters would fail the whole read on one long text of multi-byte characters.</item>
    /// <item><c>SQL_BINDS</c> holds values: it is never selected.</item>
    /// <item><c>SOURCE</c> exists from 19.21, and naming it on an older release fails the read.</item>
    /// </list>
    /// </remarks>
    /// <param name="text">The source's <c>storeRawQueryText</c>: when false, the text is not selected at all.</param>
    /// <param name="source">The view has <c>SOURCE</c>.</param>
    public static string Poll(bool text, bool source)
    {
        var textColumns = string.Concat(Enumerable.Range(0, TextPieces).Select(i => text
            ? $",\n       SYS.DBMS_LOB.SUBSTR(sql_text, {TextPieceChars}, {1 + (i * TextPieceChars)}) AS sql_text_{i + 1}"
            : $",\n       NULL AS sql_text_{i + 1}"));
        var length = text ? "SYS.DBMS_LOB.GETLENGTH(sql_text)" : "0";
        return $"""
            SELECT /* trailox-agent poll */ {PollColumns},
                   {length} AS sql_text_length{textColumns}{(source ? ",\n       source" : "")}
            FROM AUDSYS.UNIFIED_AUDIT_TRAIL
            WHERE event_timestamp_utc >= CAST(:lo AS TIMESTAMP) AND event_timestamp_utc < CAST(:hi AS TIMESTAMP)
              AND NOT (NVL(dbusername, ' ') = :me AND dbproxy_username IS NULL AND NVL(return_code, -1) = 0
                       AND NVL(action_name, ' ') IN ('SELECT', 'LOGON', 'LOGOFF', 'LOGOFF BY CLEANUP'))
            """;
    }

    /// <summary>Whether the poll's view can be read: the grant is checked when the statement is parsed, and no row is touched.</summary>
    public const string PollViewReadable = "SELECT 1 FROM AUDSYS.UNIFIED_AUDIT_TRAIL WHERE 1 = 0";

    public const string SourceColumn =
        "SELECT 1 FROM SYS.ALL_TAB_COLUMNS WHERE owner = 'AUDSYS' AND table_name = 'UNIFIED_AUDIT_TRAIL' AND column_name = 'SOURCE'";

    // ---- the probe

    public const string Product = "SELECT product, version_full FROM SYS.PRODUCT_COMPONENT_VERSION WHERE product LIKE 'Oracle%' AND ROWNUM <= 1";

    /// <summary>The session as the server sees it. <c>UNIFIED_AUDIT_SESSIONID</c> is 0 or null where the trail writes nothing.</summary>
    public const string Session = """
        SELECT USER AS own_user, SYS_CONTEXT('USERENV', 'CON_NAME') AS container,
               SYS_CONTEXT('USERENV', 'CLIENT_PROGRAM_NAME') AS program,
               SYS_CONTEXT('USERENV', 'UNIFIED_AUDIT_SESSIONID') AS audit_session_id
        FROM SYS.DUAL
        """;

    public const string RdsAdmin = "SELECT 1 FROM SYS.ALL_USERS WHERE username = 'RDSADMIN'";

    public const string Options = "SELECT parameter, value FROM SYS.V_$OPTION WHERE parameter IN ('Unified Auditing', 'Partitioning')";

    public const string Parameters = """
        SELECT name, value FROM SYS.V_$PARAMETER
        WHERE name IN ('audit_trail', 'resource_limit', 'audit_sys_operations', 'unified_audit_common_systemlog')
        """;

    /// <summary>The trail's size, from the optimizer's statistics.</summary>
    public const string TrailStatistics = $"""
        SELECT num_rows, blocks, partitioned, last_analyzed FROM SYS.DBA_TABLES
        WHERE owner = 'AUDSYS' AND table_name = '{TrailTable}'
        """;

    public const string TrailPartitioning = $"""
        SELECT interval, def_tablespace_name FROM SYS.DBA_PART_TABLES
        WHERE owner = 'AUDSYS' AND table_name = '{TrailTable}'
        """;

    public const string TrailPartitions = $"""
        SELECT partition_name, high_value, tablespace_name FROM SYS.DBA_TAB_PARTITIONS
        WHERE table_owner = 'AUDSYS' AND table_name = '{TrailTable}'
        """;

    public const string TrailConfiguredTablespace = """
        SELECT parameter_value FROM SYS.DBA_AUDIT_MGMT_CONFIG_PARAMS
        WHERE parameter_name = 'DB AUDIT TABLESPACE' AND audit_trail = 'UNIFIED AUDIT TRAIL'
        """;

    /// <summary>A tablespace's files, and what they may grow to. <c>:t</c> is the tablespace.</summary>
    public const string TablespaceFiles = """
        SELECT NVL(SUM(bytes), 0) AS file_bytes,
               NVL(SUM(CASE WHEN autoextensible = 'YES' THEN GREATEST(maxbytes, bytes) ELSE bytes END), 0) AS max_bytes
        FROM SYS.DBA_DATA_FILES WHERE tablespace_name = :t
        """;

    public const string TablespaceFree = "SELECT NVL(SUM(bytes), 0) AS free_bytes FROM SYS.DBA_FREE_SPACE WHERE tablespace_name = :t";

    /// <summary>Whether any record sits in a spillover file: the read ends at the first row.</summary>
    public const string SpillAny = "SELECT 1 FROM SYS.V_$UNIFIED_AUDIT_TRAIL WHERE ROWNUM <= 1";

    public const string EnabledPolicies = "SELECT policy_name, enabled_option, entity_name, entity_type, success, failure FROM SYS.AUDIT_UNIFIED_ENABLED_POLICIES";

    public const int MaxPolicyObjects = 500;

    /// <summary>The objects the TRAILOX_* policies name. <c>:cap</c> is <see cref="MaxPolicyObjects"/>.</summary>
    public const string PolicyObjects = """
        SELECT policy_name, object_schema, object_name FROM SYS.AUDIT_UNIFIED_POLICIES
        WHERE policy_name LIKE 'TRAILOX\_%' ESCAPE '\' AND NVL(object_name, 'NONE') <> 'NONE' AND ROWNUM <= :cap
        """;

    public const string OwnResourceLimits = "SELECT l.resource_name, l.limit FROM SYS.USER_RESOURCE_LIMITS l";

    public const string OwnAccount = "SELECT expiry_date, account_status FROM SYS.USER_USERS";

    public const string OldStatementAudit = "SELECT user_name, audit_option, success, failure FROM SYS.DBA_STMT_AUDIT_OPTS WHERE ROWNUM <= 50";

    public const string OldPrivilegeAudit = "SELECT user_name, privilege, success, failure FROM SYS.DBA_PRIV_AUDIT_OPTS WHERE ROWNUM <= 50";

    public const string CleanupJobs = "SELECT job_name, job_status, audit_trail FROM SYS.DBA_AUDIT_MGMT_CLEANUP_JOBS";

    // ---- the daily catalog

    private const string CustomerOwners = "(SELECT username FROM SYS.DBA_USERS WHERE oracle_maintained = 'N')";

    public const string Users = """
        SELECT username, account_status, lock_date, expiry_date, created, profile, authentication_type, oracle_maintained
        FROM SYS.DBA_USERS
        """;

    /// <summary>One statement for each kind of grant. <c>:cap</c> is <see cref="MaxGrantsPerKind"/>.</summary>
    public static readonly IReadOnlyList<string> Grants = new[]
    {
        """
        SELECT 'role' AS kind, grantee, granted_role AS privilege, NULL AS owner, NULL AS table_name, admin_option AS grantable
        FROM SYS.DBA_ROLE_PRIVS WHERE ROWNUM <= :cap
        """,
        """
        SELECT 'system' AS kind, grantee, privilege, NULL AS owner, NULL AS table_name, admin_option AS grantable
        FROM SYS.DBA_SYS_PRIVS WHERE ROWNUM <= :cap
        """,
        $"""
        SELECT 'table' AS kind, grantee, privilege, owner, table_name, grantable
        FROM SYS.DBA_TAB_PRIVS WHERE owner IN {CustomerOwners} AND ROWNUM <= :cap
        """,
    };

    public const string Tables = $"""
        SELECT owner, table_name, num_rows, blocks, partitioned, last_analyzed FROM SYS.DBA_TABLES
        WHERE owner IN {CustomerOwners} AND ROWNUM <= :cap
        """;

    /// <summary>The tables whose columns are read, in the order the pages cut them.</summary>
    public const string TableNames = $"""
        SELECT owner, table_name FROM (
            SELECT owner, table_name FROM SYS.DBA_TABLES WHERE owner IN {CustomerOwners} ORDER BY owner, table_name
        ) WHERE ROWNUM <= :cap
        """;

    /// <summary>The columns of one page of tables, from (<c>:o1</c>, <c>:t1</c>) to (<c>:o2</c>, <c>:t2</c>).</summary>
    public const string Columns = $"""
        SELECT owner, table_name, column_name, data_type, column_id FROM SYS.DBA_TAB_COLUMNS
        WHERE owner IN {CustomerOwners}
          AND (owner > :o1 OR (owner = :o1 AND table_name >= :t1))
          AND (owner < :o2 OR (owner = :o2 AND table_name <= :t2))
        """;

    /// <summary>The server's audit setup as <c>catalog_settings</c> ships it: name, setting and detail, each read on its own.</summary>
    public static readonly IReadOnlyList<(string Section, string Object, string Sql)> Settings = new[]
    {
        ("option", "V_$OPTION", "SELECT parameter AS name, value AS setting, NULL AS detail FROM SYS.V_$OPTION WHERE parameter IN ('Unified Auditing', 'Partitioning')"),
        ("parameter", "V_$PARAMETER", """
            SELECT name, value AS setting, NULL AS detail FROM SYS.V_$PARAMETER
            WHERE name IN ('audit_trail', 'resource_limit', 'audit_sys_operations', 'unified_audit_common_systemlog')
            """),
        ("policy", "AUDIT_UNIFIED_ENABLED_POLICIES", """
            SELECT policy_name AS name, enabled_option AS setting, entity_name || ' ' || entity_type || ' ' || success || ' ' || failure AS detail
            FROM SYS.AUDIT_UNIFIED_ENABLED_POLICIES
            """),
        ("statement_audit", "DBA_STMT_AUDIT_OPTS", "SELECT audit_option AS name, user_name AS setting, success || ' ' || failure AS detail FROM SYS.DBA_STMT_AUDIT_OPTS WHERE ROWNUM <= 1000"),
        ("privilege_audit", "DBA_PRIV_AUDIT_OPTS", "SELECT privilege AS name, user_name AS setting, success || ' ' || failure AS detail FROM SYS.DBA_PRIV_AUDIT_OPTS WHERE ROWNUM <= 1000"),
        ("cleanup_job", "DBA_AUDIT_MGMT_CLEANUP_JOBS", "SELECT job_name AS name, job_status AS setting, audit_trail AS detail FROM SYS.DBA_AUDIT_MGMT_CLEANUP_JOBS"),
        ("trail", "DBA_TABLES", $"""
            SELECT 'statistics' AS name, partitioned AS setting,
                   TO_CHAR(num_rows) || ' ' || TO_CHAR(blocks) || ' ' || TO_CHAR(last_analyzed, 'YYYY-MM-DD"T"HH24:MI:SS') AS detail
            FROM SYS.DBA_TABLES WHERE owner = 'AUDSYS' AND table_name = '{TrailTable}'
            """),
        ("trail", "DBA_PART_TABLES", $"""
            SELECT 'partitioning' AS name, interval AS setting, def_tablespace_name AS detail FROM SYS.DBA_PART_TABLES
            WHERE owner = 'AUDSYS' AND table_name = '{TrailTable}'
            """),
        ("trail", "DBA_AUDIT_MGMT_CONFIG_PARAMS", """
            SELECT 'configured_tablespace' AS name, parameter_value AS setting, NULL AS detail FROM SYS.DBA_AUDIT_MGMT_CONFIG_PARAMS
            WHERE parameter_name = 'DB AUDIT TABLESPACE' AND audit_trail = 'UNIFIED AUDIT TRAIL'
            """),
        ("own_limit", "USER_RESOURCE_LIMITS", "SELECT l.resource_name AS name, l.limit AS setting, NULL AS detail FROM SYS.USER_RESOURCE_LIMITS l"),
        ("own_password_limit", "USER_PASSWORD_LIMITS", "SELECT l.resource_name AS name, l.limit AS setting, NULL AS detail FROM SYS.USER_PASSWORD_LIMITS l"),
        ("own_account", "USER_USERS", """
            SELECT account_status AS name, TO_CHAR(expiry_date, 'YYYY-MM-DD"T"HH24:MI:SS') AS setting, NULL AS detail FROM SYS.USER_USERS
            """),
    };

    /// <summary>The limits of one day's catalog. A list that returns as many rows as its limit counts as cut.</summary>
    public const int MaxTables = 50_000;
    public const int MaxGrantsPerKind = 200_000;
    public const int MaxColumnsPerSweep = 1_000_000;
    public const int ColumnPageTables = 1000;
}
