namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// Every statement the agent runs on a PostgreSQL server. Each reads catalogs or statistics only: no size function (it
/// locks the table for the length of the call), no reset, and the statement text of <c>pg_stat_statements</c> only once a
/// day. The session runs with <c>statement_timeout</c> 30 s, <c>lock_timeout</c> 1 s and read-only transactions.
/// </summary>
public static class PgSql
{
    /// <summary>Connection options sent at start-up: the bounds hold even where the role sets none.</summary>
    public const string SessionOptions =
        "-c statement_timeout=30000 -c lock_timeout=1000 -c idle_in_transaction_session_timeout=60000 -c default_transaction_read_only=on";

    public const string ServerVersion = "SELECT current_setting('server_version_num')::int, current_setting('server_version')";

    /// <summary>The role the agent signs in as, and the two predefined roles its reads need.</summary>
    public const string OwnRole = """
        SELECT r.rolsuper, r.rolconnlimit,
               pg_has_role(current_user, 'pg_read_all_stats', 'USAGE') AS read_all_stats,
               pg_has_role(current_user, 'pg_read_all_settings', 'USAGE') AS read_all_settings
        FROM pg_roles r WHERE r.rolname = current_user
        """;

    /// <summary>The settings the probe checks and <c>catalog_settings</c> ships. A setting this server does not have is not a row.</summary>
    public const string Settings = """
        SELECT name, setting FROM pg_settings
        WHERE name IN ('log_connections', 'log_disconnections', 'log_statement', 'log_line_prefix', 'log_destination',
                       'logging_collector', 'log_directory', 'log_filename', 'log_rotation_age', 'log_rotation_size',
                       'log_file_mode', 'log_timezone', 'log_error_verbosity', 'lc_messages', 'shared_preload_libraries', 'ssl',
                       'password_encryption', 'pg_stat_statements.max', 'pg_stat_statements.track',
                       'server_version_num', 'rds.log_retention_period', 'rds.force_ssl')
           OR name LIKE 'pgaudit.%'
        ORDER BY name
        """;

    /// <summary>Whether <c>pg_stat_statements</c> is created in the database the agent signs in to.</summary>
    public const string StatStatementsVersion = "SELECT extversion FROM pg_extension WHERE extname = 'pg_stat_statements'";

    /// <summary>
    /// Aurora PostgreSQL's version, e.g. <c>17.11.1</c>: the function answers on Aurora only, and elsewhere fails with
    /// "function does not exist" (42883).
    /// </summary>
    /// <remarks>
    /// ⚠ CALL IT, DO NOT LOOK IT UP. Aurora PostgreSQL 17.11 answers <c>aurora_version()</c> but lists no function of that
    ///   name in <c>pg_proc</c> (measured as the master user and as the monitoring role), so a catalog lookup
    ///   reported every Aurora source as RDS.
    /// </remarks>
    public const string AuroraVersion = "SELECT aurora_version()";

    /// <summary>
    /// Whether <c>pg_proc</c> lists a function of an Aurora name. Aurora's own are listed nowhere, so a listed one was
    /// created in the database, and the agent calls neither (<see cref="AuroraVersion"/>, <see cref="AuroraReplicas"/>).
    /// </summary>
    /// <remarks>
    /// 🔴 NEVER RUN A PLANTED AURORA FUNCTION. Neither is in <c>pg_catalog</c>, which PostgreSQL searches first, so the call
    ///    goes through search_path: a user who may create in <c>public</c> (every user before PostgreSQL 15) could put a
    ///    function of the same name there, and the monitoring role would run it, with pg_read_all_stats. Aurora 17.11 lists
    ///    no <c>aurora%</c> function in <c>pg_proc</c>, so a listed one is never Aurora's. A
    ///    schema-qualified call is no fix: Aurora's functions are in no schema the catalog names. The check reads
    ///    <c>pg_catalog.pg_proc</c> by its full name: a search_path that names <c>pg_catalog</c> after another schema
    ///    (ALTER DATABASE ... SET) would otherwise let a <c>pg_proc</c> there answer it. It is the second line: the calls
    ///    themselves run behind <see cref="PinSearchPath"/>, since a function can be created between the check and the call.
    /// </remarks>
    public const string AuroraFunctionListed =
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_proc WHERE proname IN ('aurora_version', 'aurora_replica_status'))";

    /// <summary>
    /// Run first in the transaction of each Aurora call: until it ends, a name resolves in <c>pg_catalog</c> only, so a
    /// function created in any other schema, at any moment, cannot answer <see cref="AuroraVersion"/> or
    /// <see cref="AuroraReplicas"/>. LOCAL: the session's own search_path is back when the transaction ends.
    /// </summary>
    /// <remarks>
    /// ⚠ Aurora PostgreSQL 17.11 answers its functions with this search_path, although they are listed in no schema. If
    ///   it did not, Aurora would read as RDS again, not as a planted function.
    /// </remarks>
    public const string PinSearchPath = "SET LOCAL search_path = pg_catalog";

    /// <summary>Replicas that stream from this server. Aurora's readers do not stream and show here as none.</summary>
    public const string StreamingReplicas = "SELECT count(*) FROM pg_stat_replication";

    public const string AuroraReplicas = "SELECT count(*) - 1 FROM aurora_replica_status()";

    /// <summary>
    /// The counters of every statement shape, without their text (<c>showtext := false</c> reads no text file), for
    /// extension 1.9 and later (PostgreSQL 14+). The extension holds its lock only while it copies the entries; the joins
    /// run after.
    /// </summary>
    /// <remarks>
    /// ⚠ BY THE EXTENSION'S VERSION, NOT THE SERVER'S (<see cref="PgCatalog.SnapshotSql"/>): a major upgrade keeps the
    ///   extension's old version until <c>ALTER EXTENSION pg_stat_statements UPDATE</c>, and its old columns with it.
    /// </remarks>
    public const string Snapshot = """
        SELECT s.userid::bigint AS userid, coalesce(r.rolname, '') AS user_name, s.dbid::bigint AS dbid,
               coalesce(d.datname, '') AS database, s.queryid, s.toplevel, s.calls, s.rows, s.total_exec_time,
               s.shared_blks_hit, s.shared_blks_read, s.temp_blks_written, i.dealloc, i.stats_reset
        FROM pg_stat_statements(showtext := false) s
        LEFT JOIN pg_roles r ON r.oid = s.userid
        LEFT JOIN pg_database d ON d.oid = s.dbid
        CROSS JOIN pg_stat_statements_info i
        """;

    /// <summary>Extension 1.8 (PostgreSQL 13): no <c>toplevel</c> column and no <c>pg_stat_statements_info</c> view.</summary>
    public const string SnapshotWithoutInfo = """
        SELECT s.userid::bigint AS userid, coalesce(r.rolname, '') AS user_name, s.dbid::bigint AS dbid,
               coalesce(d.datname, '') AS database, s.queryid, NULL::boolean AS toplevel, s.calls, s.rows, s.total_exec_time,
               s.shared_blks_hit, s.shared_blks_read, s.temp_blks_written, NULL::bigint AS dealloc, NULL::timestamptz AS stats_reset
        FROM pg_stat_statements(showtext := false) s
        LEFT JOIN pg_roles r ON r.oid = s.userid
        LEFT JOIN pg_database d ON d.oid = s.dbid
        """;

    /// <summary>Extension 1.3 to 1.7, never updated since an older server: <c>total_time</c>, not yet <c>total_exec_time</c>.</summary>
    public const string SnapshotTotalTime = """
        SELECT s.userid::bigint AS userid, coalesce(r.rolname, '') AS user_name, s.dbid::bigint AS dbid,
               coalesce(d.datname, '') AS database, s.queryid, NULL::boolean AS toplevel, s.calls, s.rows, s.total_time AS total_exec_time,
               s.shared_blks_hit, s.shared_blks_read, s.temp_blks_written, NULL::bigint AS dealloc, NULL::timestamptz AS stats_reset
        FROM pg_stat_statements(showtext := false) s
        LEFT JOIN pg_roles r ON r.oid = s.userid
        LEFT JOIN pg_database d ON d.oid = s.dbid
        """;

    /// <summary>The statement text, once a day, cut to 4,096 characters; the agent removes passwords before it ships it.</summary>
    public const string StatementText = "SELECT queryid, left(query, 4096) AS query FROM pg_stat_statements(showtext := true) WHERE queryid IS NOT NULL";

    /// <summary>The databases the catalog visits; <c>rdsadmin</c> is RDS's own and takes no connection.</summary>
    public const string Databases = """
        SELECT datname FROM pg_database
        WHERE datallowconn AND NOT datistemplate AND datname <> 'rdsadmin'
        ORDER BY datname
        """;

    private const string UserRelations = """
        c.relkind IN ('r', 'p', 'v', 'm', 'f')
        AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%'
        """;

    /// <summary>Tables and views, with their size from the planner's statistics (<c>relpages</c>), never from a size function.</summary>
    public const string Tables = $"""
        SELECT n.nspname AS schema_name, c.relname AS table_name, c.relkind::text AS kind, c.reltuples::bigint AS row_estimate,
               c.relpages::bigint * current_setting('block_size')::bigint AS approx_bytes, pg_get_userbyid(c.relowner) AS owner
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE {UserRelations}
        ORDER BY n.nspname, c.relname
        LIMIT $1
        """;

    /// <summary>The relations whose columns are read, in pages of <see cref="ColumnPageTables"/> tables.</summary>
    public const string RelationIds = $"""
        SELECT c.oid::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE {UserRelations}
        ORDER BY c.oid
        LIMIT $1
        """;

    /// <summary>One page of columns: the columns of up to 1,000 tables, so no statement holds the server long.</summary>
    public const string Columns = """
        SELECT n.nspname AS schema_name, c.relname AS table_name, a.attnum::int AS position, a.attname AS column_name,
               format_type(a.atttypid, a.atttypmod) AS data_type
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE a.attrelid = ANY($1::oid[]) AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attrelid, a.attnum
        """;

    public const int ColumnPageTables = 1000;

    /// <summary>
    /// Roles, and the roles each one is a member of. Roles belong to the server, not to a database. From 16 a membership
    /// can be granted once per grantor, so the names are distinct.
    /// </summary>
    public const string Users = """
        SELECT r.rolname AS role_name, r.rolsuper, r.rolcreaterole, r.rolcreatedb, r.rolcanlogin, r.rolreplication,
               r.rolbypassrls, r.rolconnlimit, r.rolvaliduntil,
               coalesce((SELECT string_agg(DISTINCT b.rolname, ',' ORDER BY b.rolname)
                         FROM pg_auth_members m JOIN pg_roles b ON b.oid = m.roleid WHERE m.member = r.oid), '') AS member_of
        FROM pg_roles r
        ORDER BY r.rolname
        """;

    // aclexplode's grantee 0 is PUBLIC, which regrole would print as '-'.
    private const string Grantee = "CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END";

    /// <summary>Privileges granted on relations, schemas and the database itself.</summary>
    public const string Grants = $"""
        SELECT 'table' AS object_kind, n.nspname AS schema_name, c.relname AS object_name,
               {Grantee} AS grantee, a.privilege_type
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace, LATERAL aclexplode(c.relacl) a
        WHERE c.relacl IS NOT NULL AND {UserRelations}
        UNION ALL
        SELECT 'schema', n.nspname, n.nspname, {Grantee}, a.privilege_type
        FROM pg_namespace n, LATERAL aclexplode(n.nspacl) a
        WHERE n.nspacl IS NOT NULL AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%'
        UNION ALL
        SELECT 'database', '', d.datname, {Grantee}, a.privilege_type
        FROM pg_database d, LATERAL aclexplode(d.datacl) a
        WHERE d.datacl IS NOT NULL AND d.datname = current_database()
        LIMIT $1
        """;

    /// <summary>
    /// The limits of one day's catalog. A per-database list that returns as many rows as its limit is taken as cut (one
    /// more row cannot be told from none), and the stream ends with its <c>truncated</c> row.
    /// </summary>
    public const int MaxDatabases = 500;
    public const int MaxTablesPerDatabase = 50_000;
    public const int MaxGrantsPerDatabase = 200_000;
    public const int MaxColumnsPerSweep = 1_000_000;
}
