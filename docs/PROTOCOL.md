# Trailox Agent protocol, v1

This document describes everything the agent does: the requests it sends to Trailox, how it
reacts to the answers, and every SQL statement it runs on your databases. It exists so you can
audit the agent without reading its source.

In one sentence: **Trailox tells the agent what to read, the agent reads it with a fixed
`SELECT` and streams the rows back.** The agent keeps no cursor, no schedule and no state.

---

## 1. Transport and identity

| | |
|---|---|
| Base URL | `https://agent.trailox.io`, port 443. The agent only makes outbound requests |
| TLS | TLS 1.2 or newer. `HTTPS_PROXY` / `NO_PROXY` and `SSL_CERT_FILE` / `SSL_CERT_DIR` are honoured |
| Authentication | `Authorization: Bearer <agent key>` on every request. The key (`tlx_` followed by 40 characters) is shown once when the agent is enrolled in Trailox |
| Account | Derived from the key; no request names an account |
| Versioning | Request headers `X-Trailox-Protocol: 1` and `X-Trailox-Agent: <agent version>`. The check-in reply carries `minAgentVersion`; an older agent logs the reason and exits with code 4 |
| Errors | JSON `{ "error": "<code>", "message": "<text>" }` with the HTTP status below |
| Retries | Exponential backoff with jitter, from 5 seconds up to 5 minutes, indefinitely. The agent never buffers rows: the database keeps them until they are read |

| Status | What the agent does |
|---|---|
| 401 | The key is unknown or revoked: the agent exits with code 3 |
| 404 / 409 on an upload | The task is stale or already completed: skipped |
| any other error on an upload (for example 413 too large, 422 rejected, 429, 5xx) | The task is closed; the agent logs the error, reports it in the next check-in's `lastErrors`, waits for `Retry-After` when given, and moves on. Trailox re-issues the window |
| any error on a check-in | Retried with backoff, honouring `Retry-After` |

## 2. Check-in: `POST /v1/agent/checkin`

Sent at start-up, then every `checkinSeconds` (set by Trailox, default 60), and immediately after
the last task of a batch completes. It reports what the agent is configured to read and what it
found there, and receives the tasks to run.

```jsonc
// request
{
  "agentVersion": "1.3.0",
  "host": "db-jump-01",                     // the container's hostname, informational
  "startedAtUtc": "2026-09-15T10:00:00Z",
  "configFingerprint": "sha256:...",        // hash of agent.yaml, to show when the config changed
  "endpoints": [
    {
      "alias": "prod-cluster",
      "engine": "clickhouse",
      "kind": "onprem",
      "clusterName": "",
      "monitorUser": "trailox_monitor",     // the account the agent connects as
      "collectSessionLog": true,
      "storeRawQueryText": true,
      "excludedDatabases": ["hr_private"],
      "caps": {                             // what the probe (section 6) found
        "version": "25.3.2.1",
        "columns": ["is_internal", "authenticated_user", "hostname",
                    "normalized_query_hash", "used_table_functions", "used_privileges"],
        "hasSessionLog": true,
        "textReadable": true,               // false when the database masks statement text
        "earliestEventMicros": 1757894400000000,     // oldest retained statement, 0 = unknown
        "earliestSessionMicros": 1757894400000000
      },
      "probeError": null                    // the probe's error when the endpoint was unreachable
    }
  ],
  "lastErrors": [ { "alias": "prod-cluster", "at": "...", "message": "..." } ]   // last 20
}
```

The report never contains `host`, `port`, credentials or any row data.

```jsonc
// response
{
  "protocolVersion": 1,
  "minAgentVersion": "1.0.0",
  "checkinSeconds": 60,
  "endpoints": [
    { "alias": "prod-cluster", "endpointId": 31, "enabled": true, "state": "ok" },
    { "alias": "old-alias", "endpointId": null, "enabled": false, "state": "conflict",
      "message": "alias is already used by another source of this account" }
  ],
  "tasks": [                                // in order; executed one at a time
    { "chunkId": "0f1e...", "endpointId": 31, "stream": "events",
      "startMicros": 1757970000000000, "endMicros": 1757991600000000 },
    { "chunkId": "9a2b...", "endpointId": 31, "stream": "sessions",
      "startMicros": 1757970000000000, "endMicros": 1757991600000000 },
    { "chunkId": "c3d4...", "endpointId": 31, "stream": "catalog_tables" }
  ]
}
```

- An endpoint in any state other than `ok`, or with `enabled: false`, receives no tasks. The
  `message` explains why and is logged.
- A task that was issued but not completed is sent again on the next check-in, so a restarted
  agent resumes where it stopped. Uploading the same task twice is harmless.
- Windows are half-open, `(startMicros, endMicros]`, in microseconds since the Unix epoch (UTC).
  Catalog tasks have no window.

## 3. Executing a task

For each task the agent runs the one `SELECT` its engine defines for that stream (sections 6 to
6d) and streams the result straight into the upload below. Rows are gzip-compressed as they
arrive and never written to disk. One task runs at a time.

## 4. Upload: `POST /v1/agent/chunks/{chunkId}`

```
Content-Type: application/x-ndjson
Content-Encoding: gzip
X-Trailox-Rows: <number of rows the agent sent, optional>
body: one JSON object per row, as the database returned it
```

Reply `200 { "rowsAccepted": n }`. Any other reply is handled as in section 1: the agent
records the error, reports it in the next check-in's `lastErrors`, and moves on. Nothing is
retried locally, because Trailox re-issues the window.

## 5. Reporting a failed task: `POST /v1/agent/chunks/{chunkId}/failed`

`{ "message": "..." }`. Sent when the `SELECT` itself fails, for example a missing grant or an
unreachable database. The window is re-issued on a later check-in.

## 6. ClickHouse (engine `clickhouse`)

Over the ClickHouse HTTP interface as the monitoring user. Every request carries
`log_comment=trailox-agent`, and the statement streams exclude rows with that comment, so the
agent never reports its own reads. When `clusterName` is set, `system.query_log` and
`system.session_log` are read through `clusterAllReplicas('<clusterName>', ...)`.

The statement column list is the fixed list below, intersected with the columns the probe found
on `system.query_log`.

```sql
-- probe (once per probe interval)
SELECT version()
SELECT groupArray(name) FROM system.columns WHERE database = 'system' AND table = 'query_log'
  AND name IN (<the column list below>)
SELECT count() FROM system.tables WHERE database = 'system' AND name = 'session_log'
SELECT toInt64(ifNull(toUnixTimestamp64Micro(min(event_time_microseconds)), 0)) FROM system.query_log
SELECT toInt64(ifNull(toUnixTimestamp64Micro(min(event_time_microseconds)), 0)) FROM system.session_log

-- events
SELECT hostname, query_id, toString(type) AS type, event_time_microseconds, user,
       authenticated_user, os_user, initial_user, query_kind,
       normalizeQuery(query) AS normalized_query, sipHash64(query) AS raw_text_hash,
       normalized_query_hash, databases, tables, views, columns, used_table_functions,
       used_privileges, missing_privileges, exception_code, query_duration_ms, result_rows,
       result_bytes, read_rows, read_bytes, written_rows, written_bytes, memory_usage,
       toString(address) AS address, forwarded_for, interface, is_secure, client_name,
       http_user_agent, client_version_major, client_version_minor, client_version_patch,
       log_comment, is_initial_query, distributed_depth, is_internal
       [, query]                                        -- only when storeRawQueryText
FROM system.query_log
WHERE event_time_microseconds > fromUnixTimestamp64Micro(<start>)
  AND event_time_microseconds <= fromUnixTimestamp64Micro(<end>)
  AND type != 'QueryStart' AND is_initial_query = 1
  AND log_comment != 'trailox-agent'
  [AND is_internal = 0]                                 -- when the column exists
  [AND NOT hasAny(databases, [<excludedDatabases>])]    -- when configured
FORMAT JSONEachRow

-- sessions (when collectSessionLog and system.session_log exists)
SELECT hostname, toString(type) AS type, toString(auth_id) AS auth_id, session_id,
       event_time_microseconds, user, toString(auth_type) AS auth_type,
       toString(client_address) AS client_address, toString(interface) AS interface,
       client_name, client_version_major, client_version_minor, client_version_patch,
       failure_reason
FROM system.session_log
WHERE event_time_microseconds > fromUnixTimestamp64Micro(<start>)
  AND event_time_microseconds <= fromUnixTimestamp64Micro(<end>)
FORMAT JSONEachRow

-- catalog_tables (daily)
SELECT today() AS snapshot_date, database, name, engine, engine_full, total_rows, total_bytes,
       metadata_modification_time, comment, create_table_query
FROM system.tables
WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA',
                       '_table_function', '_temporary_and_external_tables')
   OR (database = 'system' AND name IN ('query_log', 'session_log'))
FORMAT JSONEachRow

-- catalog_users (daily; the second form is used on versions where auth_type is not an array)
SELECT today() AS snapshot_date, name, arrayMap(t -> toString(t), auth_type) AS auth_types,
       default_roles_list AS default_roles FROM system.users FORMAT JSONEachRow
SELECT today() AS snapshot_date, name, [toString(auth_type)] AS auth_types,
       CAST([] AS Array(String)) AS default_roles FROM system.users FORMAT JSONEachRow

-- catalog_grants (daily)
SELECT today() AS snapshot_date, user_name, role_name, toString(access_type) AS access_type,
       database, table, column, is_partial_revoke, grant_option
FROM system.grants FORMAT JSONEachRow
```

## 6b. Databricks (engine `databricks`)

Through the workspace's SQL Statement Execution API (`POST https://<workspace>/api/2.0/sql/statements/`,
`disposition: EXTERNAL_LINKS`, `format: JSON_ARRAY`), authenticated as a service principal with
the OAuth client-credentials grant at `https://<workspace>/oidc/v1/token`. Result pages are
fetched from the links the API returns. Every value arrives as a string or null; each row is
sent as one JSON object keyed by the result's column names.

| stream | source | window column | daily |
|---|---|---|---|
| `events` | `system.query.history` | `end_time` | |
| `lineage_tables` | `system.access.table_lineage` | `event_time` | |
| `lineage_columns` | `system.access.column_lineage` | `event_time` | |
| `sessions` | `system.access.audit`, `service_name = 'accounts'` | `event_time` | |
| `catalog_tables` | `system.information_schema.tables` | | yes |
| `catalog_columns` | `system.information_schema.columns` | | yes |
| `catalog_table_privileges` | `system.information_schema.table_privileges` | | yes |
| `catalog_schema_privileges` | `system.information_schema.schema_privileges` | | yes |

`excludedDatabases` lists Unity Catalog catalogs; `system`, `samples` and `__databricks_internal`
are always excluded from the catalog streams. When the probe finds statement text masked as
`<REDACTED>`, the check-in reports `textReadable: false`.

```sql
-- probe
SELECT current_version().dbsql_version
SELECT count(*) AS sampled, sum(CASE WHEN statement_text = '<REDACTED>' THEN 1 ELSE 0 END) AS redacted
  FROM (SELECT statement_text FROM system.query.history WHERE statement_text IS NOT NULL ORDER BY end_time DESC LIMIT 200)
SELECT count(*) FROM system.access.audit WHERE event_time > current_timestamp() - INTERVAL 24 HOURS AND service_name = 'accounts'
SELECT COALESCE(min(unix_micros(end_time)), 0) FROM system.query.history
SELECT COALESCE(min(unix_micros(event_time)), 0) FROM system.access.audit WHERE service_name = 'accounts'

-- streams (window bound = timestamp_micros(<n>L))
SELECT * [EXCEPT (statement_text)] FROM system.query.history WHERE end_time > <start> AND end_time <= <end>
SELECT * FROM system.access.table_lineage WHERE event_time > <start> AND event_time <= <end> AND statement_id IS NOT NULL
SELECT * FROM system.access.column_lineage WHERE event_time > <start> AND event_time <= <end> AND statement_id IS NOT NULL
SELECT * FROM system.access.audit WHERE event_time > <start> AND event_time <= <end> AND service_name = 'accounts'

-- catalog (daily)
SELECT current_date() AS snapshot_date, * FROM system.information_schema.tables WHERE table_catalog NOT IN (<excluded>)
SELECT current_date() AS snapshot_date, * FROM system.information_schema.columns WHERE table_catalog NOT IN (<excluded>)
SELECT current_date() AS snapshot_date, * FROM system.information_schema.table_privileges WHERE table_catalog NOT IN (<excluded>)
SELECT current_date() AS snapshot_date, * FROM system.information_schema.schema_privileges WHERE catalog_name NOT IN (<excluded>)
```

`[EXCEPT (statement_text)]` is used when `storeRawQueryText` is false.

## 6c. Amazon Redshift (engine `redshift`)

Over the Postgres wire protocol (port 5439, TLS required, application name `trailox-agent`) as
the monitoring user. Each connection runs `SET enable_result_cache_for_session TO off`, so the
database does not answer the agent's reads from its result cache. Values are sent as JSON
numbers, booleans, strings or null; timestamps as ISO 8601 text with microseconds.

| stream | source | window column | daily |
|---|---|---|---|
| `events` | `sys_query_history` (all columns except `query_text`) | `end_time` | |
| `query_text` | `sys_query_text` | `start_time` | |
| `query_detail` | `sys_query_detail`, scan / insert / update / delete steps | `start_time` | |
| `unloads` | `sys_unload_history` | `end_time` | |
| `sessions` | `sys_connection_log` | `record_time` | |
| `catalog_tables` | `svv_table_info` | | yes |
| `catalog_columns` | `svv_all_columns` | | yes |
| `catalog_users` | `pg_user` (name, id and privilege flags only) | | yes |
| `catalog_grants` | `svv_relation_privileges` | | yes |

`excludedDatabases` filters `database_name` on the statement, text and unload streams. The
detail stream and the catalog streams are not filtered.
With `storeRawQueryText` false, the `query_text` stream is read without its `text` column.

```sql
-- probe
SELECT version()
SELECT count(*), count(DISTINCT TRIM(username)) FROM sys_query_history WHERE start_time > DATEADD(hour, -24, GETDATE())
SELECT count(*) FROM sys_connection_log WHERE record_time > DATEADD(hour, -24, GETDATE())
SELECT COALESCE(DATEDIFF(microsecond, '1970-01-01'::timestamp, MIN(end_time)), 0) FROM sys_query_history
SELECT COALESCE(DATEDIFF(microsecond, '1970-01-01'::timestamp, MIN(record_time)), 0) FROM sys_connection_log

-- streams (window bound = DATEADD(microsecond, <n>::BIGINT, '1970-01-01'::timestamp))
SELECT <every sys_query_history column except query_text> FROM sys_query_history WHERE end_time > <start> AND end_time <= <end>
SELECT * FROM sys_query_text WHERE start_time > <start> AND start_time <= <end>          -- user_id, query_id, start_time, sequence only when storeRawQueryText is false
SELECT * FROM sys_query_detail WHERE start_time > <start> AND start_time <= <end> AND TRIM(step_name) IN ('scan', 'insert', 'update', 'delete')
SELECT * FROM sys_unload_history WHERE end_time > <start> AND end_time <= <end>
SELECT * FROM sys_connection_log WHERE record_time > <start> AND record_time <= <end>

-- catalog (daily)
SELECT CURRENT_DATE AS snapshot_date, * FROM svv_table_info
SELECT CURRENT_DATE AS snapshot_date, * FROM svv_all_columns
SELECT CURRENT_DATE AS snapshot_date, usename, usesysid, usecreatedb, usesuper, usecatupd FROM pg_user
SELECT CURRENT_DATE AS snapshot_date, * FROM svv_relation_privileges
```

## 6d. Snowflake (engine `snowflake`)

Through the Snowflake SQL API (`POST https://<account>.snowflakecomputing.com/api/v2/statements`,
then `GET .../statements/{handle}?partition=<n>` for each further result partition) with a
key-pair JWT signed by the agent: `iss = <ACCOUNT>.<USER>.SHA256:<fingerprint of the public key>`,
`sub = <ACCOUNT>.<USER>`, lifetime under one hour, header
`X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT`. Each statement names the configured
warehouse and role and has a 30-minute timeout.

The SQL API returns every value as a string or null. The agent sends them unchanged, with column
names in lower case (`QUERY_ID` becomes `query_id`).

| stream | source (`SNOWFLAKE.ACCOUNT_USAGE.`) | window column | daily |
|---|---|---|---|
| `events` | `QUERY_HISTORY` | `END_TIME` | |
| `access_history` | `ACCESS_HISTORY` | `QUERY_START_TIME` | |
| `logins` | `LOGIN_HISTORY` | `EVENT_TIMESTAMP` | |
| `sessions` | `SESSIONS` | `CREATED_ON` | |
| `catalog_tables` | `TABLES` (existing, outside `SNOWFLAKE`) | | yes |
| `catalog_columns` | `COLUMNS` (existing, outside `SNOWFLAKE`) | | yes |
| `catalog_users` | `USERS` (existing) | | yes |
| `catalog_grants` | `GRANTS_TO_ROLES` (current, on tables, views, databases and schemas) | | yes |

With `storeRawQueryText` false, `QUERY_TEXT` and `BIND_VALUES` are excluded from `events`.
`excludedDatabases` are left out of the catalog streams.

```sql
-- probe
SELECT CURRENT_VERSION()
SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY WHERE EVENT_TIMESTAMP > DATEADD(hour, -24, CURRENT_TIMESTAMP())
SELECT COALESCE(DATE_PART(epoch_microsecond, MIN(END_TIME)), 0) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
SELECT COALESCE(DATE_PART(epoch_microsecond, MIN(EVENT_TIMESTAMP)), 0) FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY

-- streams (window bound = TO_TIMESTAMP_LTZ(<n>, 6))
SELECT * [EXCLUDE (QUERY_TEXT, BIND_VALUES)] FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE END_TIME > <start> AND END_TIME <= <end>
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY WHERE QUERY_START_TIME > <start> AND QUERY_START_TIME <= <end>
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY WHERE EVENT_TIMESTAMP > <start> AND EVENT_TIMESTAMP <= <end>
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS WHERE CREATED_ON > <start> AND CREATED_ON <= <end>

-- catalog (daily)
SELECT TO_VARCHAR(CURRENT_DATE(), 'YYYY-MM-DD') AS SNAPSHOT_DATE, TABLE_ID, TABLE_NAME, TABLE_SCHEMA, TABLE_CATALOG, TABLE_OWNER,
       TABLE_TYPE, IS_TRANSIENT, ROW_COUNT, BYTES, RETENTION_TIME, CREATED, LAST_ALTERED, LAST_DDL, LAST_DDL_BY, DELETED, COMMENT
  FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES WHERE DELETED IS NULL AND TABLE_CATALOG NOT IN ('SNOWFLAKE', <excluded>)
SELECT TO_VARCHAR(CURRENT_DATE(), 'YYYY-MM-DD') AS SNAPSHOT_DATE, COLUMN_ID, COLUMN_NAME, TABLE_ID, TABLE_NAME, TABLE_SCHEMA,
       TABLE_CATALOG, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE, COMMENT, DELETED
  FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS WHERE DELETED IS NULL AND TABLE_CATALOG NOT IN ('SNOWFLAKE', <excluded>)
SELECT TO_VARCHAR(CURRENT_DATE(), 'YYYY-MM-DD') AS SNAPSHOT_DATE, USER_ID, NAME, CREATED_ON, DELETED_ON, LOGIN_NAME, HAS_PASSWORD,
       HAS_MFA, HAS_RSA_PUBLIC_KEY, HAS_PAT, DISABLED, DEFAULT_ROLE, DEFAULT_WAREHOUSE, LAST_SUCCESS_LOGIN, TYPE, OWNER
  FROM SNOWFLAKE.ACCOUNT_USAGE.USERS WHERE DELETED_ON IS NULL
SELECT TO_VARCHAR(CURRENT_DATE(), 'YYYY-MM-DD') AS SNAPSHOT_DATE, CREATED_ON, MODIFIED_ON, PRIVILEGE, GRANTED_ON, NAME, TABLE_CATALOG,
       TABLE_SCHEMA, GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, GRANTED_BY, DELETED_ON, GRANTED_BY_ROLE_TYPE, OBJECT_INSTANCE
  FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES WHERE DELETED_ON IS NULL AND GRANTED_ON IN ('TABLE', 'VIEW', 'DATABASE', 'SCHEMA')
```

## 7. Configuration

See the [README](../README.md#configuration) for `agent.yaml`, the environment variables and the
per-engine fields, and [`deploy/agent.yaml.example`](../deploy/agent.yaml.example) for a complete
example.
