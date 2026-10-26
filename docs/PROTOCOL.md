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

Sent at start-up, then every `checkinSeconds` (set by Trailox in each answer), and immediately after
the last task of a batch completes. It reports what the agent is configured to read and what it
found there, and receives the tasks to run.

```jsonc
// request
{
  "agentVersion": "1.3.0",
  "host": "db-jump-01",                     // the container's hostname, informational
  "startedAtUtc": "2026-09-15T10:00:00Z",
  "configFingerprint": "sha256:...",        // hash of agent.yaml, and from 1.5.0 of the values its ${...}
                                            // references resolved to, to show when the config changed
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
      "pollMinutes": 10,                    // agent.yaml's, when it sets one (1.4.0); omitted otherwise
      "backfillDays": 30,                   // agent.yaml's, when it sets one (1.4.0); omitted otherwise
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
  "checkinSeconds": 45,
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
- An `ok` answer may carry a `message` too: something to act on about a healthy source. Today it is
  sent when a Snowflake source polls faster than its setup script sized the warehouse's daily credit
  cap for. From 1.4.0 the agent logs it once per distinct message; earlier agents do not show it.
- `pollMinutes` and `backfillDays` are used when Trailox first registers the source. After that the
  source's settings in Trailox decide; the agent keeps reporting what agent.yaml says, and Trailox
  shows a warning when the two differ.
- `enabled` is the switch you set in Trailox, and it says so only in an answer about a registered
  endpoint: state `ok`, `disabled`, or `unreachable` (registered, and its last probe failed). From
  1.3.3 the agent stops probing an endpoint such an answer reports as `enabled: false`, and probes
  it again once one reports `true`. In any other state (`conflict`, `invalid`) the field does not
  carry your setting, and the agent goes on probing: a probe is how an endpoint registers.
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

A `mongo_ops` body (section 6e), from 1.8.2 a `mongo_slow_ops`, `mongo_connections` or
`mongo_signin_failures` body, and from 1.12.0 every body of a `postgres` endpoint (section 6f), may also contain
blank lines, before, between or after rows; a blank line is not a row, and a body of blank lines only has no rows.

Reply `200 { "rowsAccepted": n }`. Any other reply is handled as in section 1: the agent
records the error, reports it in the next check-in's `lastErrors`, and moves on. Nothing is
retried locally, because Trailox re-issues the window.

## 5. Reporting a failed task: `POST /v1/agent/chunks/{chunkId}/failed`

`{ "message": "...", "reason": "..." }`. Sent when a task fails for any reason but the agent stopping:
the `SELECT` fails (a missing grant, an unreachable database), the database does not answer in time, or
it answers with something the agent cannot read. The window is re-issued on a later check-in. When this
report cannot be delivered, the task stays unanswered and is handed back at a later check-in.

`reason` is sent only when the agent knows it (from 1.8.0): `not_yet_published` means the source has not
published the whole window yet (MongoDB Atlas log files arrive minutes late). Trailox re-issues the
same window at a later poll, and the agent does not list it among its errors.

## 6. ClickHouse (engine `clickhouse`)

Over the ClickHouse HTTP interface as the monitoring user. Every request carries
`log_comment=trailox-agent`, and the statement stream excludes the reads of the monitoring user
that carry that comment, so the agent never reports its own reads. Any user can set that comment,
so every other statement that carries it - a change, or another user's statement - is reported
like any other (from 1.12.0). When `clusterName` is set, `system.query_log` and
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
  AND NOT (log_comment = 'trailox-agent'                -- the agent's own reads only
           AND query_kind IN ('Select', 'Explain', 'Show', 'Describe', 'Exists', 'Check')
           AND user = '<username>')
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

-- catalog_settings (daily; agent 1.7 and newer). Needs SELECT ON system.settings_profile_elements
-- (or SHOW SETTINGS PROFILES). A refusal is reported as the task's failure, not as an agent error.
SELECT today() AS snapshot_date, profile_name, user_name, role_name, index AS element_index,
       setting_name, value, toString(writability) AS writability, inherit_profile
FROM system.settings_profile_elements
WHERE setting_name IN ('readonly', 'allow_ddl') OR inherit_profile IS NOT NULL
FORMAT JSONEachRow
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
| `catalog_service_principals` | the workspace's service principal directory (from 1.6.0; see below) | | yes |

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

`catalog_service_principals` (agent 1.6.0 and later) is not a statement. Databricks records a
service principal in its system tables by application id only, so to show it by name the agent
lists the workspace's service principals, as the same principal, with one read-only request per
page of 100:

```
GET https://<workspace>/api/2.0/preview/scim/v2/ServicePrincipals?attributes=applicationId,displayName,active&startIndex=<n>&count=100
```

It needs no grant. Every page is read before the upload starts, and each principal is sent as one
JSON object of four strings: `snapshot_date` (the listing's UTC date, as `current_date()` in the
catalog statements), `application_id`, `display_name` and `active` (`"true"` or `"false"`). An
entry without an application id is skipped. If the workspace refuses the listing,
the task is reported as failed with the workspace's status (`Databricks answered HTTP 403: ...`),
and nothing else is affected. Users are not listed: the system tables already name a user by email.

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
| `catalog_credentials` | `CREDENTIALS` (from 1.11.0) | | yes |

With `storeRawQueryText` false, `QUERY_TEXT` and `BIND_VALUES` are excluded from `events`.
`excludedDatabases` are left out of the catalog streams.

From 1.11.0, `catalog_users` also reads `HAS_WORKLOAD_IDENTITY`, `PASSWORD_LAST_SET_TIME`, `DISPLAY_NAME` and
`EMAIL`, and `catalog_credentials` lists every credential of the account: programmatic access tokens, key pairs,
MFA methods and workload identities, with their owner, status, role restriction and dates. Neither reads a secret:
`CREDENTIALS` holds no token or key, and `USERS` holds no password. An agent older than 1.11.0 is never asked for
`catalog_credentials`.

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
       HAS_MFA, HAS_RSA_PUBLIC_KEY, HAS_PAT, DISABLED, DEFAULT_ROLE, DEFAULT_WAREHOUSE, LAST_SUCCESS_LOGIN, TYPE, OWNER,
       HAS_WORKLOAD_IDENTITY, PASSWORD_LAST_SET_TIME, DISPLAY_NAME, EMAIL
  FROM SNOWFLAKE.ACCOUNT_USAGE.USERS WHERE DELETED_ON IS NULL
SELECT TO_VARCHAR(CURRENT_DATE(), 'YYYY-MM-DD') AS SNAPSHOT_DATE, CREATED_ON, MODIFIED_ON, PRIVILEGE, GRANTED_ON, NAME, TABLE_CATALOG,
       TABLE_SCHEMA, GRANTED_TO, GRANTEE_NAME, GRANT_OPTION, GRANTED_BY, DELETED_ON, GRANTED_BY_ROLE_TYPE, OBJECT_INSTANCE
  FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES WHERE DELETED_ON IS NULL AND GRANTED_ON IN ('TABLE', 'VIEW', 'DATABASE', 'SCHEMA')
SELECT TO_VARCHAR(CURRENT_DATE(), 'YYYY-MM-DD') AS SNAPSHOT_DATE, CREDENTIAL_ID, NAME, USER_NAME, TYPE, DOMAIN, COMMENT, STATUS,
       ADDITIONAL_DETAILS, CREATED_BY, LAST_ALTERED_BY, CREATED_ON, LAST_USED_ON, LAST_ALTERED, EXPIRATION_DATE
  FROM SNOWFLAKE.ACCOUNT_USAGE.CREDENTIALS
```

## 6e. MongoDB Atlas (engine `mongodb`, from 1.8.0)

Through the Atlas Administration API (`https://cloud.mongodb.com/api/atlas/v2`) as a service account:
OAuth client credentials (`POST /api/oauth/token`, HTTP Basic client id and secret,
`grant_type=client_credentials`) for a one-hour token, reused until two minutes before it expires. Every
request is a GET; JSON with `Accept: application/vnd.atlas.2023-01-01+json`, log files with
`application/vnd.atlas.2023-02-01+gzip`. A 429 or 5xx waits for `Retry-After` (else 2, 4, 8, 16 s, with
jitter) and is retried up to four times. The agent connects to the cluster itself only with the optional
catalog user (from 1.9.0, below), to list databases and collections and their sizes; it never reads a
document.

| call | when |
|---|---|
| `GET /groups/{project}/clusters/{clusterName}` | probe; the cluster's tier, topology, version and member hosts |
| `GET /groups/{project}/processes` | probe and every 15 minutes; only the hosts in the cluster's standard connection string are kept, and on a sharded cluster only its `SHARD_MONGOS` processes (from 1.8.1) |
| `GET /groups/{project}/clusters/{host}/logs/{mongodb\|mongos}.gz?startDate&endDate` | each server-log task, per member (per mongos router on a sharded cluster); the probe, one minute |
| `GET /groups/{project}/clusters/{host}/logs/{mongodb-audit-log\|mongos-audit-log}.gz?...` | each `mongo_ops` task, per member (per mongos router on a sharded cluster) |
| `GET /groups/{project}/events?minDate&maxDate` | each `atlas_events` task; the probe, one item |
| `GET /groups/{project}/databaseUsers`, `/customDBRoles/roles`, `/accessList` | daily catalog; the probe, one item |
| `GET /groups/{project}/processes/{processId}/databases` | each `catalog_databases` task, per primary (from 1.9.0); `processId` is `host:port` as `/processes` gives it, escaped as one path segment |
| `GET /groups/{project}/processes/{processId}/databases/{database}/measurements?granularity=PT1H&period=P1D` | each `catalog_databases` task, per database of each primary (from 1.9.0) |

**Refused by the probe:** Free (M0), Flex and Serverless clusters, which keep no downloadable logs, and a
paused cluster.

**The Atlas S3 log export (from 1.10.0).** With `options.log_source: s3` the log files come from the
customer's own bucket, where the Atlas log integration writes them, instead of the `logs/*.gz` calls above;
every other call stays, and the service account needs Project Read Only only. The agent reads the bucket as its
own AWS identity, from the AWS SDK's default chain (IRSA, EKS Pod Identity, ECS, the instance profile, or
`AWS_*` variables), in `options.s3_region`. Keys are
`{s3_prefix}/{clusterName}/{host}/{dir}/[{yyyy-MM-dd}/]{yyyyMMddTHHmmss.fffZ}-{dir}[.partNNNNN-of-NNNNN].log`, where
`host` is the process's internal name (its `/processes` id without the port) and `dir` is `mongod` and
`mongod_audit`, or `mongos` and `mongos_audit` for a sharded cluster's routers; the date folder is Atlas's
"Daily folders" path choice. Both layouts are listed at every call, and nothing about a folder is cached: the folder
itself, and each date folder from the day before a time range to its last day (a folder's date need not be the name's). Each object is plain JSON lines, each line an envelope whose `log`
member is the server's line as a JSON string, re-encoded by the export (keys in any order, arrays as objects
keyed `"1"`..`"n"`, nulls left out); the envelope's own `timestamp` is never read.

| call | when |
|---|---|
| `ListObjectsV2` of a process's folder (and its date folders) from `StartAfter` = the window's start | each log task, per process and log type: the objects named from the window's start to its end plus 5 minutes (an object is named after its newest line, and at most 174 s after its oldest, measured) |
| `ListObjectsV2` from the window's end plus 5 minutes, and `GetObject` of the first 64 KiB (1 MiB when the first record is longer) of each object it lists, until one whose first record is past the window's end plus 60 s | each log task, per process and log type: after an export stall, the first object holds the stalled lines under a late name; usually the first object already ends it |
| `GetObject`, up to 8 at a time per process | each log task, every object selected; lines outside the window are dropped by their own time |
| `ListObjectsV2` of a server-log folder from the window's end plus 5 minutes | completeness, per process |
| `ListObjectsV2`, and `GetObject` of an object's first 64 KiB (1 MiB when its first record is longer) | the probe's `s3` check, and the export's live start until one is found |

The probe's `s3` check fails when a process's server-log folder has no object, or its newest object is not an
export record (an empty object is passed over), and warns when the export wrote nothing for a process in the last 10
minutes. `earliestEventMicros` is the export's live start, rounded down to the minute: the latest, across the
processes, of the first object named less than 10 minutes after its first line. Before it are the export's first
objects, written when it was enabled and holding each process's log from before then, which are never read. While
every object is still that backfill, the check warns, the newest object's name is reported, and the next probe looks
again; a live start once found is kept for the life of the process. **Completeness** in this form: each process's server-log folder must
hold an object named after the window's end plus 5 minutes. The export reorders lines within an object by up to 38 s,
so a minute ships once the reader is 60 s past it, not 2 s (below).

**The optional catalog user (from 1.9.0).** With `catalog_username` set, and only then, the agent also
connects to the cluster itself, with the MongoDB driver, to its SRV connection string
(`connectionStrings.standardSrv` from the cluster read above): TLS, SCRAM against `admin` as that user,
application name `trailox-agent/<version> catalog`, a 20-second server selection timeout, at most 8
connections. It opens the connection for one task or one probe and closes it after. These are the only
commands it sends, and none reads a document:

| command | when |
|---|---|
| `connectionStatus` with `showPrivileges: true`, on `admin` | the probe's `catalog` check |
| `listDatabases` with `nameOnly: true`, `authorizedDatabases: true` | each `catalog_collections` task |
| `listCollections` with `nameOnly: true`, `authorizedCollections: true`, per database | each `catalog_collections` task |
| `aggregate` of the one stage `{$collStats: {storageStats: {}, count: {}}}`, per collection, 8 at a time | each `catalog_collections` task |

The role it needs is `listDatabases`, `listCollections`, `collStats` and `dbStats` on all databases. Its
`catalog` check reports `ok`; `warn` when `connectionStatus` shows an action that reads documents (`find`,
`changeStream`) or changes data, collections, indexes, users or roles (`insert`, `update`, `remove`,
`createCollection`, `dropCollection`, ...), naming them; and `fail` when the agent cannot connect or sign in,
with what to check (the agent's address on the project's IP access list, or PrivateLink; the user and its
password). The check never fails the probe. With a catalog user, the check-in's `monitorUser` is that user,
the account that signs in to the cluster; without one it is the service account's client id.

**Windows** are whole minutes. A task whose start or end is not a whole minute fails. A line belongs to the
window with `start <= t < end`, by its own time (`t.$date` in the server log, `ts.$date` in the audit log).
Atlas returns whole published chunks, minutes before and after the asked window, and with auditing off the
last audit file again: every line outside the window is dropped.

**Completeness:** before shipping a window younger than two hours, the agent checks that each member's server
log reaches past the window's end. If not, it reports `not_yet_published` (section 5). A window that ends
before a member existed (its `created` in `/processes`), and whose log has no line at all near the window's
end, is complete and empty for that member (from 1.8.2): a backfill that reaches back before a new cluster
does not wait.

**Streaming:** every log stream's upload streams while the files are read: `mongo_ops` while the audit files
are, and from 1.8.2 the server-log streams too, which until then held all of a window's rows until the upload.
What a task holds does not grow with the window.

**Keep-alive:** a busy cluster with few distinct operations, or a server log with few slow operations or failed
sign-ins, sends few rows while its files are read, so the body can be quiet for minutes. While it is open, the agent flushes
the compressor at least every 15 s, so a proxy or firewall on the path never sees an idle connection. When no
row was sent since the last flush, it first writes a blank line, never inside a row.

**Shape** (version 1): every value replaced by its type (`?string`, `?number`, `?bool`, `?date`, `?objectId`,
`?binary`, `?uuid`, ...), an array of values by `?array<type>`, consecutive equal documents of an array written
once, the driver's envelope (`lsid`, `$clusterTime`, `txnNumber`, `$db`, `$readPreference`, `readConcern`,
`writeConcern`, `maxTimeMS`, `comment`, ...) left out, keys sorted at every depth, an object keyed `"1"`..`"n"`
read as an array, and null fields left out. `shape_hash` is XxHash3-64 of the shape's UTF-8 text.

**Groups:** at most 20,000 groups a minute per member; past that, a minute's new keys go to one row whose text
columns are `__other__`, with the real counts. A line for a minute already shipped in the same task goes into
that minute's next `part`. A minute ships once the file is 2 s past it (the Admin API's files are out of order by at
most 0.1 s), or 60 s from the S3 export (from 1.10.0).

Times are ISO 8601 UTC strings with microseconds. Every column is sent in every row. An audited operation whose line cannot be parsed is still shipped, counted, with `command` = `?unparsed`; a document nested deeper than the shape follows is written `?deep`.

| stream | from | one row is | columns |
|---|---|---|---|
| `mongo_ops` | audit `authCheck`, DDL and user/role types, `startup` | a (minute, part, key) group; a `startup` line on its own | `minute`, `part`, `process`, `process_type`, `atype`, `user`, `auth_db`, `is_system_user`, `client_ip`, `direct_ip`, `connection_uuid`, `command`, `ns`, `shape_hash`, `shape`, `shape_version`, `result`, `event_count`, `first_ts`, `last_ts`, `audit_filter`, `audit_successes` |
| `mongo_slow_ops` | server log `51803` | a (minute, part, key) group | `minute`, `part`, `process`, `process_type`, `conn`, `client_ip`, `app_name`, `command`, `ns`, `shape_hash`, `shape`, `shape_version`, `server_shape_hash`, `err_code`, `is_user_connection`, `event_count`, `first_ts`, `last_ts`, `nreturned`, `docs_examined`, `keys_examined`, `reslen`, `duration_ms_sum`, `duration_ms_max` |
| `mongo_connections` | server log `22943`, `22944`, `5286306`, `4615611` | a line (`event`: `accepted`, `ended`, `signin`, `process_start`) | `ts`, `process`, `event`, `conn_id`, `connection_uuid`, `user`, `auth_db`, `mechanism`, `client_ip`, `client_port`, `app_name`, `driver_name`, `driver_version`, `is_cluster_member`, `pid`, `host` |
| `mongo_signin_failures` | server log `5286307` | a (minute, part, key) group, one per connection | `minute`, `part`, `process`, `user`, `auth_db`, `client_ip`, `app_name`, `driver_name`, `mechanism`, `result`, `reason`, `conn`, `event_count` |
| `atlas_events` | project events, except the agent's own log downloads (`MONGODB_LOGS_DOWNLOADED` with its client id as username); every other event done with its service account is shipped | an event | `event_id`, `created`, `event_type`, `username`, `remote_address`, `target_username`, `database`, `cluster_name`, `raw` |
| `catalog_users` | `databaseUsers` | a database user | `snapshot_date`, `username`, `database_name`, `roles`, `scopes`, `aws_iam_type`, `ldap_auth_type`, `oidc_auth_type`, `x509_type`, `delete_after_date` |
| `catalog_roles` | `customDBRoles/roles` | a custom role | `snapshot_date`, `role_name`, `actions`, `inherited_roles` |
| `catalog_access_list` | `accessList` | an entry | `snapshot_date`, `cidr_block`, `ip_address`, `aws_security_group`, `comment`, `delete_after_date` |
| `catalog_databases` (from 1.9.0) | each primary's `databases` and their `measurements` | a (primary, database) | `snapshot_date`, `process`, `database`, `objects`, `data_size`, `storage_size`, `index_size`, `collections`, `views`, `indexes` |
| `catalog_collections` (from 1.9.0; only with the catalog user) | `listCollections` and `$collStats` | a collection or view | `snapshot_date`, `database`, `collection`, `type`, `count`, `size`, `storage_size`, `index_size`, `indexes`, `capped`, `sharded` |

**`catalog_databases`:** the primaries are the cluster's processes whose `typeName` ends in `_PRIMARY`
(`REPLICA_PRIMARY`, and on a sharded cluster each shard's `SHARD_PRIMARY` or `SHARD_CONFIG_PRIMARY`), never a
`SHARD_MONGOS` router. `process` is the primary's `/processes` id (`host:port`). Each number is the newest point
with a value of `DATABASE_OBJECT_COUNT`, `DATABASE_DATA_SIZE`, `DATABASE_STORAGE_SIZE`, `DATABASE_INDEX_SIZE`,
`DATABASE_COLLECTION_COUNT`, `DATABASE_VIEW_COUNT` and `DATABASE_INDEX_COUNT` in the last day of hourly points; a
metric with none is 0. `admin`, `local` and `config` are left out. It needs no permission beyond Project Read Only.

**`catalog_collections`:** every database the user may list but `admin`, `local` and `config`, and every
collection in it but `system.*`, in name order. `collection` is the name as it is, its own dots included. `type`
is `listCollections`' type (`collection`, `view`, `timeseries`); a view is one row of zeros, with no `$collStats`.
`count` is the count stage's, the other numbers `storageStats`' `size`, `storageSize`, `totalIndexSize` and
`nindexes`. A sharded collection answers once per shard that holds it: counts and sizes are summed, `indexes` is
the largest, and `sharded` is true. At most 10,000 collection rows; past that the listing stops and one more row
says so, with `database` and `collection` empty, `type` `truncated`, zeros and false. Without a catalog user the
task is reported as failed with a message that starts `not configured:` (section 5), and is not listed among the
agent's errors.

`process` is the member's host as the logs endpoint takes it (`userAlias`), except in `catalog_databases`. `connection_uuid` is the canonical
UUID string in both `mongo_ops` (from the audit line's base64, read big-endian) and `mongo_connections`. `reason`
is the error's code name (`UserNotFound`, `AuthenticationFailed`), never its message. `result` on a `__other__`
row is -1. `roles`, `scopes`, `actions` and `inherited_roles` are JSON text.

## 6f. PostgreSQL (engine `postgres`, from 1.12.0)

PostgreSQL 13 and later. The agent reads the **server log** of each server of the source, and signs in to each
server for `pg_stat_statements` and the catalog.

**Sign-in:** SCRAM-SHA-256 only (Npgsql `Require Auth=ScramSHA256`): a request for a clear-text or MD5 password, or
a sign-in without one, is refused before the password is sent. With `tls: true`, `SSL Mode=Require` (no certificate
check) and `Channel Binding=Require`: the sign-in is SCRAM-SHA-256-PLUS, bound to the server's certificate, so it
cannot be passed on by a machine in between; a server or pooler that does not offer it is refused. With
`tls: false`, no TLS and no channel binding. Either refusal is the probe's failure, with what to change.

**Where the log comes from** (`options.log_source`):
- `rds_api`, Amazon RDS and Aurora: as the agent's own AWS identity, from the AWS SDK's default chain, in
  `options.rds_region`. Only stderr files (`postgresql.log.YYYY-MM-DD[-HH[MM]]`) are read.

  | call | when |
  |---|---|
  | `DescribeDBLogFiles`, `FileLastWritten` = the window's start, `FilenameContains` = `postgresql.log.<date>` of the window's start and end | each `pg_log` task, per instance (two calls when the window crosses midnight) |
  | `DownloadDBLogFilePortion`, `Marker` = `"0"` and then the marker returned, 3,000 lines a call | each file that can hold the window, from its start |
  | `DownloadDBLogFilePortion` with no marker, 10,000 lines | once per task, on the newest file when the window starts inside it and it is over 1 MB: when those last lines reach back to the window's start, nothing else of that file is read |
  | `DescribeDBLogFiles`, every file | the probe, per instance: the oldest file and the files' length |

  At most 4 calls a second per source, 2 for a window that ended more than an hour ago. An answer of 1 MB, or one
  that holds `[Your log message was truncated]`, may have lost lines: it is asked again from the same marker with
  half the lines. A single line over 1 MB ships cut, with `truncated` true. A file deleted while it is read
  (`DBLogFileNotFoundFault`) is skipped; an instance that does not exist (`DBInstanceNotFound`) is skipped and
  named in the count row.
- `file`, a self-managed server: the directory `options.log_directory`, mounted read-only. Only `.json` files are
  read when there are any, else `.csv`, else stderr. A file covers [the time in its name, its last write].

**Windows:** a record belongs to the window when `start <= t < end`, by its own time, in UTC (`log_timezone` must
be UTC). The files that can hold the window are read in order, and reading stops at the first record 5 s past the
window's end, which can be in the next file: the logging collector rotates on its own timer. A window that ended
less than 30 s ago fails with `not_yet_published` (section 5).

**Keep-alive:** a log file with few sign-ins, or a catalog database with many tables, yields few rows for
minutes. While a body is open, the agent flushes the compressor at least every 15 s, first writing a blank line
when no row was sent since the last flush (section 4), for every stream of this engine.

**Records:** stderr with the prefix `%t:%r:%u@%d:[%p]:` (RDS's) or `%m:%r:%u@%d:[%p]:%l:%e:%s:%v:%x:%c:%q%a:`;
csvlog (24 columns on 13, 26 on 14-18); jsonlog. In stderr a message's `DETAIL`, `HINT` and `STATEMENT` are lines of
their own with the same pid, and a line without the prefix continues the one before. A record is shipped when it is
one of these, by its level and the start of its message (English messages only):

| kind | record |
|---|---|
| connection | `LOG` `connection received:`, `connection authenticated:`, `connection authorized:`, `replication connection authorized:`, `disconnection:` |
| failure | any `FATAL` or `PANIC`, with its `DETAIL` |
| DDL | `LOG` `statement: ...` (the simple protocol) and `execute <name>: ...` (the extended protocol) |

Every other record is counted, not shipped. A record's `STATEMENT` is never shipped, nor the `DETAIL` of a DDL
record (its parameters).

**Passwords** are replaced by `<redacted>` in every text the agent ships, the log's and `pg_stat_statements`': the
string after the word `PASSWORD` (also after `password =` and `password =>`) in any quoting (`'...'`, `E'...'`,
`U&'...'`, `$tag$...$tag$`); inside any string, a connection string's `password=` value and the password of a
`postgresql://user:password@host` URI. When the string after `PASSWORD` is cut, everything from the keyword on is
replaced.

| stream | rows | columns |
|---|---|---|
| `pg_log` | a shipped record; and per instance one count row, `level` = `summary` | `instance`, `ts`, `pid`, `user`, `database`, `remote_host`, `remote_port`, `level`, `sql_state`, `application_name`, `session_id`, `message`, `detail`, `command`, `format`, `truncated` |
| `pg_stat_snapshot` | an entry of `pg_stat_statements`, per instance, taken when the task runs | `userid`, `user_name`, `dbid`, `database`, `queryid`, `toplevel`, `calls`, `rows`, `total_exec_time`, `shared_blks_hit`, `shared_blks_read`, `temp_blks_written`, `dealloc`, `stats_reset`, `instance`, `taken_at` |
| `catalog_tables` | a table or view, per database | `schema_name`, `table_name`, `kind`, `row_estimate`, `approx_bytes`, `owner`, `snapshot_date`, `database` |
| `catalog_columns` | a column, per database | `schema_name`, `table_name`, `position`, `column_name`, `data_type`, `snapshot_date`, `database` |
| `catalog_users` | a role | `role_name`, `rolsuper`, `rolcreaterole`, `rolcreatedb`, `rolcanlogin`, `rolreplication`, `rolbypassrls`, `rolconnlimit`, `rolvaliduntil`, `member_of`, `snapshot_date` |
| `catalog_grants` | a privilege on a table, a schema or the database, per database | `object_kind`, `schema_name`, `object_name`, `grantee`, `privilege_type`, `snapshot_date`, `database` |
| `catalog_settings` | a logging setting, per instance | `name`, `setting`, `instance`, `snapshot_date` |
| `catalog_statement_text` | a statement's text, per instance; none with `storeRawQueryText: false` | `queryid`, `query`, `instance`, `snapshot_date` |

- `pg_log`: `application_name` is null for stderr, which carries it only on the connection line; `command` is the
  first keyword of a DDL statement and, for `CREATE`, `ALTER` and `DROP`, the kind of object; `message` and `detail`
  are cut at 262,144 characters, with `truncated` true; with `storeRawQueryText: false` a DDL record's `message` is
  empty. The count row's `message` is JSON: `records`, `shipped`, `out_of_window`, `unreadable`, `counted` (per
  kind), `files`, `bytes`, `first_log` (the start of the oldest log found, when it starts after the window's start)
  and `missing` (why the instance could not be read). From 1.13.0:
  - `preamble`: lines before a file's first record (a new RDS replica's restore output), counted once, in the window
    of that record's time.
  - `counted.unrecognised`: on a server whose `lc_messages` is not English, LOG records of no kind the agent knows.
  - `missing` for a log directory: `"deleted"` when no file started for the window within one `log_rotation_age` and
    a newer file exists; `"stopped"` when that newer file's startup process wrote "database system was shut down
    at" (jsonlog or csvlog only).
- `pg_stat_snapshot`: the statement follows the version of the extension installed in `options.database`, not
  the server's (a major upgrade keeps the old version until `ALTER EXTENSION pg_stat_statements UPDATE`): before
  1.9 (PostgreSQL 13) `toplevel`, `dealloc` and `stats_reset` are null, and before 1.8 `total_exec_time` is read
  from `total_time`. Without `pg_stat_statements` on any server the task fails as `not configured:` and is not
  listed among the agent's errors.
- Servers: `pg_stat_snapshot`, `catalog_settings` and `catalog_statement_text` read every server listed. A server
  the agent cannot sign in to (deleted, unreachable) is skipped and the others are read; the task fails only when
  no server could be read. The probe's `instances` check names such a server.
- The catalog: `catalog_tables`, `catalog_columns`, `catalog_users` and `catalog_grants` are read on the first
  replica listed, on the primary while that replica cannot be reached, else on the primary; `catalog_settings`
  and `catalog_statement_text` on every server. `snapshot_date` is the task's start (UTC), one date for the whole
  task. At most 500 databases, 50,000 tables and 200,000 grants per database and 1,000,000 columns per day; a list
  that returns as many rows as its limit counts as cut. A stream cut at a limit ends with one more row of its own
  columns, every one empty or 0 but `snapshot_date` and the kind (`kind`, `data_type` or `object_kind`), which
  reads `truncated`. Columns are read 1,000 tables per statement.
- Times: a time PostgreSQL stores as `infinity` or `-infinity` (`rolvaliduntil` of a role whose password never
  expires), or outside the years 1900 to 2299, is sent as null.

**The statements** are in [`PgSql.cs`](../src/Trailox.Agent/Engines/Postgres/PgSql.cs). Each session sets
`statement_timeout` 30 s, `lock_timeout` 1 s, `idle_in_transaction_session_timeout` 60 s and
`default_transaction_read_only`, with application name `trailox-agent`; one connection at a time. No size
function, no reset of `pg_stat_statements`, and its text only in `catalog_statement_text`.

**The probe** reports `caps.postgres` in the check-in: `platform` (`rds`, `aurora`, `self_managed`), `logSource`,
`logFormat`, `serverVersionNum`, `statStatements`, `instances` (`id`, `role`, `oldestLogMicros`, `logFileMinutes`)
and `checks` (`name`, `status` `ok`/`warn`/`fail`, `message`): `role`, `logging`, `lc_messages`, `log_timezone`,
`logging_collector` (self-managed), `pg_stat_statements` (a warning before extension 1.8), `instances` (each replica
signed in to once), `replicas`, `log_files`. A failed check does not fail the probe. `earliestEventMicros` and
`earliestSessionMicros` are the start of the oldest log file. A rejected password (SQLSTATE 28P01) fails the probe
or the task, and the agent then does not sign in to that source again, for a probe or a task, until it restarts.

**Instances** (`options.instances`, `rds_api`): the RDS instance identifiers, primary first, each `id` or
`id=host`. A replica given as `id` alone gets the primary's host with its first label replaced, only when the
primary's host is its own RDS instance endpoint (`<primary id>.<hash>.<region>.rds.amazonaws.com`); for any other
host (an address, a CNAME, an Aurora cluster endpoint) each replica must be `id=host`, or the endpoint is
invalid.

## 7. Configuration

See the [README](../README.md#configuration) for `agent.yaml`, the environment variables and the
per-engine fields, and [`deploy/agent.yaml.example`](../deploy/agent.yaml.example) for a complete
example.
