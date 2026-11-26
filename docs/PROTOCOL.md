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
        "earliestSessionMicros": 1757894400000000,
        "queryLogCreatedMicros": 1760000000000000,   // ClickHouse, 1.16.0: when system.query_log was created
        "movedLogAttachedMicros": 1760000600000000   // ClickHouse, 1.16.0: newest moved copy attached; omitted when none
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
- From 1.16.0 a task may also carry `readLimitSeconds`, the limit on each call of its read (section 6g), and `partBytes` and
  `partSeconds`, which say that its rows may be sent in parts (section 4). A task without them runs as it always did.

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
`mongo_signin_failures` body, from 1.12.0 every body of a `postgres` endpoint (section 6f), and from 1.16.0 every body
of an `oracle` endpoint (section 6g), may also contain blank lines, before, between or after rows; a blank line is not a
row, and a body of blank lines only has no rows.

Reply `200 { "rowsAccepted": n }`. Any other reply is handled as in section 1: the agent
records the error, reports it in the next check-in's `lastErrors`, and moves on. Nothing is
retried locally, because Trailox re-issues the window.

### One read in several parts (from 1.16.0)

One read can hold more rows than one upload takes. A task with `partBytes` greater than 0 sends its rows as numbered uploads
to the same `chunkId`, from 1, one at a time, and then says that it has finished:

```
POST /v1/agent/chunks/{chunkId}?part=1           -> 200 { "rowsAccepted": n }
POST /v1/agent/chunks/{chunkId}?part=2           -> 200 { "rowsAccepted": n }
POST /v1/agent/chunks/{chunkId}/complete         { "parts": 2, "readMillis": 41250 }
                                                 -> 200 { "rowsAccepted": <all parts>, "watermarkMicros": n }
```

- A part is an ordinary upload: the same headers and the same body. It holds whole rows, and a row is never cut.
- A part's body is at most `partBytes` before compression, not counting blank lines. It ends at that size, or `partSeconds`
  after the part began, whichever comes first. With no `partSeconds` that is 600 seconds.
- A part begins only when there are rows for it, so no upload stands open while the database reads with nothing to send.
- A part waits for the reply to the one before. All the parts of a read carry one `chunkId`, so its rows stay one chunk.
- `/complete` follows the last part, with how many were sent. Only then is the task done, and only then is the window read.
  A body has no marker of its own, because the last part is not known until the rows have ended. A read with no rows sends no
  part, and `/complete` with `parts` 0.
- `readMillis` on `/complete` is how long the read waited on the database: its calls on the server, summed, with no upload time
  in it. An engine that does not measure leaves it out.
- A read that stops part of the way sends no `/complete`. It is reported as in section 5, or, when a reply to a part stopped it,
  handled as in section 1. Its window is read again from the start, under a new `chunkId`.
- A task without `partBytes` is one upload with no `part`, and no `/complete`.

## 5. Reporting a failed task: `POST /v1/agent/chunks/{chunkId}/failed`

`{ "message": "...", "reason": "...", "readMillis": n }`. Sent when a task fails for any reason but the agent stopping:
the `SELECT` fails (a missing grant, an unreachable database), the database does not answer in time, or
it answers with something the agent cannot read. The window is re-issued on a later check-in. When this
report cannot be delivered, the task stays unanswered and is handed back at a later check-in.

`reason` is sent only when the agent knows it (from 1.8.0): `not_yet_published` means the source has not
published the whole window yet (MongoDB Atlas log files arrive minutes late). Trailox re-issues the
same window at a later poll, and the agent does not list it among its errors. From 1.15.0,
`window_too_long` means the database stopped the read at its time limit (Snowflake's statement timeout,
error `000630`): the same window would stop the same way, so Trailox issues shorter ones. The agent still
lists it among its errors. From 1.16.0, `too_large_at_source` means a bound stopped the read (Oracle's per-call limits,
`ORA-02393` and `ORA-02395`, or the agent's limit on a call): Trailox issues shorter windows, and the agent lists it among its
errors. The agent sends either reason whether the read stopped before the first row or during the upload. `readMillis` (from
1.16.0) is as in section 4, up to where the read stopped, and an engine that does not measure leaves it out.

## 6. ClickHouse (engine `clickhouse`)

Over the ClickHouse HTTP interface as the monitoring user. Every request carries
`log_comment=trailox-agent`, and the statement stream excludes the reads of the monitoring user
that carry that comment, so the agent never reports its own reads. Any user can set that comment,
so every other statement that carries it - a change, or another user's statement - is reported
like any other (from 1.12.0). When `clusterName` is set, `system.query_log` and
`system.session_log` are read through `clusterAllReplicas('<clusterName>', ...)`.

A server can move its log aside: ClickHouse Cloud does when it replaces a service's servers (the
new servers start with an empty `system.query_log` and, minutes later, attach the old servers'
tables as `system.query_log_1`, `_2`, ...), and a self-hosted server renames its log when an
upgrade changes the log's columns. From 1.16.0, when the probe can see such a copy, the statement
windows read `merge('system', '^query_log(_[0-9]+)?$')` instead of `system.query_log`, and the
sign-in windows the same for `session_log`, bounded by the window's days as well
(`AND event_date BETWEEN toDate(<start>) AND toDate(<end>)`); the oldest-row probe still reads the live
log only. A probe of the log tables that fails keeps what the last one saw. The copies need their own grant,
`GRANT SELECT ON system.query_log*` and `system.session_log*` (ClickHouse 24.10 or later); without
it the copies cannot be read, and every statement is as before (see below). The probe reports when the log was
created and when the newest copy was attached (`queryLogCreatedMicros`, `movedLogAttachedMicros`
in the caps, section 2). From 1.18.1 the probe first proves the read across the copies: it runs
each window's own SELECT over `merge()` with `LIMIT 0`, with `wait_end_of_query=1`. A user can see a
copy it may not read - the setup script's `SHOW TABLES` lists it, and without the grant above the read
is refused; ClickHouse 23.10 also runs `merge()` only for a user holding `CREATE TEMPORARY TABLE`. And
a copy written before an upgrade can lack a column the window reads (a server upgraded from 23.10 keeps
copies without `hostname`). When the server refuses the read or names a missing column, the windows
read the live log alone, and no copy is reported; any other failure of the proof keeps what the last
probe decided. Before 1.18.1 such a server failed every statement and sign-in window.

The oldest server the agent reads is ClickHouse 23.10 (from 1.18.1; before it, the probe asked for
the `hostname` column, which ClickHouse added to every system log in 23.11). A server whose logs
have no `hostname` column is read with `FQDN() AS hostname` in its place, the value the column holds
where it exists.

The statement column list is the fixed list below, intersected with the columns the probe found
on `system.query_log`.

```sql
-- probe (once per probe interval)
SELECT version()
SELECT groupArray(name) FROM system.columns WHERE database = 'system' AND table = 'query_log'
  AND name IN (<the column list below>)
SELECT count() FROM system.tables WHERE database = 'system' AND name = 'session_log'
SELECT toInt64(toUnixTimestamp(maxIf(metadata_modification_time, name = 'query_log'))) * 1000000,
       toInt64(toUnixTimestamp(maxIf(metadata_modification_time, match(name, '^query_log_[0-9]+$')))) * 1000000,
       countIf(match(name, '^session_log_[0-9]+$'))
FROM system.tables WHERE database = 'system' AND match(name, '^(query|session)_log(_[0-9]+)?$')   -- 1.16.0
SELECT toInt64(ifNull(toUnixTimestamp64Micro(min(event_time_microseconds)), 0)) FROM system.query_log
SELECT toInt64(ifNull(toUnixTimestamp64Micro(min(event_time_microseconds)), 0)) FROM system.session_log
-- 1.18.1, only when the probe saw a copy: the events and the sessions SELECT below, over merge(), with LIMIT 0

-- events
SELECT hostname, query_id, toString(type) AS type, event_time_microseconds, user,   -- FQDN() AS hostname before 23.11
       authenticated_user, os_user, initial_user, query_kind,
       normalizeQuery(query) AS normalized_query, sipHash64(query) AS raw_text_hash,
       normalized_query_hash, databases, tables, views, columns, used_table_functions,
       used_privileges, missing_privileges, exception_code, query_duration_ms, result_rows,
       result_bytes, read_rows, read_bytes, written_rows, written_bytes, memory_usage,
       toString(address) AS address, forwarded_for, interface, is_secure, client_name,
       http_user_agent, client_version_major, client_version_minor, client_version_patch,
       log_comment, is_initial_query, distributed_depth, is_internal
       [, query]                                        -- only when storeRawQueryText
FROM system.query_log                                   -- or the merge() above, when the probe saw a copy
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
SELECT hostname, toString(type) AS type, toString(auth_id) AS auth_id, session_id,   -- FQDN() AS hostname before 23.11
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
-- The six logging settings and the three apply_to columns are new after 1.15: SHOW SETTINGS PROFILES
-- also reads system.settings_profiles, so no grant is added. A user's or a role's row is sent with
-- apply_to_all 0 and two empty lists, whatever the server's join_use_nulls.
SELECT today() AS snapshot_date, e.profile_name AS profile_name, e.user_name AS user_name,
       e.role_name AS role_name, e.index AS element_index, e.setting_name AS setting_name,
       e.value AS value, toString(e.writability) AS writability, e.inherit_profile AS inherit_profile,
       ifNull(p.apply_to_all, 0) AS apply_to_all, p.apply_to_list AS apply_to_list, p.apply_to_except AS apply_to_except
FROM system.settings_profile_elements AS e
LEFT JOIN system.settings_profiles AS p ON p.name = e.profile_name
WHERE setting_name IN ('readonly', 'allow_ddl', 'log_queries', 'log_queries_min_type',
                       'log_queries_min_query_duration_ms', 'log_queries_probability', 'log_query_settings',
                       'log_queries_cut_to_length') OR inherit_profile IS NOT NULL
FORMAT JSONEachRow

-- catalog_role_grants (daily; agent 1.17 and newer). Needs SELECT ON system.role_grants and SHOW USERS,
-- SHOW ROLES, which every setup script grants. The last row has both names null and an empty role: it says
-- the read ran, also when no one holds a role.
SELECT today() AS snapshot_date, user_name, role_name, granted_role_name, granted_role_is_default, with_admin_option
FROM system.role_grants
UNION ALL SELECT today(), NULL, NULL, '', 0, 0
FORMAT JSONEachRow

-- catalog_show_grants (daily; agent 1.17 and newer). Every grant as SHOW GRANTS prints it, so that a wildcard
-- grant (ON db*.*) can be told from an exact one. Needs SHOW USERS and SHOW ROLES, which every setup script
-- grants. SHOW GRANTS names its one column after itself and cannot carry a date, so the agent reads the date
-- first and sends each statement as {"snapshot_date": "<that date>", "statement": "<the statement>"}.
SELECT toString(today())
SHOW GRANTS FOR ALL
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
| `catalog_other_grants` | catalog privileges and the owners of catalogs, schemata and tables (from 1.20.0) | | yes |
| `catalog_directory` | the grant read: who reaches each table through a group (from 1.20.0; see below) | | yes |

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

`catalog_other_grants` (agent 1.20.0 and later) is one statement: each row of
`system.information_schema.catalog_privileges`, and the owner of each catalog, schema and table, with `kind` saying
which (`catalog_privilege`, `catalog_owner`, `schema_owner`, `table_owner`). An owner holds every privilege on its
object without a grant, and the privilege views never list it.

`catalog_directory` (agent 1.20.0 and later) is the grant read. Trailox needs to know who reaches a table through a
group, and Unity Catalog's views name only the group. The read runs as the same principal, in this order, with SELECT
statements and read-only GET requests:

1. **Visibility statement.** For each catalog, whether this principal may read every grant on it: READ METADATA or
   MANAGE on the catalog or the metastore, held directly or through its groups, or owning the catalog. It also lists
   every group that holds SELECT, MODIFY or ALL PRIVILEGES or owns a table.
2. **Directory.** The workspace's users, service principals and groups, from its SCIM API, 100 per page:
   ```
   GET /api/2.0/preview/scim/v2/Users?attributes=id,userName,active,groups&startIndex=<n>&count=100
   GET /api/2.0/preview/scim/v2/ServicePrincipals?attributes=id,applicationId,active,groups&startIndex=<n>&count=100
   GET /api/2.0/preview/scim/v2/Groups?attributes=id,displayName,members,groups&startIndex=<n>&count=100
   ```
3. **Groups by name.** Each group that holds a grant and was not listed:
   `GET /api/2.0/preview/scim/v2/Groups?filter=displayName eq "<name>"&attributes=...`.
4. **Reads by id.** Each group that came without members and whose members reach a grant, until the workspace refuses
   one. Each member the listings did not return. Paths: `.../Groups/<id>`, `.../Users/<id>`,
   `.../ServicePrincipals/<id>`.
5. **Effective permissions**, only for groups that still show no members:
   - a statement listing, for each group, up to 20 tables on which it holds SELECT or MODIFY;
   - on the fewest such tables that cover those groups, Unity Catalog's effective permissions, asked for each of those
     groups and each active identity:
     ```
     GET /api/2.1/unity-catalog/effective-permissions/table/<catalog.schema.table>?max_results=0&principal=<name>
     ```
   An identity whose answer names a group as a privilege's principal is in that group.

**Bounds:** 400 reads one by one, 10 tables, 1,000 effective-permission requests (not started when more would be
needed), 5 pages per answer, and 5 minutes for those requests.

**What is sent.** Every answer is kept, a refusal included, and sent when the read ends. Each answer is one JSON
object:
- `snapshot_date`: the read's UTC date;
- `read_id`: one id per read, so a task run twice is never taken for one read;
- `seq`: its place in the read, from 1;
- `kind`: `sql` or `get`;
- `request`: `visibility` or `candidates` for a statement, the path and query for a request;
- `status`: the HTTP status, 200 for a statement, 0 when there was no answer;
- `body`: the answer as the workspace gave it; for a statement, its rows as a JSON array of arrays.

**When something fails.** If the visibility statement fails, the task fails. If a listing fails, the read ends there,
and what was read before it is sent.

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
| `catalog_role_grants` | `svv_user_grants`, `svv_role_grants`, `pg_group` (from 1.18.0) | | yes |
| `catalog_other_grants` | `svv_system_privileges`, `svv_schema_privileges`, `svv_database_privileges`, `pg_tables`, `pg_views` (from 1.18.0) | | yes |

From 1.18.0 the agent also reads who holds which role or group (`catalog_role_grants`, with one last row whose names are
all empty: it says the read ran, also when no one holds a role), and the grants that reach tables beyond
`svv_relation_privileges` (`catalog_other_grants`): `ALTER TABLE`, `DROP TABLE` and `TRUNCATE TABLE` held as system
privileges, permissions `FOR TABLES IN SCHEMA` or `DATABASE`, and each table's and view's owner. The `svv` views need
`ACCESS SYSTEM TABLE`, which `sys:monitor` holds, so no grant is added. An agent older than 1.18.0 is never asked for
either stream.

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
-- catalog_role_grants (from 1.18.0)
SELECT CURRENT_DATE AS snapshot_date, TRIM(user_name) AS member, 'user' AS member_type, TRIM(role_name) AS role, 'role' AS role_type, admin_option FROM svv_user_grants
  UNION ALL SELECT CURRENT_DATE, TRIM(role_name), 'role', TRIM(granted_role_name), 'role', FALSE FROM svv_role_grants
  UNION ALL SELECT CURRENT_DATE, TRIM(u.usename), 'user', TRIM(g.groname), 'group', FALSE FROM pg_group g JOIN pg_user u ON u.usesysid = ANY(g.grolist)
  UNION ALL SELECT CURRENT_DATE, '', '', '', '', FALSE
-- catalog_other_grants (from 1.18.0); <system> = ('pg_catalog', 'information_schema', 'pg_internal', 'pg_auto_copy')
SELECT CURRENT_DATE AS snapshot_date, 'system' AS kind, TRIM(identity_name) AS identity_name, TRIM(identity_type) AS identity_type,
       TRIM(system_privilege) AS privilege_type, '' AS namespace_name, '' AS relation_name, '' AS database_name, FALSE AS admin_option
  FROM svv_system_privileges WHERE TRIM(system_privilege) IN ('ALTER TABLE', 'DROP TABLE', 'TRUNCATE TABLE')
  UNION ALL SELECT CURRENT_DATE, 'schema', TRIM(identity_name), TRIM(identity_type), TRIM(privilege_type), TRIM(namespace_name), '', '', admin_option
    FROM svv_schema_privileges WHERE TRIM(privilege_scope) = 'TABLES'
  UNION ALL SELECT CURRENT_DATE, 'database', TRIM(identity_name), TRIM(identity_type), TRIM(privilege_type), '', '', TRIM(database_name), admin_option
    FROM svv_database_privileges WHERE TRIM(privilege_scope) = 'TABLES'
  UNION ALL SELECT CURRENT_DATE, 'owner', TRIM(tableowner), 'user', 'OWNERSHIP', TRIM(schemaname), TRIM(tablename), '', FALSE
    FROM pg_tables WHERE schemaname NOT IN <system> AND schemaname NOT LIKE 'pg_temp%'
  UNION ALL SELECT CURRENT_DATE, 'owner', TRIM(viewowner), 'user', 'OWNERSHIP', TRIM(schemaname), TRIM(viewname), '', FALSE
    FROM pg_views WHERE schemaname NOT IN <system> AND schemaname NOT LIKE 'pg_temp%'
```

## 6d. Snowflake (engine `snowflake`)

Through the Snowflake SQL API (`POST https://<account>.snowflakecomputing.com/api/v2/statements`,
then `GET .../statements/{handle}?partition=<n>` for each further result partition) with a
key-pair JWT signed by the agent: `iss = <ACCOUNT>.<USER>.SHA256:<fingerprint of the public key>`,
`sub = <ACCOUNT>.<USER>`, lifetime under one hour, header
`X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT`. Each statement names the configured
warehouse and role and has a 30-minute timeout. An answer of 429, 503 or 504 (from 1.15.0 also 500 and
502) is retried up to four times (2, 4, 8, 16 s) under the same `requestId`; from 1.15.0 the resubmit
adds `retry=true`, so Snowflake does not run a statement it has already run. A statement that runs past
a time limit - the request's own 30 minutes (the API answers 408) or a lower statement timeout of the
warehouse, the user or the account (error `000630`) - is reported with reason `window_too_long` from
1.15.0.

`host` is the account identifier. Snowsight's data-sharing form `myorg.myaccount` names no host, so from
1.15.0 it is read as `myorg-myaccount` (and `myorg.myaccount.privatelink` as `myorg-myaccount.privatelink`); a
legacy locator keeps its region, cloud and `privatelink` parts, up to five in all
(`xy12345.fhplus.us-gov-west-1.aws.privatelink`).

The SQL API returns every value as a string or null. The agent sends them unchanged, with column
names in lower case (`QUERY_ID` becomes `query_id`).

| stream | source (`SNOWFLAKE.ACCOUNT_USAGE.`) | window column | daily |
|---|---|---|---|
| `events` | `QUERY_HISTORY` | `END_TIME` | |
| `access_history` | `ACCESS_HISTORY` | the statement's `END_TIME` (from 1.15.0; `QUERY_START_TIME` before) | |
| `logins` | `LOGIN_HISTORY` | `EVENT_TIMESTAMP` | |
| `sessions` | `SESSIONS` | `CREATED_ON`, and from 1.15.0 also the older sessions of the window's statements | |
| `catalog_tables` | `TABLES` (existing, outside `SNOWFLAKE`) | | yes |
| `catalog_columns` | `COLUMNS` (existing, outside `SNOWFLAKE`) | | yes |
| `catalog_users` | `USERS` (existing) | | yes |
| `catalog_grants` | `GRANTS_TO_ROLES` (current, on tables, views, databases and schemas; from 1.18.0 also roles and database roles) | | yes |
| `catalog_credentials` | `CREDENTIALS` (from 1.11.0) | | yes |
| `catalog_role_grants` | `GRANTS_TO_USERS` (current; from 1.18.0) | | yes |

From 1.18.0, `catalog_grants` also sends the roles and database roles each role holds (`GRANTED_ON` `ROLE` or
`DATABASE_ROLE`), and `catalog_role_grants` the roles each user holds, so that Trailox can follow a user's privileges
through its roles. `SNOWFLAKE.SECURITY_VIEWER` covers `GRANTS_TO_USERS`, so no grant is added. An agent older than 1.18.0
is never asked for `catalog_role_grants`.

From 1.15.0, `access_history` and `sessions` are cut by the statements that ended in the window, as `events` is: the
access row of a statement that ran for hours comes once the statement is complete, and a statement's session comes
however long ago it opened. Each of the two reads names the window's statements in `QUERY_HISTORY` once, and the access
read first reads their oldest start in a statement of its own. An access row that belongs to no statement in
`QUERY_HISTORY` is not sent. The sessions read has no time bound for those ids, so it may read the whole `SESSIONS` view
(a year of sessions) each window, and a session comes again, with `ACCESS_TIME`, `IS_OPEN` and `CLOSED_REASON` as they
are at that read, in each window that has its statements.

`BIND_VALUES`, the values a statement was run with, is never read (from 1.15.0; before, it was sent when
`storeRawQueryText` was true). With `storeRawQueryText` false, `QUERY_TEXT` is excluded too.
`excludedDatabases` are left out of the catalog streams.

From 1.16.0 each statement also carries its shape, computed by Snowflake: `QUERY_SHAPE_TEXT`, the text with every
literal masked and every name kept (comments dropped, a list of values written `(?)`), and `QUERY_SHAPE_HASH`, that
text's hash as unsigned 64-bit text. Both are sent whatever `storeRawQueryText` says, so statements that differ only in
their values share one shape while the values stay with you. With `storeRawQueryText` false, each statement's names
(tables, columns, functions) and keywords still leave the account in `QUERY_SHAPE_TEXT`; no literal value does. For a
statement Snowflake shows no text for (its own `SYSTEM` user's, for one), `QUERY_SHAPE_TEXT` is empty and
`QUERY_SHAPE_HASH` is the hash of `QUERY_PARAMETERIZED_HASH` instead, so such statements do not all share one hash. The
masking text and the hash expression are part of this protocol: a change to either gives statements a new
`QUERY_SHAPE_HASH`, so it comes only with a new version.

From 1.11.0, `catalog_users` also reads `HAS_WORKLOAD_IDENTITY`, `PASSWORD_LAST_SET_TIME`, `DISPLAY_NAME` and
`EMAIL`, and `catalog_credentials` lists every credential of the account: programmatic access tokens, key pairs,
MFA methods and workload identities, with their owner, status, role restriction and dates. Neither reads a secret:
`CREDENTIALS` holds no token or key, and `USERS` holds no password. An agent older than 1.11.0 is never asked for
`catalog_credentials`.

```sql
-- probe
SELECT CURRENT_VERSION()
SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY WHERE EVENT_TIMESTAMP > DATEADD(hour, -24, CURRENT_TIMESTAMP())
-- the oldest rows: from 1.15.0 at most once a day, not at every probe (a failed read waits a day; the last value is kept)
SELECT COALESCE(DATE_PART(epoch_microsecond, MIN(END_TIME)), 0) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
SELECT COALESCE(DATE_PART(epoch_microsecond, MIN(EVENT_TIMESTAMP)), 0) FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY

-- streams (window bound = TO_TIMESTAMP_LTZ(<n>, 6))
SELECT * EXCLUDE ([QUERY_TEXT, ]BIND_VALUES), <shape> AS QUERY_SHAPE_TEXT,
       TO_VARCHAR(IFF(COALESCE(QUERY_TEXT, '') = '', HASH(COALESCE(QUERY_PARAMETERIZED_HASH, '')), HASH(<shape>)) + 9223372036854775808) AS QUERY_SHAPE_HASH
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE END_TIME > <start> AND END_TIME <= <end>
-- <shape> (from 1.16.0): six REGEXP_REPLACE over COALESCE(QUERY_TEXT, ''), exactly as SfRawSelects.ShapeText
-- access_history: first the oldest start of the window's statements, then their rows from <from> = that start
-- less a minute (a day before the window at most; the window's end when it has no statement; <start - 24 h> when
-- the answer is not a whole number)
SELECT COALESCE(DATE_PART(epoch_microsecond, MIN(START_TIME)), 0) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE END_TIME > <start> AND END_TIME <= <end>
WITH wq AS (SELECT QUERY_ID FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE END_TIME > <start> AND END_TIME <= <end>)
SELECT a.* FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a WHERE a.QUERY_START_TIME >= <from> AND a.QUERY_START_TIME <= <end>
  AND a.QUERY_ID IN (SELECT wq.QUERY_ID FROM wq)
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY WHERE EVENT_TIMESTAMP > <start> AND EVENT_TIMESTAMP <= <end>
WITH wq AS (SELECT DISTINCT SESSION_ID FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE END_TIME > <start> AND END_TIME <= <end>)
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS WHERE CREATED_ON > <start> AND CREATED_ON <= <end>
  UNION ALL SELECT s.* FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS s WHERE s.CREATED_ON <= <start> AND s.SESSION_ID IN (SELECT wq.SESSION_ID FROM wq)

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
  FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES WHERE DELETED_ON IS NULL AND GRANTED_ON IN ('TABLE', 'VIEW', 'DATABASE', 'SCHEMA', 'ROLE', 'DATABASE_ROLE')
  -- before 1.18.0: GRANTED_ON IN ('TABLE', 'VIEW', 'DATABASE', 'SCHEMA')
SELECT TO_VARCHAR(CURRENT_DATE(), 'YYYY-MM-DD') AS SNAPSHOT_DATE, CREATED_ON, DELETED_ON, ROLE, GRANTED_TO, GRANTEE_NAME, GRANTED_BY
  FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS WHERE DELETED_ON IS NULL      -- catalog_role_grants, from 1.18.0
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

**Microsoft Entra sign-in** (from 1.23.0, `options.azure_sign_in: entra`; Azure Database for PostgreSQL flexible
server, `log_source` `azure_files` or `azure_log_analytics`): no password is read. The password is an access token
for scope `https://ossrdbms-aad.database.windows.net/.default`, from the agent's Azure identity as for the log (its
managed identity, else a service principal from `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_CLIENT_SECRET` or
`AZURE_CLIENT_CERTIFICATE_PATH`), asked for when a connection opens with no token held, and kept until 5 minutes before
its expiry. `SSL Mode=VerifyFull` (the server's certificate checked against `host` by the system's CA store, whatever
`tls` says), `Require Auth=Password` (the token goes only to a server that asks for a password, never as SCRAM or
MD5) and `Channel Binding=Disable` (SCRAM's, which does not apply). agent.yaml must give `tls: true` and a `host` that
ends in `.postgres.database.azure.com`. A refusal (SQLSTATE 28P01 or 28000) of a token kept from an earlier
connection asks for a new token and signs in once more; any other refusal stops every sign-in to the source, as a
rejected password does, and the probe's error names the SQL that maps the role to the token's object id
(`pgaadauth_create_principal_with_oid`). A token that cannot be had fails the probe or the task, without a sign-in.

**Where the log comes from** (`options.log_source`):
- `rds_api`, Amazon RDS and Aurora: as the agent's own AWS identity, from the AWS SDK's default chain, in
  `options.rds_region`. Only stderr files (`postgresql.log.YYYY-MM-DD[-HH[MM]]`) are read.

  | call | when |
  |---|---|
  | `DescribeDBLogFiles`, `FileLastWritten` = 5 s before the window's start (from 1.14.0; before, the window's start), `FilenameContains` = `postgresql.log.<date>` of the window's start and end | each `pg_log` task, per instance (two calls when the window crosses midnight) |
  | `DownloadDBLogFilePortion`, `Marker` = `"0"` and then the marker returned, 3,000 lines a call | each file that can hold the window, from its start |
  | `DownloadDBLogFilePortion` with no marker, 10,000 lines | once per task, on the newest file when the window starts inside it and it is over 1 MB: when those last lines reach back to the window's start, nothing else of that file is read. From 1.14.0 a capped answer is asked again with half the lines, down to 300, before the file is read from its start |
  | `DescribeDBLogFiles`, every file | the probe, per instance: the oldest file and the files' length |

  RDS can list the file before the window as last written a few milliseconds before the window's start while it holds
  the window's first records, written into it just before it rotated: listed from the window's start, those records
  were read by no window.

  At most 4 calls a second per source, 2 for a window that ended more than an hour ago. An answer of 1 MB, or one
  that holds `[Your log message was truncated]`, may have lost lines: it is asked again from the same marker with
  half the lines. A single line over 1 MB ships cut, with `truncated` true. A file deleted while it is read
  (`DBLogFileNotFoundFault`) is skipped; an instance that does not exist (`DBInstanceNotFound`) is skipped and
  named in the count row.
- `azure_files` (from 1.22.0), Azure Database for PostgreSQL flexible server: Azure's server log download
  (`logfiles.download_enable`), one stderr file an hour (`postgresql_YYYY_MM_DD_HH_00_00.log`), as the agent's own
  Azure identity: its managed identity (workload identity, then the host's own), else a service principal from
  `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_CLIENT_SECRET` or `AZURE_CLIENT_CERTIFICATE_PATH`. No developer
  sign-in is tried.

  | call | when |
  |---|---|
  | `GET https://management.azure.com/subscriptions/{azure_subscription}/resourceGroups/{azure_resource_group}/providers/Microsoft.DBforPostgreSQL/flexibleServers/{azure_server}/logFiles?api-version=2024-08-01`, then each `nextLink` on `management.azure.com` (any other host is refused) | the probe; each `pg_log` task, once |
  | `GET` of a file's signed link with `Range`, 64 KB | to find where a window begins in a file that starts before it: the file is halved on the times of its lines; and for a recent window, once, on the newest file's end |
  | `GET` of a file's signed link with `Range`, 4 MB | from that offset, until a line 5 s past the window's end |

  The list needs `Microsoft.DBforPostgreSQL/flexibleServers/logFiles/read` on the server. Each link is a read-only
  blob link that expires about 5 minutes after the list and takes no token; one that answers 403 or 404 is listed
  again. `sizeInKb` is in thousands of bytes, rounded down. List calls are paced to 2 a second per subscription, file
  reads to 4 a second (2 for a window that ended more than an hour ago). Azure writes the files about every 20
  minutes, each time the current hour's and the hour before's, so a window that ended within 2 hours fails with
  `not_yet_published` until the newest file holds a line 5 s past its end. The lines are JSON string content: `\"` is a quote, `\n` a newline, and a
  record's `statement` and `detail_log` follow its text on the same line as `","statement": "...`.
- `azure_log_analytics` (from 1.22.0), Azure Database for PostgreSQL flexible server: the rows a diagnostic setting
  sends to Log Analytics (category `PostgreSQLLogs`), through the Log Analytics query API, as the same Azure identity
  as `azure_files`, with a token for `https://api.loganalytics.io`.

  | call | when |
  |---|---|
  | `POST https://api.loganalytics.azure.com/v1/subscriptions/{azure_subscription}/resourceGroups/{azure_resource_group}/providers/Microsoft.DBforPostgreSQL/flexibleServers/{azure_server}/query` (with `azure_workspace_id`: `.../v1/workspaces/{azure_workspace_id}/query`, the rows filtered on the server's `_ResourceId`) | every query |
  | `union isfuzzy=true` of both tables, the server's rows in the last day; then the chosen table's oldest row | the probe |
  | the table's first row stamped 5 minutes past the window's end, if any | a window that ended within the hour |
  | the window's rows from a second before it to a minute past it, in order of `TimeGenerated` then `_ItemId`, 20,000 a page, each page after the last row of the one before | each `pg_log` task, until a line past the window's end; and a look-back |

  The resource route needs `Microsoft.Insights/logs/*/read` on the server (measured alone: it reaches the server's
  own log tables, nothing else); the workspace route `Microsoft.OperationalInsights/workspaces/read`,
  `workspaces/query/read` and `workspaces/query/PGSQLServerLogs/read` on the workspace (measured: `search *` then
  reaches that table only; without `query/read` it is refused), or Log Analytics Reader. Every query sends
  `Cache-Control: no-cache`: Azure answers repeated text from a cache for about 2 minutes (measured). The table is `options.azure_log_table` (`PGSQLServerLogs` or `AzureDiagnostics`), else the one
  the probe found the server's rows in, `PGSQLServerLogs` first. Each row is made the download's line: its `Message`
  as JSON string content, then `Detail`, `DetailLog`, `Context` and `Statement` as `","detail": "...` and so on, so
  one parser reads both sources (measured: the same stretch read from both gives every Log Analytics row as a
  download line, to the character; the download also holds Azure's own `[Microsoft Entra]`, `[pg-availability]` and
  `pg_qs` lines). An answer past 500,000 rows or about 64 MB comes back 200 with a `PartialError`: the page is asked
  again with half as many rows, down to 100. A 429 or 503 is sent again after its `Retry-After` (else 10, 20, 30 s),
  3 times. Queries carry `Prefer: wait=600`, are paced to 2 a second for the agent, and give `bytes` (the answers'
  size) and no `files`. A window that ended within the hour fails with `not_yet_published` until a row stamped 5
  minutes past its end has arrived: rows arrive 120 to 347 s after they are written, in batches that can come after
  a newer one, at worst 176 s behind the newest row already there (6,480 rows over 5.5 hours, measured).
- `file`, a self-managed server: the directory `options.log_directory`, mounted read-only. Only `.json` files are
  read when there are any, else `.csv`, else stderr. A file covers [the time in its name, its last write]. From
  1.22.0 a stderr file of any prefix but RDS's that starts before the window is read from where the window begins,
  found by halving the file on the times of its lines.

**Windows:** a record belongs to the window when `start <= t < end`, by its own time, in UTC (`log_timezone` must
be UTC). The files that can hold the window are read in order, and reading stops at the first record 5 s past the
window's end, which can be in the next file: the logging collector rotates on its own timer. A window that ended
less than 30 s ago fails with `not_yet_published` (section 5).

**Keep-alive:** a log file with few sign-ins, or a catalog database with many tables, yields few rows for
minutes. While a body is open, the agent flushes the compressor at least every 15 s, first writing a blank line
when no row was sent since the last flush (section 4), for every stream of this engine.

**Records:** stderr with the prefix `%t:%r:%u@%d:[%p]:` (RDS's) or `%m:%r:%u@%d:[%p]:%l:%e:%s:%v:%x:%c:%q%a:`, and
from 1.22.0 with the server's own `log_line_prefix`, read at each probe: a line is that prefix's escapes (`%t`, `%m`,
`%n`, `%p`, `%c`, `%u`, `%d`, `%a`, `%r`, `%h`, `%e`, `%b`, with paddings and `%q`), the level and the message. A
field the prefix lacks comes from the record's session (`%c`; its process is the part after the dot, in hex) or,
with no session id, from the newest sign-in of its process (`%p`) with no disconnection since: `user`, `database`,
`remote_host`, `remote_port` and `application_name`, from the session's `connection received` and `connection
authorized` lines. Before its sign-in a session's lines have `user` and `database` `[unknown]`, as `%u` writes them;
a refused sign-in's user is the one its FATAL text names. A session that signed in before what the window reads is
looked for once: with a session id, the minute after the start the id says; with a process only, up to 2 hours
before the read; at most 200 look-backs per window. With none of `%u`, `%c` and `%p` a statement, pgaudit or FATAL
record names no one and counts as `unreadable`. Where the prefix writes a name a client chose (`%a`, `%u`, `%d`), a
record taken as written inside that name counts as `unreadable` and in `counted.forged`, and is never shipped. The
server cuts each such name to 63 bytes (an application name is then written with `\xNN` for each byte outside
printable ASCII, so up to 252 characters), so a record whose prefix reads a longer one is forged, as is a refused
sign-in whose FATAL names another user than `%u` read; and its text is looked at only as far as such names and the
rest of the prefix reach from the first such escape. With `%a`, which any signed-in client sets: any record with a
level marker (`LOG:  `, `ERROR:  `...) there. With `%u` or `%d` only, which only a refused client chooses: a
connection line with a level marker there, and any record with the rest of the prefix and `FATAL:  ` there, as every
refused client's line ends. RDS's two prefixes keep their own reader;
csvlog (24 columns on 13, 26 on 14-18); jsonlog. In stderr a message's `DETAIL`, `HINT` and `STATEMENT` are lines of
their own with the same pid, and a line without the prefix continues the one before. A record is shipped when it is
one of these, by its level and the start of its message (English messages only):

| kind | record |
|---|---|
| connection | `LOG` `connection received:`, `connection authenticated:`, `connection authorized:`, `replication connection authorized:`, `disconnection:` |
| failure | any `FATAL` or `PANIC`, with its `DETAIL` |
| DDL | `LOG` `statement: ...` (the simple protocol) and `execute <name>: ...` (the extended protocol) |
| pgaudit (from 1.14.0) | `LOG` `AUDIT: ...` of class `READ` or `WRITE`, grouped per minute (below) |

Every other record is counted, not shipped. A record's `STATEMENT` is never shipped, nor the `DETAIL` of a DDL
record (its parameters).

**Passwords** are replaced by `<redacted>` in every text the agent ships, the log's and `pg_stat_statements`': the
string after the word `PASSWORD` (also after `password =` and `password =>`) in any quoting (`'...'`, `E'...'`,
`U&'...'`, `$tag$...$tag$`); inside any string, a connection string's `password=` value and the password of a
`postgresql://user:password@host` URI. When the string after `PASSWORD` is cut, everything from the keyword on is
replaced.

**pgaudit (from 1.14.0).** A pgaudit line is `AUDIT: AUDIT_TYPE,STATEMENT_ID,SUBSTATEMENT_ID,CLASS,COMMAND,OBJECT_TYPE,
OBJECT_NAME,STATEMENT,PARAMETER`, each field CSV-quoted when it holds a comma, a double quote or a line break.
- The `PARAMETER` field is never read or shipped.
- A line with a `CONTEXT` was written from inside a function (`RAISE LOG`), not by pgaudit, which writes none: it is
  counted, never shipped. `log_error_verbosity = terse` writes no `CONTEXT` on any line, so the probe warns about it
  wherever pgaudit is loaded. A line of seven fields (`pgaudit.log_statement` off) is a statement with no text.
- The lines of one backend with the same statement and substatement ids are one statement. Its objects are the
  distinct `OBJECT_NAME`s of each class: one line per relation with `pgaudit.log_relation` on, and a relation under
  both object and session audit written twice. A statement that reads some relations and writes others (`INSERT ...
  SELECT`) is two statements, one per class.
- A `COPY`'s command is `COPY TO` or `COPY FROM`, from its own text: with `pgaudit.log_relation` on, pgaudit names its
  lines by the rights the copy uses (`SELECT`, `INSERT`).
- Statements of class `READ` and `WRITE` are grouped per minute by (user, database, client address, application,
  class, command, objects, shape), one row per group. On stderr, which carries no application name, the pid takes
  the application's place in the key. The row is the group's first statement, with `event_count` statements.
- A statement's shape is its text with every string and number (hex, octal and binary ones too) replaced by `?`; a
  list of them is one `?`, and so is a list of rows (`VALUES (?), (?)`). Comments are removed and white space
  collapsed; `$1` placeholders stay. A string that is cut ends the shape, and so does a plain string that holds a
  backslash when its two possible ends (with `standard_conforming_strings` on and off) differ. From 1.19.0, in a
  statement that starts with `SET`, `ALTER` or `CREATE`, after a `SET` outside parentheses, everything after the
  first `=` or `TO` outside parentheses, up to the statement's end, is one `?`, words too:
  `ALTER ROLE r SET search_path TO ?`. The shape is the row's `message`, cut at 16,384 characters, so no value is
  shipped. Its 64-bit hash, 16 hexadecimal digits, is the row's
  `detail`. From 1.19.0 both are shipped with or without `storeRawQueryText` (before, the shape only with it).
- Lines of other classes (`DDL`, `ROLE`, `FUNCTION`, `MISC`) are counted, not shipped: DDL and role changes come
  from `log_statement`.
- Past 500 shapes of one user in one minute, that user's further statements are grouped without their shape: the
  row's `message` is `<many shapes>`, and its user, database, class, command and objects stay.
- Past 10,000 groups in one minute, that minute's further statements go into one row whose `audit_class` is
  `__other__`, with only its count.

| stream | rows | columns |
|---|---|---|
| `pg_log` | a shipped record or pgaudit group; and per instance one count row, `level` = `summary` | `instance`, `ts`, `pid`, `user`, `database`, `remote_host`, `remote_port`, `level`, `sql_state`, `application_name`, `session_id`, `message`, `detail`, `command`, `format`, `truncated`, and from 1.14.0 `audit_class`, `objects`, `event_count` |
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
  empty. From 1.19.0 a DDL record's `detail` is its statement's shape, as a pgaudit row's (below), with or without
  `storeRawQueryText`: the statement without its `statement:` or `execute <name>:` prefix, every string and number
  replaced by `?`, comments removed, cut at 16,384 characters (`truncated` true). The shape is of the statement as
  logged, so a password's string is masked with every other. The count row's `message` is JSON: `records`, `shipped`, `out_of_window`, `unreadable`, `counted` (per
  kind), `files`, `bytes`, `first_log` (the start of the oldest log found, when it starts after the window's start)
  and `missing` (why the instance could not be read). From 1.13.0:
  - `preamble`: lines before a file's first record (a new RDS replica's restore output), counted once, in the window
    of that record's time.
  - `counted.unrecognised`: on a server whose `lc_messages` is not English, LOG records of no kind the agent knows.
  - `missing` for a log directory: `"deleted"` when no file started for the window within one `log_rotation_age` and
    a newer file exists; `"stopped"` when that newer file's startup process wrote "database system was shut down
    at" (jsonlog or csvlog only).

  From 1.14.0, every row carries `audit_class` (pgaudit's class; empty on any other row), `objects` (the relations a
  pgaudit statement names; empty on any other row) and `event_count` (the statements a pgaudit row stands for; 1 on
  any other row). A pgaudit row's `command` is pgaudit's command. The count row adds `audit_lines` (the `READ` and
  `WRITE` lines read), `audit_rows` (the pgaudit rows shipped), `audit_overflow` (statements in an `__other__` row),
  `audit_shapeless` (statements grouped without their shape), `audit_late` (lines of a minute already shipped, which
  open a second row for it), `counted.audit` (lines of other classes), `counted.audit_context` (lines written from
  a function) and `counted.audit_unreadable` (lines whose first seven fields cannot be read). `log_bytes` is the size
  of the window's own lines as the server wrote them (RDS API only; `bytes` is everything downloaded).

  From 1.22.0 the count row adds `look_backs` (reads of the log before the window for a session's sign-in),
  `counted.no_user` (records shipped without a user: their session's sign-in was not found) and `counted.forged`;
  `log_bytes` is also given for `azure_files` and `azure_log_analytics`.
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
- The platform's own (from 1.21.0): on Azure Database for PostgreSQL (`platform` `azure`), the role `azuresu` and the
  databases `azure_sys` and `azure_maintenance` run the service. `pg_stat_snapshot` and `catalog_statement_text` leave
  out their entries, `catalog_users` the role, and the per-database streams the databases. `rdsadmin` is never
  visited, on any platform.
- Times: a time PostgreSQL stores as `infinity` or `-infinity` (`rolvaliduntil` of a role whose password never
  expires), or outside the years 1900 to 2299, is sent as null.

**The statements** are in [`PgSql.cs`](../src/Trailox.Agent/Engines/Postgres/PgSql.cs). Each session sets
`statement_timeout` 30 s, `lock_timeout` 1 s, `idle_in_transaction_session_timeout` 60 s and
`default_transaction_read_only`, with application name `trailox-agent`; one connection at a time. No size
function, no reset of `pg_stat_statements`, and its text only in `catalog_statement_text`.

**The probe** reports `caps.postgres` in the check-in: `platform` (`rds`, `aurora`, `self_managed`, and from 1.21.0
`azure` for Azure Database for PostgreSQL flexible server, where the server defines Azure's own setting
`azure.extensions`), `logSource` (`rds_api`, `file`, from 1.22.0 `azure_files` and `azure_log_analytics`),
`logFormat`, `serverVersionNum`, `statStatements`, `instances` (`id`, `role`, `oldestLogMicros`, `logFileMinutes`)
and `checks` (`name`, `status` `ok`/`warn`/`fail`, `message`): from 1.23.0 `entra_sign_in` (with
`azure_sign_in: entra`: `ok`, naming the identity the token was issued to and the host whose certificate was
verified; a sign-in that fails fails the probe instead), `role`, `logging`, `lc_messages`, `log_timezone`,
`logging_collector` (self-managed, not on Azure; from 1.22.0 only when no log file is found), `pg_stat_statements`
(a warning before extension 1.8, and from 1.21.0 when `pg_stat_statements.track` is `none`), `log_error_verbosity`
(where pgaudit is loaded: a warning at `terse`), `instances` (each replica signed in to once), `replicas`,
`log_files` (for `azure_files`, the files listed and the oldest), `log_analytics` (for `azure_log_analytics`, the
server's rows in the last day and the oldest kept: a failure with none, a warning when another table also holds them),
and from 1.21.0 `log_line_prefix` (the prefix, and
which of `%u`, `%c` and `%p` it lacks; in 1.21.0 a warning when it is not a prefix the agent reads stderr lines of;
from 1.22.0 `ok` when a line's user can be found, a warning when none of the three is there or `%a` comes before
more of the prefix, and a failure with no time). A failed check does not fail the probe. `earliestEventMicros` and
`earliestSessionMicros` are the start of the oldest log file. A rejected password (SQLSTATE 28P01) fails the probe
or the task, and the agent then does not sign in to that source again, for a probe or a task, until it restarts; so
does a refused Microsoft Entra token (above).

**Instances** (`options.instances`, `rds_api`): the RDS instance identifiers, primary first, each `id` or
`id=host`. A replica given as `id` alone gets the primary's host with its first label replaced, only when the
primary's host is its own RDS instance endpoint (`<primary id>.<hash>.<region>.rds.amazonaws.com`); for any other
host (an address, a CNAME, an Aurora cluster endpoint) each replica must be `id=host`, or the endpoint is
invalid.

## 6g. Oracle (engine `oracle`, from 1.16.0)

Oracle 19c and later: Amazon RDS for Oracle and self-managed servers, one endpoint for each PDB or non-CDB. The agent
signs in with `Oracle.ManagedDataAccess.Core`, one connection for each task and for each page of columns, closed after,
with no pool. The descriptor is built from `host`, `port` and `options.service_name`; with `tls: true` the protocol is
`tcps` and `SSL_SERVER_DN_MATCH` is on, so the server's certificate must match the host.

**Each session** runs `ALTER SESSION SET TIME_ZONE = 'UTC' NLS_COMP = 'BINARY' NLS_SORT = 'BINARY'` and `ALTER SESSION DISABLE
PARALLEL QUERY`. Each call on the server has 60 seconds: an execute and each fetch, so a long window of quick fetches runs,
and the time the upload holds the fetch back does not count. The audit poll's own calls have the task's `readLimitSeconds`: at
most 3,600, and 60 when the task has none. A limit stops one call, not the whole read. Every object is named with its owner,
so no public synonym can stand in for one. The agent calls no procedure, and of the functions in packages only
`SYS.DBMS_LOB.SUBSTR` and `SYS.DBMS_LOB.GETLENGTH`.

A call that runs for minutes sends nothing, so the agent marks itself alive while it waits, within the call's limit. A call
that has passed its limit gets 5 seconds to stop before the next statement runs; if it has not stopped by then, the session's
further calls fail at once as timed out.

**The audit poll** reads `AUDSYS.UNIFIED_AUDIT_TRAIL` for one window:

```sql
WHERE event_timestamp_utc >= CAST(:lo AS TIMESTAMP) AND event_timestamp_utc < CAST(:hi AS TIMESTAMP)
  AND NOT (NVL(dbusername, ' ') = :me AND dbproxy_username IS NULL AND NVL(return_code, -1) = 0
           AND NVL(action_name, ' ') = 'SELECT')
```

`start <= t < end`, in UTC, with no `ORDER BY`. `:me` is the agent's user in upper case: its own successful reads are left
out. A sign-in by the monitoring user is recorded: its sign-ins and sign-offs, its changes and its failed sign-ins are read,
and so is anything done as it through a proxy. `SQL_TEXT` is a CLOB, and no LOB enters the result: the poll selects its
length and its first 4,000 characters, in four pieces of 1,000, each of which fits a
`VARCHAR2(4000)` in any character set. `SQL_BINDS` is never selected. `SOURCE` is selected only where the probe found the
column (19.21 and later). With `storeRawQueryText: false` the text is not selected at all. A record's trailing NUL, which
the trail stores, is removed.

**Keep-alive:** a poll reads its whole day's partition before the first row of a late window, so a body can go quiet. It
works as in section 6f, for every stream of this engine.

**Text.** `sql_text` is always a shape, in two steps, on every record: passwords are redacted, then every string and number
is replaced. No pattern can find a password that dynamic SQL spells in pieces, so the strings are not left to a pattern.

Step one replaces passwords by `<redacted>` in every text and in `dp_text_parameters1`, whatever Oracle masked itself:
- after `IDENTIFIED BY`, the next password, in any quoting or none, up to the next ASCII white space (a password that is no
  valid word fails its statement, and the statement is still audited); `IDENTIFIED BY VALUES '...'`; the old or new password
  after each `REPLACE` or `SET`. Anything but a clause Oracle takes after `IDENTIFIED`, or after a password, replaces the
  rest of the text;
- the next word, name or string after `SECRET` in `ADMINISTER KEY MANAGEMENT`, and after `ENCRYPT USING` or `DECRYPT USING`;
- the first string or quoted name within the next twelve tokens after a name that holds `pass`, `pwd`, `secret`,
  `private_key`, `api_key`, `access_key` or `token`, or is `pw` or ends in `_pw`, and the strings joined to it;
- every string in a call to `DBMS_CREDENTIAL.CREATE_CREDENTIAL` or `UPDATE_CREDENTIAL`, `DBMS_CLOUD.CREATE_CREDENTIAL` or
  `UPDATE_CREDENTIAL`, `DBMS_SCHEDULER.CREATE_CREDENTIAL`, `DBMS_VECTOR.CREATE_CREDENTIAL`,
  `DBMS_VECTOR_CHAIN.CREATE_CREDENTIAL`, `UTL_HTTP.SET_AUTHENTICATION`, `SET_WALLET`, `SET_PROXY`, `REQUEST`, `REQUEST_PIECES`
  or `CREATE_REQUEST_CONTEXT`, `UTL_SMTP.AUTH`, `DBMS_LDAP.SIMPLE_BIND_S` or `BIND_S`, `DBMS_DATAPUMP.SET_PARAMETER`;
- a whole string or quoted name that holds any of these itself, or a URL with a password.

Any string or quoted name that never ends replaces the rest of the text.

Step two is the statement's shape: every string (`'...'`, `N'...'`, `q'[...]'` with any delimiter, `nq'...'`) and number is
replaced by `?`; a list of them is one `?`, and so is a list of rows. Comments and hints are removed and white space collapsed.
Words, operators, quoted names and bind placeholders stay; a quoted name compared with something is a value, and becomes `?`.
The shape ends with `?` at a string or quoted name that never ends, and at a word that touches a quote (`'O'Brien'`), where the
quotes are not where they seem.

**Grouping.** Records of the actions `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `UPSERT`, `EXECUTE` and `PL/SQL EXECUTE`
are grouped per minute, one row for each (instance, user, proxy user, action, return code, object, program, OS user, client
host and address, policies, privilege, audit type, shape). The row is the first record seen, with `event_count` records
and the first time. Every other record is a row of its own, with `event_count` 1 and an empty `shape_hash`.
- `shape_hash`, 16 hexadecimal digits, identifies the row: the group's key, its minute and its part. No two grouped rows share
  one. It does not identify the shape: the same shape from two clients has two.
- A minute ships when a record 2 minutes past its end has been read, or when 20,000 groups are open. The poll has no order,
  so a record that comes later opens a second row for that minute and key, with a `shape_hash` of its own; the counts add up.
- Past 500 keys of one user in one minute, that user's further records are grouped without their shape and client: `sql_text`
  is `<many shapes>`, and instance, user, proxy user, action, return code and object stay. Past 2,000 objects, or 500 for
  records that failed, they are grouped without their object too: `object_name` is `<many objects>`, and the key is the
  instance, user, proxy user, action and whether it failed.
- Past 10,000 groups in one minute, everyone's further keys go straight to that last form, so every record stays under its
  own user's name.

| stream | rows | columns |
|---|---|---|
| `ora_audit` | an audit record or group, and per window one count row, `action_name` = `summary`, stamped with the window's start | `event_time`, `instance_id`, `dbusername`, `dbproxy_username`, `action_name`, `return_code`, `object_schema`, `object_name`, `client_program_name`, `os_username`, `userhost`, `terminal`, `authentication_type`, `unified_audit_policies`, `system_privilege_used`, `audit_type`, `sessionid`, `entry_id`, `statement_id`, `sql_text`, `sql_text_length`, `truncated`, `dp_text_parameters1`, `summary`, `event_count`, `shape_hash` |
| `catalog_users` | a user | `username`, `account_status`, `lock_date`, `expiry_date`, `created`, `profile`, `authentication_type`, `oracle_maintained`, `snapshot_date` |
| `catalog_grants` | a role, system or table grant | `kind` (`role`, `system`, `table`), `grantee`, `privilege`, `owner`, `table_name`, `grantable`, `snapshot_date` |
| `catalog_tables` | a table | `owner`, `table_name`, `num_rows`, `blocks`, `partitioned`, `last_analyzed`, `snapshot_date` |
| `catalog_columns` | a column | `owner`, `table_name`, `column_name`, `data_type`, `column_id`, `snapshot_date` |
| `catalog_settings` | one setting | `name`, `setting`, `snapshot_date` |

- `ora_audit`: `event_time` is `EVENT_TIMESTAMP_UTC`, in microseconds. `authentication_type` is as the server wrote it,
  with the client's address inside. A column with no value is an empty string, or 0 for a number; a record with no return code
  has `return_code` -1. `sql_text_length` is the whole text's length as the server counts it, the trailing NUL included, and
  `truncated` is 1 when it is over 4,000, else 0. The count row has `event_count` 1, and its `summary` is JSON: `records` (read),
  `shipped` (rows of their own), `by_action`, `file_records` (records from spillover files, null where the view has no
  `SOURCE`), `audit_records`, `audit_rows`, `audit_shapeless` and `audit_late`. On every other row `summary` is empty.
- The catalog: `catalog_grants`, `catalog_tables` and `catalog_columns` read schemas whose owner has `ORACLE_MAINTAINED` =
  `N` (role and system grants read every grantee). `snapshot_date` is the task's start (UTC). At most 50,000 tables, 200,000
  grants of each kind and 1,000,000 columns; a list that returns as many rows as its limit counts as cut, and the stream
  ends with one more row of its own columns, whose `kind`, `partitioned` or `data_type` reads `truncated`. Columns are read
  1,000 tables per statement and connection. `DATE` columns are as the server stores them, in its own local time.
- `catalog_settings`: each `name` starts with its part: `option.` (`V$OPTION`), `parameter.` (`V$PARAMETER`), `policy.`
  (`AUDIT_UNIFIED_ENABLED_POLICIES`), `statement_audit.`, `privilege_audit.`, `cleanup_job.`, `trail.`, `own_limit.`,
  `own_password_limit.` and `own_account.`. A part that cannot be read is a row named `unreadable.` and the object, whose
  `setting` is the Oracle error, and the others are read.

**The statements** are in [`OraSql.cs`](../src/Trailox.Agent/Engines/Oracle/OraSql.cs). None counts the trail, and none reads
`DBA_SEGMENTS`, `V$ACTIVE_SESSION_HISTORY`, `DBA_HIST_*` or `V$SQL_MONITOR`.

**The probe** reports `caps.oracle` in the check-in: `platform` (`rds`, `self_managed`), `product`, `edition`, `container`,
`databaseRole`, `ownUser`, `ownProgram` (both as the server gives them), `probedAtMicros`, `unifiedMode` (`pure`, `mixed`),
`auditTrail`, `trailPartitioned`, `trailInterval`, `trailRows`, `trailBlocks` (statistics, never a count), `todayPartition`,
`todayPartitionEndMicros`, `todayTablespace`, `configuredTablespace`, `roomBytes`, `growthBytesPerDay`, `spilled`,
`sourceColumn`, `observedLevel` (`logs_only`, `partial`, `selective`, `full`; empty when the policies
cannot be read), `policiesRead`, `policies` (`name`, `enabledOption`, `entityName`, `entityType`, `success`, `failure`: the
`TRAILOX_*` ones enabled), `policyObjectsRead`, `policyObjects` (`policy`, `schema`, `name`: the objects those policies name, at most
500) and `checks` (`name`, `status` `ok`/`warn`/`fail`, `message`): `container`,
`platform`, `poll_view`, `audit_trail`, `views`, `partition`, `room`, `spillover`, `failed_signins`, `default_policies`, `level`,
`profile`, `resource_limit`, `source_column`, `old_audit_settings`, `purge_job`, `password_expiry`.

`earliestEventMicros` and `earliestSessionMicros` are 0: the trail's oldest record is never read.

`databaseRole` is `SYS_CONTEXT('USERENV', 'DATABASE_ROLE')` as the server gives it: `PRIMARY`, `PHYSICAL STANDBY`,
`LOGICAL STANDBY` or `SNAPSHOT STANDBY`. It needs no grant and decides no check, and it is absent when it cannot be read. A
read-only standby always writes to spillover files, so `spilled` means something different there.

`growthBytesPerDay` is the used space of the trail's tablespace at this process's probes of the last 24 hours, projected to a
day; it takes two probes an hour apart. Space that falls, a purge, counts as no growth. `room` warns when the tablespace holds
under 8 days of it. The size of a day fails no check.

`failed_signins` says whether a failed sign-in reaches the trail, from the enabled policies: `ok` when `ORA_LOGON_FAILURES`,
`ORA_LOGIN_LOGOUT` or `TRAILOX_LOGON` is enabled for failures for every user, or every user but those it names. It warns when one
covers some users only, when none is enabled for failures (a policy of your own is not looked into), and when the policies
cannot be read. `purge_job` says how the trail is purged, from `DBA_AUDIT_MGMT_CLEANUP_JOBS` and
`DBA_AUDIT_MGMT_LAST_ARCH_TS`: a purge job, the time the trail is purged up to (UTC, to the minute), or that no purge has run. A
job of the database's own scheduler shows only by that time. It is `ok` either way, and warns only when neither view can be
read: whether to purge is your call.

An optional read that fails for any reason but a lost session is listed in `views`, and the probe goes on. One that a limit
stopped is listed there too, as slow rather than as a missing grant, and fails no check, `poll_view` included.

`policiesRead` and `policyObjectsRead` are true only when that view was read, so an empty list after a missing grant is not taken
for a policy that is not enabled.

**A failed check refuses the source:** the check-in reports `probeError`, the failed checks' messages joined, with the
caps and every check beside it. The agent runs no task on a refused source, and probes it again every 5 minutes.

A sign-in the server refuses (`ORA-01017`, `ORA-28001`, `ORA-01045`) fails the probe or the task, and the agent then does not
sign in to that source again, for a probe or a task, until it restarts. `ORA-28000`, a locked account, is held the same way
for an hour; then one sign-in is tried, and the agent goes on with no restart once the account is open. Network errors keep
the retry at each check-in.

## 7. Configuration

See the [README](../README.md#configuration) for `agent.yaml`, the environment variables and the
per-engine fields, and [`deploy/agent.yaml.example`](../deploy/agent.yaml.example) for a complete
example.
