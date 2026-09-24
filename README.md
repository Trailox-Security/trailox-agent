# Trailox Agent

A small, stateless container you run inside your own network. It reads your database's **own
audit and catalog views** with a read-only account you create, and streams the rows to Trailox
over HTTPS, **outbound only, on port 443**. Nothing inbound, no IP allow-list, no VPN, and the
database credentials never leave your network.

Supported databases: **ClickHouse**, **Databricks**, **Amazon Redshift** and **Snowflake**.
One agent can read any number of endpoints across them.

## What the agent does

- It runs **only `SELECT` statements**, against the database's built-in audit and catalog views.
  The complete SQL for every engine is in one file per engine under
  [`src/Trailox.Agent/Engines`](src/Trailox.Agent/Engines), and reproduced in
  [`docs/PROTOCOL.md`](docs/PROTOCOL.md).
- It keeps **no state and no buffer**. It asks Trailox which time windows to read, reads each one,
  streams the result straight into a gzip upload, and repeats. If it stops, the next start
  re-reads the same window, so a restart repeats work but never loses data.
- It holds **no logic about what the rows mean**. Classification and analysis happen in Trailox.

## What leaves your network

- The rows of the audit views for the requested windows: statements run, and logins and sessions
  where the database records them. See each engine below for the exact views.
- Once a day, a catalog snapshot: tables, columns, users and grants.
- **Statement text is optional.** With `storeRawQueryText: false` the text is never selected,
  so it never leaves the database.
- `excludedDatabases` is applied in the `SELECT` itself, and what it leaves out depends on the
  engine:
  - **ClickHouse:** statements that touch those databases. The catalog snapshot still lists them.
  - **Amazon Redshift:** statements, their text and `UNLOAD` records. The per-step detail
    (`sys_query_detail`) and the catalog snapshot still include them.
  - **Snowflake** and **Databricks:** the catalog snapshot only. Statements that touch those
    databases are still shipped.
- Never: table data, credentials, or anything outside the audit and catalog views listed below.

## Quick start (docker compose)

1. In Trailox, open **Settings → Trailox Agents → Enrol an agent**. Copy the key; it is shown once.
2. On the same page, generate the setup script for your database and run it. It creates a
   read-only monitoring account (see the per-engine notes below).
3. Save [`deploy/agent.yaml.example`](deploy/agent.yaml.example) as `agent.yaml` next to
   [`deploy/docker-compose.yml`](deploy/docker-compose.yml) and describe your endpoints.
4. Put the agent key and each endpoint's credential in the environment (or a `.env` file), or
   mount them as files:

       TRAILOX_AGENT_KEY=tlx_...
       TRAILOX_CH_PROD_PASSWORD=...

5. `docker compose up -d`. Your endpoints appear under **Database sources** within a minute.

Kubernetes: [`deploy/kubernetes/agent.yaml`](deploy/kubernetes/agent.yaml), or the Helm chart:

    helm install trailox-agent oci://ghcr.io/trailox-security/charts/trailox-agent --version <version> -f values.yaml

The image is `ghcr.io/trailox-security/trailox-agent`, tagged with each release version and with
`1` for the latest 1.x release. The chart runs the agent version it was released with, so
`helm upgrade --version <new>` upgrades the agent; set `image.tag` to run another. `1` moves with each
release, but a machine that has already pulled it keeps that copy: with compose, upgrade with
`docker compose pull && docker compose up -d`.

## Configuration

`agent.yaml` (default path `/etc/trailox/agent.yaml`, override with `TRAILOX_CONFIG`). **It never
contains a secret**: credentials are referenced by environment variable or file, so the file can
be committed. Any other value can be written in the file or taken from the environment; see
[Values from the environment](#values-from-the-environment).

| Key | Meaning |
|---|---|
| `gateway` | `https://agent.trailox.io` unless Trailox tells you otherwise |
| `probeIntervalMinutes` | How often each endpoint is re-probed (default 60). An endpoint whose probe failed is re-probed at every check-in until it succeeds |
| `endpoints[].alias` | Short name for the source in Trailox (`[A-Za-z0-9_-]{1,64}`), unique per account |
| `endpoints[].engine` | `clickhouse`, `databricks`, `redshift` or `snowflake` |
| `endpoints[].kind` | `cloud` or `onprem`; informational |
| `host`, `port`, `tls` | How the **agent** reaches the database. Meaning per engine below; Trailox never learns them |
| `clusterName` | Engine-specific: the cluster, warehouse or workgroup (see below) |
| `username` | The monitoring account the setup script created |
| `password_env` / `password_file` | Where the credential comes from: an environment variable or a mounted file |
| `options` | Engine-specific extras (see below) |
| `collectSessionLog` | Also ship login and session events (default `true`) |
| `storeRawQueryText` | `false` keeps statement text inside your network (default `true`) |
| `excludedDatabases` | Databases left out of what is shipped; what that covers depends on the engine (see [What leaves your network](#what-leaves-your-network)) |
| `pollMinutes` | How often Trailox collects the database, 1-1440 minutes. Optional; Trailox's default is 10 for ClickHouse, 60 for Databricks and Snowflake, 180 for Redshift. **Used when Trailox first registers the source**: after that its setting in Trailox decides, and Trailox warns when this file differs. **On Snowflake a shorter poll needs a larger daily credit cap** than the setup script sets (6 credits for 60 minutes, 72 for 5), or Snowflake suspends the warehouse. From 1.4.0 |
| `backfillDays` | How far back the first collection reads, 0-365 days (default 30). **Used only when Trailox first registers the source**; changing it later re-reads nothing. From 1.4.0 |

### Values from the environment

From 1.5.0 any value in `agent.yaml` can come from the agent's environment instead of the file, so a
Helm chart or a compose file can supply it. A value written in the file works as before.

| Written | Result |
|---|---|
| `${NAME}` | The variable's value. When it is not set or empty the agent does not start (exit 2), and names the line, the key and the variable |
| `${NAME:-default}` | The variable's value, or `default` when it is not set or empty |
| `${NAME:-}` | The variable's value; when it is not set or empty the key is left out, so its default applies (a list item is dropped) |
| `$${` | A literal `${` |

    endpoints:
      - alias: prod-cluster
        engine: clickhouse
        host: ${CH_HOST}
        port: ${CH_PORT:-8443}
        clusterName: ${CH_CLUSTER:-}
        username: trailox_monitor
        password_env: TRAILOX_CH_PROD_PASSWORD
        excludedDatabases: ["${CH_EXCLUDED}"]

- A reference can be the whole value or part of one (`ch-${ENV}.internal`), in any value: numbers,
  `true`/`false`, list items and `options`. Keys and comments are never read, so a commented-out block
  may name variables that are not set.
- Inside `[...]` or `{...}` quote it, as above: there an unquoted `{` is YAML syntax.
- `NAME` is letters, digits and `_`, not starting with a digit. Anything else after `${`, such as
  `${NAME:?error}` or a default that contains `${`, is a config problem.
- The variable's content is taken as text: YAML in it is not read, and `${...}` in it is not expanded
  again. Trailing newlines are removed, since a value from a file-backed ConfigMap or Secret ends in one;
  any other control character is a config problem.
- Credentials still come only through `password_env` and `password_file`, which are never expanded. The
  agent key's variable, and every variable a `password_env` names, cannot be used as a value: values are
  reported to Trailox and can appear in error messages.
- Tools that expand `${...}` themselves need it escaped to pass it on: `$${...}` in a Terraform
  template or in a compose file that inlines `agent.yaml`, and a quoted delimiter (`<<'EOF'`) for a
  shell heredoc. Helm does not expand it.
- The environment is read at start: restart the agent after changing a variable.
- Agents before 1.5.0 read `${...}` literally and stop with a config problem.

With the Helm chart, put the references in `config` and the variables under `env` (a value, or
`valueFrom` a ConfigMap or Secret), or list whole ConfigMaps under `extraEnvFrom`, where each key
becomes a variable of exactly that name:

    config:
      version: 1
      gateway: https://agent.trailox.io
      endpoints:
        - alias: prod-cluster
          engine: clickhouse
          host: ${CH_HOST}
          port: ${CH_PORT:-8443}
          password_env: TRAILOX_CH_PROD_PASSWORD
    env:
      - name: CH_HOST
        valueFrom:
          configMapKeyRef: { name: db-endpoints, key: ch_host }
    # or a ConfigMap whose keys are the variables themselves (CH_HOST, CH_PORT, ...):
    # extraEnvFrom:
    #   - configMapRef: { name: trailox-agent-settings }

After changing a ConfigMap or Secret that the variables come from, `kubectl rollout restart
deploy/<release>`.

Environment: `TRAILOX_AGENT_KEY` (or `TRAILOX_AGENT_KEY_FILE`), `TRAILOX_CONFIG`,
`TRAILOX_LOG_LEVEL`, the standard `HTTPS_PROXY`, `NO_PROXY`, `SSL_CERT_FILE`, `SSL_CERT_DIR`, and any
variable `agent.yaml` refers to as `${NAME}`.

Commands: `run` (default), `validate-config`, `healthcheck` (exit 0 when the loop ran in the last
5 minutes), `version`. Exit codes: 2 config invalid, 3 agent key rejected, 4 agent version too old.
Anything else (database or network unavailable) is retried in place with backoff.

**Turning a source off.** A source you turn off in Trailox is left alone by the agent too: from
1.3.3 it is not probed, and nothing is read from it, until you turn it on again. The log says so
once, when it changes. Earlier versions went on probing it, and on Databricks and Snowflake a probe
runs statements, which can start a warehouse. The agent learns that a source is off from the answer
to a check-in, so after a restart every endpoint in `agent.yaml` is probed once before that answer
arrives. To keep the agent away from a database altogether, remove its block from `agent.yaml`.

## Databases

### ClickHouse

Self-managed or ClickHouse Cloud, over the HTTP interface (usually 8443 with TLS).

- **Reads:** `system.query_log`, `system.session_log`, and daily `system.tables`,
  `system.users`, `system.grants`. Every request carries `log_comment=trailox-agent`, and the
  agent excludes its own reads.
- **Account:** a `readonly = 2` user with `SELECT` on those system tables; the Agents page
  prints the script.
- **agent.yaml:** `host`/`port`/`tls` = the HTTP endpoint; `clusterName` = the cluster to
  read across all replicas (`default` on ClickHouse Cloud, empty on a single node; a wrong value
  silently reads only part of the cluster); `password_env`/`password_file` = the user's password.
- **Without statement text:** a normalized shape and a hash of the text are sent instead.

### Databricks

A Unity Catalog workspace, through the SQL Statement Execution API over HTTPS.

- **Reads:** `system.query.history`, `system.access.table_lineage`,
  `system.access.column_lineage`, `system.access.audit` (sign-ins), and daily the
  `information_schema` tables, columns and privileges of your catalogs.
- **Account:** a service principal with an OAuth secret that only the agent holds. It needs
  `USE CATALOG` on `system`; `USE SCHEMA` and `SELECT` on `system.query`, `system.access` and
  `system.information_schema`; `BROWSE` on each catalog to inventory; and `CAN USE` on the SQL
  warehouse.
- **agent.yaml:** `host` = the workspace hostname; `clusterName` = the SQL warehouse id
  (16 hex characters); `username` = the principal's application id; `password_env`/`password_file`
  = its OAuth secret. `port`/`tls` are ignored (HTTPS 443).
- **Statement text** is shown as `<REDACTED>` by Databricks unless the principal is in
  `databricks_pii_access`. The agent detects this and reports it, and then no text is stored.
  `excludedDatabases` here means catalogs; `system`, `samples` and `__databricks_internal` are
  always excluded.

### Amazon Redshift

A provisioned cluster or Serverless workgroup, over the Postgres wire protocol (5439, TLS required).

- **Reads:** `sys_query_history`, `sys_query_text`, `sys_query_detail` (scan and write steps
  only), `sys_unload_history`, `sys_connection_log`, and daily `svv_table_info`,
  `svv_all_columns`, `pg_user` and `svv_relation_privileges`.
- **Account:** a database user with the `sys:monitor` role; the password stays with the agent.
- **agent.yaml:** `host` = the cluster or workgroup endpoint; `port` 5439; `clusterName` = the
  cluster identifier or workgroup name (descriptive); `username`; `password_env`/`password_file`;
  `options.database` = the database to sign in to (default `dev`).
- The cluster needs no public endpoint: the agent runs inside your VPC. A paused Serverless
  workgroup is resumed by the agent's first read.

### Snowflake

Through the Snowflake SQL API over HTTPS, as a key-pair service user.

- **Reads:** `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`, `ACCESS_HISTORY`, `LOGIN_HISTORY`,
  `SESSIONS`, and daily `TABLES`, `COLUMNS`, `USERS` and `GRANTS_TO_ROLES`.
- **Account:** a `TYPE = SERVICE` user with a key pair you generate. The private key stays with
  the agent; Trailox never sees either half. Its role needs the `SNOWFLAKE.GOVERNANCE_VIEWER`,
  `SNOWFLAKE.SECURITY_VIEWER` and `SNOWFLAKE.OBJECT_VIEWER` database roles and `USAGE` on a
  warehouse. The setup script also caps that warehouse at 2 credits a day with a resource monitor.
- **agent.yaml:** `host` = the account identifier (`myorg-myaccount`); `clusterName` = the
  warehouse; `username` = the service user; `password_file` (or `password_env`) = its
  unencrypted PKCS#8 PEM private key; `options.role` = the role (default `TRAILOX_MONITOR`).
  `port`/`tls` are ignored (HTTPS 443).
- Every read resumes the warehouse (60-second minimum billing), so reads are scheduled hourly by
  default. Enterprise Edition or higher is required for table and column attribution
  (`ACCESS_HISTORY`).

## Security

- Minimal image (`mcr.microsoft.com/dotnet/runtime:10.0-noble-chiseled`): no shell, no package manager.
- Runs as UID 10001, read-only root filesystem, all capabilities dropped, `no-new-privileges`.
- Listens on nothing. Outbound HTTPS to Trailox and connections to your databases only; proxy and
  private-CA aware through the standard environment variables.
- Images are signed with Sigstore cosign and carry SPDX and CycloneDX SBOM attestations:

      cosign verify ghcr.io/trailox-security/trailox-agent:1 \
        --certificate-identity-regexp '^https://github.com/Trailox-Security/trailox-agent/' \
        --certificate-oidc-issuer https://token.actions.githubusercontent.com

- The agent key is a bearer credential. Rotate or revoke it from the Agents page; a revoked key
  stops working on the agent's next request.

## Building

    dotnet test
    docker build -t trailox-agent:dev .

## Protocol

[`docs/PROTOCOL.md`](docs/PROTOCOL.md): the requests the agent sends and the SQL it runs.

## License

Apache-2.0. See [LICENSE](LICENSE).
