using System.Text.Json;
using System.Text.Json.Serialization;

namespace Trailox.Agent.Protocol;

/// <summary>
/// The wire contract with the Trailox gateway, protocol v1. Prose in docs/PROTOCOL.md.
/// Every property is optional on the wire: the gateway may send more than this build knows,
/// and this build must keep working against a gateway that sends less.
/// </summary>
public static class ProtocolInfo
{
    public const int Version = 1;
    public const string ProtocolHeader = "X-Trailox-Protocol";
    public const string AgentVersionHeader = "X-Trailox-Agent";
    public const string RowsHeader = "X-Trailox-Rows";

    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public static class Streams
    {
        public const string Events = "events";
        public const string Sessions = "sessions";
        public const string CatalogTables = "catalog_tables";
        public const string CatalogUsers = "catalog_users";
        public const string CatalogGrants = "catalog_grants";

        // ClickHouse (agent 1.7): the user settings that make a user read-only, so Trailox can tell which
        // users a readonly setting stops from writing.
        public const string CatalogSettings = "catalog_settings";

        // Databricks (agent 1.1): statements and lineage are joined on the Trailox side; the
        // catalog is four information_schema views, one stream each.
        public const string LineageTables = "lineage_tables";
        public const string LineageColumns = "lineage_columns";
        public const string CatalogColumns = "catalog_columns";
        public const string CatalogTablePrivileges = "catalog_table_privileges";
        public const string CatalogSchemaPrivileges = "catalog_schema_privileges";

        // Databricks (agent 1.6): the workspace's service principal directory, so that an
        // automated identity shows by its name rather than its application id.
        public const string CatalogServicePrincipals = "catalog_service_principals";

        // Redshift (agent 1.2): text sequences, plan steps and unloads are joined on the Trailox side.
        public const string QueryText = "query_text";
        public const string QueryDetail = "query_detail";
        public const string Unloads = "unloads";

        // Snowflake (agent 1.3): ACCESS_HISTORY is joined to the statements and LOGIN_HISTORY to
        // the sessions on the Trailox side.
        public const string AccessHistory = "access_history";
        public const string Logins = "logins";

        // MongoDB Atlas (agent 1.8): per-minute aggregates built from Atlas log files, Atlas project events,
        // and three catalog snapshots read from the Atlas Admin API.
        public const string MongoOps = "mongo_ops";
        public const string MongoSlowOps = "mongo_slow_ops";
        public const string MongoConnections = "mongo_connections";
        public const string MongoSigninFailures = "mongo_signin_failures";
        public const string AtlasEvents = "atlas_events";
        public const string CatalogRoles = "catalog_roles";
        public const string CatalogAccessList = "catalog_access_list";
    }
}

public sealed class CheckinRequest
{
    public string AgentVersion { get; set; } = "";
    public string Host { get; set; } = "";
    public DateTime? StartedAtUtc { get; set; }
    public string ConfigFingerprint { get; set; } = "";
    public List<EndpointReport> Endpoints { get; set; } = new();
    public List<ErrorReport> LastErrors { get; set; } = new();
}

public sealed class EndpointReport
{
    public string Alias { get; set; } = "";
    public string Engine { get; set; } = "clickhouse";
    public string Kind { get; set; } = "onprem";
    public string ClusterName { get; set; } = "";
    public string MonitorUser { get; set; } = "";
    public bool CollectSessionLog { get; set; } = true;
    public bool StoreRawQueryText { get; set; } = true;
    public List<string> ExcludedDatabases { get; set; } = new();
    public EndpointCaps Caps { get; set; } = new();
    public string? ProbeError { get; set; }

    /// <summary>agent.yaml's pollMinutes, or null - then not sent. Trailox uses it when it first registers the source. (1.4.0)</summary>
    public int? PollMinutes { get; set; }

    /// <summary>agent.yaml's backfillDays, or null - then not sent. The same rule as <see cref="PollMinutes"/>. (1.4.0)</summary>
    public int? BackfillDays { get; set; }
}

public sealed class EndpointCaps
{
    public string Version { get; set; } = "";
    public List<string> Columns { get; set; } = new();
    public bool HasSessionLog { get; set; }
    public long EarliestEventMicros { get; set; }
    public long EarliestSessionMicros { get; set; }

    /// <summary>
    /// Whether the source shows this agent statement TEXT. Databricks masks it unless the
    /// principal is in databricks_pii_access; ClickHouse always shows it. Trailox stores no
    /// text and no shapes when this is false.
    /// </summary>
    public bool TextReadable { get; set; } = true;

    /// <summary>MongoDB Atlas only (agent 1.8): what the probe found in the project. Null, and not sent, for every other engine.</summary>
    public AtlasCaps? Atlas { get; set; }
}

/// <summary>What the probe of a MongoDB Atlas cluster found. Every field is bounded by the gateway.</summary>
public sealed class AtlasCaps
{
    public string ProjectId { get; set; } = "";
    public string ClusterName { get; set; } = "";

    /// <summary>The instance size, e.g. M10; M0 and Flex are refused by the probe.</summary>
    public string Tier { get; set; } = "";

    /// <summary>REPLICASET or SHARDED (clusterType).</summary>
    public string Topology { get; set; } = "";

    /// <summary>atlas_api or s3: where the agent reads the log files.</summary>
    public string LogSource { get; set; } = "";

    /// <summary>The processes whose logs the agent reads: the replica set members, or a sharded cluster's mongos.</summary>
    public List<AtlasProcess> Processes { get; set; } = new();

    /// <summary>The probe's checks, as the source page shows them.</summary>
    public List<AtlasCheck> Checks { get; set; } = new();
}

public sealed class AtlasProcess
{
    /// <summary>The host as the logs endpoint takes it (userAlias), e.g. cluster1-shard-00-02.abcde.mongodb.net.</summary>
    public string Host { get; set; } = "";

    /// <summary>typeName: REPLICA_PRIMARY, REPLICA_SECONDARY, SHARD_MONGOS...</summary>
    public string Type { get; set; } = "";
}

public sealed class AtlasCheck
{
    public string Name { get; set; } = "";

    /// <summary>ok, warn or fail.</summary>
    public string Status { get; set; } = "";
    public string Message { get; set; } = "";
}

public sealed class ErrorReport
{
    public string Alias { get; set; } = "";
    public DateTime? At { get; set; }
    public string Message { get; set; } = "";
}

public sealed class CheckinResponse
{
    public int ProtocolVersion { get; set; } = ProtocolInfo.Version;
    public string MinAgentVersion { get; set; } = "0.0.0";
    public int CheckinSeconds { get; set; } = 60;
    public List<EndpointAssignment> Endpoints { get; set; } = new();
    public List<AgentTask> Tasks { get; set; } = new();
}

public sealed class EndpointAssignment
{
    public string Alias { get; set; } = "";
    public int? EndpointId { get; set; }
    public bool Enabled { get; set; }
    public string State { get; set; } = "";
    public string? Message { get; set; }
}

public sealed class AgentTask
{
    public string ChunkId { get; set; } = "";
    public int EndpointId { get; set; }
    public string Stream { get; set; } = "";
    public long StartMicros { get; set; }
    public long EndMicros { get; set; }
}

public sealed class ChunkAccepted
{
    public long RowsAccepted { get; set; }
    public long WatermarkMicros { get; set; }
}

public sealed class ChunkFailed
{
    /// <summary>The source has not published the whole window yet; Trailox plans it again later (agent 1.8).</summary>
    public const string NotYetPublished = "not_yet_published";

    public string Message { get; set; } = "";

    /// <summary>Why, when the agent knows: <see cref="NotYetPublished"/>. Null, and not sent, otherwise.</summary>
    public string? Reason { get; set; }
}

public sealed class GatewayError
{
    public string Error { get; set; } = "";
    public string Message { get; set; } = "";
}
