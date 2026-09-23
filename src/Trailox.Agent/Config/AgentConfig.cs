using YamlDotNet.Serialization;

namespace Trailox.Agent.Config;

/// <summary>
/// agent.yaml as written by the customer. No secret ever appears in it: credentials are
/// referenced by environment variable or by file, so the YAML can be committed and shared.
/// Validation lives in <see cref="ConfigLoader"/> and, per engine, in the engine itself.
/// </summary>
public sealed class AgentConfig
{
    /// <summary>Schema version of this file. See <see cref="ConfigLoader.SupportedConfigVersions"/>.</summary>
    public int Version { get; set; }

    public string Gateway { get; set; } = "https://agent.trailox.io";

    /// <summary>How often an endpoint is re-probed (server version, columns, oldest row). Minutes.</summary>
    public int ProbeIntervalMinutes { get; set; } = 60;

    public List<EndpointConfig> Endpoints { get; set; } = new();
}

/// <summary>
/// One database the agent reads. The fields common to every engine are typed; what an engine
/// needs beyond them lives in <see cref="Options"/> and is validated by that engine.
/// </summary>
public sealed class EndpointConfig
{
    /// <summary>Short customer-chosen name; becomes part of every identity on this source.</summary>
    public string Alias { get; set; } = "";

    /// <summary>One of <c>Engines.EngineRegistry.Names</c>. No default: the customer says what it is.</summary>
    public string Engine { get; set; } = "";

    /// <summary>onprem | cloud. Informational for the Trailox UI.</summary>
    public string Kind { get; set; } = "onprem";

    /// <summary>Where the AGENT reaches the database. Meaning and requiredness are the engine's.</summary>
    public string Host { get; set; } = "";

    /// <summary>0 = the engine's default.</summary>
    public int Port { get; set; }

    public bool Tls { get; set; } = true;

    /// <summary>Engine-specific: ClickHouse cluster for clusterAllReplicas; Snowflake warehouse; ...</summary>
    public string ClusterName { get; set; } = "";

    /// <summary>The monitoring user the setup script created. No default: it is per engine.</summary>
    public string Username { get; set; } = "";

    // ApplyNamingConventions = false: the camelCase convention would otherwise rewrite the
    // alias itself to "passwordEnv" and the documented snake_case key would never bind.
    [YamlMember(Alias = "password_env", ApplyNamingConventions = false)]
    public string? PasswordEnv { get; set; }

    [YamlMember(Alias = "password_file", ApplyNamingConventions = false)]
    public string? PasswordFile { get; set; }

    public bool CollectSessionLog { get; set; } = true;
    public bool StoreRawQueryText { get; set; } = true;
    public List<string> ExcludedDatabases { get; set; } = new();

    /// <summary>
    /// How often Trailox collects this database, in minutes (1-1440). Null: Trailox's default for the engine.
    /// </summary>
    /// <remarks>
    /// Used when Trailox first registers the source. After that the source's own setting in Trailox
    /// decides, and Trailox warns when this says something else - change it there. (1.4.0)
    /// </remarks>
    public int? PollMinutes { get; set; }

    /// <summary>
    /// How far back the first collection reads, in days (0-365). Null: 30. Used when Trailox first
    /// registers the source; a later change is ignored, because history already collected is not re-read. (1.4.0)
    /// </summary>
    public int? BackfillDays { get; set; }

    /// <summary>Anything an engine needs beyond the common fields, validated by that engine.</summary>
    public Dictionary<string, string> Options { get; set; } = new();

    /// <summary>Resolved at start-up by the loader; never serialized, never logged.</summary>
    [YamlIgnore]
    public string Password { get; internal set; } = "";
}
