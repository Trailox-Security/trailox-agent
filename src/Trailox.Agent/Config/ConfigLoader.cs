using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Trailox.Agent.Engines;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Trailox.Agent.Config;

/// <summary>
/// Parses and validates agent.yaml. Common rules live here; every engine-specific rule
/// (which fields it needs, what they must look like) is the engine's own, via
/// <see cref="IEngine.Validate"/>.
/// </summary>
public static class ConfigLoader
{
    /// <summary>The agent.yaml schema versions this build understands.</summary>
    public static readonly IReadOnlySet<int> SupportedConfigVersions = new HashSet<int> { 1 };

    private static readonly Regex AliasPattern = new("^[A-Za-z0-9_-]{1,64}$", RegexOptions.Compiled);
    private static readonly Regex DatabasePattern = new("^[A-Za-z0-9_-]{1,255}$", RegexOptions.Compiled);

    private static readonly IDeserializer Yaml = new DeserializerBuilder()
        .WithNamingConvention(CamelCaseNamingConvention.Instance)
        .IgnoreUnmatchedProperties()
        .Build();

    /// <summary>Reads the file and parses it. The fingerprint is of the file's bytes, for the check-in.</summary>
    public static (AgentConfig Config, string Fingerprint) Load(string path, ISecretReader secrets) =>
        Parse(File.ReadAllText(path), secrets);

    /// <summary>Parses, validates and resolves credentials. Throws <see cref="ConfigException"/> listing every problem.</summary>
    public static (AgentConfig Config, string Fingerprint) Parse(string yamlText, ISecretReader secrets)
    {
        var config = Yaml.Deserialize<AgentConfig>(yamlText) ?? new AgentConfig();

        // A list key with nothing but comments under it - `endpoints:` with every example commented
        // out, which is how an agent.yaml starts - reads as null and overrides the empty default.
        // Treat it as empty, so the answer is "add an endpoint" rather than a NullReferenceException.
        config.Endpoints ??= new List<EndpointConfig>();
        config.Endpoints.RemoveAll(e => e is null);
        foreach (var e in config.Endpoints)
        {
            e.ExcludedDatabases ??= new List<string>();
            e.Options ??= new Dictionary<string, string>();
        }

        var problems = Validate(config, secrets);
        if (problems.Count > 0)
        {
            throw new ConfigException(problems);
        }
        return (config, "sha256:" + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(yamlText))).ToLowerInvariant());
    }

    /// <summary>Every problem, in order. Resolves each endpoint's password as a side effect.</summary>
    internal static List<string> Validate(AgentConfig config, ISecretReader secrets)
    {
        var problems = new List<string>();
        if (!SupportedConfigVersions.Contains(config.Version))
        {
            problems.Add($"version: {config.Version} is not supported by this agent (supported: {string.Join(", ", SupportedConfigVersions)})");
        }
        if (!Uri.TryCreate(config.Gateway, UriKind.Absolute, out var gateway) || gateway.Scheme != "https")
        {
            problems.Add($"gateway: '{config.Gateway}' must be an https:// URL");
        }
        if (config.ProbeIntervalMinutes < 1)
        {
            problems.Add("probeIntervalMinutes: must be at least 1");
        }
        if (config.Endpoints.Count == 0)
        {
            problems.Add("endpoints: at least one endpoint is required");
        }

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var e in config.Endpoints)
        {
            var where = $"endpoints[{e.Alias}]";
            if (!AliasPattern.IsMatch(e.Alias))
            {
                problems.Add($"{where}.alias: letters, digits, '-' and '_' only, 1-64 characters (it becomes part of every identity on this source)");
            }
            else if (!seen.Add(e.Alias))
            {
                problems.Add($"{where}.alias: duplicated");
            }
            if (e.Kind is not ("onprem" or "cloud"))
            {
                problems.Add($"{where}.kind: '{e.Kind}' must be onprem or cloud");
            }
            foreach (var db in e.ExcludedDatabases)
            {
                if (!DatabasePattern.IsMatch(db))
                {
                    problems.Add($"{where}.excludedDatabases: '{db}' is not a plain database name");
                }
            }

            var engine = EngineRegistry.Find(e.Engine);
            if (engine == null)
            {
                problems.Add($"{where}.engine: '{e.Engine}' is not supported by this agent version (supported: {string.Join(", ", EngineRegistry.Names)})");
            }
            else
            {
                e.Engine = engine.Name;
                problems.AddRange(engine.Validate(e).Select(p => $"{where}.{p}"));
            }

            ResolvePassword(e, where, secrets, problems);
        }
        return problems;
    }

    private static void ResolvePassword(EndpointConfig e, string where, ISecretReader secrets, List<string> problems)
    {
        var hasEnv = !string.IsNullOrWhiteSpace(e.PasswordEnv);
        var hasFile = !string.IsNullOrWhiteSpace(e.PasswordFile);
        if (hasEnv == hasFile)
        {
            problems.Add($"{where}: exactly one of password_env or password_file is required");
            return;
        }
        try
        {
            var password = hasEnv ? secrets.FromEnvironment(e.PasswordEnv!) : secrets.FromFile(e.PasswordFile!);
            if (string.IsNullOrEmpty(password))
            {
                problems.Add(hasEnv
                    ? $"{where}: environment variable {e.PasswordEnv} is not set or empty"
                    : $"{where}: {e.PasswordFile} is empty");
                return;
            }
            e.Password = password;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            problems.Add($"{where}: cannot read {e.PasswordFile}: {ex.Message}");
        }
    }
}
