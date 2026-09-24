using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Trailox.Agent.Engines;
using YamlDotNet.Core;
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

    // \z, not $: in .NET $ also matches just before a final newline, so "hr_private\n" passed.
    private static readonly Regex AliasPattern = new(@"^[A-Za-z0-9_-]{1,64}\z", RegexOptions.Compiled);
    private static readonly Regex DatabasePattern = new(@"^[A-Za-z0-9_-]{1,255}\z", RegexOptions.Compiled);

    private static readonly IDeserializer Yaml = new DeserializerBuilder()
        .WithNamingConvention(CamelCaseNamingConvention.Instance)
        .IgnoreUnmatchedProperties()
        .Build();

    /// <summary>Reads the file and parses it. The fingerprint is for the check-in; see <see cref="Fingerprint"/>.</summary>
    public static (AgentConfig Config, string Fingerprint) Load(string path, ISecretReader secrets) =>
        Parse(File.ReadAllText(path), secrets);

    /// <summary>Parses, validates and resolves credentials. Throws <see cref="ConfigException"/> listing every problem.</summary>
    public static (AgentConfig Config, string Fingerprint) Parse(string yamlText, ISecretReader secrets)
    {
        var (config, uses) = Deserialize(yamlText, secrets);

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
        return (config, Fingerprint(yamlText, uses));
    }

    /// <summary>
    /// The file as YAML, every <c>${NAME}</c> in its values read from the environment
    /// (<see cref="EnvironmentValues"/>). A syntax error, a value of the wrong type, or a reference that
    /// cannot be resolved becomes a problem that says where it is: the file is edited by hand, and a block
    /// uncommented one line too far used to end in "Unhandled exception" and a stack trace.
    /// </summary>
    private static (AgentConfig Config, IReadOnlyList<EnvironmentValues.Use> Uses) Deserialize(string yamlText, ISecretReader secrets)
    {
        try
        {
            var expanded = EnvironmentValues.Expand(yamlText, secrets.FromEnvironment);
            if (expanded.Problems.Count > 0)
            {
                // Before deserializing: what would follow from a value that is not there is only noise.
                throw new ConfigException(expanded.Problems);
            }
            return (Yaml.Deserialize<AgentConfig>(new EventReplay(expanded.Events)) ?? new AgentConfig(), expanded.Uses);
        }
        catch (YamlException ex)
        {
            // A wrong-typed value arrives wrapped ("Exception during deserialization"); its inner
            // message is the one that says what is wrong.
            var what = ex.InnerException?.Message ?? ex.Message;
            var where = ex.Start.Line > 0 ? $"line {ex.Start.Line}, column {ex.Start.Column}: " : "";
            throw new ConfigException(new[] { where + what });
        }
    }

    /// <summary>
    /// Of the file's text: unchanged from 1.4 for a file without <c>${NAME}</c>, so an upgrade does not look
    /// like a config change. With references, of the values they resolved to as well, because the same file
    /// then describes a different config in a different environment. No credential is among them.
    /// </summary>
    private static string Fingerprint(string yamlText, IReadOnlyList<EnvironmentValues.Use> uses)
    {
        var hashed = uses.Count == 0 ? yamlText : yamlText + "\n" + JsonSerializer.Serialize(uses);
        return "sha256:" + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(hashed))).ToLowerInvariant();
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
            // The ranges Trailox accepts for a source it connects to directly, so a value that passes here
            // is one Trailox will use.
            if (e.PollMinutes is < 1 or > 1440)
            {
                problems.Add($"{where}.pollMinutes: {e.PollMinutes} must be between 1 and 1440 minutes");
            }
            if (e.BackfillDays is < 0 or > 365)
            {
                problems.Add($"{where}.backfillDays: {e.BackfillDays} must be between 0 and 365 days");
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
        // Never expanded: they already say where the credential is, and ${CH_PW} would make the password
        // itself the name - then printed in the problem below.
        if (hasEnv && e.PasswordEnv!.Contains("${"))
        {
            problems.Add($"{where}.password_env: password_env takes the variable's name, e.g. password_env: TRAILOX_CH_PROD_PASSWORD; ${{...}} is not expanded here");
            return;
        }
        if (hasFile && e.PasswordFile!.Contains("${"))
        {
            problems.Add($"{where}.password_file: password_file takes a path, e.g. password_file: /run/secrets/ch_prod; ${{...}} is not expanded here");
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
