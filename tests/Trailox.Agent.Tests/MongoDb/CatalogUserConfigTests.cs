using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Protocol;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Tests.MongoDb;

/// <summary>
/// The optional catalog user in agent.yaml (1.9.0): <c>catalog_username</c> with exactly one of
/// <c>catalog_password_env</c> or <c>catalog_password_file</c>, on a mongodb endpoint only, and the monitoring user
/// it makes the source report.
/// </summary>
public class CatalogUserConfigTests
{
    private const string Password = "catalog-pw-3b1e-never-printed";

    private const string Atlas = """
        version: 1
        gateway: https://agent.trailox.io
        endpoints:
          - alias: atlas-prod
            engine: mongodb
            host: 0123456789abcdef01234567
            clusterName: Cluster0
            username: mdb_sa_id_0123456789abcdef01234567
            password_env: ATLAS_SECRET
        """;

    private sealed class FakeSecrets : ISecretReader
    {
        public Dictionary<string, string> Env { get; } = new() { ["ATLAS_SECRET"] = "sa-secret", ["CATALOG_PW"] = Password };
        public Dictionary<string, string> Files { get; } = new() { ["/run/secrets/atlas_catalog"] = Password + "\n" };
        public List<string> Asked { get; } = new();

        public string? FromEnvironment(string variable)
        {
            Asked.Add(variable);
            return Env.TryGetValue(variable, out var v) ? v : null;
        }

        public string FromFile(string path) => Files.TryGetValue(path, out var v) ? v.TrimEnd('\r', '\n') : throw new FileNotFoundException(path);
    }

    private static string With(params string[] lines) => Atlas + string.Concat(lines.Select(l => "\n    " + l));

    private static ConfigException Refused(string yaml, FakeSecrets? secrets = null)
    {
        var ex = Assert.Throws<ConfigException>(() => ConfigLoader.Parse(yaml, secrets ?? new FakeSecrets()));
        Assert.DoesNotContain(Password, ex.Message, StringComparison.Ordinal);
        return ex;
    }

    [Fact]
    public void Without_the_keys_there_is_no_catalog_user()
    {
        var (config, _) = ConfigLoader.Parse(Atlas, new FakeSecrets());

        Assert.Equal("", config.Endpoints[0].CatalogUsername);
        Assert.Equal("", config.Endpoints[0].CatalogPassword);
    }

    [Fact]
    public void A_catalog_user_with_its_password_by_variable()
    {
        var (config, _) = ConfigLoader.Parse(With("catalog_username: trailox_catalog", "catalog_password_env: CATALOG_PW"), new FakeSecrets());

        var e = config.Endpoints[0];
        Assert.Equal("trailox_catalog", e.CatalogUsername);
        Assert.Equal(Password, e.CatalogPassword);
        // The service account's secret is still its own.
        Assert.Equal("sa-secret", e.Password);
    }

    [Fact]
    public void A_catalog_user_with_its_password_by_file_loses_the_trailing_newline()
    {
        var (config, _) = ConfigLoader.Parse(With("catalog_username: trailox_catalog", "catalog_password_file: /run/secrets/atlas_catalog"), new FakeSecrets());

        Assert.Equal(Password, config.Endpoints[0].CatalogPassword);
    }

    [Theory]
    [InlineData("catalog_username: trailox_catalog")]
    [InlineData("catalog_username: trailox_catalog", "catalog_password_env: CATALOG_PW", "catalog_password_file: /run/secrets/atlas_catalog")]
    public void A_catalog_user_needs_exactly_one_password_reference(params string[] lines)
    {
        var ex = Refused(With(lines));

        Assert.Contains("endpoints[atlas-prod]: exactly one of catalog_password_env or catalog_password_file is required with catalog_username", ex.Problems);
    }

    /// <summary>A password without a name says the customer meant a catalog user: it is refused, not ignored.</summary>
    [Theory]
    [InlineData("catalog_password_env: CATALOG_PW")]
    [InlineData("catalog_password_file: /run/secrets/atlas_catalog")]
    public void A_catalog_password_without_a_catalog_user_is_refused(string line)
    {
        var ex = Refused(With(line));

        Assert.Contains(ex.Problems, p => p.StartsWith("endpoints[atlas-prod].catalog_username: required with catalog_password_env or catalog_password_file", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("engine: clickhouse", "catalog_username: trailox_catalog", "catalog_password_env: CATALOG_PW")]
    [InlineData("engine: snowflake", "catalog_password_env: CATALOG_PW")]
    public void The_keys_are_for_mongodb_only(string engine, params string[] lines)
    {
        var yaml = With(lines).Replace("engine: mongodb", engine);

        var ex = Refused(yaml);

        Assert.Contains("endpoints[atlas-prod]: catalog_username, catalog_password_env and catalog_password_file are for engine mongodb only", ex.Problems);
    }

    /// <summary>The name becomes the source's monitoring user in Trailox, which takes these characters only.</summary>
    [Theory]
    [InlineData("'trailox catalog'")]
    [InlineData("'cat;alog'")]
    [InlineData("'cat/alog'")]
    [InlineData("a234567890123456789012345678901234567890123456789012345678901234x")]
    public void A_catalog_username_Trailox_would_not_accept_is_refused(string name)
    {
        var ex = Refused(With($"catalog_username: {name}", "catalog_password_env: CATALOG_PW"));

        Assert.Contains(ex.Problems, p => p.StartsWith("endpoints[atlas-prod].catalog_username: the catalog database user's name", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("trailox_catalog")]
    [InlineData("trailox.catalog@example.com")]
    [InlineData("trailox-catalog")]
    public void A_catalog_username_of_the_accepted_characters_is_trimmed_and_kept(string name)
    {
        var (config, _) = ConfigLoader.Parse(With($"catalog_username: '  {name} '", "catalog_password_env: CATALOG_PW"), new FakeSecrets());

        Assert.Equal(name, config.Endpoints[0].CatalogUsername);
    }

    [Fact]
    public void An_unset_catalog_password_variable_is_named()
    {
        var secrets = new FakeSecrets();
        secrets.Env.Remove("CATALOG_PW");

        var ex = Refused(With("catalog_username: trailox_catalog", "catalog_password_env: CATALOG_PW"), secrets);

        Assert.Contains("endpoints[atlas-prod]: environment variable CATALOG_PW is not set or empty", ex.Problems);
    }

    [Fact]
    public void An_unreadable_catalog_password_file_is_named()
    {
        var ex = Refused(With("catalog_username: trailox_catalog", "catalog_password_file: /run/secrets/missing"));

        Assert.Contains(ex.Problems, p => p.StartsWith("endpoints[atlas-prod]: cannot read /run/secrets/missing", StringComparison.Ordinal));
    }

    [Fact]
    public void Catalog_password_env_takes_a_variable_name_and_is_not_expanded()
    {
        var secrets = new FakeSecrets();

        var ex = Refused(With("catalog_username: trailox_catalog", "catalog_password_env: ${CATALOG_PW}"), secrets);

        Assert.Contains(ex.Problems, p => p.Contains("catalog_password_env takes the variable's name", StringComparison.Ordinal));
        Assert.DoesNotContain("CATALOG_PW", secrets.Asked);
    }

    /// <summary>🔴 The catalog user's password is a credential like the others: it can never become a value elsewhere.</summary>
    [Fact]
    public void The_variable_catalog_password_env_names_cannot_be_used_as_a_value()
    {
        var secrets = new FakeSecrets();
        var yaml = With("catalog_username: trailox_catalog", "catalog_password_env: CATALOG_PW").Replace("clusterName: Cluster0", "clusterName: ${CATALOG_PW}");

        var ex = Refused(yaml, secrets);

        Assert.Contains("${CATALOG_PW} holds a credential", Assert.Single(ex.Problems), StringComparison.Ordinal);
        Assert.DoesNotContain("CATALOG_PW", secrets.Asked);
    }

    [Fact]
    public void The_catalog_password_never_appears_in_the_serialized_config()
    {
        var (config, _) = ConfigLoader.Parse(With("catalog_username: trailox_catalog", "catalog_password_env: CATALOG_PW"), new FakeSecrets());

        var yaml = new YamlDotNet.Serialization.SerializerBuilder().Build().Serialize(config);

        Assert.DoesNotContain(Password, yaml, StringComparison.Ordinal);
    }

    /// <summary>The secret goes through its keys only: options take none (the engine refuses unknown options).</summary>
    [Fact]
    public void Options_take_no_catalog_password()
    {
        var ex = Refused(With("catalog_username: trailox_catalog", "catalog_password_env: CATALOG_PW", "options: { catalog_password: x }"));

        Assert.Contains(ex.Problems, p => p.StartsWith("endpoints[atlas-prod].options.catalog_password: not an option", StringComparison.Ordinal));
    }

    // ------------------------------------------------------------ monitorUser

    private sealed class NoSource : IEngineSource
    {
        public Task<EndpointCaps> ProbeAsync(CancellationToken ct) => Task.FromResult(new EndpointCaps());
        public Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct) => Task.FromResult<RowStream?>(null);
    }

    /// <summary>
    /// The account that signs in to the cluster is the catalog user, so Trailox files its own sign-ins and commands as the
    /// platform's (the collector's MONITOR arm). Without one it stays the service account's client id, as in 1.8.
    /// </summary>
    [Theory]
    [InlineData("catalog_username: trailox_catalog", "trailox_catalog")]
    [InlineData(null, "mdb_sa_id_0123456789abcdef01234567")]
    public void The_monitoring_user_is_the_catalog_user_when_there_is_one(string? line, string expected)
    {
        var yaml = line == null ? Atlas : With(line, "catalog_password_env: CATALOG_PW");
        var (config, _) = ConfigLoader.Parse(yaml, new FakeSecrets());

        var report = new EndpointSession(config.Endpoints[0], new NoSource(), TimeSpan.FromMinutes(60)).Report();

        Assert.Equal(expected, report.MonitorUser);
    }

    [Fact]
    public void Another_engines_monitoring_user_is_unchanged()
    {
        var report = new EndpointSession(new EndpointConfig { Alias = "ch", Engine = "clickhouse", Username = "trailox_monitor" }, new NoSource(), TimeSpan.FromMinutes(60)).Report();

        Assert.Equal("trailox_monitor", report.MonitorUser);
    }
}
