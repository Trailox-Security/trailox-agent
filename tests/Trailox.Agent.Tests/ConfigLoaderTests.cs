using Trailox.Agent.Config;

namespace Trailox.Agent.Tests;

public class ConfigLoaderTests
{
    private const string Good = """
        version: 1
        gateway: https://agent.trailox.io
        probeIntervalMinutes: 30
        endpoints:
          - alias: prod-cluster
            engine: clickhouse
            kind: onprem
            host: clickhouse.internal
            port: 8443
            tls: true
            clusterName: ""
            username: trailox_monitor
            password_env: CH_PW
            collectSessionLog: true
            storeRawQueryText: false
            excludedDatabases: [hr_private]
        """;

    /// <summary>A secret source for tests: no environment, no filesystem.</summary>
    private sealed class FakeSecrets : ISecretReader
    {
        public Dictionary<string, string> Env { get; } = new();
        public Dictionary<string, string> Files { get; } = new();

        public string? FromEnvironment(string variable) => Env.TryGetValue(variable, out var v) ? v : null;

        public string FromFile(string path) => Files.TryGetValue(path, out var v) ? v.TrimEnd('\r', '\n') : throw new FileNotFoundException(path);
    }

    private static FakeSecrets WithPassword() => new() { Env = { ["CH_PW"] = "s3cret" } };

    /// <summary>
    /// Every example commented out is how an agent.yaml starts. YAML reads the bare key as null;
    /// 1.3.0 died on it with a NullReferenceException instead of saying what to do.
    /// </summary>
    [Fact]
    public void Endpoints_with_only_comments_under_them_is_a_clear_problem_not_a_crash()
    {
        const string yaml = """
            version: 1
            gateway: https://agent.trailox.io
            endpoints:
              # - alias: prod-cluster
              #   engine: clickhouse
            """;

        var ex = Assert.Throws<ConfigException>(() => ConfigLoader.Parse(yaml, new FakeSecrets()));

        Assert.Contains("endpoints: at least one endpoint is required", ex.Problems);
    }

    /// <summary>
    /// A block uncommented one line too far - its heading comment along with it - is how agent.yaml
    /// usually breaks by hand. 1.3.1 died on it with "Unhandled exception" and a stack trace.
    /// </summary>
    [Fact]
    public void A_yaml_syntax_error_is_a_problem_that_says_where_not_a_crash()
    {
        var yaml = Good + "\n  An Amazon Redshift cluster, uncommented by mistake:\n";

        var ex = Assert.Throws<ConfigException>(() => ConfigLoader.Parse(yaml, WithPassword()));

        var problem = Assert.Single(ex.Problems);
        Assert.StartsWith("line 17, column 3: ", problem);
        Assert.DoesNotContain("Idx", problem);
    }

    [Fact]
    public void A_value_of_the_wrong_type_says_where_it_is()
    {
        var yaml = Good.Replace("port: 8443", "port: eighty");

        var ex = Assert.Throws<ConfigException>(() => ConfigLoader.Parse(yaml, WithPassword()));

        Assert.StartsWith("line 9, column ", Assert.Single(ex.Problems));
    }

    [Fact]
    public void An_unreadable_agent_yaml_is_a_config_failure_not_a_crash()
    {
        Assert.True(Program.IsConfigFailure(new UnauthorizedAccessException("Access to the path '/etc/trailox/agent.yaml' is denied.")));
        Assert.True(Program.IsConfigFailure(new FileNotFoundException("agent.yaml")));
        Assert.False(Program.IsConfigFailure(new InvalidOperationException()));
    }

    [Fact]
    public void An_empty_excluded_databases_key_is_an_empty_list()
    {
        var yaml = Good.Replace("excludedDatabases: [hr_private]", "excludedDatabases:");

        var (config, _) = ConfigLoader.Parse(yaml, WithPassword());

        Assert.Empty(config.Endpoints[0].ExcludedDatabases);
    }

    [Fact]
    public void A_good_config_parses_resolves_the_password_and_fingerprints_the_text()
    {
        var (config, fingerprint) = ConfigLoader.Parse(Good, WithPassword());

        var e = Assert.Single(config.Endpoints);
        Assert.Equal("prod-cluster", e.Alias);
        Assert.Equal("clickhouse", e.Engine);
        Assert.Equal("s3cret", e.Password);
        Assert.False(e.StoreRawQueryText);
        Assert.Equal(new[] { "hr_private" }, e.ExcludedDatabases);
        Assert.Equal(30, config.ProbeIntervalMinutes);
        Assert.StartsWith("sha256:", fingerprint);
        Assert.Equal(7 + 64, fingerprint.Length);
    }

    [Fact]
    public void Engine_defaults_apply_only_when_the_field_is_absent()
    {
        // Whole lines, indentation included, or the following key inherits eight spaces and the YAML is invalid.
        var yaml = Good.Replace("    port: 8443\n", "").Replace("    username: trailox_monitor\n", "");
        var (config, _) = ConfigLoader.Parse(yaml, WithPassword());
        Assert.Equal(8443, config.Endpoints[0].Port);
        Assert.Equal("trailox_monitor", config.Endpoints[0].Username);
    }

    [Fact]
    public void The_password_never_appears_in_the_serialized_config()
    {
        var (config, _) = ConfigLoader.Parse(Good, WithPassword());
        var yaml = new YamlDotNet.Serialization.SerializerBuilder().Build().Serialize(config);
        Assert.DoesNotContain("s3cret", yaml);
    }

    [Fact]
    public void A_password_file_is_read_and_trailing_newlines_dropped()
    {
        var yaml = Good.Replace("password_env: CH_PW", "password_file: /run/secrets/ch");
        var secrets = new FakeSecrets { Files = { ["/run/secrets/ch"] = "pw-from-file\n" } };
        var (config, _) = ConfigLoader.Parse(yaml, secrets);
        Assert.Equal("pw-from-file", config.Endpoints[0].Password);
    }

    [Theory]
    [InlineData("alias: prod-cluster", "alias: 'has space'", ".alias")]
    [InlineData("engine: clickhouse", "engine: postgres", ".engine")]
    [InlineData("engine: clickhouse", "engine: ''", ".engine")]
    [InlineData("host: clickhouse.internal", "host: ''", ".host")]
    [InlineData("port: 8443", "port: 70000", ".port")]
    [InlineData("clusterName: \"\"", "clusterName: 'a b'", ".clusterName")]
    [InlineData("username: trailox_monitor", "username: 'x;y'", ".username")]
    [InlineData("excludedDatabases: [hr_private]", "excludedDatabases: ['drop table']", ".excludedDatabases")]
    [InlineData("gateway: https://agent.trailox.io", "gateway: http://agent.trailox.io", "gateway")]
    [InlineData("version: 1", "version: 2", "version")]
    [InlineData("probeIntervalMinutes: 30", "probeIntervalMinutes: 0", "probeIntervalMinutes")]
    public void Every_field_that_reaches_sql_or_the_network_is_validated(string from, string to, string expectedInProblem)
    {
        var yaml = Good.Replace(from, to);
        var ex = Assert.Throws<ConfigException>(() => ConfigLoader.Parse(yaml, WithPassword()));
        Assert.Contains(ex.Problems, p => p.Contains(expectedInProblem));
    }

    [Fact]
    public void A_missing_password_source_is_reported_not_defaulted()
    {
        var ex = Assert.Throws<ConfigException>(() => ConfigLoader.Parse(Good, new FakeSecrets()));
        Assert.Contains(ex.Problems, p => p.Contains("CH_PW is not set"));

        var both = Good.Replace("password_env: CH_PW", "password_env: CH_PW\n    password_file: /x");
        var ex2 = Assert.Throws<ConfigException>(() => ConfigLoader.Parse(both, WithPassword()));
        Assert.Contains(ex2.Problems, p => p.Contains("exactly one of"));
    }

    [Fact]
    public void Duplicate_aliases_and_unknown_keys_are_handled()
    {
        var dup = Good + "\n  - alias: prod-cluster\n    engine: clickhouse\n    host: other\n    password_env: CH_PW\n    unknownKey: ignored\n";
        var ex = Assert.Throws<ConfigException>(() => ConfigLoader.Parse(dup, WithPassword()));
        Assert.Contains(ex.Problems, p => p.Contains("duplicated"));
    }
}
