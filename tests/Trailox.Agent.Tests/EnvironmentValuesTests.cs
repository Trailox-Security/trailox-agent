using System.Security.Cryptography;
using System.Text;
using Trailox.Agent.Config;

namespace Trailox.Agent.Tests;

/// <summary>
/// A value in agent.yaml can come from the agent's environment (1.5.0): <c>${NAME}</c>,
/// <c>${NAME:-default}</c>, or part of a value. A Helm chart can then supply the address, the port and
/// the rest from its own values, a ConfigMap or a Secret, while a value written in the file keeps working.
/// </summary>
public class EnvironmentValuesTests
{
    private const string Secret = "s3cret-f7e1-never-printed";
    private const string AgentKey = "agent-key-9c2a-never-printed";

    /// <summary>Every value written out, as before 1.5.0. The line numbers matter to the tests below.</summary>
    private const string Literal = """
        version: 1
        gateway: https://agent.trailox.io
        probeIntervalMinutes: 30
        endpoints:
          - alias: prod-cluster
            engine: clickhouse
            host: clickhouse.internal
            port: 8443
            tls: true
            clusterName: ""
            username: trailox_monitor
            password_env: CH_PW
            excludedDatabases: [hr_private]
        """;

    private const string Redshift = """
        version: 1
        gateway: https://agent.trailox.io
        endpoints:
          - alias: warehouse
            engine: redshift
            host: my-wg.123456789012.eu-west-1.redshift-serverless.amazonaws.com
            username: trailox_agent
            password_env: RS_PW
        """;

    /// <summary>The agent's environment for a test, and every variable the loader asked it for.</summary>
    private sealed class FakeEnvironment : ISecretReader
    {
        public Dictionary<string, string> Env { get; } = new() { ["CH_PW"] = Secret, ["TRAILOX_AGENT_KEY"] = AgentKey };
        public Dictionary<string, string> Files { get; } = new();
        public List<string> Asked { get; } = new();

        public string? FromEnvironment(string variable)
        {
            Asked.Add(variable);
            return Env.TryGetValue(variable, out var v) ? v : null;
        }

        public string FromFile(string path) => Files.TryGetValue(path, out var v) ? v.TrimEnd('\r', '\n') : throw new FileNotFoundException(path);
    }

    private static FakeEnvironment With(params (string Name, string Value)[] variables)
    {
        var env = new FakeEnvironment();
        foreach (var (name, value) in variables)
        {
            env.Env[name] = value;
        }
        return env;
    }

    /// <summary>Lines added at the end of the (first) endpoint block.</summary>
    private static string Plus(string yaml, string lines) => yaml + "\n    " + lines.Replace("\n", "\n    ");

    private static EndpointConfig Endpoint(string yaml, FakeEnvironment env) =>
        ConfigLoader.Parse(yaml, env).Config.Endpoints.Single();

    private static ConfigException Refused(string yaml, FakeEnvironment env) =>
        Assert.Throws<ConfigException>(() => ConfigLoader.Parse(yaml, env));

    // ------------------------------------------------------------ reading values

    [Fact]
    public void A_value_can_come_from_the_environment()
    {
        var yaml = Literal.Replace("host: clickhouse.internal", "host: ${CH_HOST}");

        var endpoint = Endpoint(yaml, With(("CH_HOST", "clickhouse.prod.internal")));

        Assert.Equal("clickhouse.prod.internal", endpoint.Host);
        Assert.Equal(8443, endpoint.Port);
    }

    [Fact]
    public void Numbers_and_true_or_false_arrive_as_their_types()
    {
        var yaml = Plus(Literal
                .Replace("version: 1", "version: ${V}")
                .Replace("gateway: https://agent.trailox.io", "gateway: ${GATEWAY}")
                .Replace("probeIntervalMinutes: 30", "probeIntervalMinutes: ${PROBE}")
                .Replace("port: 8443", "port: ${CH_PORT}")
                .Replace("tls: true", "tls: ${CH_TLS}"),
            "pollMinutes: ${POLL}\nbackfillDays: ${BACKFILL}");
        var env = With(("V", "1"), ("GATEWAY", "https://agent.example.test"), ("PROBE", "15"), ("CH_PORT", "9440"),
            ("CH_TLS", "false"), ("POLL", "5"), ("BACKFILL", "90"));

        var (config, _) = ConfigLoader.Parse(yaml, env);

        var endpoint = config.Endpoints.Single();
        Assert.Equal(1, config.Version);
        Assert.Equal("https://agent.example.test", config.Gateway);
        Assert.Equal(15, config.ProbeIntervalMinutes);
        Assert.Equal(9440, endpoint.Port);
        Assert.False(endpoint.Tls);
        Assert.Equal(5, endpoint.PollMinutes);
        Assert.Equal(90, endpoint.BackfillDays);
    }

    [Theory]
    [InlineData("port: ${CH_PORT}", "tls: ${CH_TLS}")]
    [InlineData("port: \"${CH_PORT}\"", "tls: '${CH_TLS}'")]
    public void A_quoted_placeholder_reads_like_an_unquoted_one(string port, string tls)
    {
        var yaml = Literal.Replace("port: 8443", port).Replace("tls: true", tls);

        var endpoint = Endpoint(yaml, With(("CH_PORT", "9440"), ("CH_TLS", "false")));

        Assert.Equal(9440, endpoint.Port);
        Assert.False(endpoint.Tls);
    }

    [Theory]
    [InlineData(null, 9000)]
    [InlineData("", 9000)]
    [InlineData("\n", 9000)]
    [InlineData("9440", 9440)]
    public void A_default_applies_when_the_variable_is_unset_or_empty(string? value, int port)
    {
        var yaml = Literal.Replace("port: 8443", "port: ${CH_PORT:-9000}");
        var env = value == null ? new FakeEnvironment() : With(("CH_PORT", value));

        Assert.Equal(port, Endpoint(yaml, env).Port);
    }

    /// <summary>
    /// 🔴 <c>${NAME:-}</c> MEANS "THE DEFAULT", NEVER "EMPTY". YamlDotNet reads an empty plain value as null
    ///    and a null true/false as false: passed on as written, <c>tls: ${CH_TLS:-}</c> would have turned
    ///    TLS off without a word. The key is left out instead, so the documented default applies.
    /// </summary>
    [Fact]
    public void An_empty_default_leaves_the_key_out_so_its_documented_default_applies()
    {
        var yaml = Plus(Literal
                .Replace("probeIntervalMinutes: 30", "probeIntervalMinutes: ${PROBE:-}")
                .Replace("port: 8443", "port: ${CH_PORT:-}")
                .Replace("tls: true", "tls: ${CH_TLS:-}")
                .Replace("clusterName: \"\"", "clusterName: ${CH_CLUSTER:-}")
                .Replace("username: trailox_monitor", "username: ${CH_USER:-}")
                .Replace("excludedDatabases: [hr_private]", "excludedDatabases:\n      - hr_private\n      - ${EXTRA_EXCLUDED:-}"),
            "kind: ${CH_KIND:-}\npollMinutes: ${POLL:-}");

        var (config, _) = ConfigLoader.Parse(yaml, new FakeEnvironment());

        var endpoint = config.Endpoints.Single();
        Assert.Equal(60, config.ProbeIntervalMinutes);
        Assert.True(endpoint.Tls);
        Assert.Equal(8443, endpoint.Port);
        Assert.Equal("onprem", endpoint.Kind);
        Assert.Equal("", endpoint.ClusterName);
        Assert.Equal("trailox_monitor", endpoint.Username);
        Assert.Null(endpoint.PollMinutes);
        Assert.Equal(new[] { "hr_private" }, endpoint.ExcludedDatabases);
    }

    [Fact]
    public void An_empty_default_inside_braces_leaves_the_key_out_too()
    {
        var endpoint = Endpoint(Plus(Redshift, "options: { database: \"${RS_DB:-}\" }"), With(("RS_PW", "pw")));

        Assert.Equal("dev", endpoint.Options["database"]);
    }

    [Fact]
    public void An_alias_of_an_expanded_value_reads_the_expanded_value()
    {
        var yaml = Plus(Literal.Replace("host: clickhouse.internal", "host: &host ${CH_HOST}"), "options:\n  note: *host");

        var endpoint = Endpoint(yaml, With(("CH_HOST", "clickhouse.prod.internal")));

        Assert.Equal("clickhouse.prod.internal", endpoint.Host);
        Assert.Equal("clickhouse.prod.internal", endpoint.Options["note"]);
    }

    [Fact]
    public void An_anchored_value_cannot_be_left_out()
    {
        var yaml = Literal.Replace("tls: true", "tls: &tls ${CH_TLS:-}");

        var problem = Assert.Single(Refused(yaml, new FakeEnvironment()).Problems);

        Assert.Contains("endpoints[prod-cluster].tls", problem);
    }

    [Fact]
    public void A_placeholder_can_be_part_of_a_value_and_a_value_can_hold_several()
    {
        var yaml = Literal.Replace("host: clickhouse.internal", "host: ch-${CH_SHARD}.${CH_ENV}.internal");

        var endpoint = Endpoint(yaml, With(("CH_SHARD", "s1"), ("CH_ENV", "prod")));

        Assert.Equal("ch-s1.prod.internal", endpoint.Host);
    }

    [Theory]
    [InlineData("options:\n  database: ${RS_DB}\nexcludedDatabases:\n  - ${RS_EXCLUDED}")]
    [InlineData("options: { database: \"${RS_DB}\" }\nexcludedDatabases: [\"${RS_EXCLUDED}\"]")]
    public void Options_and_list_items_take_placeholders_in_block_or_quoted_flow_form(string lines)
    {
        var env = With(("RS_PW", "pw"), ("RS_DB", "analytics"), ("RS_EXCLUDED", "hr_private"));

        var endpoint = Endpoint(Plus(Redshift, lines), env);

        Assert.Equal("analytics", endpoint.Options["database"]);
        Assert.Equal(new[] { "hr_private" }, endpoint.ExcludedDatabases);
    }

    // ------------------------------------------------------------ what is refused, and how it is said

    [Fact]
    public void An_unset_variable_is_a_problem_naming_the_line_the_endpoint_the_field_and_the_variable()
    {
        var yaml = Literal.Replace("host: clickhouse.internal", "host: ${CH_HOST}");

        var problem = Assert.Single(Refused(yaml, new FakeEnvironment()).Problems);

        Assert.Equal("line 7, column 11: endpoints[prod-cluster].host: environment variable CH_HOST is not set or empty", problem);
    }

    [Fact]
    public void A_problem_outside_an_endpoint_names_its_key()
    {
        var yaml = Literal.Replace("gateway: https://agent.trailox.io", "gateway: ${TRAILOX_GATEWAY}");

        var problem = Assert.Single(Refused(yaml, new FakeEnvironment()).Problems);

        Assert.Equal("line 2, column 10: gateway: environment variable TRAILOX_GATEWAY is not set or empty", problem);
    }

    [Fact]
    public void An_endpoint_is_named_by_its_alias_even_when_the_alias_comes_later()
    {
        const string yaml = """
            version: 1
            gateway: https://agent.trailox.io
            endpoints:
              - engine: clickhouse
                host: ${CH_HOST}
                alias: prod-cluster
                password_env: CH_PW
            """;

        var problem = Assert.Single(Refused(yaml, new FakeEnvironment()).Problems);

        Assert.Equal("line 5, column 11: endpoints[prod-cluster].host: environment variable CH_HOST is not set or empty", problem);
    }

    [Fact]
    public void An_alias_from_the_environment_names_the_endpoint_in_problems()
    {
        var yaml = Literal.Replace("alias: prod-cluster", "alias: ${CH_ALIAS}").Replace("host: clickhouse.internal", "host: ${CH_HOST}");

        var problem = Assert.Single(Refused(yaml, With(("CH_ALIAS", "prod-a"))).Problems);

        Assert.Contains("endpoints[prod-a].host: environment variable CH_HOST", problem);
    }

    [Theory]
    [InlineData("")]
    [InlineData("\n")]
    [InlineData("\r\n")]
    public void An_empty_variable_is_refused_like_an_unset_one(string value)
    {
        var yaml = Literal.Replace("host: clickhouse.internal", "host: ${CH_HOST}");

        var problem = Assert.Single(Refused(yaml, With(("CH_HOST", value))).Problems);

        Assert.EndsWith("environment variable CH_HOST is not set or empty", problem);
    }

    [Fact]
    public void Every_missing_variable_is_listed_at_once()
    {
        var yaml = Literal.Replace("host: clickhouse.internal", "host: ${CH_HOST}").Replace("port: 8443", "port: ${CH_PORT}");

        var problems = Refused(yaml, new FakeEnvironment()).Problems;

        Assert.Equal(2, problems.Count);
        Assert.Contains(problems, p => p.Contains("endpoints[prod-cluster].host: environment variable CH_HOST"));
        Assert.Contains(problems, p => p.Contains("endpoints[prod-cluster].port: environment variable CH_PORT"));
    }

    [Theory]
    [InlineData("${CH-HOST}")]
    [InlineData("${}")]
    [InlineData("${1HOST}")]
    [InlineData("${CH_HOST")]
    [InlineData("${CH_HOST:?required}")]
    [InlineData("${CH_HOST:-${OTHER_HOST}}")]
    public void A_malformed_reference_is_a_problem_not_a_literal(string value)
    {
        var yaml = Literal.Replace("host: clickhouse.internal", "host: " + value);

        var problem = Assert.Single(Refused(yaml, new FakeEnvironment()).Problems);

        Assert.StartsWith("line 7, column 11: endpoints[prod-cluster].host: '" + value + "' is not a valid reference", problem);
    }

    /// <summary>Inside [...] or {...} an unquoted { is YAML syntax, so a placeholder there must be quoted.</summary>
    [Fact]
    public void Unquoted_inside_brackets_is_a_yaml_problem_with_its_line_not_a_crash()
    {
        var yaml = Literal.Replace("excludedDatabases: [hr_private]", "excludedDatabases: [${EXTRA_EXCLUDED}]");

        var problem = Assert.Single(Refused(yaml, With(("EXTRA_EXCLUDED", "finance"))).Problems);

        Assert.StartsWith("line 13, column ", problem);
    }

    [Fact]
    public void A_value_of_the_wrong_type_points_at_its_placeholder()
    {
        var yaml = Literal.Replace("port: 8443", "port: ${CH_PORT}");

        var problem = Assert.Single(Refused(yaml, With(("CH_PORT", "eighty"))).Problems);

        Assert.StartsWith("line 8, column 11: ", problem);
    }

    // ------------------------------------------------------------ a value is text

    [Fact]
    public void Two_dollar_signs_make_a_literal_placeholder()
    {
        var env = new FakeEnvironment();

        var endpoint = Endpoint(Plus(Literal, "options:\n  note: \"$${NOT_A_VARIABLE}\""), env);

        Assert.Equal("${NOT_A_VARIABLE}", endpoint.Options["note"]);
        Assert.DoesNotContain("NOT_A_VARIABLE", env.Asked);
    }

    [Fact]
    public void A_value_from_the_environment_is_not_expanded_again()
    {
        var env = With(("NOTE", "${OTHER}"));

        var endpoint = Endpoint(Plus(Literal, "options:\n  note: ${NOTE}"), env);

        Assert.Equal("${OTHER}", endpoint.Options["note"]);
        Assert.DoesNotContain("OTHER", env.Asked);
    }

    [Fact]
    public void Yaml_inside_a_value_is_just_text()
    {
        var note = Endpoint(Plus(Literal, "options:\n  note: ${NOTE}"), With(("NOTE", "a: b # not a comment")));
        Assert.Equal("a: b # not a comment", note.Options["note"]);

        var list = Literal.Replace("excludedDatabases: [hr_private]", "excludedDatabases:\n      - ${EXCLUDED}");
        var problem = Assert.Single(Refused(list, With(("EXCLUDED", "[hr_private, finance]"))).Problems);
        Assert.Equal("endpoints[prod-cluster].excludedDatabases: '[hr_private, finance]' is not a plain database name", problem);
    }

    [Fact]
    public void Null_from_the_environment_is_the_text_null_never_a_missing_value()
    {
        var cluster = Literal.Replace("clusterName: \"\"", "clusterName: ${CH_CLUSTER}");
        Assert.Equal("null", Endpoint(cluster, With(("CH_CLUSTER", "null"))).ClusterName);

        var tls = Literal.Replace("tls: true", "tls: ${CH_TLS}");
        var problem = Assert.Single(Refused(tls, With(("CH_TLS", "null"))).Problems);
        Assert.StartsWith("line 9, column 10: ", problem);
    }

    /// <summary>
    /// 🔴 A VALUE FROM A FILE-BACKED CONFIGMAP OR SECRET ENDS IN A NEWLINE, and .NET's <c>$</c> matches just
    ///    before a final one: "hr_private\n" passes the database-name check and then excludes nothing, because
    ///    no database has that name. Trailing newlines go, as they do from a password_file.
    /// </summary>
    [Theory]
    [InlineData("hr_private\n")]
    [InlineData("hr_private\r\n")]
    public void A_trailing_newline_is_dropped(string value)
    {
        var yaml = Literal.Replace("excludedDatabases: [hr_private]", "excludedDatabases: [\"${EXCLUDED}\"]");

        Assert.Equal(new[] { "hr_private" }, Endpoint(yaml, With(("EXCLUDED", value))).ExcludedDatabases);
    }

    [Theory]
    [InlineData("hr\tprivate")]
    [InlineData("hr\nprivate")]
    [InlineData("hr_private\0")]
    public void Any_other_control_character_is_a_problem_that_does_not_print_the_value(string value)
    {
        var yaml = Literal.Replace("excludedDatabases: [hr_private]", "excludedDatabases: [\"${EXCLUDED}\"]");

        var problem = Assert.Single(Refused(yaml, With(("EXCLUDED", value))).Problems);

        Assert.StartsWith("line 13, column ", problem);
        Assert.EndsWith(": endpoints[prod-cluster].excludedDatabases: environment variable EXCLUDED holds a control character", problem);
    }

    [Fact]
    public void Comments_and_keys_are_never_read()
    {
        var env = new FakeEnvironment();

        ConfigLoader.Parse(Plus(Literal, "# host: ${UNSET_IN_A_COMMENT}\n${UNSET_IN_A_KEY}: 1"), env);

        Assert.DoesNotContain(env.Asked, v => v.StartsWith("UNSET_"));
    }

    // ------------------------------------------------------------ credentials stay credentials

    /// <summary>
    /// 🔴 A CREDENTIAL MUST NEVER BECOME A VALUE. Values are reported to Trailox (clusterName, username,
    ///    excludedDatabases, ...) and echoed by error messages - a bad true/false prints the text. A variable
    ///    holding the agent key, or a password that password_env names, is refused before it is read.
    /// </summary>
    [Theory]
    [InlineData("tls: true", "tls: ${CH_PW}", "CH_PW")]
    [InlineData("username: trailox_monitor", "username: ${CH_PW}", "CH_PW")]
    [InlineData("clusterName: \"\"", "clusterName: ${TRAILOX_AGENT_KEY}", "TRAILOX_AGENT_KEY")]
    [InlineData("clusterName: \"\"", "clusterName: ${trailox_agent_key}", "trailox_agent_key")]   // Windows reads either
    [InlineData("username: trailox_monitor", "username: ${ch_pw}", "ch_pw")]
    public void A_variable_holding_a_credential_cannot_be_a_value(string from, string to, string variable)
    {
        var env = new FakeEnvironment();

        var ex = Refused(Literal.Replace(from, to), env);

        Assert.Contains("${" + variable + "} holds a credential", Assert.Single(ex.Problems));
        Assert.DoesNotContain(Secret, ex.Message);
        Assert.DoesNotContain(AgentKey, ex.Message);
        Assert.DoesNotContain(variable, env.Asked);
    }

    [Fact]
    public void A_password_that_a_later_endpoint_names_is_refused_too()
    {
        var yaml = Literal.Replace("clusterName: \"\"", "clusterName: ${CH2_PW}")
            + "\n  - alias: second\n    engine: clickhouse\n    host: ch2.internal\n    password_env: CH2_PW";
        var env = With(("CH2_PW", Secret));

        var ex = Refused(yaml, env);

        Assert.Contains("${CH2_PW} holds a credential", Assert.Single(ex.Problems));
        Assert.DoesNotContain(Secret, ex.Message);
        Assert.DoesNotContain("CH2_PW", env.Asked);
    }

    /// <summary>
    /// The mistake the next test answers, with the same variable used as a value elsewhere: its name is
    /// taken from inside the reference, or the value would be read and a bad true/false would print it.
    /// </summary>
    [Fact]
    public void A_reference_written_in_password_env_still_makes_its_variable_a_credential()
    {
        var env = new FakeEnvironment();
        var yaml = Literal.Replace("password_env: CH_PW", "password_env: ${CH_PW}").Replace("tls: true", "tls: ${CH_PW}");

        var ex = Refused(yaml, env);

        Assert.Contains("${CH_PW} holds a credential", Assert.Single(ex.Problems));
        Assert.DoesNotContain(Secret, ex.Message);
        Assert.DoesNotContain("CH_PW", env.Asked);
    }

    [Fact]
    public void A_password_env_given_by_a_yaml_alias_is_refused_too()
    {
        var env = new FakeEnvironment();
        var yaml = Literal
            .Replace("password_env: CH_PW", "options: { pw: &pw CH_PW }\n    password_env: *pw")
            .Replace("tls: true", "tls: ${CH_PW}");

        var ex = Refused(yaml, env);

        Assert.Contains("${CH_PW} holds a credential", Assert.Single(ex.Problems));
        Assert.DoesNotContain(Secret, ex.Message);
        Assert.DoesNotContain("CH_PW", env.Asked);
    }

    [Fact]
    public void A_value_that_password_env_aliases_is_never_expanded()
    {
        var env = new FakeEnvironment();
        var yaml = Literal.Replace("password_env: CH_PW", "options: { pw: &pw \"${CH_PW}\" }\n    password_env: *pw");

        var ex = Refused(yaml, env);

        Assert.Contains("password_env takes the variable's name", Assert.Single(ex.Problems));
        Assert.DoesNotContain(Secret, ex.Message);
        Assert.DoesNotContain("CH_PW", env.Asked);
    }

    [Fact]
    public void Password_env_takes_a_variable_name_and_is_not_expanded()
    {
        var env = new FakeEnvironment();

        var ex = Refused(Literal.Replace("password_env: CH_PW", "password_env: ${CH_PW}"), env);

        Assert.Contains("password_env takes the variable's name", Assert.Single(ex.Problems));
        Assert.DoesNotContain(Secret, ex.Message);
        Assert.DoesNotContain("CH_PW", env.Asked);
    }

    [Fact]
    public void Password_file_takes_a_path_and_is_not_expanded()
    {
        var env = With(("PW_FILE", "/run/secrets/ch"));
        env.Files["/run/secrets/ch"] = "pw";

        var ex = Refused(Literal.Replace("password_env: CH_PW", "password_file: ${PW_FILE}"), env);

        Assert.Contains("password_file takes a path", Assert.Single(ex.Problems));
        Assert.DoesNotContain("PW_FILE", env.Asked);
    }

    // ------------------------------------------------------------ the fingerprint

    [Fact]
    public void A_file_without_placeholders_keeps_the_fingerprint_it_had_before_1_5()
    {
        var (_, fingerprint) = ConfigLoader.Parse(Literal, new FakeEnvironment());

        Assert.Equal("sha256:" + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(Literal))).ToLowerInvariant(), fingerprint);
    }

    [Fact]
    public void The_fingerprint_follows_the_values_the_placeholders_resolve_to()
    {
        var yaml = Literal.Replace("host: clickhouse.internal", "host: ${CH_HOST}");
        string Print(string host) => ConfigLoader.Parse(yaml, With(("CH_HOST", host))).Fingerprint;

        Assert.Equal(Print("a.internal"), Print("a.internal"));
        Assert.NotEqual(Print("a.internal"), Print("b.internal"));
    }

    [Fact]
    public void The_fingerprint_tells_which_variable_supplied_a_value()
    {
        var yaml = Literal.Replace("clusterName: \"\"", "clusterName: ${CH_CLUSTER:-}").Replace("username: trailox_monitor", "username: ${CH_USER:-}");

        var one = ConfigLoader.Parse(yaml, With(("CH_CLUSTER", "prod"))).Fingerprint;
        var other = ConfigLoader.Parse(yaml, With(("CH_USER", "prod"))).Fingerprint;

        Assert.NotEqual(one, other);
    }
}
