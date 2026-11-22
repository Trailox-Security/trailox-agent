using Trailox.Agent.Config;
using Trailox.Agent.Engines.ClickHouse;
using Trailox.Agent.Engines.Redshift;

namespace Trailox.Agent.Tests;

/// <summary>
/// A key written with nothing after it, and a value with a line break in it (1.5.2).
/// </summary>
/// <remarks>
/// YamlDotNet reads <c>tls:</c> as null, and null as the type's default: a true/false became false - TLS
/// off, sign-ins and statement text dropped - and a name crashed the checks with a stack trace instead of
/// a config problem. A value ending in a line break passed every check, because .NET's <c>$</c> matches
/// just before a final newline: "hr_private\n" was accepted and then excluded nothing.
/// </remarks>
public class NoValueAndControlCharacterTests
{
    private const string NoValue = "no value - write one, or remove the line to use the default";
    private const string NoItem = "an item with no value - write one, or remove it";
    private const string Control = "holds a control character - remove it";

    private const string ClickHouse = """
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
            storeRawQueryText: true
            excludedDatabases: [hr_private]
            pollMinutes: 10
            backfillDays: 30
        """;

    private const string Redshift = """
        version: 1
        gateway: https://agent.trailox.io
        endpoints:
          - alias: warehouse
            engine: redshift
            host: my-wg.123456789012.eu-west-1.redshift-serverless.amazonaws.com
            clusterName: my-wg
            username: trailox_agent
            password_env: RS_PW
            options:
              database: dev
        """;

    private const string Snowflake = """
        version: 1
        gateway: https://agent.trailox.io
        endpoints:
          - alias: snowflake
            engine: snowflake
            host: myorg-myaccount
            clusterName: TRAILOX_MONITOR_WH
            username: TRAILOX_AGENT
            password_env: SF_KEY
        """;

    private const string Databricks = """
        version: 1
        gateway: https://agent.trailox.io
        endpoints:
          - alias: lakehouse
            engine: databricks
            host: dbc-xxxxxxxx-xxxx.cloud.databricks.com
            clusterName: 0123456789abcdef
            username: 00000000-0000-0000-0000-000000000000
            password_env: DBX_SECRET
        """;

    private static readonly Dictionary<string, string> Bases = new()
    {
        ["clickhouse"] = ClickHouse, ["redshift"] = Redshift, ["snowflake"] = Snowflake, ["databricks"] = Databricks,
    };

    private sealed class FakeSecrets : ISecretReader
    {
        public string? FromEnvironment(string variable) => variable is "CH_PW" or "RS_PW" or "SF_KEY" or "DBX_SECRET" ? "s3cret" : null;
        public string FromFile(string path) => throw new FileNotFoundException(path);
    }

    private static AgentConfig Parse(string yaml) => ConfigLoader.Parse(yaml, new FakeSecrets()).Config;

    private static ConfigException Refused(string yaml) => Assert.Throws<ConfigException>(() => ConfigLoader.Parse(yaml, new FakeSecrets()));

    // ------------------------------------------------------------ a key with no value

    /// <summary>
    /// Every key that holds one value: each crashed, silently became false or 0, or said something
    /// beside the point. Now each is the same config problem, naming its line, endpoint and field.
    /// </summary>
    [Theory]
    [InlineData("clickhouse", "alias: prod-cluster", "alias:", "endpoints[].alias")]
    [InlineData("clickhouse", "host: clickhouse.internal", "host:", "endpoints[prod-cluster].host")]
    [InlineData("clickhouse", "clusterName: \"\"", "clusterName:", "endpoints[prod-cluster].clusterName")]
    [InlineData("clickhouse", "tls: true", "tls:", "endpoints[prod-cluster].tls")]
    [InlineData("clickhouse", "collectSessionLog: true", "collectSessionLog:", "endpoints[prod-cluster].collectSessionLog")]
    [InlineData("clickhouse", "storeRawQueryText: true", "storeRawQueryText:", "endpoints[prod-cluster].storeRawQueryText")]
    [InlineData("clickhouse", "port: 8443", "port:", "endpoints[prod-cluster].port")]
    [InlineData("clickhouse", "username: trailox_monitor", "username:", "endpoints[prod-cluster].username")]
    [InlineData("clickhouse", "engine: clickhouse", "engine:", "endpoints[prod-cluster].engine")]
    [InlineData("clickhouse", "kind: onprem", "kind:", "endpoints[prod-cluster].kind")]
    [InlineData("clickhouse", "pollMinutes: 10", "pollMinutes:", "endpoints[prod-cluster].pollMinutes")]
    [InlineData("clickhouse", "backfillDays: 30", "backfillDays:", "endpoints[prod-cluster].backfillDays")]
    [InlineData("clickhouse", "password_env: CH_PW", "password_env:", "endpoints[prod-cluster].password_env")]
    [InlineData("clickhouse", "version: 1", "version:", "version")]
    [InlineData("clickhouse", "gateway: https://agent.trailox.io", "gateway:", "gateway")]
    [InlineData("clickhouse", "probeIntervalMinutes: 30", "probeIntervalMinutes:", "probeIntervalMinutes")]
    [InlineData("redshift", "host: my-wg.123456789012.eu-west-1.redshift-serverless.amazonaws.com", "host:", "endpoints[warehouse].host")]
    [InlineData("redshift", "clusterName: my-wg", "clusterName:", "endpoints[warehouse].clusterName")]
    [InlineData("redshift", "username: trailox_agent", "username:", "endpoints[warehouse].username")]
    [InlineData("redshift", "database: dev", "database:", "endpoints[warehouse].options.database")]
    [InlineData("snowflake", "host: myorg-myaccount", "host:", "endpoints[snowflake].host")]
    [InlineData("snowflake", "clusterName: TRAILOX_MONITOR_WH", "clusterName:", "endpoints[snowflake].clusterName")]
    [InlineData("snowflake", "username: TRAILOX_AGENT", "username:", "endpoints[snowflake].username")]
    [InlineData("databricks", "host: dbc-xxxxxxxx-xxxx.cloud.databricks.com", "host:", "endpoints[lakehouse].host")]
    [InlineData("databricks", "clusterName: 0123456789abcdef", "clusterName:", "endpoints[lakehouse].clusterName")]
    [InlineData("databricks", "username: 00000000-0000-0000-0000-000000000000", "username:", "endpoints[lakehouse].username")]
    public void A_key_with_no_value_is_a_config_problem_naming_its_field(string engine, string from, string to, string label)
    {
        var yaml = Bases[engine].Replace(from, to);

        var problem = Assert.Single(Refused(yaml).Problems);

        Assert.StartsWith("line ", problem);
        Assert.EndsWith($": {label}: {NoValue}", problem);
    }

    /// <summary>🔴 THE ONE THAT WAS SILENT: tls: turned TLS off, and ClickHouse was then read over http.</summary>
    [Theory]
    [InlineData("tls:")]
    [InlineData("tls: ~")]
    [InlineData("tls: null")]
    [InlineData("tls: Null")]
    [InlineData("tls: NULL")]
    [InlineData("tls: !!null")]
    public void Every_way_of_writing_nothing_is_the_same_problem(string line)
    {
        var problem = Assert.Single(Refused(ClickHouse.Replace("tls: true", line)).Problems);

        Assert.StartsWith("line 10, ", problem);
        Assert.EndsWith($"endpoints[prod-cluster].tls: {NoValue}", problem);
    }

    [Fact]
    public void An_empty_string_written_as_one_is_a_value_and_so_is_the_word_null_in_quotes()
    {
        Assert.Equal("", Parse(ClickHouse).Endpoints[0].ClusterName);

        Assert.Equal("null", Parse(ClickHouse.Replace("clusterName: \"\"", "clusterName: \"null\"")).Endpoints[0].ClusterName);
    }

    [Fact]
    public void Every_key_with_no_value_is_listed_at_once()
    {
        var problems = Refused(ClickHouse.Replace("host: clickhouse.internal", "host:").Replace("tls: true", "tls:")).Problems;

        Assert.Equal(2, problems.Count);
        Assert.EndsWith($"endpoints[prod-cluster].host: {NoValue}", problems[0]);
        Assert.EndsWith($"endpoints[prod-cluster].tls: {NoValue}", problems[1]);
    }

    /// <summary>A list or a map may be left empty: `endpoints:` is how the template starts.</summary>
    [Fact]
    public void A_list_or_map_key_with_no_value_is_still_empty()
    {
        var endpoint = Parse(ClickHouse.Replace("excludedDatabases: [hr_private]", "excludedDatabases:") + "\n    options:").Endpoints[0];

        Assert.Empty(endpoint.ExcludedDatabases);
        Assert.Empty(endpoint.Options);
        Assert.Contains("endpoints: at least one endpoint is required",
            Refused("version: 1\ngateway: https://agent.trailox.io\nendpoints:\n  # - alias: prod-cluster\n").Problems);
    }

    [Theory]
    [InlineData("excludedDatabases: [hr_private, ~]")]
    [InlineData("excludedDatabases:\n      - hr_private\n      -")]
    public void A_list_item_with_no_value_is_a_config_problem(string lines)
    {
        var problem = Assert.Single(Refused(ClickHouse.Replace("excludedDatabases: [hr_private]", lines)).Problems);

        Assert.EndsWith($"endpoints[prod-cluster].excludedDatabases: {NoItem}", problem);
    }

    [Fact]
    public void A_key_the_agent_does_not_know_stays_ignored_even_with_no_value()
    {
        var config = Parse(ClickHouse + "\n    aSettingOfALaterVersion:\naTopLevelOneToo:");

        Assert.Equal("clickhouse.internal", config.Endpoints[0].Host);
    }

    /// <summary>${NAME:-} is how to ask for the default; it is not a key with no value.</summary>
    [Fact]
    public void An_empty_default_still_leaves_the_key_out()
    {
        Assert.True(Parse(ClickHouse.Replace("tls: true", "tls: ${CH_TLS:-}")).Endpoints[0].Tls);
    }

    // ------------------------------------------------------------ line breaks and other control characters

    /// <summary>The rule values from the environment follow since 1.5.0: a YAML `|` block adds a line break nobody meant.</summary>
    [Theory]
    [InlineData("host: clickhouse.internal", "host: \"clickhouse.internal\\n\"")]
    [InlineData("host: clickhouse.internal", "host: |\n      clickhouse.internal")]
    [InlineData("host: clickhouse.internal", "host: \"clickhouse.internal\\r\\n\"")]
    public void A_trailing_line_break_in_a_value_written_in_the_file_is_dropped(string from, string to)
    {
        Assert.Equal("clickhouse.internal", Parse(ClickHouse.Replace(from, to)).Endpoints[0].Host);
    }

    [Fact]
    public void Every_kind_of_value_loses_its_trailing_line_break()
    {
        var yaml = ClickHouse
            .Replace("alias: prod-cluster", "alias: \"prod-cluster\\n\"")
            .Replace("gateway: https://agent.trailox.io", "gateway: \"https://agent.trailox.io\\n\"")
            .Replace("excludedDatabases: [hr_private]", "excludedDatabases: [\"hr_private\\n\"]")
            .Replace("port: 8443", "port: \"9440\\n\"");

        var config = Parse(yaml);

        Assert.Equal("https://agent.trailox.io", config.Gateway);
        Assert.Equal("prod-cluster", config.Endpoints[0].Alias);
        Assert.Equal(new[] { "hr_private" }, config.Endpoints[0].ExcludedDatabases);
        Assert.Equal(9440, config.Endpoints[0].Port);
        Assert.Equal("dev", Parse(Redshift.Replace("database: dev", "database: \"dev\\n\"")).Endpoints[0].Options["database"]);
    }

    [Theory]
    [InlineData("host: clickhouse.internal", "host: \"clickhouse\\tinternal\"", "endpoints[prod-cluster].host")]
    [InlineData("host: clickhouse.internal", "host: \"clickhouse\\ninternal\"", "endpoints[prod-cluster].host")]
    [InlineData("username: trailox_monitor", "username: \"trailox\\u0000monitor\"", "endpoints[prod-cluster].username")]
    [InlineData("excludedDatabases: [hr_private]", "excludedDatabases: [\"hr\\nprivate\"]", "endpoints[prod-cluster].excludedDatabases")]
    [InlineData("alias: prod-cluster", "alias: \"prod\\tcluster\"", "endpoints[].alias")]
    public void Any_other_control_character_is_a_problem_that_does_not_print_the_value(string from, string to, string label)
    {
        var ex = Refused(ClickHouse.Replace(from, to));

        Assert.EndsWith($"{label}: {Control}", Assert.Single(ex.Problems));
        // Ordinal: a culture-aware search ignores \0 and "finds" it at position 0 of any string.
        Assert.DoesNotContain("\t", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("\u0000", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_key_the_agent_does_not_know_is_not_read_for_control_characters_either()
    {
        Assert.Equal("clickhouse.internal", Parse(ClickHouse + "\n    notes: \"one\\ttwo\"").Endpoints[0].Host);
    }

    // ------------------------------------------------------------ the checks themselves: \z, not $

    /// <summary>
    /// The loader drops a trailing line break before any check runs, so this asks the checks directly:
    /// each must refuse one on its own, whatever reaches it later.
    /// </summary>
    [Theory]
    [InlineData("host")]
    [InlineData("clusterName")]
    [InlineData("username")]
    public void The_clickhouse_checks_refuse_a_trailing_line_break(string field)
    {
        var e = new EndpointConfig { Alias = "prod-cluster", Engine = "clickhouse", Host = "clickhouse.internal", ClusterName = "", Username = "trailox_monitor" };
        switch (field)
        {
            case "host": e.Host += "\n"; break;
            case "clusterName": e.ClusterName = "default\n"; break;
            default: e.Username += "\n"; break;
        }

        Assert.StartsWith(field + ":", Assert.Single(new ClickHouseEngine().Validate(e)));
    }

    [Theory]
    [InlineData("clusterName")]
    [InlineData("options.database")]
    public void The_redshift_checks_refuse_a_trailing_line_break(string field)
    {
        var e = new EndpointConfig
        {
            Alias = "warehouse", Engine = "redshift", Host = "my-wg.123456789012.eu-west-1.redshift-serverless.amazonaws.com",
            ClusterName = "my-wg", Username = "trailox_agent",
        };
        if (field == "clusterName")
        {
            e.ClusterName += "\n";
        }
        else
        {
            e.Options["database"] = "dev\n";
        }

        Assert.StartsWith(field + ":", Assert.Single(new RedshiftEngine().Validate(e)));
    }

    [Fact]
    public void The_alias_and_database_name_checks_refuse_a_trailing_line_break()
    {
        var config = new AgentConfig
        {
            Version = 1,
            Endpoints =
            {
                new EndpointConfig
                {
                    Alias = "prod-cluster\n", Engine = "clickhouse", Host = "clickhouse.internal", PasswordEnv = "CH_PW",
                    ExcludedDatabases = { "hr_private\n" },
                },
            },
        };

        var problems = ConfigLoader.Validate(config, new FakeSecrets());

        Assert.Contains(problems, p => p.Contains(".alias: letters, digits"));
        Assert.Contains(problems, p => p.Contains(".excludedDatabases: 'hr_private\n' is not a plain database name"));
    }
}
