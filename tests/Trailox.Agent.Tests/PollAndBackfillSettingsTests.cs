using System.Text.Json;
using Microsoft.Extensions.Logging;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Protocol;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Tests;

/// <summary>
/// pollMinutes and backfillDays in agent.yaml (1.4.0): how often Trailox collects a source, and how far
/// back its first collection reads.
/// </summary>
/// <remarks>
/// They are used when Trailox first registers the source. After that the source's own settings in
/// Trailox decide, and Trailox shows a warning when agent.yaml says something else - so the agent's
/// part is to read them, check them, and report them. Agents before 1.4.0 drop both keys unread.
/// </remarks>
public class PollAndBackfillSettingsTests
{
    private const string Head = """
        version: 1
        gateway: https://agent.trailox.io
        endpoints:
          - alias: prod-cluster
            engine: clickhouse
            host: clickhouse.internal
            username: trailox_monitor
            password_env: CH_PW
        """;

    private sealed class FakeSecrets : ISecretReader
    {
        public string? FromEnvironment(string variable) => variable == "CH_PW" ? "s3cret" : null;
        public string FromFile(string path) => throw new FileNotFoundException(path);
    }

    private static AgentConfig Parse(string extra) =>
        ConfigLoader.Parse(Head + (extra.Length == 0 ? "" : "\n    " + extra.Replace("\n", "\n    ")), new FakeSecrets()).Config;

    private static ConfigException Refused(string extra) =>
        Assert.Throws<ConfigException>(() => Parse(extra));

    // ------------------------------------------------------------ reading and checking

    [Fact]
    public void Both_are_read_when_present()
    {
        var endpoint = Parse("pollMinutes: 5\nbackfillDays: 90").Endpoints.Single();

        Assert.Equal(5, endpoint.PollMinutes);
        Assert.Equal(90, endpoint.BackfillDays);
    }

    [Fact]
    public void Both_are_optional_and_absent_means_not_set()
    {
        var endpoint = Parse("").Endpoints.Single();

        Assert.Null(endpoint.PollMinutes);
        Assert.Null(endpoint.BackfillDays);
    }

    /// <summary>The same ranges Trailox accepts for a source it connects to directly.</summary>
    [Theory]
    [InlineData("pollMinutes: 1")]
    [InlineData("pollMinutes: 1440")]
    [InlineData("backfillDays: 0")]
    [InlineData("backfillDays: 365")]
    public void The_edges_of_the_ranges_are_accepted(string line)
    {
        Parse(line);
    }

    [Theory]
    [InlineData("pollMinutes: 0", "endpoints[prod-cluster].pollMinutes: 0 must be between 1 and 1440 minutes")]
    [InlineData("pollMinutes: 1441", "endpoints[prod-cluster].pollMinutes: 1441 must be between 1 and 1440 minutes")]
    [InlineData("backfillDays: -1", "endpoints[prod-cluster].backfillDays: -1 must be between 0 and 365 days")]
    [InlineData("backfillDays: 366", "endpoints[prod-cluster].backfillDays: 366 must be between 0 and 365 days")]
    public void A_value_outside_the_range_is_a_config_problem_naming_the_entry_and_the_field(string line, string problem)
    {
        Assert.Contains(problem, Refused(line).Problems);
    }

    /// <summary>A word where a number goes is a YAML error with its line, like any other - never a crash.</summary>
    [Fact]
    public void A_value_that_is_not_a_whole_number_is_a_config_problem_too()
    {
        Assert.NotEmpty(Refused("pollMinutes: ten").Problems);
    }

    // ------------------------------------------------------------ reporting

    private sealed class NoSource : IEngineSource
    {
        public Task<EndpointCaps> ProbeAsync(CancellationToken ct) => Task.FromResult(new EndpointCaps());
        public Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct) => Task.FromResult<RowStream?>(null);
    }

    private static EndpointReport ReportOf(EndpointConfig config) =>
        new EndpointSession(config, new NoSource(), TimeSpan.FromMinutes(60)).Report();

    [Fact]
    public void The_report_carries_what_agent_yaml_says()
    {
        var report = ReportOf(new EndpointConfig { Alias = "prod-cluster", Engine = "clickhouse", PollMinutes = 5, BackfillDays = 90 });

        Assert.Equal(5, report.PollMinutes);
        Assert.Equal(90, report.BackfillDays);
    }

    /// <summary>Not set means not sent: a gateway that predates the fields sees exactly the report it knows.</summary>
    [Fact]
    public void A_setting_agent_yaml_leaves_out_is_left_out_of_the_report_on_the_wire()
    {
        var json = JsonSerializer.Serialize(ReportOf(new EndpointConfig { Alias = "prod-cluster", Engine = "clickhouse" }), ProtocolInfo.Json);

        Assert.DoesNotContain("pollMinutes", json);
        Assert.DoesNotContain("backfillDays", json);
    }

    [Fact]
    public void A_setting_agent_yaml_sets_goes_on_the_wire_by_its_agent_yaml_name()
    {
        var json = JsonSerializer.Serialize(ReportOf(new EndpointConfig { Alias = "prod-cluster", Engine = "clickhouse", PollMinutes = 5 }), ProtocolInfo.Json);

        Assert.Contains("\"pollMinutes\":5", json);
    }

    // ------------------------------------------------------------ what Trailox says about a healthy source

    private sealed class Lines : ILogger
    {
        public List<string> Written { get; } = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Written.Add(logLevel + ": " + formatter(state, exception));
    }

    private static CheckinResponse Answer(params EndpointAssignment[] assignments) => new() { Endpoints = assignments.ToList() };

    /// <summary>
    /// 🔴 A NOTE ABOUT A HEALTHY SOURCE MATTERS AND WAS NEVER SHOWN. Trailox may send a message with an
    ///    "ok" answer - today, that a Snowflake source polling faster than its setup script planned for
    ///    needs a larger daily credit cap, or Snowflake suspends the warehouse. Before 1.4.0 the agent
    ///    logged messages only for a source that was not ok.
    /// ⚠ ONCE, not every 45 seconds: the answer repeats it at every check-in.
    /// </summary>
    [Fact]
    public void A_note_about_a_healthy_source_is_logged_once()
    {
        var lines = new Lines();
        var seen = new HashSet<string>();
        var answer = Answer(new EndpointAssignment { Alias = "sf", State = "ok", Enabled = true, EndpointId = 7, Message = "needs a larger cap" });

        AgentLoop.LogAssignments(answer, lines, seen);
        AgentLoop.LogAssignments(answer, lines, seen);

        var line = Assert.Single(lines.Written);
        Assert.Equal("Warning: endpoint sf: needs a larger cap", line);
    }

    [Fact]
    public void A_different_note_is_logged_again()
    {
        var lines = new Lines();
        var seen = new HashSet<string>();

        AgentLoop.LogAssignments(Answer(new EndpointAssignment { Alias = "sf", State = "ok", Enabled = true, Message = "needs 36" }), lines, seen);
        AgentLoop.LogAssignments(Answer(new EndpointAssignment { Alias = "sf", State = "ok", Enabled = true, Message = "needs 72" }), lines, seen);

        Assert.Equal(2, lines.Written.Count);
    }

    [Fact]
    public void A_healthy_source_without_a_note_logs_nothing()
    {
        var lines = new Lines();

        AgentLoop.LogAssignments(Answer(new EndpointAssignment { Alias = "sf", State = "ok", Enabled = true }), lines, new HashSet<string>());

        Assert.Empty(lines.Written);
    }

    /// <summary>A source that is not ok is still reported at every check-in, as before.</summary>
    [Fact]
    public void A_source_that_is_not_ok_is_still_reported_every_time()
    {
        var lines = new Lines();
        var seen = new HashSet<string>();
        var answer = Answer(new EndpointAssignment { Alias = "old", State = "conflict", Message = "alias is used" });

        AgentLoop.LogAssignments(answer, lines, seen);
        AgentLoop.LogAssignments(answer, lines, seen);

        Assert.Equal(2, lines.Written.Count);
        Assert.All(lines.Written, l => Assert.Equal("Warning: endpoint old: conflict - alias is used", l));
    }
}
