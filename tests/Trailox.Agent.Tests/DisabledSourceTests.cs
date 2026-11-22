using Microsoft.Extensions.Logging;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Protocol;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Tests;

/// <summary>
/// A source you turn off in Trailox is left alone by the agent too.
/// </summary>
/// <remarks>
/// Every check-in answer has carried whether each source is enabled, and until 1.3.3 nothing read it.
/// Sessions come from agent.yaml and are probed on a timer, so a source turned off in Trailox was
/// still probed - and on Databricks and Snowflake a probe runs statements, so a warehouse kept waking,
/// on the customer's bill, for a source they had switched off. A source whose probe was failing was
/// worse: a failed probe is retried on every check-in.
/// </remarks>
public class DisabledSourceTests
{
    private sealed class CountingSource : IEngineSource
    {
        private readonly bool _fails;
        public CountingSource(bool fails = false) => _fails = fails;

        public int Probes { get; private set; }

        public Task<EndpointCaps> ProbeAsync(CancellationToken ct)
        {
            Probes++;
            return _fails ? throw new SourceException("the warehouse would not start") : Task.FromResult(new EndpointCaps { Version = "1" });
        }

        public Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct) => Task.FromResult<RowStream?>(null);
    }

    private sealed class Lines : ILogger
    {
        public List<string> Written { get; } = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Written.Add(logLevel + ": " + formatter(state, exception));
    }

    private static EndpointSession Session(string alias, IEngineSource source) =>
        new(new EndpointConfig { Alias = alias, Engine = "databricks", Kind = "cloud" }, source, TimeSpan.FromMinutes(60));

    private static EndpointAssignment Assignment(string alias, string state, bool enabled, int? id = 7) =>
        new() { Alias = alias, State = state, Enabled = enabled, EndpointId = id };

    [Fact]
    public void Until_the_server_has_answered_a_source_is_probed_which_is_how_it_registers()
    {
        var session = Session("lakehouse", new CountingSource());

        Assert.Null(session.EnabledOnServer);
        Assert.True(session.NeedsProbe);
    }

    [Fact]
    public async Task A_source_turned_off_in_Trailox_is_not_probed_even_though_its_last_probe_failed()
    {
        var source = new CountingSource(fails: true);
        var session = Session("lakehouse", source);
        await session.ProbeAsync(new Lines(), CancellationToken.None);
        Assert.NotNull(session.ProbeError);
        Assert.True(session.NeedsProbe);          // a failed probe is retried on every check-in ...

        session.HeardFromServer(enabled: false, new Lines());

        Assert.False(session.NeedsProbe);         // ... unless the customer has turned the source off
        Assert.Equal(1, source.Probes);
    }

    [Fact]
    public async Task A_source_turned_on_again_is_probed_again()
    {
        var session = Session("lakehouse", new CountingSource(fails: true));
        await session.ProbeAsync(new Lines(), CancellationToken.None);
        session.HeardFromServer(enabled: false, new Lines());

        session.HeardFromServer(enabled: true, new Lines());

        Assert.True(session.NeedsProbe);
    }

    [Fact]
    public void Turning_a_source_off_is_said_once_not_on_every_check_in()
    {
        var log = new Lines();
        var session = Session("lakehouse", new CountingSource());

        session.HeardFromServer(enabled: false, log);
        session.HeardFromServer(enabled: false, log);
        session.HeardFromServer(enabled: false, log);

        var line = Assert.Single(log.Written);
        Assert.Contains("lakehouse", line);
        Assert.Contains("turned off in Trailox", line);
    }

    /// <summary>
    /// Only an answer about a REGISTERED source says whether it is enabled. "conflict" and "invalid"
    /// carry the field at its default, false, which would switch off a source nobody switched off -
    /// and a source that is not probed can never register.
    /// </summary>
    [Theory]
    [InlineData("ok", true, true)]
    [InlineData("disabled", false, false)]
    [InlineData("unreachable", false, false)]   // turned off AND its last probe failed: the state says only the second
    [InlineData("unreachable", true, true)]
    [InlineData("conflict", false, null)]
    [InlineData("invalid", false, null)]
    [InlineData("a-state-this-agent-has-never-heard-of", false, null)]
    public void Only_an_answer_about_a_registered_source_says_whether_it_is_enabled(string state, bool enabledOnTheWire, bool? expected)
    {
        Assert.Equal(expected, AgentLoop.EnabledAccordingTo(Assignment("lakehouse", state, enabledOnTheWire)));
    }

    [Fact]
    public void A_check_in_answer_reaches_the_source_it_names_and_no_other()
    {
        var off = Session("lakehouse", new CountingSource());
        var conflicted = Session("warehouse", new CountingSource());
        var untouched = Session("prod-cluster", new CountingSource());
        var response = new CheckinResponse
        {
            Endpoints =
            {
                Assignment("lakehouse", "disabled", enabled: false),
                Assignment("warehouse", "conflict", enabled: false),
            },
        };

        AgentLoop.ApplyAssignments(new[] { off, conflicted, untouched }, response, new Lines());

        Assert.False(off.EnabledOnServer);
        Assert.Null(conflicted.EnabledOnServer);     // a conflict is not the customer turning it off
        Assert.Null(untouched.EnabledOnServer);
        Assert.True(conflicted.NeedsProbe);
    }
}
