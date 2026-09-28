using System.Net;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Gateway;
using Trailox.Agent.Protocol;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Tests;

/// <summary>
/// Every failure of a task ends as that task's reported failure, and the agent moves on to the next
/// task - except the agent stopping, which is not a failure of anything.
/// </summary>
/// <remarks>
/// Until 1.6.2 only database and network errors were handled here. Anything else - an answer that was
/// not JSON, a request that ran out of time - escaped the task runner: the check-in's remaining tasks
/// were abandoned for every endpoint, the log blamed the check-in, and the task was never answered, so
/// it came back, and failed the same way, at every check-in.
/// </remarks>
public class TaskFailureTests
{
    /// <summary>Plays the gateway: records what the agent sends, accepts uploads, answers failure reports.</summary>
    private sealed class FakeGateway : HttpMessageHandler
    {
        public Func<HttpResponseMessage> Failed { get; set; } = () => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{}") };

        public List<(string Path, string Body)> Seen { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var path = request.RequestUri!.AbsolutePath;
            if (path.EndsWith("/failed", StringComparison.Ordinal))
            {
                Seen.Add((path, await request.Content!.ReadAsStringAsync(cancellationToken)));
                return Failed();
            }
            Seen.Add((path, "(rows)"));
            await request.Content!.CopyToAsync(Stream.Null, cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("""{"rowsAccepted":1}""", Encoding.UTF8, "application/json") };
        }
    }

    /// <summary>A source whose tasks fail however a test says, or yield one row.</summary>
    private sealed class ScriptedSource : IEngineSource
    {
        private readonly Func<AgentTask, CancellationToken, RowStream?> _open;
        public ScriptedSource(Func<AgentTask, CancellationToken, RowStream?> open) => _open = open;

        public Task<EndpointCaps> ProbeAsync(CancellationToken ct) => Task.FromResult(new EndpointCaps { Version = "1" });

        public Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct) => Task.FromResult(_open(task, ct));
    }

    private static RowStream OneRow() => new(new MemoryStream(Encoding.UTF8.GetBytes("{\"n\":1}\n")), new MemoryStream());

    private static (TaskExecutor Executor, FakeGateway Gateway, ErrorRing Errors) Runner()
    {
        var gateway = new FakeGateway();
        var errors = new ErrorRing();
        var client = new GatewayClient(new HttpClient(gateway), new Uri("https://gateway.example.test/"), "tlx_not-a-real-key", "0.0.0-test");
        return (new TaskExecutor(client, errors, NullLogger.Instance), gateway, errors);
    }

    private static EndpointSession Session(IEngineSource source) =>
        new(new EndpointConfig { Alias = "lakehouse", Engine = "databricks", Kind = "cloud" }, source, TimeSpan.FromMinutes(60));

    private static AgentTask Window(string chunk) =>
        new() { ChunkId = chunk, EndpointId = 7, Stream = ProtocolInfo.Streams.Events, StartMicros = 1_000_000, EndMicros = 2_000_000 };

    [Fact]
    public async Task An_unexpected_failure_is_reported_as_the_tasks_own_and_the_next_task_still_runs()
    {
        var (executor, gateway, errors) = Runner();
        var session = Session(new ScriptedSource((task, _) => task.ChunkId == "chunk-a"
            ? throw new InvalidOperationException("the answer came in a form nobody expected")
            : OneRow()));

        Assert.True(await executor.ExecuteAsync(Window("chunk-a"), session, CancellationToken.None));
        Assert.True(await executor.ExecuteAsync(Window("chunk-b"), session, CancellationToken.None));

        var failed = Assert.Single(gateway.Seen, s => s.Path.EndsWith("/chunk-a/failed", StringComparison.Ordinal));
        Assert.Contains("failed unexpectedly (InvalidOperationException: the answer came in a form nobody expected)", failed.Body);
        Assert.Contains(gateway.Seen, s => s.Path.EndsWith("/chunks/chunk-b", StringComparison.Ordinal));
        Assert.DoesNotContain(gateway.Seen, s => s.Path.EndsWith("/chunk-b/failed", StringComparison.Ordinal));
        var recorded = Assert.Single(errors.Snapshot());
        Assert.Equal("lakehouse", recorded.Alias);
        Assert.Contains("failed unexpectedly", recorded.Message);
    }

    [Fact]
    public async Task A_request_that_ran_out_of_time_is_reported_as_not_finishing_in_time()
    {
        var (executor, gateway, _) = Runner();
        var session = Session(new ScriptedSource((_, _) =>
            throw new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout of 1800 seconds elapsing.")));

        Assert.True(await executor.ExecuteAsync(Window("chunk-a"), session, CancellationToken.None));

        var failed = Assert.Single(gateway.Seen, s => s.Path.EndsWith("/chunk-a/failed", StringComparison.Ordinal));
        Assert.Contains("did not finish in time", failed.Body);
        Assert.Contains("HttpClient.Timeout of 1800 seconds", failed.Body);
    }

    /// <summary>The agent stopping cancels its own token: not the task's failure, and nothing is reported.</summary>
    [Fact]
    public async Task The_agent_stopping_is_not_a_failure_of_the_task()
    {
        var (executor, gateway, errors) = Runner();
        using var stopping = new CancellationTokenSource();
        var session = Session(new ScriptedSource((_, ct) =>
        {
            stopping.Cancel();
            ct.ThrowIfCancellationRequested();
            return OneRow();
        }));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => executor.ExecuteAsync(Window("chunk-a"), session, stopping.Token));

        Assert.Empty(gateway.Seen);
        Assert.Empty(errors.Snapshot());
    }

    /// <summary>
    /// Reporting the failure can fail too - the gateway slow or out of reach. That must not stop the
    /// check-in either: the task stays unanswered and comes back later.
    /// </summary>
    [Theory]
    [InlineData("timeout")]
    [InlineData("unreachable")]
    public async Task A_failure_report_the_gateway_cannot_take_does_not_stop_the_check_in(string how)
    {
        var (executor, gateway, _) = Runner();
        gateway.Failed = () => throw (how == "timeout"
            ? new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout of 30 seconds elapsing.")
            : (Exception)new HttpRequestException("Connection refused (gateway.example.test:443)"));
        var session = Session(new ScriptedSource((task, _) => task.ChunkId == "chunk-a"
            ? throw new SourceException("the warehouse would not start")
            : OneRow()));

        Assert.True(await executor.ExecuteAsync(Window("chunk-a"), session, CancellationToken.None));
        Assert.True(await executor.ExecuteAsync(Window("chunk-b"), session, CancellationToken.None));

        Assert.Contains(gateway.Seen, s => s.Path.EndsWith("/chunk-a/failed", StringComparison.Ordinal));
        Assert.Contains(gateway.Seen, s => s.Path.EndsWith("/chunks/chunk-b", StringComparison.Ordinal));
    }

    /// <summary>A rejected key still stops the agent, whichever failure it was rejected while reporting.</summary>
    [Theory]
    [InlineData("source")]
    [InlineData("unexpected")]
    public async Task A_rejected_key_while_reporting_still_stops_the_agent(string failure)
    {
        var (executor, gateway, _) = Runner();
        gateway.Failed = () => new HttpResponseMessage(HttpStatusCode.Unauthorized)
        {
            Content = new StringContent("""{"error":"unauthorized","message":"the agent key was revoked"}""", Encoding.UTF8, "application/json"),
        };
        var session = Session(new ScriptedSource((_, _) => throw (failure == "source"
            ? new SourceException("the warehouse would not start")
            : (Exception)new InvalidOperationException("the answer came in a form nobody expected"))));

        var ex = await Assert.ThrowsAsync<GatewayException>(() => executor.ExecuteAsync(Window("chunk-a"), session, CancellationToken.None));

        Assert.True(ex.IsUnauthorized);
    }
}
