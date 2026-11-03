using System.IO.Compression;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Gateway;
using Trailox.Agent.Protocol;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Tests;

/// <summary>A task with <c>partBytes</c>: what reaches the gateway, in what order, and what a stopped read leaves unsaid.</summary>
public class TaskPartsTests
{
    private sealed class FakeGateway : HttpMessageHandler
    {
        /// <summary>The answer to the upload of this part, when it is not an acceptance.</summary>
        public (int Part, HttpStatusCode Status)? Refuse { get; init; }

        /// <summary>Runs when a part has landed, before its reply.</summary>
        public Action? AfterPart { get; init; }

        public List<(string Path, string Body)> Seen { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var path = request.RequestUri!.PathAndQuery;
            if (path.EndsWith("/complete", StringComparison.Ordinal) || path.EndsWith("/failed", StringComparison.Ordinal))
            {
                Seen.Add((path, await request.Content!.ReadAsStringAsync(cancellationToken)));
                return Reply("""{"rowsAccepted":3,"watermarkMicros":42}""");
            }
            using var unzipped = new MemoryStream();
            var compressed = await request.Content!.ReadAsByteArrayAsync(cancellationToken);
            await using (var gzip = new GZipStream(new MemoryStream(compressed), CompressionMode.Decompress))
            {
                await gzip.CopyToAsync(unzipped, cancellationToken);
            }
            Seen.Add((path, Encoding.UTF8.GetString(unzipped.ToArray())));
            if (Refuse is { } refuse && path.EndsWith($"part={refuse.Part}", StringComparison.Ordinal))
            {
                return new HttpResponseMessage(refuse.Status) { Content = new StringContent("""{"error":"rejected","message":"no"}""") };
            }
            AfterPart?.Invoke();
            return Reply("""{"rowsAccepted":1}""");
        }

        private static HttpResponseMessage Reply(string json) => new(HttpStatusCode.OK) { Content = new StringContent(json, Encoding.UTF8, "application/json") };
    }

    private sealed class ScriptedSource : IEngineSource
    {
        private readonly Func<CancellationToken, Task<RowStream?>> _open;

        public ScriptedSource(Func<CancellationToken, Task<RowStream?>> open) => _open = open;

        public Task<EndpointCaps> ProbeAsync(CancellationToken ct) => Task.FromResult(new EndpointCaps { Version = "1" });

        public Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct) => _open(ct);
    }

    private const string Rows = "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n";

    private static RowStream Stream(string body) => new(new MemoryStream(Encoding.UTF8.GetBytes(body)), new MemoryStream());

    private static AgentTask Read(long? partBytes, int? partSeconds = null) => new()
    {
        ChunkId = "c1",
        EndpointId = 1,
        Stream = "ora_audit",
        StartMicros = 1_000_000,
        EndMicros = 2_000_000,
        PartBytes = partBytes,
        PartSeconds = partSeconds,
    };

    private static async Task<(FakeGateway Gateway, ErrorRing Errors)> RunAsync(AgentTask task, Func<CancellationToken, Task<RowStream?>> open, FakeGateway? gateway = null)
    {
        gateway ??= new FakeGateway();
        var errors = new ErrorRing();
        var client = new GatewayClient(new HttpClient(gateway), new Uri("https://gateway.example.test/"), "tlx_not-a-real-key", "0.0.0-test");
        var session = new EndpointSession(new EndpointConfig { Alias = "ledger", Engine = "oracle" }, new ScriptedSource(open), TimeSpan.FromMinutes(60));
        Assert.True(await new TaskExecutor(client, errors, NullLogger.Instance).ExecuteAsync(task, session, CancellationToken.None));
        return (gateway, errors);
    }

    [Fact]
    public async Task A_read_goes_in_numbered_parts_to_one_chunk_and_complete_ends_it()
    {
        var (gateway, _) = await RunAsync(Read(partBytes: 16), _ => Task.FromResult<RowStream?>(Stream(Rows)));

        Assert.Equal(new[] { "/v1/agent/chunks/c1?part=1", "/v1/agent/chunks/c1?part=2", "/v1/agent/chunks/c1/complete" }, gateway.Seen.Select(s => s.Path));
        Assert.Equal(Rows, gateway.Seen[0].Body + gateway.Seen[1].Body);
        Assert.Equal(2, JsonDocument.Parse(gateway.Seen[2].Body).RootElement.GetProperty("parts").GetInt32());
    }

    /// <summary>The read's own time goes out with the end of the read, and only an engine that measured it sends one.</summary>
    [Fact]
    public async Task Complete_carries_the_reads_own_time_and_an_engine_that_does_not_measure_leaves_it_out()
    {
        var task = Read(partBytes: 1000);
        task.ReadTime.Add(TimeSpan.FromSeconds(41.25));
        var (gateway, _) = await RunAsync(task, _ => Task.FromResult<RowStream?>(Stream(Rows)));
        Assert.Equal(41250, JsonDocument.Parse(gateway.Seen[^1].Body).RootElement.GetProperty("readMillis").GetInt64());

        (gateway, _) = await RunAsync(Read(partBytes: 1000), _ => Task.FromResult<RowStream?>(Stream(Rows)));
        Assert.False(JsonDocument.Parse(gateway.Seen[^1].Body).RootElement.TryGetProperty("readMillis", out JsonElement _));
    }

    [Fact]
    public async Task A_read_with_no_rows_sends_no_part_and_complete_says_so()
    {
        var (gateway, _) = await RunAsync(Read(partBytes: 1000), _ => Task.FromResult<RowStream?>(Stream("")));

        var complete = Assert.Single(gateway.Seen);
        Assert.Equal("/v1/agent/chunks/c1/complete", complete.Path);
        Assert.Equal(0, JsonDocument.Parse(complete.Body).RootElement.GetProperty("parts").GetInt32());
    }

    /// <summary>A quiet read stopped at its limit never opened an upload, so its failure is the only place its time can go.</summary>
    [Fact]
    public async Task A_read_stopped_before_any_row_reports_its_time()
    {
        var task = Read(partBytes: 1000);

        var (gateway, _) = await RunAsync(task, _ =>
        {
            task.ReadTime.Add(TimeSpan.FromSeconds(60));
            throw new SourceException("stopped") { TooLargeAtSource = true };
        });

        var failed = Assert.Single(gateway.Seen);
        Assert.EndsWith("/failed", failed.Path);
        Assert.Equal(60000, JsonDocument.Parse(failed.Body).RootElement.GetProperty("readMillis").GetInt64());
    }

    /// <summary>Nothing splits unless the task asks for it.</summary>
    [Theory]
    [InlineData(null)]
    [InlineData(0L)]
    [InlineData(-1L)]
    public async Task A_task_that_does_not_ask_for_parts_is_one_upload(long? partBytes)
    {
        var (gateway, _) = await RunAsync(Read(partBytes), _ => Task.FromResult<RowStream?>(Stream(Rows)));

        Assert.Equal(("/v1/agent/chunks/c1", Rows), Assert.Single(gateway.Seen));
    }

    /// <summary>The window counts as read only on <c>/complete</c>, so a refused part must never be followed by one.</summary>
    [Fact]
    public async Task A_part_the_gateway_refuses_ends_the_read_with_no_complete()
    {
        var gateway = new FakeGateway { Refuse = (2, HttpStatusCode.UnprocessableEntity) };

        var (_, errors) = await RunAsync(Read(partBytes: 8), _ => Task.FromResult<RowStream?>(Stream(Rows)), gateway);

        Assert.Equal(new[] { "/v1/agent/chunks/c1?part=1", "/v1/agent/chunks/c1?part=2" }, gateway.Seen.Select(s => s.Path));
        Assert.NotEmpty(errors.Snapshot());
    }

    /// <summary>A bound that stops the read after some parts landed keeps its reason, and nothing says the window was read.</summary>
    [Fact]
    public async Task A_read_stopped_after_a_part_landed_reports_its_reason_and_no_complete()
    {
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes(Rows.Split('\n')[0] + '\n'));
        var gateway = new FakeGateway { AfterPart = () => pipe.Writer.Complete(new SourceException("stopped") { TooLargeAtSource = true }) };

        await RunAsync(Read(partBytes: 8), _ => Task.FromResult<RowStream?>(new RowStream(pipe.Reader.AsStream(), new MemoryStream())), gateway);

        Assert.Equal(new[] { "/v1/agent/chunks/c1?part=1", "/v1/agent/chunks/c1/failed" }, gateway.Seen.Select(s => s.Path));
        Assert.Equal(ChunkFailed.TooLargeAtSource, JsonDocument.Parse(gateway.Seen[1].Body).RootElement.GetProperty("reason").GetString());
    }

    /// <summary>The same stop in the middle of a part reaches the runner through the HTTP client.</summary>
    [Fact]
    public async Task A_read_stopped_inside_a_part_keeps_its_reason_too()
    {
        var pipe = new Pipe();
        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes(Rows.Split('\n')[0] + '\n'));
        pipe.Writer.Complete(new SourceException("stopped") { TooLargeAtSource = true });

        var (gateway, _) = await RunAsync(Read(partBytes: 1000), _ => Task.FromResult<RowStream?>(new RowStream(pipe.Reader.AsStream(), new MemoryStream())));

        var failed = Assert.Single(gateway.Seen, s => s.Path.EndsWith("/failed", StringComparison.Ordinal));
        Assert.Equal(ChunkFailed.TooLargeAtSource, JsonDocument.Parse(failed.Body).RootElement.GetProperty("reason").GetString());
        Assert.DoesNotContain(gateway.Seen, s => s.Path.EndsWith("/complete", StringComparison.Ordinal));
    }

    /// <summary>What a gateway sends, under the names the protocol gives them; a field an older gateway leaves out stays null.</summary>
    [Fact]
    public void The_task_fields_arrive_by_name()
    {
        var task = JsonSerializer.Deserialize<AgentTask>("""{"chunkId":"c1","readLimitSeconds":300,"partBytes":1073741824,"partSeconds":900}""", ProtocolInfo.Json)!;
        Assert.Equal((300, 1_073_741_824L, 900), (task.ReadLimitSeconds, task.PartBytes, task.PartSeconds));

        var older = JsonSerializer.Deserialize<AgentTask>("""{"chunkId":"c1"}""", ProtocolInfo.Json)!;
        Assert.Equal(((int?)null, (long?)null, (int?)null), (older.ReadLimitSeconds, older.PartBytes, older.PartSeconds));
    }

    /// <summary>What the engine measures is the agent's own: it goes out on the reports, never as part of the task.</summary>
    [Fact]
    public void The_read_time_is_no_field_of_the_task()
    {
        var task = new AgentTask();
        task.ReadTime.Add(TimeSpan.FromSeconds(1));

        Assert.DoesNotContain("readTime", JsonSerializer.Serialize(task, ProtocolInfo.Json));
    }

    [Theory]
    [InlineData(0L, null)]
    [InlineData(1L, 1L)]
    [InlineData(TimeSpan.TicksPerMillisecond * 7, 7L)]
    public void The_time_is_whole_milliseconds_and_never_reads_as_none_once_counted(long ticks, long? millis)
    {
        var time = new ReadTime();
        time.Add(TimeSpan.FromTicks(ticks));

        Assert.Equal(millis, time.Millis);
    }
}
