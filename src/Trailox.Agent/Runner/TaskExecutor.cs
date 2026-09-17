using Microsoft.Extensions.Logging;
using Trailox.Agent.Engines;
using Trailox.Agent.Gateway;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Runner;

/// <summary>
/// Executes one task the gateway issued: open the rows for its window on the engine, stream
/// them to the gateway, report the outcome. One task at a time; nothing is retried here - the
/// gateway re-plans any window that did not land.
/// </summary>
public sealed class TaskExecutor
{
    private readonly GatewayClient _gateway;
    private readonly ErrorRing _errors;
    private readonly ILogger _logger;

    public TaskExecutor(GatewayClient gateway, ErrorRing errors, ILogger logger)
    {
        _gateway = gateway;
        _errors = errors;
        _logger = logger;
    }

    /// <summary>Returns false only when the agent must stop (the key was rejected).</summary>
    public async Task<bool> ExecuteAsync(AgentTask task, EndpointSession session, CancellationToken ct)
    {
        var alias = session.Config.Alias;
        try
        {
            await using var rows = await session.Source.OpenAsync(task, ct);
            if (rows == null)
            {
                _logger.LogWarning("endpoint {Alias}: task {Chunk} asks for stream '{Stream}' this engine does not produce; reporting it", alias, task.ChunkId, task.Stream);
                await ReportFailureAsync(task, alias, $"stream '{task.Stream}' is not produced by the {session.Config.Engine} engine of agent {Program.Version}", ct);
                return true;
            }

            var accepted = await _gateway.UploadChunkAsync(task.ChunkId, rows.Body, ct);
            _logger.LogInformation("endpoint {Alias}: {Stream} {Window} -> {Rows} rows accepted", alias, task.Stream, Describe(task), accepted.RowsAccepted);
            return true;
        }
        catch (Exception ex) when (ex is SourceException or HttpRequestException)
        {
            // The database refused the statement or was unreachable mid-task. Report it; the
            // probe will say more at the next check-in.
            _errors.Add(alias, $"{task.Stream} {Describe(task)}: {ex.Message}");
            _logger.LogWarning("endpoint {Alias}: {Stream} {Window} failed on the database: {Message}", alias, task.Stream, Describe(task), EndpointSession.Trim(ex.Message));
            await ReportFailureAsync(task, alias, ex.Message, ct);
            return true;
        }
        catch (GatewayException ex) when (ex.IsUnauthorized)
        {
            return false;
        }
        catch (GatewayException ex) when (ex.StatusCode is 404 or 409)
        {
            // Stale task (re-planned) or already accepted: nothing to do, the next check-in tells us more.
            _logger.LogInformation("endpoint {Alias}: chunk {Chunk} {Code}; moving on", alias, task.ChunkId, ex.Code);
            return true;
        }
        catch (GatewayException ex)
        {
            // 413 / 422 / 429 / 5xx: the gateway closed the chunk with its error and re-plans. Nothing to buffer.
            _errors.Add(alias, $"{task.Stream} {Describe(task)}: upload {ex.Message}");
            _logger.LogWarning("endpoint {Alias}: upload of {Stream} {Window} rejected: {Message}", alias, task.Stream, Describe(task), ex.Message);
            if (ex.RetryAfter is { } wait)
            {
                await Task.Delay(wait, ct);
            }
            return true;
        }
    }

    private async Task ReportFailureAsync(AgentTask task, string alias, string message, CancellationToken ct)
    {
        try
        {
            await _gateway.FailChunkAsync(task.ChunkId, EndpointSession.Trim(message), ct);
        }
        catch (GatewayException ex) when (!ex.IsUnauthorized)
        {
            _logger.LogInformation("endpoint {Alias}: could not report the failure of chunk {Chunk} ({Message}); it will be re-planned", alias, task.ChunkId, ex.Message);
        }
    }

    private static string Describe(AgentTask task) =>
        task.EndMicros == 0
            ? "(catalog)"
            : $"{Micros(task.StartMicros):yyyy-MM-dd HH:mm}Z..{Micros(task.EndMicros):HH:mm}Z";

    private static DateTime Micros(long micros) => DateTimeOffset.FromUnixTimeMilliseconds(micros / 1000).UtcDateTime;
}
