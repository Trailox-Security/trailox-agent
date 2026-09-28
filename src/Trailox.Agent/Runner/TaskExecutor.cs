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
    /// <remarks>
    /// 🔴 EVERY FAILURE OF A TASK ENDS HERE, AS THAT TASK'S FAILURE - except the agent stopping.
    ///    A failure that escaped this method abandoned the rest of the check-in's tasks, for every
    ///    endpoint, was logged as a failed check-in, and left the task unanswered, so it was handed
    ///    back - and failed the same way - at every check-in.
    /// </remarks>
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
        catch (Exception ex) when (!IsShutdown(ex, ct))
        {
            // Anything the engines do not name themselves: an answer in a form nobody expected, a
            // request that ran out of time, a fault of the agent's own. Reported like a database
            // failure, with the error's type so it can be told apart.
            var timedOut = ex is OperationCanceledException;
            var reason = timedOut
                ? "did not finish in time (" + ex.Message + ")"
                : $"failed unexpectedly ({ex.GetType().Name}: {ex.Message})";
            _errors.Add(alias, $"{task.Stream} {Describe(task)}: {reason}");
            if (timedOut)
            {
                _logger.LogWarning("endpoint {Alias}: {Stream} {Window} {Reason}", alias, task.Stream, Describe(task), EndpointSession.Trim(reason));
            }
            else
            {
                _logger.LogWarning(ex, "endpoint {Alias}: {Stream} {Window} {Reason}", alias, task.Stream, Describe(task), EndpointSession.Trim(reason));
            }
            await ReportFailureAsync(task, alias, reason, ct);
            return true;
        }
    }

    /// <summary>
    /// The agent stopping: a cancellation of its own token. Any other cancellation is a request that
    /// ran out of time, which is a failure of the task.
    /// </summary>
    internal static bool IsShutdown(Exception ex, CancellationToken ct) => ex is OperationCanceledException && ct.IsCancellationRequested;

    private async Task ReportFailureAsync(AgentTask task, string alias, string message, CancellationToken ct)
    {
        try
        {
            await _gateway.FailChunkAsync(task.ChunkId, EndpointSession.Trim(message), ct);
        }
        catch (Exception ex) when (!IsShutdown(ex, ct) && ex is not GatewayException { IsUnauthorized: true })
        {
            // Refused, not answered in time, or the gateway unreachable: the task is not lost.
            // Unanswered, it is handed back at a later check-in. A rejected key still stops the agent.
            _logger.LogInformation("endpoint {Alias}: could not report the failure of chunk {Chunk} ({Message}); it will be handed back at a later check-in", alias, task.ChunkId, EndpointSession.Trim(ex.Message));
        }
    }

    private static string Describe(AgentTask task) =>
        task.EndMicros == 0
            ? "(catalog)"
            : $"{Micros(task.StartMicros):yyyy-MM-dd HH:mm}Z..{Micros(task.EndMicros):HH:mm}Z";

    private static DateTime Micros(long micros) => DateTimeOffset.FromUnixTimeMilliseconds(micros / 1000).UtcDateTime;
}
