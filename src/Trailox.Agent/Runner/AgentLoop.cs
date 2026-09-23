using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Trailox.Agent.Config;
using Trailox.Agent.Gateway;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Runner;

/// <summary>
/// The whole agent: check in, run the tasks the gateway returned in order, check in again.
/// No cursor, no schedule and no classification live here - the gateway plans, this executes.
/// </summary>
public sealed class AgentLoop : BackgroundService
{
    private readonly AgentConfig _config;
    private readonly string _fingerprint;
    private readonly IReadOnlyList<EndpointSession> _sessions;
    private readonly GatewayClient _gateway;
    private readonly TaskExecutor _executor;
    private readonly ErrorRing _errors;
    private readonly HealthFile _health;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly ILogger<AgentLoop> _logger;
    private readonly DateTime _startedAtUtc = DateTime.UtcNow;

    private enum Cycle { Idle, MoreWork, Stop }

    private TimeSpan IdleDelay { get; set; } = TimeSpan.FromSeconds(60);

    public AgentLoop(
        AgentConfig config, string fingerprint, IReadOnlyList<EndpointSession> sessions, GatewayClient gateway,
        TaskExecutor executor, ErrorRing errors, HealthFile health, IHostApplicationLifetime lifetime, ILogger<AgentLoop> logger)
    {
        _config = config;
        _fingerprint = fingerprint;
        _sessions = sessions;
        _gateway = gateway;
        _executor = executor;
        _errors = errors;
        _health = health;
        _lifetime = lifetime;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var backoff = new Backoff();
        _logger.LogInformation("trailox-agent {Version} starting: {Endpoints} endpoint(s), gateway {Gateway}", Program.Version, _sessions.Count, _config.Gateway);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var outcome = await CycleAsync(stoppingToken);
                backoff.Reset();
                if (outcome == Cycle.Stop)
                {
                    return;
                }
                await Task.Delay(outcome == Cycle.MoreWork ? TimeSpan.FromSeconds(1) : IdleDelay, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (OperationCanceledException)
            {
                // A per-request timeout (check-in or probe), not a shutdown: back off and retry.
                var wait = backoff.Next();
                _logger.LogWarning("check-in timed out after {Timeout:F0}s; retrying in {Wait:F0}s", GatewayClient.CheckinTimeout.TotalSeconds, wait.TotalSeconds);
                try
                {
                    await Task.Delay(wait, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
            catch (GatewayException ex) when (ex.IsUnauthorized)
            {
                RejectedKey();
                return;
            }
            catch (Exception ex)
            {
                var wait = backoff.Next(ex is GatewayException g ? g.RetryAfter : null);
                _logger.LogWarning("check-in failed ({Message}); retrying in {Wait:F0}s", EndpointSession.Trim(ex.Message), wait.TotalSeconds);
                try
                {
                    await Task.Delay(wait, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

   

    /// <summary>One check-in and the tasks it returned, in order.</summary>
    private async Task<Cycle> CycleAsync(CancellationToken ct)
    {
        foreach (var session in _sessions.Where(s => s.NeedsProbe))
        {
            await session.ProbeAsync(_logger, ct);
        }

        var response = await _gateway.CheckinAsync(BuildRequest(), ct);
        _health.Touch();
        IdleDelay = TimeSpan.FromSeconds(Math.Max(10, response.CheckinSeconds));

        if (!VersionOk(Program.Version, response.MinAgentVersion))
        {
            _logger.LogCritical("this agent ({Version}) is below the minimum the gateway accepts ({Min}); update the image", Program.Version, response.MinAgentVersion);
            Stop(Program.ExitVersionTooOld);
            return Cycle.Stop;
        }

        ApplyAssignments(_sessions, response, _logger);
        LogAssignments(response);
        var byId = MapAssignments(response);

        foreach (var task in response.Tasks)
        {
            ct.ThrowIfCancellationRequested();
            if (!byId.TryGetValue(task.EndpointId, out var session))
            {
                _logger.LogWarning("task {Chunk} names endpoint {Id}, which is not in this agent's config; skipping", task.ChunkId, task.EndpointId);
                continue;
            }
            if (!await _executor.ExecuteAsync(task, session, ct))
            {
                RejectedKey();
                return Cycle.Stop;
            }
            _health.Touch();
        }

        // Right after a batch: back at once for the next windows. Idle: the gateway's cadence.
        return response.Tasks.Count > 0 ? Cycle.MoreWork : Cycle.Idle;
    }

    /// <summary>
    /// Whether the server says this source is turned on - or null when this answer does not say.
    /// </summary>
    /// <remarks>
    /// Only an answer about a REGISTERED source carries the customer's setting: "ok", "disabled", and
    /// "unreachable" (registered, and its last probe failed - which is the state a turned-off source with
    /// a failing probe is reported in, so the state alone is not enough). "conflict" and "invalid" carry
    /// the field at its default, false. Reading that as "turned off" would stop probing a source nobody
    /// turned off, and a source that is not probed can never register. Anything unrecognised is null
    /// too: not knowing means carrying on as before.
    /// </remarks>
    internal static bool? EnabledAccordingTo(EndpointAssignment assignment) =>
        assignment.State is "ok" or "disabled" or "unreachable" ? assignment.Enabled : null;

    /// <summary>Tells each source what this check-in said about it.</summary>
    internal static void ApplyAssignments(IReadOnlyList<EndpointSession> sessions, CheckinResponse response, ILogger logger)
    {
        foreach (var assignment in response.Endpoints)
        {
            var enabled = EnabledAccordingTo(assignment);
            if (enabled == null)
            {
                continue;
            }
            sessions.FirstOrDefault(s => string.Equals(s.Config.Alias, assignment.Alias, StringComparison.Ordinal))
                ?.HeardFromServer(enabled.Value, logger);
        }
    }

    private Dictionary<int, EndpointSession> MapAssignments(CheckinResponse response)
    {
        var byId = new Dictionary<int, EndpointSession>();
        foreach (var assignment in response.Endpoints.Where(a => a.EndpointId is not null))
        {
            var session = _sessions.FirstOrDefault(s => string.Equals(s.Config.Alias, assignment.Alias, StringComparison.Ordinal));
            if (session != null)
            {
                byId[assignment.EndpointId!.Value] = session;
            }
        }
        return byId;
    }

    private CheckinRequest BuildRequest() => new()
    {
        AgentVersion = Program.Version,
        Host = Environment.MachineName,
        StartedAtUtc = _startedAtUtc,
        ConfigFingerprint = _fingerprint,
        Endpoints = _sessions.Select(s => s.Report()).ToList(),
        LastErrors = _errors.Snapshot(),
    };

    /// <summary>Notes about healthy sources already written, so each is written once, not every check-in.</summary>
    private readonly HashSet<string> _notesLogged = new(StringComparer.Ordinal);

    private void LogAssignments(CheckinResponse response) => LogAssignments(response, _logger, _notesLogged);

    /// <summary>What this check-in said about each source that needs saying.</summary>
    /// <remarks>
    /// A source that is not ok is reported on every check-in, as it always was. A message about a source
    /// that IS ok is new in 1.4.0 - today, that a Snowflake source polling faster than its setup script
    /// planned for needs a larger daily credit cap - and it is written once per distinct message: the
    /// answer repeats it at every check-in. Agents before 1.4.0 never showed it at all.
    /// </remarks>
    internal static void LogAssignments(CheckinResponse response, ILogger logger, ISet<string> notesLogged)
    {
        // A source the customer turned off is not a problem to warn about on every check-in; its session
        // said so once, when it changed.
        foreach (var a in response.Endpoints.Where(a => a.State != "ok" && EnabledAccordingTo(a) != false))
        {
            logger.LogWarning("endpoint {Alias}: {State}{Message}", a.Alias, a.State, string.IsNullOrEmpty(a.Message) ? "" : " - " + a.Message);
        }
        foreach (var a in response.Endpoints.Where(a => a.State == "ok" && !string.IsNullOrEmpty(a.Message)))
        {
            if (notesLogged.Add(a.Alias + "\n" + a.Message))
            {
                logger.LogWarning("endpoint {Alias}: {Message}", a.Alias, a.Message);
            }
        }
        if (response.Tasks.Count > 0)
        {
            logger.LogInformation("check-in: {Tasks} task(s) to run", response.Tasks.Count);
        }
    }

    private void RejectedKey()
    {
        _logger.LogCritical("the gateway rejected this agent's key (TRAILOX_AGENT_KEY): it is unknown, revoked or rotated. Stopping.");
        Stop(Program.ExitKeyRejected);
    }

    private void Stop(int exitCode)
    {
        Environment.ExitCode = exitCode;
        _lifetime.StopApplication();
    }

    /// <summary>major.minor.patch comparison; anything unparsable is treated as acceptable rather than fatal.</summary>
    internal static bool VersionOk(string agent, string minimum)
    {
        static Version? Parse(string s)
        {
            var core = s.Split('-', '+')[0];
            return Version.TryParse(core.Count(c => c == '.') == 0 ? core + ".0" : core, out var v) ? v : null;
        }
        var a = Parse(agent);
        var m = Parse(minimum);
        return a == null || m == null || a >= m;
    }
}
