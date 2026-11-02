using System.Diagnostics;
using Trailox.Agent.Protocol;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Engines.Oracle;

/// <summary>
/// Holds every call on a session to one limit, a fetch as much as an execute, for any driver.
/// </summary>
/// <remarks>
/// 🔴 A DRIVER'S COMMAND TIMEOUT MAY COVER THE EXECUTE ALONE, and a scan can spend its time in a later fetch. Without this a
///    read would be stopped by nothing but the profile, whose limits the server enforces only with <c>RESOURCE_LIMIT</c> on.
///    Each call has its own limit, so a long window of quick fetches runs, and the time the upload holds the fetch back
///    does not count.
/// 🔴 A CALL THAT WAITS MARKS THE AGENT ALIVE, UNTIL ITS LIMIT. One long fetch sends no row, and the liveness probe would
///    restart the agent in the middle of it, and then the same read would start again, every time. The beats come from a
///    timer of the session's, so a driver whose async call blocks its caller is covered too, and they stop at the limit, so
///    a call stuck for good still ends in a restart.
/// 🔴 A STOPPED CALL MAY STILL BE ON THE CONNECTION, and the next statement must not meet it. It gets a moment to end; past
///    that the session's further calls fail at once as timed out, so that a probe reports what it has instead of waiting
///    out one limit for each read that is left. Closing on that connection could wait as long, so it is left behind the
///    read: what stopped the read must still reach the task's report.
/// </remarks>
internal sealed class OraBoundedSession : IOraSession
{
    private static readonly TimeSpan StopGrace = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan DefaultBeatEvery = TimeSpan.FromSeconds(30);

    private readonly IOraSession _session;
    private readonly TimeSpan _limit;
    private readonly ReadTime? _time;
    private readonly Timer _beats;

    /// <summary>When the running call's limit ends, on <see cref="Environment.TickCount64"/>; 0 with no call running.</summary>
    private long _callEndsAt;

    /// <summary>The last call stopped at its limit; it may not have ended yet.</summary>
    private Task? _stopped;

    private bool Stuck => _stopped is { IsCompleted: false };

    /// <param name="time">Counts what the calls wait on the server.</param>
    /// <param name="beat">Called while a call waits; the liveness heartbeat unless a test says.</param>
    public OraBoundedSession(IOraSession session, TimeSpan limit, ReadTime? time = null, Action? beat = null, TimeSpan? beatEvery = null)
    {
        _session = session;
        _limit = limit;
        _time = time;
        var alive = beat ?? TaskHeartbeat.Beat;
        var every = beatEvery ?? DefaultBeatEvery;
        _beats = new Timer(_ =>
        {
            if (Environment.TickCount64 < Volatile.Read(ref _callEndsAt))
            {
                alive();
            }
        }, null, every, every);
    }

    public Task ExecuteAsync(string sql, CancellationToken ct) => CallAsync(async token =>
    {
        await _session.ExecuteAsync(sql, token);
        return true;
    }, _limit, ct);

    public async Task<IOraRows> QueryAsync(string sql, IReadOnlyList<OraBind> binds, CancellationToken ct, TimeSpan? limit = null)
    {
        var bound = limit ?? _limit;
        return new Rows(this, await CallAsync(token => _session.QueryAsync(sql, binds, token, limit), bound, ct), bound);
    }

    public async ValueTask DisposeAsync()
    {
        await _beats.DisposeAsync();
        if (Stuck)
        {
            _ = Task.Run(() => _session.DisposeAsync().AsTask());
            return;
        }
        await _session.DisposeAsync();
    }

    /// <summary>
    /// One call, abandoned at the limit even if the driver does not stop at its token: disposing the session then ends it.
    /// </summary>
    private async Task<T> CallAsync<T>(Func<CancellationToken, Task<T>> call, TimeSpan limit, CancellationToken ct)
    {
        if (Stuck)
        {
            throw new OraDriverException(0, "the session is still stopping a call that took too long", timedOut: true);
        }
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(limit);
        var started = Stopwatch.GetTimestamp();
        Volatile.Write(ref _callEndsAt, Environment.TickCount64 + (long)limit.TotalMilliseconds);
        Task<T>? running = null;
        try
        {
            running = call(bounded.Token);
            return await running.WaitAsync(bounded.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            if (running != null)
            {
                _stopped = running;
                await Task.WhenAny(running, Task.Delay(limit < StopGrace ? limit : StopGrace, ct));
            }
            throw new OraDriverException(0, $"the call took more than {limit.TotalSeconds:0} seconds and was stopped", timedOut: true);
        }
        finally
        {
            Volatile.Write(ref _callEndsAt, 0);
            _time?.Add(Stopwatch.GetElapsedTime(started));
        }
    }

    private sealed class Rows : IOraRows
    {
        private readonly OraBoundedSession _session;
        private readonly IOraRows _rows;
        private readonly TimeSpan _limit;

        public Rows(OraBoundedSession session, IOraRows rows, TimeSpan limit)
        {
            _session = session;
            _rows = rows;
            _limit = limit;
        }

        public IReadOnlyList<string> Columns => _rows.Columns;

        public Task<bool> ReadAsync(CancellationToken ct) => _session.CallAsync(_rows.ReadAsync, _limit, ct);

        public object? Value(int column) => _rows.Value(column);

        public ValueTask DisposeAsync() => _session.Stuck ? ValueTask.CompletedTask : _rows.DisposeAsync();
    }
}
