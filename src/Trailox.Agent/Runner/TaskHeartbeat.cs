namespace Trailox.Agent.Runner;

/// <summary>
/// Keeps the liveness file fresh while one long task makes progress. The loop touches the file only
/// between tasks, and a task that reads minutes of MongoDB logs would otherwise get a healthy agent
/// restarted mid-task by its liveness probe (5 minutes, <see cref="HealthFile.MaxAge"/>), and then the
/// same task again, every time.
/// </summary>
/// <remarks>
/// An engine beats while it makes progress (lines read), never on a timer: a read that stops moving
/// stops the beats, and the liveness probe then restarts the agent as it does today.
/// </remarks>
public static class TaskHeartbeat
{
    private const long MinIntervalMs = 30_000;

    private static HealthFile? _health;
    private static long _lastBeatMs = long.MinValue;

    internal static void Attach(HealthFile? health)
    {
        Volatile.Write(ref _health, health);
        Interlocked.Exchange(ref _lastBeatMs, long.MinValue);
    }

    /// <summary>Called by an engine as it makes progress; touches the liveness file at most every 30 seconds.</summary>
    public static void Beat()
    {
        var now = Environment.TickCount64;
        var last = Interlocked.Read(ref _lastBeatMs);
        if (last != long.MinValue && now - last < MinIntervalMs)
        {
            return;
        }
        if (Interlocked.CompareExchange(ref _lastBeatMs, now, last) != last)
        {
            return;
        }
        Volatile.Read(ref _health)?.Touch();
    }
}
