namespace Trailox.Agent.Runner;

/// <summary>Exponential backoff with jitter, 5 s to 5 min. The agent never gives up: the source retains the data.</summary>
public sealed class Backoff
{
    private static readonly TimeSpan Min = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Max = TimeSpan.FromMinutes(5);
    private TimeSpan _current = Min;

    public TimeSpan Next(TimeSpan? retryAfter = null)
    {
        var wait = retryAfter ?? _current;
        _current = TimeSpan.FromMilliseconds(Math.Min(Max.TotalMilliseconds, _current.TotalMilliseconds * 2));
        var jitter = Random.Shared.NextDouble() * 0.25 * wait.TotalMilliseconds;
        return wait + TimeSpan.FromMilliseconds(jitter);
    }

    public void Reset() => _current = Min;
}
