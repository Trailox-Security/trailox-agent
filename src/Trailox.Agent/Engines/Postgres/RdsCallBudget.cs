using System.Collections.Concurrent;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// The RDS API calls this agent makes in one Region, shared by every PostgreSQL source it reads there.
/// </summary>
/// <remarks>
/// AWS meters the RDS API per account and Region: a burst, then a fixed rate per second, shared with everything else the
/// account calls (the console, infrastructure tools, monitoring). Each reader is paced on its own
/// (<see cref="RdsLogReader"/>); this caps their sum, so several busy servers read at once cannot take the account's
/// whole rate. A call waits until both its reader and the budget allow it.
/// ⚠ ONE BUDGET PER REGION IN ONE PROCESS. Sources of two AWS accounts in one Region share it, which slows them, never
///   speeds them; a second agent process has a budget of its own, which is how to read two busy accounts at full rate.
/// </remarks>
public sealed class RdsCallBudget
{
    /// <summary>Half of the RDS API's documented steady rate for most actions, so the account keeps the other half.</summary>
    public const double DefaultCallsPerSecond = 6;

    private static readonly ConcurrentDictionary<string, RdsCallBudget> ByRegion = new(StringComparer.Ordinal);

    private readonly TimeSpan _interval;
    private readonly object _lock = new();
    private DateTime _next = DateTime.MinValue;

    public RdsCallBudget(double callsPerSecond) => _interval = TimeSpan.FromSeconds(1 / Math.Max(0.1, callsPerSecond));

    /// <summary>The budget of <paramref name="region"/> in this process.</summary>
    public static RdsCallBudget For(string region) => ByRegion.GetOrAdd(region, _ => new RdsCallBudget(DefaultCallsPerSecond));

    /// <summary>Takes the first free slot at or after <paramref name="at"/>, and returns how long after it the slot is.</summary>
    public TimeSpan Reserve(DateTime at)
    {
        lock (_lock)
        {
            var slot = _next > at ? _next : at;
            _next = slot + _interval;
            return slot - at;
        }
    }
}
