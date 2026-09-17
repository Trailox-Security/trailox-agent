using Trailox.Agent.Protocol;

namespace Trailox.Agent.Runner;

/// <summary>The last N errors, oldest dropped, reported on every check-in so the Agents page can show them.</summary>
public sealed class ErrorRing
{
    private readonly object _lock = new();
    private readonly Queue<ErrorReport> _errors = new();
    private readonly int _capacity;

    public ErrorRing(int capacity = 20)
    {
        _capacity = capacity;
    }

    public void Add(string alias, string message)
    {
        lock (_lock)
        {
            _errors.Enqueue(new ErrorReport { Alias = alias, At = DateTime.UtcNow, Message = EndpointSession.Trim(message) });
            while (_errors.Count > _capacity)
            {
                _errors.Dequeue();
            }
        }
    }

    public List<ErrorReport> Snapshot()
    {
        lock (_lock)
        {
            return _errors.ToList();
        }
    }
}
