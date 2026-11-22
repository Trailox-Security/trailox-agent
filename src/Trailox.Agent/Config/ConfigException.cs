namespace Trailox.Agent.Config;

public sealed class ConfigException : Exception
{
    public IReadOnlyList<string> Problems { get; }

    public ConfigException(IReadOnlyList<string> problems)
        : base("agent.yaml has " + problems.Count + " problem(s):\n  - " + string.Join("\n  - ", problems))
    {
        Problems = problems;
    }
}
