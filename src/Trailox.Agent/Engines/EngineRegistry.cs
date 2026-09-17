using Trailox.Agent.Engines.ClickHouse;
using Trailox.Agent.Engines.Databricks;
using Trailox.Agent.Engines.Redshift;
using Trailox.Agent.Engines.Snowflake;

namespace Trailox.Agent.Engines;

/// <summary>The engines this build supports. One place; add a line here to add an engine.</summary>
public static class EngineRegistry
{
    public static readonly IReadOnlyList<IEngine> All = new IEngine[]
    {
        new ClickHouseEngine(),
        new DatabricksEngine(),
        new RedshiftEngine(),
        new SnowflakeEngine(),
    };

    public static IEnumerable<string> Names => All.Select(e => e.Name);

    public static IEngine? Find(string? name) =>
        string.IsNullOrWhiteSpace(name) ? null : All.FirstOrDefault(e => string.Equals(e.Name, name.Trim(), StringComparison.OrdinalIgnoreCase));

    public static IEngine Require(string name) =>
        Find(name) ?? throw new ArgumentException($"engine '{name}' is not supported (supported: {string.Join(", ", Names)})");
}
