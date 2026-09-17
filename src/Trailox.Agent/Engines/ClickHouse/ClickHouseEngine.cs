using System.Text.RegularExpressions;
using Trailox.Agent.Config;

namespace Trailox.Agent.Engines.ClickHouse;

/// <summary>ClickHouse over its HTTP(S) interface. Everything it runs is in <see cref="RawSelects"/>.</summary>
public sealed class ClickHouseEngine : IEngine
{
    public const string DefaultUsername = "trailox_monitor";
    public const int DefaultPort = 8443;

    private static readonly Regex HostPattern = new(@"^[A-Za-z0-9.\-_:\[\]]{1,255}$", RegexOptions.Compiled);
    private static readonly Regex ClusterPattern = new("^[A-Za-z0-9_-]{0,64}$", RegexOptions.Compiled);
    private static readonly Regex UsernamePattern = new("^[A-Za-z0-9_.@-]{1,64}$", RegexOptions.Compiled);

    public string Name => "clickhouse";

    public IReadOnlyList<string> Validate(EndpointConfig e)
    {
        var problems = new List<string>();
        if (e.Port == 0)
        {
            e.Port = DefaultPort;
        }
        if (string.IsNullOrWhiteSpace(e.Username))
        {
            e.Username = DefaultUsername;
        }

        if (!HostPattern.IsMatch(e.Host))
        {
            problems.Add("host: required, a hostname or IP the agent can reach");
        }
        if (e.Port is < 1 or > 65535)
        {
            problems.Add($"port: {e.Port} is not a valid port");
        }
        if (!ClusterPattern.IsMatch(e.ClusterName))
        {
            problems.Add("clusterName: letters, digits, '-' and '_' only, up to 64 characters ('default' on ClickHouse Cloud, empty on a single node)");
        }
        if (!UsernamePattern.IsMatch(e.Username))
        {
            problems.Add("username: letters, digits, '.', '@', '-' and '_' only, 1-64 characters");
        }
        return problems;
    }

    public IEngineSource CreateSource(EndpointConfig endpoint, IHttpClientFactory httpClientFactory) =>
        new ChSource(httpClientFactory.CreateClient(Name), endpoint);
}
