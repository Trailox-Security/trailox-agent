using System.Text.RegularExpressions;
using Trailox.Agent.Config;

namespace Trailox.Agent.Engines.Databricks;

/// <summary>
/// Databricks through the workspace's SQL Statement Execution API, as a service principal with
/// an OAuth secret the customer holds. Everything it runs is in <see cref="DbxRawSelects"/>.
/// </summary>
/// <remarks>
/// agent.yaml for this engine: <c>host</c> = the workspace hostname, <c>clusterName</c> = the
/// SQL warehouse id (16 hex characters), <c>username</c> = the service principal's application
/// id (a UUID), and the password reference holds its OAuth secret. <c>port</c>/<c>tls</c> are
/// ignored: the API is HTTPS on 443 only.
/// </remarks>
public sealed class DatabricksEngine : IEngine
{
    public const string LinksClientName = "databricks-links";

    private static readonly Regex HostPattern = new(@"^[A-Za-z0-9.\-]{1,255}$", RegexOptions.Compiled);
    private static readonly Regex WarehousePattern = new("^[0-9a-f]{16}$", RegexOptions.Compiled);
    private static readonly Regex ApplicationIdPattern = new("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", RegexOptions.Compiled);

    public string Name => "databricks";

    public IReadOnlyList<string> Validate(EndpointConfig e)
    {
        var problems = new List<string>();
        e.Host = e.Host.Trim().ToLowerInvariant();
        e.ClusterName = e.ClusterName.Trim().ToLowerInvariant();
        e.Username = e.Username.Trim();
        e.Port = 443;
        e.Tls = true;

        if (!HostPattern.IsMatch(e.Host) || !e.Host.Contains('.'))
        {
            problems.Add("host: the workspace hostname, e.g. dbc-xxxxxxxx-xxxx.cloud.databricks.com (no scheme, no path)");
        }
        if (!WarehousePattern.IsMatch(e.ClusterName))
        {
            problems.Add("clusterName: the SQL warehouse id, 16 hex characters (SQL Warehouses -> the warehouse -> Connection details)");
        }
        if (!ApplicationIdPattern.IsMatch(e.Username))
        {
            problems.Add("username: the service principal's application id (a UUID shown on its page)");
        }
        return problems;
    }

    public IEngineSource CreateSource(EndpointConfig endpoint, IHttpClientFactory httpClientFactory) =>
        new DbxSource(httpClientFactory.CreateClient(Name), httpClientFactory.CreateClient(LinksClientName), endpoint);
}
