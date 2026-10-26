using System.Text.RegularExpressions;
using Trailox.Agent.Config;

namespace Trailox.Agent.Engines.Redshift;

/// <summary>
/// Amazon Redshift (provisioned or Serverless) over the Postgres wire protocol, as a database
/// user with the <c>sys:monitor</c> role whose password the customer holds. Everything it runs
/// is in <see cref="RsRawSelects"/>.
/// </summary>
/// <remarks>
/// agent.yaml: <c>host</c> = the cluster or workgroup endpoint, <c>port</c> 5439, <c>clusterName</c>
/// = the cluster identifier or workgroup name (descriptive only), <c>username</c> = the database
/// user, the password reference its password, <c>options.database</c> = the database to sign
/// in to (default <c>dev</c>). TLS is always required.
/// </remarks>
public sealed class RedshiftEngine : IEngine
{
    public const int DefaultPort = 5439;
    public const string DefaultDatabase = "dev";
    public const string DatabaseOption = "database";

    // \z, not $: in .NET $ also matches just before a final newline.
    private static readonly Regex HostPattern = new(@"^[A-Za-z0-9.\-]{1,255}\z", RegexOptions.Compiled);
    private static readonly Regex ClusterPattern = new(@"^[A-Za-z0-9_-]{0,64}\z", RegexOptions.Compiled);
    private static readonly Regex UsernamePattern = new(@"^[a-z_][a-z0-9_]{0,63}\z", RegexOptions.Compiled);
    private static readonly Regex DatabasePattern = new(@"^[A-Za-z0-9_]{1,64}\z", RegexOptions.Compiled);

    public string Name => "redshift";

    public IReadOnlyList<string> Validate(EndpointConfig e)
    {
        var problems = new List<string>();
        e.Host = e.Host.Trim();
        e.Username = e.Username.Trim();
        e.Tls = true;
        if (e.Port == 0)
        {
            e.Port = DefaultPort;
        }
        if (!e.Options.ContainsKey(DatabaseOption) || string.IsNullOrWhiteSpace(e.Options[DatabaseOption]))
        {
            e.Options[DatabaseOption] = DefaultDatabase;
        }

        if (!HostPattern.IsMatch(e.Host) || !e.Host.Contains('.'))
        {
            problems.Add("host: the cluster or workgroup endpoint hostname, e.g. my-wg.123456789012.eu-west-1.redshift-serverless.amazonaws.com");
        }
        if (e.Port is < 1 or > 65535)
        {
            problems.Add($"port: {e.Port} is not a valid port (Redshift listens on 5439)");
        }
        if (!ClusterPattern.IsMatch(e.ClusterName))
        {
            problems.Add("clusterName: the cluster identifier or workgroup name - letters, digits, '-' and '_' only, up to 64 characters");
        }
        if (!UsernamePattern.IsMatch(e.Username))
        {
            problems.Add("username: a Redshift user name - lower-case letters, digits and underscores, up to 64 characters");
        }
        if (!DatabasePattern.IsMatch(e.Options[DatabaseOption]))
        {
            problems.Add("options.database: letters, digits and underscores only");
        }
        return problems;
    }

    public IEngineSource CreateSource(EndpointConfig endpoint, IHttpClientFactory httpClientFactory) => new RsSource(endpoint);
}
