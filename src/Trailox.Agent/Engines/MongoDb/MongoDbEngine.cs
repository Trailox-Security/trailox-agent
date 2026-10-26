using System.Text.RegularExpressions;
using Trailox.Agent.Config;

namespace Trailox.Agent.Engines.MongoDb;

/// <summary>
/// MongoDB Atlas (agent 1.8), through the Atlas Administration API as a service account the customer holds.
/// The agent downloads the cluster's log files, replaces every value in every command by its type in this
/// process (<see cref="MongoShape"/>), and ships one row per minute and group, so no literal value leaves the
/// customer's network.
/// </summary>
/// <remarks>
/// agent.yaml for this engine: <c>host</c> = the Atlas project id (24 hex), <c>clusterName</c> = the cluster's
/// name, <c>username</c> = the service account's client id, and the password reference holds its secret. The
/// service account needs Project Read Only and Project Cluster Log Viewer. <c>port</c>/<c>tls</c> are ignored:
/// the API is HTTPS on 443.
/// From 1.10.0 the log files can come from the customer's Atlas S3 log export instead (<c>options.log_source: s3</c>,
/// with <c>s3_bucket</c>, <c>s3_region</c> and <c>s3_prefix</c>), read with the agent's own AWS identity
/// (<see cref="AtlasS3Export"/>). The service account is still needed, with Project Read Only alone.
/// 🔴 THE AGENT CONNECTS TO THE CLUSTER ONLY AS THE OPTIONAL CATALOG USER (1.9.0): <c>catalog_username</c> and
///    its password reference, a database user whose role lists databases and collections and their sizes
///    (<see cref="MongoCatalog"/>). It never reads a document. Without one, nothing connects to the cluster.
/// </remarks>
public sealed class MongoDbEngine : IEngine
{
    public const string LogSourceApi = "atlas_api";
    public const string LogSourceS3 = "s3";

    private static readonly Regex ProjectIdPattern = new(@"^[0-9a-f]{24}\z", RegexOptions.Compiled);
    private static readonly Regex ClusterPattern = new(@"^[A-Za-z0-9][A-Za-z0-9-]{0,63}\z", RegexOptions.Compiled);
    private static readonly Regex ClientIdPattern = new(@"^[A-Za-z0-9_\-]{1,64}\z", RegexOptions.Compiled);

    // What Trailox accepts as a source's monitoring user, which the catalog user becomes (EndpointSession.Report).
    private static readonly Regex CatalogUserPattern = new(@"^[A-Za-z0-9_.@-]{1,64}\z", RegexOptions.Compiled);
    private static readonly HashSet<string> KnownOptions = new(StringComparer.Ordinal) { "log_source", "s3_bucket", "s3_region", "s3_prefix" };

    // S3's bucket naming rules (3-63 characters; lower-case letters, digits, '.', '-'; a letter or digit at each end).
    private static readonly Regex BucketPattern = new(@"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]\z", RegexOptions.Compiled);
    // AWS region names: eu-west-1, us-gov-west-1, and the European Sovereign Cloud's eusc-de-east-1.
    private static readonly Regex RegionPattern = new(@"^[a-z]{2,4}(-[a-z]+)+-[0-9]+\z", RegexOptions.Compiled);
    // The integration's prefix as typed in Atlas: path segments, no leading or trailing '/'.
    private static readonly Regex PrefixPattern = new(@"^[A-Za-z0-9!_.*'()-]+(/[A-Za-z0-9!_.*'()-]+)*\z", RegexOptions.Compiled);

    public string Name => "mongodb";

    public bool TakesCatalogUser => true;

    public IReadOnlyList<string> Validate(EndpointConfig e)
    {
        var problems = new List<string>();
        e.Host = e.Host.Trim().ToLowerInvariant();
        e.ClusterName = e.ClusterName.Trim();
        e.Username = e.Username.Trim();
        e.CatalogUsername = e.CatalogUsername.Trim();
        e.Port = 443;
        e.Tls = true;
        e.Kind = "cloud";

        if (!ProjectIdPattern.IsMatch(e.Host))
        {
            problems.Add("host: the Atlas project id, 24 hex characters (Project Settings -> Project ID)");
        }
        if (!ClusterPattern.IsMatch(e.ClusterName) || string.Equals(e.ClusterName, "default", StringComparison.OrdinalIgnoreCase))
        {
            problems.Add("clusterName: the cluster's name as Atlas shows it, e.g. Cluster0 (letters, digits and '-')");
        }
        if (!ClientIdPattern.IsMatch(e.Username))
        {
            problems.Add("username: the service account's client id, e.g. mdb_sa_id_0123456789abcdef01234567");
        }
        if (e.CatalogUsername.Length > 0 && !CatalogUserPattern.IsMatch(e.CatalogUsername))
        {
            problems.Add("catalog_username: the catalog database user's name, e.g. trailox_catalog (letters, digits and '_', '.', '@', '-', at most 64)");
        }
        foreach (var key in e.Options.Keys.Where(k => !KnownOptions.Contains(k)))
        {
            // Secrets go through password_env/password_file (and the catalog_ pair) only, never through options.
            problems.Add($"options.{key}: not an option of the mongodb engine (known: {string.Join(", ", KnownOptions)})");
        }
        var source = e.Options.GetValueOrDefault("log_source", LogSourceApi);
        if (source is not (LogSourceApi or LogSourceS3))
        {
            problems.Add($"options.log_source: '{source}' must be {LogSourceApi} or {LogSourceS3}");
        }
        else if (source == LogSourceS3)
        {
            ValidateS3(e.Options, problems);
        }
        else
        {
            foreach (var key in new[] { "s3_bucket", "s3_region", "s3_prefix" }.Where(e.Options.ContainsKey))
            {
                problems.Add($"options.{key}: only with log_source: {LogSourceS3}");
            }
        }
        return problems;
    }

    /// <summary>The S3 export's options (1.10.0): the bucket the Atlas log integration writes to, its region, its prefix.</summary>
    private static void ValidateS3(Dictionary<string, string> options, List<string> problems)
    {
        foreach (var key in new[] { "s3_bucket", "s3_region", "s3_prefix" }.Where(options.ContainsKey))
        {
            options[key] = options[key].Trim();
        }
        var bucket = options.GetValueOrDefault("s3_bucket", "");
        if (bucket.StartsWith("arn:", StringComparison.Ordinal))
        {
            problems.Add("options.s3_bucket: the bucket's name; an access point or Multi-Region Access Point ARN is not supported yet");
        }
        else if (!BucketPattern.IsMatch(bucket))
        {
            problems.Add("options.s3_bucket: the name of the bucket the Atlas log integration writes to, e.g. my-atlas-logs "
                + "(a DNS-compatible name: a legacy name with upper case or '_' is not supported)");
        }
        if (!RegionPattern.IsMatch(options.GetValueOrDefault("s3_region", "")))
        {
            problems.Add("options.s3_region: the bucket's AWS region, e.g. eu-west-1");
        }
        var prefix = options.GetValueOrDefault("s3_prefix", "");
        if (prefix.Length > 0 && !PrefixPattern.IsMatch(prefix))
        {
            problems.Add("options.s3_prefix: the integration's prefix as typed in Atlas, with no leading or trailing '/'");
        }
    }

    public IEngineSource CreateSource(EndpointConfig endpoint, IHttpClientFactory httpClientFactory) =>
        new MongoSource(httpClientFactory.CreateClient(Name), endpoint);
}
