using System.Text.RegularExpressions;
using Trailox.Agent.Config;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// PostgreSQL 13 and later (agent 1.12): Amazon RDS and Aurora, read through the RDS API, and self-managed servers, read
/// from their log directory. The agent reads the server log for sign-ins, failed sign-ins and DDL, and signs in to the
/// server for <c>pg_stat_statements</c> counters and the catalog.
/// </summary>
/// <remarks>
/// agent.yaml for this engine: <c>host</c>/<c>port</c> = the primary's address, <c>username</c> = the monitoring role,
/// and its password reference. <c>options</c>:
/// <list type="bullet">
/// <item><c>log_source</c>: <c>rds_api</c> or <c>file</c>;</item>
/// <item><c>rds_api</c>: <c>instances</c> (the instance identifiers, primary first, each <c>id</c> or <c>id=host</c>)
///   and <c>rds_region</c>; the agent's own AWS identity reads the log files;</item>
/// <item><c>file</c>: <c>log_directory</c>, the server's log directory mounted read-only in the agent's container;</item>
/// <item><c>database</c>: where the agent signs in for the counters (default <c>postgres</c>).</item>
/// </list>
/// </remarks>
public sealed class PostgresEngine : IEngine
{
    public const string LogSourceRdsApi = "rds_api";
    public const string LogSourceFile = "file";
    public const string DefaultDatabase = "postgres";

    private static readonly HashSet<string> KnownOptions = new(StringComparer.Ordinal) { "log_source", "instances", "rds_region", "log_directory", "database" };

    // RDS instance identifiers: a letter, then letters, digits and '-', at most 63.
    private static readonly Regex InstancePattern = new(@"^[A-Za-z][A-Za-z0-9-]{0,62}\z", RegexOptions.Compiled);
    private static readonly Regex HostPattern = new(@"^[A-Za-z0-9.\-:\[\]]{1,253}\z", RegexOptions.Compiled);
    private static readonly Regex RegionPattern = new(@"^[a-z]{2,4}(-[a-z]+)+-[0-9]+\z", RegexOptions.Compiled);
    private static readonly Regex RolePattern = new(@"^[A-Za-z0-9_.@\-]{1,63}\z", RegexOptions.Compiled);
    private static readonly Regex DatabasePattern = new(@"^[A-Za-z0-9_\-]{1,63}\z", RegexOptions.Compiled);

    // An RDS instance endpoint, <instance id>.<account hash>.<region>.rds.amazonaws.com: a replica's differs only in its first
    // label. An Aurora cluster endpoint's second label is cluster-<hash> or cluster-ro-<hash>, and does not match.
    private static readonly Regex RdsInstanceEndpoint = new(@"^(?<id>[A-Za-z][A-Za-z0-9-]{0,62})\.[a-z0-9]+\.[a-z0-9-]+\.rds\.amazonaws\.com(\.cn)?\z",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    public string Name => "postgres";

    public IReadOnlyList<string> Validate(EndpointConfig e)
    {
        var problems = new List<string>();
        e.Host = e.Host.Trim();
        e.Username = e.Username.Trim();
        if (e.Port == 0)
        {
            e.Port = 5432;
        }
        if (!HostPattern.IsMatch(e.Host))
        {
            problems.Add("host: the primary's address, e.g. orders.abc123.eu-west-1.rds.amazonaws.com or 127.0.0.1");
        }
        if (e.Port is < 1 or > 65535)
        {
            problems.Add("port: 1-65535 (PostgreSQL listens on 5432 by default)");
        }
        if (!RolePattern.IsMatch(e.Username))
        {
            problems.Add("username: the monitoring role the setup script created, e.g. trailox_monitor");
        }
        foreach (var key in e.Options.Keys.Where(k => !KnownOptions.Contains(k)))
        {
            // Secrets go through password_env/password_file only, never through options.
            problems.Add($"options.{key}: not an option of the postgres engine (known: {string.Join(", ", KnownOptions)})");
        }
        foreach (var key in e.Options.Keys.ToList())
        {
            e.Options[key] = e.Options[key].Trim();
        }
        if (e.Options.TryGetValue("database", out var database) && !DatabasePattern.IsMatch(database))
        {
            problems.Add("options.database: the database the agent signs in to for pg_stat_statements, e.g. postgres");
        }

        var source = e.Options.GetValueOrDefault("log_source", "");
        if (source == LogSourceRdsApi)
        {
            e.Kind = "cloud";
            ValidateRds(e, problems);
        }
        else if (source == LogSourceFile)
        {
            ValidateFile(e, problems);
        }
        else
        {
            problems.Add($"options.log_source: {LogSourceRdsApi} (Amazon RDS and Aurora) or {LogSourceFile} (a self-managed server's log directory)");
        }
        return problems;
    }

    private static void ValidateRds(EndpointConfig e, List<string> problems)
    {
        if (e.Options.ContainsKey("log_directory"))
        {
            problems.Add($"options.log_directory: only with log_source: {LogSourceFile}");
        }
        if (!RegionPattern.IsMatch(e.Options.GetValueOrDefault("rds_region", "")))
        {
            problems.Add("options.rds_region: the instances' AWS region, e.g. eu-west-1");
        }
        var instances = Instances(e, out var instanceProblems);
        problems.AddRange(instanceProblems);
        if (instanceProblems.Count == 0 && instances.Select(i => i.Id).Distinct(StringComparer.OrdinalIgnoreCase).Count() != instances.Count)
        {
            problems.Add("options.instances: an instance is listed twice");
        }
    }

    private static void ValidateFile(EndpointConfig e, List<string> problems)
    {
        foreach (var key in new[] { "instances", "rds_region" }.Where(e.Options.ContainsKey))
        {
            problems.Add($"options.{key}: only with log_source: {LogSourceRdsApi}");
        }
        var dir = e.Options.GetValueOrDefault("log_directory", "");
        if (!dir.StartsWith('/'))
        {
            problems.Add("options.log_directory: the absolute path where the server's log directory is mounted in the agent's container, e.g. /var/log/trailox-pg");
        }
    }

    /// <summary>
    /// The instances of an <c>rds_api</c> source, primary first. A replica's host, when not given, is derived from the
    /// primary's (<see cref="ReplicaHost"/>); where it cannot be, the replica must be listed as <c>id=host</c>, and
    /// <paramref name="problems"/> says so.
    /// </summary>
    /// <param name="problems">What is wrong with <c>options.instances</c>, as Validate reports it; empty when nothing is.</param>
    public static IReadOnlyList<PgInstance> Instances(EndpointConfig e, out List<string> problems)
    {
        problems = new List<string>();
        var malformed = new List<string>();
        var list = new List<PgInstance>();
        var raw = e.Options.GetValueOrDefault("instances", "");
        foreach (var item in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var eq = item.IndexOf('=');
            var id = eq < 0 ? item : item[..eq].Trim();
            var host = eq < 0 ? "" : item[(eq + 1)..].Trim();
            if (!InstancePattern.IsMatch(id) || (host.Length > 0 && !HostPattern.IsMatch(host)))
            {
                malformed.Add(item);
                continue;
            }
            if (host.Length == 0 && list.Count > 0)
            {
                if (ReplicaHost(e.Host, list[0].Id, id) is not { } derived)
                {
                    problems.Add($"options.instances: list {id} as {id}=<its host>: the host is not the RDS instance endpoint of {list[0].Id} "
                        + $"({list[0].Id}.<...>.rds.amazonaws.com), so a replica's cannot be derived from it");
                    continue;
                }
                host = derived;
            }
            list.Add(new PgInstance(id, host.Length == 0 ? e.Host : host, list.Count == 0 ? "primary" : "replica"));
        }
        if (malformed.Count > 0 || (list.Count == 0 && problems.Count == 0))
        {
            problems.Insert(0, "options.instances: the RDS instance identifiers, primary first, separated by commas, each as id or id=host "
                + (malformed.Count > 0 ? $"(not an identifier: {string.Join(", ", malformed)})" : "(none given)"));
        }
        return list;
    }

    /// <summary>
    /// A replica's host, from the primary's: only when that is the primary's own RDS instance endpoint, whose first label is
    /// the instance identifier and which differs from a replica's only there. Null for an address, a CNAME or an Aurora
    /// cluster endpoint, whose replica host cannot be guessed.
    /// </summary>
    internal static string? ReplicaHost(string primaryHost, string primaryId, string replicaId)
    {
        var m = RdsInstanceEndpoint.Match(primaryHost);
        return m.Success && string.Equals(m.Groups["id"].Value, primaryId, StringComparison.OrdinalIgnoreCase)
            ? replicaId + primaryHost[m.Groups["id"].Length..]
            : null;
    }

    public IEngineSource CreateSource(EndpointConfig endpoint, IHttpClientFactory httpClientFactory) => new PgSource(endpoint);
}

/// <summary>One server of a source: its RDS instance identifier (or, for a log directory, the source's host), the address the agent signs in to, and its role.</summary>
public sealed record PgInstance(string Id, string Host, string Role);
