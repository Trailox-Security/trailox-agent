using System.Text.RegularExpressions;
using Trailox.Agent.Config;

namespace Trailox.Agent.Engines.Oracle;

/// <summary>
/// Oracle 19c and later (agent 1.16): Amazon RDS for Oracle and self-managed servers, one source for each PDB or non-CDB.
/// The agent reads the unified audit trail with <c>SELECT</c>s and needs no AWS identity, no client install and no wallet.
/// </summary>
/// <remarks>
/// agent.yaml for this engine: <c>host</c>/<c>port</c> = the listener, <c>tls</c> = TCPS, <c>kind</c> = <c>cloud</c> (RDS) or
/// <c>onprem</c>, <c>username</c> = the monitoring user, and its password reference. <c>options</c>: <c>service_name</c>, the
/// service of the PDB or non-CDB, never the CDB root.
/// </remarks>
public sealed class OracleEngine : IEngine
{
    public const int DefaultPort = 1521;

    /// <summary>The port of the SSL option on RDS, and the usual one for TCPS elsewhere.</summary>
    public const int DefaultTlsPort = 2484;

    private static readonly HashSet<string> KnownOptions = new(StringComparer.Ordinal) { "service_name" };

    private static readonly Regex HostPattern = new(@"^[A-Za-z0-9.\-:\[\]]{1,253}\z", RegexOptions.Compiled);

    // An unquoted user name: Oracle stores it in upper case.
    private static readonly Regex UserPattern = new(@"^[A-Za-z][A-Za-z0-9_$#]{0,127}\z", RegexOptions.Compiled);

    // Letters, digits and what Oracle allows in a service name: nothing that can end a clause of the connect descriptor.
    private static readonly Regex ServicePattern = new(@"^[A-Za-z0-9][A-Za-z0-9_.$#\-]{0,254}\z", RegexOptions.Compiled);

    public string Name => "oracle";

    public IReadOnlyList<string> Validate(EndpointConfig e)
    {
        var problems = new List<string>();
        e.Host = e.Host.Trim();
        e.Username = e.Username.Trim();
        if (e.Port == 0)
        {
            e.Port = e.Tls ? DefaultTlsPort : DefaultPort;
        }
        if (!HostPattern.IsMatch(e.Host))
        {
            problems.Add("host: the listener's address, e.g. ledger.abc123def456.eu-west-1.rds.amazonaws.com or 192.0.2.10");
        }
        if (e.Port is < 1 or > 65535)
        {
            problems.Add($"port: 1-65535 (Oracle listens on {DefaultPort} by default, {DefaultTlsPort} for TCPS on RDS)");
        }
        if (UserPattern.IsMatch(e.Username))
        {
            // The name the server writes in its trail and answers to: the collected records are matched by it exactly.
            e.Username = e.Username.ToUpperInvariant();
        }
        else
        {
            problems.Add("username: the unquoted monitoring user the setup script created, e.g. TRAILOX_MONITOR");
        }
        foreach (var key in e.Options.Keys.Where(k => !KnownOptions.Contains(k)))
        {
            // Secrets go through password_env/password_file only, never through options.
            problems.Add($"options.{key}: not an option of the oracle engine (known: {string.Join(", ", KnownOptions)})");
        }
        foreach (var key in e.Options.Keys.ToList())
        {
            e.Options[key] = e.Options[key].Trim();
        }
        if (!ServicePattern.IsMatch(e.Options.GetValueOrDefault("service_name", "")))
        {
            problems.Add("options.service_name: the service of the PDB or non-CDB, e.g. LEDGER (letters, digits, '_', '.', '$', '#' and '-')");
        }
        return problems;
    }

    public IEngineSource CreateSource(EndpointConfig endpoint, IHttpClientFactory httpClientFactory) => new OraSource(endpoint);
}
