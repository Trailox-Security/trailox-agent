using System.Text.RegularExpressions;
using Trailox.Agent.Config;

namespace Trailox.Agent.Engines.Snowflake;

/// <summary>
/// Snowflake through its SQL API, as a <c>TYPE = SERVICE</c> user authenticating with a key pair
/// whose private half only this agent holds. Everything it runs is in <see cref="SfRawSelects"/>.
/// </summary>
/// <remarks>
/// agent.yaml: <c>host</c> = the account identifier (<c>myorg-myaccount</c>, or a legacy
/// <c>locator.region</c>; a trailing <c>.snowflakecomputing.com</c> is accepted),
/// <c>clusterName</c> = the warehouse, <c>username</c> = the service user, the password reference
/// = its PKCS#8 PEM private key, <c>options.role</c> = the role (default <c>TRAILOX_MONITOR</c>).
/// <c>port</c>/<c>tls</c> are ignored: the API is HTTPS on 443 only.
/// </remarks>
public sealed class SnowflakeEngine : IEngine
{
    public const string RoleOption = "role";
    public const string DefaultRole = "TRAILOX_MONITOR";
    public const string DefaultWarehouse = "TRAILOX_MONITOR_WH";
    public const string DomainSuffix = ".snowflakecomputing.com";

    private static readonly Regex AccountPattern = new(@"^[A-Za-z0-9_-]{1,255}(\.[A-Za-z0-9_-]{1,64}){0,2}$", RegexOptions.Compiled);
    // Within the Trailox gateway's limits for clusterName and username: no $, at most 64 characters.
    private static readonly Regex IdentPattern = new(@"^[A-Za-z_][A-Za-z0-9_]{0,63}$", RegexOptions.Compiled);

    public string Name => "snowflake";

    public IReadOnlyList<string> Validate(EndpointConfig e)
    {
        var problems = new List<string>();
        e.Host = e.Host.Trim();
        if (e.Host.EndsWith(DomainSuffix, StringComparison.OrdinalIgnoreCase))
        {
            e.Host = e.Host[..^DomainSuffix.Length];
        }
        e.ClusterName = string.IsNullOrWhiteSpace(e.ClusterName) ? DefaultWarehouse : e.ClusterName.Trim().ToUpperInvariant();
        e.Username = e.Username.Trim().ToUpperInvariant();
        e.Port = 443;
        e.Tls = true;
        e.Options[RoleOption] = e.Options.TryGetValue(RoleOption, out var role) && !string.IsNullOrWhiteSpace(role)
            ? role.Trim().ToUpperInvariant()
            : DefaultRole;

        if (!AccountPattern.IsMatch(e.Host))
        {
            problems.Add("host: the account identifier, e.g. myorg-myaccount (Snowsight -> account menu -> Account details), no scheme or path");
        }
        if (!IdentPattern.IsMatch(e.ClusterName))
        {
            problems.Add("clusterName: the warehouse name - letters, digits and '_', starting with a letter or '_', up to 64 characters");
        }
        if (!IdentPattern.IsMatch(e.Username))
        {
            problems.Add("username: the service user's name - letters, digits and '_', starting with a letter or '_', up to 64 characters");
        }
        if (!IdentPattern.IsMatch(e.Options[RoleOption]))
        {
            problems.Add("options.role: a role name - letters, digits and '_', starting with a letter or '_', up to 64 characters");
        }
        return problems;
    }

    /// <summary>The API hostname: the identifier lower-cased, with '_' as '-' (the form Snowflake serves certificates for).</summary>
    public static string ApiHost(string account) => account.ToLowerInvariant().Replace('_', '-') + DomainSuffix;

    /// <summary>The account as the JWT names it: the part before any region, upper-cased.</summary>
    public static string JwtAccount(string account) => account.Split('.')[0].ToUpperInvariant();

    public IEngineSource CreateSource(EndpointConfig endpoint, IHttpClientFactory httpClientFactory) =>
        new SfSource(httpClientFactory.CreateClient(Name), endpoint);
}
