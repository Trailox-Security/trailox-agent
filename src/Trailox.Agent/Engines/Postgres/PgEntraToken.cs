using System.Text.Json;
using System.Text.RegularExpressions;
using Azure.Core;
using Azure.Identity;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// Who a Microsoft Entra token was issued to, from its own claims (<c>oid</c>, <c>appid</c>, <c>tid</c>,
/// <c>xms_mirid</c>): said in the probe's check, and in the SQL a refused sign-in names. Read without checking the
/// signature: the agent only reports it, and the server checks the token.
/// </summary>
/// <param name="ObjectId">The principal's object id: what the server maps the PostgreSQL role to.</param>
/// <param name="ManagedIdentity">The managed identity's name (the last part of its resource id); null for any other principal.</param>
public sealed record PgEntraIdentity(string ObjectId, string? AppId, string? TenantId, string? ManagedIdentity, bool User)
{
    private static readonly Regex Guid = new(@"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\z", RegexOptions.Compiled);

    /// <summary>The identity of a token; null when it is not a JWT with an object id.</summary>
    public static PgEntraIdentity? Of(string token)
    {
        var parts = token.Split('.');
        if (parts.Length != 3)
        {
            return null;
        }
        try
        {
            var payload = parts[1].Replace('-', '+').Replace('_', '/');
            payload = payload.PadRight(payload.Length + (4 - payload.Length % 4) % 4, '=');
            using var doc = JsonDocument.Parse(Convert.FromBase64String(payload));
            var claims = doc.RootElement;
            string? Claim(string name) => claims.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;
            if (Claim("oid") is not { } oid || !Guid.IsMatch(oid))
            {
                return null;
            }
            var resource = Claim("xms_mirid");
            var name = resource?.TrimEnd('/') is { Length: > 0 } r ? r[(r.LastIndexOf('/') + 1)..] : null;
            return new PgEntraIdentity(oid, Claim("appid") ?? Claim("azp"), Claim("tid"), name,
                Claim("idtyp") == "user" || Claim("upn") != null);
        }
        catch (Exception ex) when (ex is FormatException or JsonException)
        {
            return null;
        }
    }

    /// <summary>What the identity is, as a customer finds it in Azure.</summary>
    public override string ToString() =>
        ManagedIdentity != null ? $"the managed identity {ManagedIdentity} (object id {ObjectId})"
        : User ? $"the user with object id {ObjectId}"
        : AppId != null ? $"the service principal of application {AppId} (object id {ObjectId})"
        : $"the principal with object id {ObjectId}";

    /// <summary>The object type <c>pgaadauth_create_principal_with_oid</c> takes for it.</summary>
    public string ObjectType => User ? "user" : "service";
}

/// <summary>
/// The agent's Microsoft Entra token for Azure Database for PostgreSQL (<c>options.azure_sign_in: entra</c>), from the
/// same identity as the log reads (<see cref="ArmAzureLogFiles.Credential"/>). Asked for when a connection opens and
/// none is held, and kept until <see cref="Renew"/> before it expires: the agent opens a connection per task, and a
/// token lasts about an hour.
/// </summary>
public sealed class PgEntraToken
{
    /// <summary>The scope of Azure Database for PostgreSQL and MySQL in Azure's public cloud.</summary>
    public const string Scope = "https://ossrdbms-aad.database.windows.net/.default";

    /// <summary>A token this close to its expiry is not sent: a new one is asked for.</summary>
    internal static readonly TimeSpan Renew = TimeSpan.FromMinutes(5);

    private static readonly TokenRequestContext Request = new(new[] { Scope });

    private readonly TokenCredential _credential;
    private readonly Func<DateTimeOffset> _utcNow;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private AccessToken? _token;

    public PgEntraToken(TokenCredential credential, Func<DateTimeOffset>? utcNow = null)
    {
        _credential = credential;
        _utcNow = utcNow ?? (() => DateTimeOffset.UtcNow);
    }

    /// <summary>Whether a token is held that the next sign-in would send without asking Entra again.</summary>
    public bool Held => _token is { } t && t.ExpiresOn - _utcNow() > Renew;

    /// <summary>Who the newest token was issued to; null before one, or when it names no object id.</summary>
    public PgEntraIdentity? Identity { get; private set; }

    /// <summary>The token held, or a new one. A token that cannot be had says why; no sign-in is tried without one.</summary>
    public async Task<string> TokenAsync(string host, CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            if (!Held)
            {
                try
                {
                    _token = await _credential.GetTokenAsync(Request, ct);
                }
                catch (AuthenticationFailedException ex)
                {
                    throw new SourceException($"PostgreSQL {host}: no Microsoft Entra token for the agent ({Runner.EndpointSession.Trim(ex.Message)}). "
                        + "Give the agent a managed identity, or a service principal in AZURE_TENANT_ID, AZURE_CLIENT_ID and AZURE_CLIENT_SECRET");
                }
                Identity = PgEntraIdentity.Of(_token.Value.Token);
            }
            return _token!.Value.Token;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>The server refused the token held: the next sign-in asks for a new one.</summary>
    public void Drop() => _token = null;

    /// <summary>
    /// The SQL that maps the role to the identity, as a Microsoft Entra admin of the server runs it in database
    /// <c>postgres</c>; the object id is a placeholder when the token named none.
    /// </summary>
    public static string MappingSql(string role, PgEntraIdentity? identity) =>
        $"SELECT * FROM pgaadauth_create_principal_with_oid('{role}', '{identity?.ObjectId ?? "<object id>"}', "
        + $"'{identity?.ObjectType ?? "service"}', false, false)";
}
