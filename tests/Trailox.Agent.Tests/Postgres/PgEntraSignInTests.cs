using System.Text;
using System.Text.Json;
using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.Postgres;
using Trailox.Agent.Protocol;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Tests.Postgres;

/// <summary>
/// <c>options.azure_sign_in: entra</c>: an Azure Database for PostgreSQL flexible server signed in to with the agent's
/// Microsoft Entra token instead of a password, over TLS with the server's certificate verified.
/// </summary>
public class PgEntraSignInTests
{
    private const string ObjectId = "11111111-2222-3333-4444-555555555555";
    private const string AppId = "66666666-7777-8888-9999-000000000000";

    private static EndpointConfig Entra(string source = "azure_files", string host = "orders.postgres.database.azure.com") => new()
    {
        Alias = "orders",
        Engine = "postgres",
        Host = host,
        Port = 5432,
        Username = "trailox_monitor",
        Options = new Dictionary<string, string>
        {
            ["log_source"] = source,
            ["azure_subscription"] = "00000000-0000-0000-0000-000000000000",
            ["azure_resource_group"] = "rg-databases",
            ["azure_server"] = "orders",
            ["azure_sign_in"] = "entra",
        },
    };

    internal static string Jwt(object claims)
    {
        static string Part(string json) => Convert.ToBase64String(Encoding.UTF8.GetBytes(json)).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        return Part("{\"alg\":\"none\",\"typ\":\"JWT\"}") + "." + Part(JsonSerializer.Serialize(claims)) + ".";
    }

    private static readonly string ServicePrincipalToken = Jwt(new { oid = ObjectId, appid = AppId, tid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", idtyp = "app" });

    /// <summary>A stand-in for the agent's Azure identity: counts the tokens asked for, each valid for <see cref="Lifetime"/>.</summary>
    private sealed class Identity : TokenCredential
    {
        private readonly Func<DateTimeOffset> _now;

        public Identity(Func<DateTimeOffset>? now = null) => _now = now ?? (() => DateTimeOffset.UtcNow);

        public int Asked { get; private set; }
        public TimeSpan Lifetime { get; set; } = TimeSpan.FromHours(1);
        public Exception? Fails { get; set; }
        public string Token { get; set; } = ServicePrincipalToken;

        public override AccessToken GetToken(TokenRequestContext request, CancellationToken ct)
        {
            Asked++;
            Assert.Equal(new[] { PgEntraToken.Scope }, request.Scopes);
            if (Fails != null)
            {
                throw Fails;
            }
            return new AccessToken(Token, _now() + Lifetime);
        }

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext request, CancellationToken ct) => new(GetToken(request, ct));
    }

    private static PostgresException Refused(string sqlState, string text = "password authentication failed for user \"trailox_monitor\"") =>
        new(text, "FATAL", "FATAL", sqlState);

    // ----- agent.yaml -----

    [Theory]
    [InlineData("azure_files")]
    [InlineData("azure_log_analytics")]
    public void An_azure_source_may_sign_in_with_entra(string source)
    {
        Assert.Empty(new PostgresEngine().Validate(Entra(source)));
    }

    [Fact]
    public void Password_is_the_default_and_may_be_written()
    {
        var e = Entra();
        e.Options["azure_sign_in"] = "password";

        Assert.Empty(new PostgresEngine().Validate(e));
        Assert.False(PostgresEngine.EntraSignIn(e));
        Assert.Null(new PostgresEngine().SignsInWithout(e));
    }

    [Theory]
    [InlineData("Entra")]
    [InlineData("aad")]
    [InlineData("")]
    public void Another_sign_in_is_refused(string value)
    {
        var e = Entra();
        e.Options["azure_sign_in"] = value;

        Assert.Contains(new PostgresEngine().Validate(e), p => p.StartsWith("options.azure_sign_in: entra (", StringComparison.Ordinal));
    }

    [Fact]
    public void Entra_sign_in_is_for_azure_only()
    {
        var e = Entra();
        e.Options.Remove("azure_subscription");
        e.Options.Remove("azure_resource_group");
        e.Options.Remove("azure_server");
        e.Options["log_source"] = "rds_api";
        e.Options["instances"] = "orders";
        e.Options["rds_region"] = "eu-west-1";

        Assert.Contains(new PostgresEngine().Validate(e), p => p == "options.azure_sign_in: entra only with log_source: azure_files or "
            + "azure_log_analytics (Azure Database for PostgreSQL flexible server)");
    }

    /// <summary>🔴 The token is a clear-text password: never without TLS, and never to a host that is not the server's own Azure name.</summary>
    [Fact]
    public void Entra_sign_in_needs_tls_and_the_servers_own_azure_name()
    {
        var plain = Entra();
        plain.Tls = false;
        Assert.Contains(new PostgresEngine().Validate(plain), p => p.StartsWith("tls: true with options.azure_sign_in: entra", StringComparison.Ordinal));

        foreach (var host in new[] { "10.0.0.5", "orders.internal.example.com", "orders.postgres.database.azure.com.example.com", ".postgres.database.azure.com" })
        {
            Assert.Contains(new PostgresEngine().Validate(Entra(host: host)), p => p.StartsWith("host: the server's own name", StringComparison.Ordinal));
        }
        Assert.Empty(new PostgresEngine().Validate(Entra(host: "Orders.Postgres.Database.Azure.com")));
    }

    private const string EntraYaml = """
        version: 1
        endpoints:
          - alias: orders
            engine: postgres
            host: orders.postgres.database.azure.com
            username: trailox_monitor
        {0}    options:
              log_source: azure_files
              azure_subscription: 00000000-0000-0000-0000-000000000000
              azure_resource_group: rg-databases
              azure_server: orders
              azure_sign_in: entra
        """;

    [Fact]
    public void An_entra_endpoint_loads_without_a_password()
    {
        var (config, _) = ConfigLoader.Parse(string.Format(EntraYaml, ""), new NoSecrets());

        var e = config.Endpoints.Single();
        Assert.True(PostgresEngine.EntraSignIn(e));
        Assert.Equal("", e.Password);
        Assert.Equal("cloud", e.Kind);
    }

    [Theory]
    [InlineData("password_env: PG_PW")]
    [InlineData("password_file: /run/secrets/pg")]
    public void A_password_with_entra_sign_in_is_refused_not_ignored(string key)
    {
        var ex = Assert.Throws<ConfigException>(() => ConfigLoader.Parse(string.Format(EntraYaml, "    " + key + "\n"), new NoSecrets()));

        Assert.Contains(ex.Problems, p => p == "endpoints[orders]: no password_env or password_file with options.azure_sign_in: entra: "
            + "the agent signs in with a Microsoft Entra token of its Azure identity");
    }

    [Fact]
    public void Without_entra_a_password_is_still_required()
    {
        var yaml = string.Format(EntraYaml, "").Replace("azure_sign_in: entra", "azure_sign_in: password");

        var ex = Assert.Throws<ConfigException>(() => ConfigLoader.Parse(yaml, new NoSecrets()));

        Assert.Contains(ex.Problems, p => p.Contains("exactly one of password_env or password_file is required", StringComparison.Ordinal));
    }

    private sealed class NoSecrets : ISecretReader
    {
        public string? FromEnvironment(string variable) => "pw";

        public string FromFile(string path) => "pw";
    }

    // ----- the connection -----

    /// <summary>
    /// 🔴 The token travels as a clear-text password, so the server's certificate is verified against its name (system CA
    ///    store) whatever tls says, and only a server that asks for a password gets it. A password endpoint is unchanged.
    /// </summary>
    [Fact]
    public void An_entra_sign_in_is_clear_text_only_inside_verified_tls()
    {
        var e = Entra();
        e.Tls = false;   // refused by Validate; the connection still never goes without a verified certificate
        var entra = new NpgsqlConnectionStringBuilder(new PgCatalog(e, new Identity()).ConnectionString("orders.postgres.database.azure.com", "postgres"));

        Assert.Equal(SslMode.VerifyFull, entra.SslMode);
        Assert.Equal("Password", entra.RequireAuth);
        Assert.Equal(ChannelBinding.Disable, entra.ChannelBinding);
        Assert.Null(entra.Password);
        Assert.Null(entra.RootCertificate);
        Assert.False(entra.Pooling);
        Assert.Equal("trailox-agent", entra.ApplicationName);
        Assert.Contains("default_transaction_read_only=on", entra.Options);

        var password = Entra();
        password.Options["azure_sign_in"] = "password";
        var scram = new NpgsqlConnectionStringBuilder(new PgCatalog(password).ConnectionString("orders.postgres.database.azure.com", "postgres"));
        Assert.Equal(SslMode.Require, scram.SslMode);
        Assert.Equal("ScramSHA256", scram.RequireAuth);
        Assert.Equal(ChannelBinding.Require, scram.ChannelBinding);
        Assert.Null(new PgCatalog(password).Entra);
    }

    [Fact]
    public async Task A_token_is_kept_until_five_minutes_before_it_expires()
    {
        var now = new DateTimeOffset(2026, 1, 15, 10, 0, 0, TimeSpan.Zero);
        var identity = new Identity(() => now);
        var token = new PgEntraToken(identity, () => now);

        await token.TokenAsync("h", CancellationToken.None);
        now = now.AddMinutes(54);
        await token.TokenAsync("h", CancellationToken.None);
        Assert.Equal(1, identity.Asked);
        Assert.True(token.Held);

        now = now.AddMinutes(1).AddSeconds(1);   // 4:59 left
        Assert.False(token.Held);
        await token.TokenAsync("h", CancellationToken.None);
        Assert.Equal(2, identity.Asked);

        token.Drop();
        await token.TokenAsync("h", CancellationToken.None);
        Assert.Equal(3, identity.Asked);
    }

    [Fact]
    public async Task No_token_says_what_to_give_the_agent_and_stops_nothing()
    {
        var catalog = new PgCatalog(Entra(), new Identity { Fails = new CredentialUnavailableException("ManagedIdentityCredential authentication unavailable") });
        var opened = 0;

        var ex = await Assert.ThrowsAsync<SourceException>(() => catalog.SignInWithEntraAsync("orders.postgres.database.azure.com",
            _ => Task.FromResult(++opened), CancellationToken.None));

        Assert.False(ex.AuthenticationRejected);
        Assert.Equal(0, opened);
        Assert.StartsWith("PostgreSQL orders.postgres.database.azure.com: no Microsoft Entra token for the agent (ManagedIdentityCredential", ex.Message);
        Assert.Contains("AZURE_TENANT_ID, AZURE_CLIENT_ID and AZURE_CLIENT_SECRET", ex.Message);
    }

    /// <summary>
    /// 🔴 An identity the server does not know signs in once: the first token is always new, so its refusal stops every
    ///    sign-in, and the server's log gets one failed sign-in, not one per task.
    /// </summary>
    [Theory]
    [InlineData(PostgresErrorCodes.InvalidPassword)]
    [InlineData(PostgresErrorCodes.InvalidAuthorizationSpecification)]
    public async Task A_refused_identity_signs_in_once_and_then_not_until_the_agent_restarts(string sqlState)
    {
        var identity = new Identity();
        var catalog = new PgCatalog(Entra(), identity);
        var tries = 0;

        var ex = await Assert.ThrowsAsync<SourceException>(() => catalog.SignInWithEntraAsync<int>("orders.postgres.database.azure.com",
            _ => { tries++; throw Refused(sqlState); }, CancellationToken.None));

        Assert.True(ex.AuthenticationRejected);
        Assert.Equal(1, tries);
        Assert.Equal("PostgreSQL orders.postgres.database.azure.com: the sign-in as trailox_monitor with the agent's Microsoft Entra token was refused "
            + "(password authentication failed for user \"trailox_monitor\")", ex.Message);
        Assert.Equal("as a Microsoft Entra admin of the server, in database postgres, run SELECT * FROM "
            + $"pgaadauth_create_principal_with_oid('trailox_monitor', '{ObjectId}', 'service', false, false) and the setup script's grants",
            ex.AuthenticationFix);

        // A documentation address: had the agent tried to connect, the open would fail on the network, not on the latch.
        var again = await Assert.ThrowsAsync<SourceException>(() => catalog.OpenAsync("192.0.2.1", "postgres", CancellationToken.None));
        Assert.True(again.AuthenticationRejected);
        Assert.Equal(ex.AuthenticationFix, again.AuthenticationFix);
        Assert.Equal(1, identity.Asked);
    }

    /// <summary>A token from the cache that the server refuses (expired by its clock) is replaced once, and the sign-in goes on.</summary>
    [Fact]
    public async Task A_refused_cached_token_is_replaced_once()
    {
        var identity = new Identity();
        var catalog = new PgCatalog(Entra(), identity);
        var tries = 0;
        await catalog.SignInWithEntraAsync("h", _ => Task.FromResult(++tries), CancellationToken.None);

        var result = await catalog.SignInWithEntraAsync("h", _ => ++tries == 2 ? throw Refused(PostgresErrorCodes.InvalidPassword) : Task.FromResult(tries),
            CancellationToken.None);

        Assert.Equal(3, result);
        Assert.Equal(2, identity.Asked);
    }

    [Fact]
    public async Task A_new_token_refused_too_stops_every_sign_in()
    {
        var identity = new Identity();
        var catalog = new PgCatalog(Entra(), identity);
        var tries = 0;
        await catalog.SignInWithEntraAsync("h", _ => Task.FromResult(++tries), CancellationToken.None);

        var ex = await Assert.ThrowsAsync<SourceException>(() => catalog.SignInWithEntraAsync<int>("h",
            _ => { tries++; throw Refused(PostgresErrorCodes.InvalidPassword); }, CancellationToken.None));

        Assert.True(ex.AuthenticationRejected);
        Assert.Equal(3, tries);
        Assert.Equal(2, identity.Asked);
    }

    [Fact]
    public async Task Another_failure_is_not_a_refusal()
    {
        var catalog = new PgCatalog(Entra(), new Identity());
        var tries = 0;

        var ex = await Assert.ThrowsAsync<SourceException>(() => catalog.SignInWithEntraAsync<int>("h",
            _ => { tries++; throw Refused(PostgresErrorCodes.TooManyConnections, "too many connections for role \"trailox_monitor\""); },
            CancellationToken.None));

        Assert.False(ex.AuthenticationRejected);
        Assert.Equal(1, tries);
        Assert.Equal("PostgreSQL h: 53300: too many connections for role \"trailox_monitor\"", ex.Message);
    }

    [Fact]
    public void A_certificate_that_does_not_verify_says_to_use_the_servers_own_name()
    {
        var catalog = new PgCatalog(Entra(), new Identity());
        var npgsql = new NpgsqlException("Exception while performing SSL handshake",
            new System.Security.Authentication.AuthenticationException("The remote certificate was rejected by the provided RemoteCertificateValidationCallback."));

        var message = catalog.Explain("10.0.0.5", PgCatalog.Detail(npgsql));

        Assert.StartsWith("PostgreSQL 10.0.0.5: the server's certificate does not verify (Exception while performing SSL handshake: The remote "
            + "certificate was rejected", message);
        Assert.EndsWith("use the server's own name, <server>.postgres.database.azure.com, never an address or another name", message);
    }

    /// <summary>A server that asks for SCRAM does not take a token: its authentication is set to PostgreSQL only (measured message).</summary>
    [Fact]
    public void A_server_that_asks_for_scram_is_told_to_take_entra()
    {
        var message = new PgCatalog(Entra(), new Identity()).Explain("h", "\"ScramSHA256\" authentication method is not allowed. Allowed methods: Password");

        Assert.StartsWith("PostgreSQL h: the server did not ask for a Microsoft Entra token for role trailox_monitor", message);
        Assert.Contains("\"PostgreSQL and Microsoft Entra authentication\" or \"Microsoft Entra authentication only\"", message);
    }

    /// <summary>A password sent to a server set to Entra only is refused by pg_hba.conf (measured): the probe says what to use.</summary>
    [Fact]
    public void A_password_refused_by_an_entra_only_server_points_at_entra_sign_in()
    {
        var password = Entra();
        password.Options.Remove("azure_sign_in");
        const string refused = "28000: pg_hba.conf rejects connection for host \"192.0.2.10\", user \"trailox_monitor\", database \"postgres\", SSL encryption";

        Assert.Equal($"PostgreSQL h: {refused}. A server set to Microsoft Entra authentication only refuses every password so: sign in with "
            + "options.azure_sign_in: entra", new PgCatalog(password).Explain("h", refused));

        var selfManaged = Entra();
        selfManaged.Options.Remove("azure_sign_in");
        selfManaged.Options["log_source"] = "file";
        Assert.Equal($"PostgreSQL h: {refused}", new PgCatalog(selfManaged).Explain("h", refused));
    }

    /// <summary>The probe error is cut at 600 characters: with a long host and a long refusal, the SQL and the restart still fit.</summary>
    [Fact]
    public async Task The_probe_error_says_what_to_run_and_to_restart()
    {
        var e = Entra(host: new string('a', 63) + ".postgres.database.azure.com");
        var catalog = new PgCatalog(e, new Identity());
        var refused = await Assert.ThrowsAsync<SourceException>(() => catalog.SignInWithEntraAsync<int>(e.Host,
            _ => throw Refused(PostgresErrorCodes.InvalidPassword, new string('x', 400)), CancellationToken.None));
        var session = new EndpointSession(e, new Failing(refused), TimeSpan.FromHours(1));

        await session.ProbeAsync(NullLogger.Instance, CancellationToken.None);

        Assert.Contains($"pgaadauth_create_principal_with_oid('trailox_monitor', '{ObjectId}', 'service', false, false)", session.ProbeError);
        Assert.EndsWith("The agent stops signing in to this database until it restarts: as a Microsoft Entra admin of the server, in database "
            + "postgres, run SELECT * FROM pgaadauth_create_principal_with_oid('trailox_monitor', "
            + $"'{ObjectId}', 'service', false, false) and the setup script's grants, then restart the agent", session.ProbeError);
        Assert.False(session.NeedsProbe);
    }

    private sealed class Failing(SourceException ex) : IEngineSource
    {
        public Task<EndpointCaps> ProbeAsync(CancellationToken ct) => throw ex;

        public Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct) => throw ex;
    }

    // ----- who the token is for -----

    [Fact]
    public void A_service_principals_token_names_its_application_and_object_id()
    {
        var identity = PgEntraIdentity.Of(ServicePrincipalToken)!;

        Assert.Equal($"the service principal of application {AppId} (object id {ObjectId})", identity.ToString());
        Assert.Equal("service", identity.ObjectType);
    }

    [Fact]
    public void A_managed_identitys_token_names_it()
    {
        var token = Jwt(new
        {
            oid = ObjectId,
            appid = AppId,
            idtyp = "app",
            xms_mirid = "/subscriptions/00000000-0000-0000-0000-000000000000/resourcegroups/rg-agents/providers/Microsoft.ManagedIdentity/userAssignedIdentities/agent-identity",
        });

        Assert.Equal($"the managed identity agent-identity (object id {ObjectId})", PgEntraIdentity.Of(token)!.ToString());
    }

    [Theory]
    [InlineData("not-a-token")]
    [InlineData("a.b.c")]
    [InlineData("")]
    public void A_token_that_names_no_object_id_names_no_one(string token)
    {
        Assert.Null(PgEntraIdentity.Of(token));
        Assert.Null(PgEntraIdentity.Of(Jwt(new { oid = "x'); DROP ROLE trailox_monitor; --" })));
        Assert.Contains("'<object id>', 'service'", PgEntraToken.MappingSql("trailox_monitor", null));
    }

    [Fact]
    public void The_check_says_who_signed_in_and_that_the_certificate_was_verified()
    {
        var check = PgProbeChecks.EntraSignIn("orders.postgres.database.azure.com", "trailox_monitor", PgEntraIdentity.Of(ServicePrincipalToken));

        Assert.Equal("entra_sign_in", check.Name);
        Assert.Equal("ok", check.Status);
        Assert.Equal($"signed in as trailox_monitor with a Microsoft Entra token of the service principal of application {AppId} (object id "
            + $"{ObjectId}), over TLS with the server's certificate verified for orders.postgres.database.azure.com", check.Message);
    }
}
