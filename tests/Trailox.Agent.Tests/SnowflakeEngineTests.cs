using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.Snowflake;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests;

/// <summary>
/// The SQL the agent runs on a Snowflake account is the whole of what it does there, so its
/// shape is pinned; so are the JWT it signs and the API's result envelope it reads.
/// </summary>
public class SnowflakeEngineTests
{
    private static EndpointConfig Endpoint(bool text = true, params string[] excluded) => new()
    {
        Alias = "snowflake", Engine = "snowflake", Host = "MYORG-MYACCOUNT", ClusterName = "TRAILOX_WH",
        Username = "TRAILOX_AGENT", StoreRawQueryText = text, ExcludedDatabases = excluded.ToList(),
    };

    [Fact]
    public void Registry_knows_snowflake()
    {
        Assert.Equal("snowflake", EngineRegistry.Require("Snowflake").Name);
    }

    [Fact]
    public void Validate_accepts_an_account_defaults_the_role_and_pins_https()
    {
        var e = Endpoint();
        e.Host = "myorg-myaccount.snowflakecomputing.com";
        e.Username = "trailox_agent";
        e.Port = 5439;
        Assert.Empty(new SnowflakeEngine().Validate(e));
        Assert.Equal("myorg-myaccount", e.Host);
        Assert.Equal("TRAILOX_AGENT", e.Username);
        Assert.Equal("TRAILOX_MONITOR", e.Options[SnowflakeEngine.RoleOption]);
        Assert.Equal(443, e.Port);
        Assert.True(e.Tls);
    }

    [Theory]
    [InlineData("host", "https://myorg-acct.snowflakecomputing.com")]
    [InlineData("host", "myorg-acct/path")]
    [InlineData("clusterName", "MY WH")]
    [InlineData("username", "1USER")]
    [InlineData("options.role", "ROLE;DROP")]
    public void Validate_refuses_a_wrong_shape_by_field(string field, string value)
    {
        var e = Endpoint();
        switch (field)
        {
            case "host": e.Host = value; break;
            case "clusterName": e.ClusterName = value; break;
            case "username": e.Username = value; break;
            default: e.Options[SnowflakeEngine.RoleOption] = value; break;
        }
        var problems = new SnowflakeEngine().Validate(e);
        Assert.Single(problems);
        Assert.StartsWith(field + ":", problems[0]);
    }

    [Theory]
    [InlineData("MYORG-MYACCOUNT", "myorg-myaccount.snowflakecomputing.com", "MYORG-MYACCOUNT")]
    [InlineData("myorg-my_account", "myorg-my-account.snowflakecomputing.com", "MYORG-MY_ACCOUNT")]
    [InlineData("xy12345.eu-central-1", "xy12345.eu-central-1.snowflakecomputing.com", "XY12345")]
    public void Account_forms_map_to_the_api_host_and_the_jwt_account(string account, string host, string jwt)
    {
        Assert.Equal(host, SnowflakeEngine.ApiHost(account));
        Assert.Equal(jwt, SnowflakeEngine.JwtAccount(account));
    }

    [Fact]
    public void Jwt_is_rs256_signed_with_the_fingerprinted_issuer_and_at_most_an_hour()
    {
        using var rsa = RSA.Create(2048);
        var pem = rsa.ExportPkcs8PrivateKeyPem();
        using var auth = new SfAuth("myorg-myaccount", "trailox_agent", pem);
        var now = DateTimeOffset.FromUnixTimeSeconds(1789632000);

        var parts = auth.Sign(now).Split('.');
        Assert.Equal(3, parts.Length);
        using var header = JsonDocument.Parse(FromB64(parts[0]));
        Assert.Equal("RS256", header.RootElement.GetProperty("alg").GetString());
        using var payload = JsonDocument.Parse(FromB64(parts[1]));
        var fp = "SHA256:" + Convert.ToBase64String(SHA256.HashData(rsa.ExportSubjectPublicKeyInfo()));
        Assert.Equal("MYORG-MYACCOUNT.TRAILOX_AGENT." + fp, payload.RootElement.GetProperty("iss").GetString());
        Assert.Equal("MYORG-MYACCOUNT.TRAILOX_AGENT", payload.RootElement.GetProperty("sub").GetString());
        var lifetime = payload.RootElement.GetProperty("exp").GetInt64() - payload.RootElement.GetProperty("iat").GetInt64();
        Assert.InRange(lifetime, 60, 3600);
        Assert.True(rsa.VerifyData(Encoding.ASCII.GetBytes(parts[0] + "." + parts[1]), FromB64(parts[2]), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1));
    }

    [Fact]
    public void An_unreadable_key_fails_without_echoing_it()
    {
        // Assembled at run time so the repository's secret scan does not flag a fake key block.
        var armour = "ENCRYPTED PRIVATE" + " KEY-----";
        var secret = "-----BEGIN " + armour + "\nTOPSECRETMATERIAL\n-----END " + armour;
        var ex = Assert.Throws<SfException>(() => new SfAuth("a-b", "u", secret));
        Assert.DoesNotContain("TOPSECRET", ex.Message);
        Assert.Contains("PKCS#8", ex.Message);
    }

    [Fact]
    public async Task A_bad_key_is_the_endpoints_probe_error_not_a_startup_crash()
    {
        var e = Endpoint();
        e.Password = "not a key";
        var source = new SnowflakeEngine().CreateSource(e, new NoHttp());
        await Assert.ThrowsAsync<SfException>(() => source.ProbeAsync(CancellationToken.None));
    }

    [Fact]
    public void Events_windows_on_end_time_and_drops_text_and_binds_when_the_customer_said_so()
    {
        Assert.Equal("SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE END_TIME > TO_TIMESTAMP_LTZ(1000, 6) AND END_TIME <= TO_TIMESTAMP_LTZ(2000, 6)",
            SfRawSelects.Events(Endpoint(), 1000, 2000));
        Assert.StartsWith("SELECT * EXCLUDE (QUERY_TEXT, BIND_VALUES) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY", SfRawSelects.Events(Endpoint(text: false), 1000, 2000));
    }

    [Fact]
    public void Every_stream_the_profile_names_has_a_select_and_nothing_is_joined_or_classified()
    {
        var streams = new[]
        {
            ProtocolInfo.Streams.Events, ProtocolInfo.Streams.AccessHistory, ProtocolInfo.Streams.Logins, ProtocolInfo.Streams.Sessions,
            ProtocolInfo.Streams.CatalogTables, ProtocolInfo.Streams.CatalogColumns, ProtocolInfo.Streams.CatalogUsers, ProtocolInfo.Streams.CatalogGrants,
        };
        foreach (var stream in streams)
        {
            var sql = SfRawSelects.ForTask(Endpoint(), new AgentTask { Stream = stream, StartMicros = 1, EndMicros = 2 });
            Assert.NotNull(sql);
            Assert.StartsWith("SELECT ", sql);
            Assert.DoesNotContain("JOIN", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("FLATTEN", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("CASE", sql);
        }
        Assert.Null(SfRawSelects.ForTask(Endpoint(), new AgentTask { Stream = ProtocolInfo.Streams.LineageTables }));
    }

    [Fact]
    public void Catalog_is_live_objects_outside_snowflake_and_the_customer_list_with_a_text_snapshot_date()
    {
        var sql = SfRawSelects.CatalogTables(Endpoint(true, "HR_PRIVATE", "it's"));
        Assert.StartsWith("SELECT TO_VARCHAR(CURRENT_DATE(), 'YYYY-MM-DD') AS SNAPSHOT_DATE, TABLE_ID,", sql);
        Assert.EndsWith("WHERE DELETED IS NULL AND TABLE_CATALOG NOT IN ('SNOWFLAKE', 'HR_PRIVATE', 'it''s')", sql);
        Assert.EndsWith("GRANTED_ON IN ('TABLE', 'VIEW', 'DATABASE', 'SCHEMA')", SfRawSelects.CatalogGrants);
        Assert.EndsWith("USERS WHERE DELETED_ON IS NULL", SfRawSelects.CatalogUsers);
    }

    [Fact]
    public void The_result_envelope_gives_columns_partitions_and_the_first_rows()
    {
        const string body = """
            {"resultSetMetaData":{"numRows":4068,"format":"jsonv2",
              "partitionInfo":[{"rowCount":158},{"rowCount":1506},{"rowCount":133}],
              "rowType":[{"name":"QUERY_ID","type":"text"},{"name":"END_TIME","type":"timestamp_ltz"}]},
             "data":[["01c7","1789632814.973000000"],["01c8",null]],
             "statementHandle":"01c720d1-0000"}
            """;
        using var doc = JsonDocument.Parse(body);
        var result = SfStatements.Parse(doc.RootElement);
        Assert.Equal(new[] { "QUERY_ID", "END_TIME" }, result.Columns);
        Assert.Equal(3, result.Partitions);
        Assert.Equal(4068, result.TotalRows);
        Assert.Equal("01c720d1-0000", result.Handle);
        Assert.Equal(2, result.FirstRows.GetArrayLength());
    }

    [Fact]
    public void Error_text_is_snowflakes_code_and_message()
    {
        Assert.Equal("002003 SQL compilation error: Object does not exist",
            SfStatements.ErrorText("""{"code":"002003","message":"SQL compilation error: Object does not exist","sqlState":"02000"}"""));
        Assert.Equal("Bad gateway", SfStatements.ErrorText("Bad gateway"));
    }

    private static byte[] FromB64(string s)
    {
        var t = s.Replace('-', '+').Replace('_', '/');
        return Convert.FromBase64String(t.PadRight(t.Length + (4 - t.Length % 4) % 4, '='));
    }

    private sealed class NoHttp : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => new();
    }
}
