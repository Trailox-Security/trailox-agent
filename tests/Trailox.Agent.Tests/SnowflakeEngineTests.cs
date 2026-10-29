using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
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

    /// <summary>
    /// 1.15: Snowsight's data-sharing form org.account names no host, so it is read as org-account; a region, which
    /// always has a '-', and a private-link suffix are left as they are, and the legacy Azure private-link form fits.
    /// </summary>
    [Theory]
    [InlineData("myorg.myaccount", "myorg-myaccount")]
    [InlineData("MyOrg.My_Account.snowflakecomputing.com", "MyOrg-My_Account")]
    [InlineData("myorg-myaccount", "myorg-myaccount")]
    [InlineData("myorg-myaccount.privatelink", "myorg-myaccount.privatelink")]
    [InlineData("myorg.myaccount.privatelink", "myorg-myaccount.privatelink")]
    [InlineData("xy12345.fhplus.us-gov-west-1.aws.privatelink", "xy12345.fhplus.us-gov-west-1.aws.privatelink")]
    [InlineData("xy12345", "xy12345")]
    [InlineData("xy12345.privatelink", "xy12345.privatelink")]
    [InlineData("xy12345.eu-central-1", "xy12345.eu-central-1")]
    [InlineData("xy12345.east-us-2.azure", "xy12345.east-us-2.azure")]
    [InlineData("xy12345.west-europe.azure.privatelink", "xy12345.west-europe.azure.privatelink")]
    public void Validate_reads_every_account_form_snowsight_shows(string host, string normalized)
    {
        var e = Endpoint();
        e.Host = host;
        Assert.Empty(new SnowflakeEngine().Validate(e));
        Assert.Equal(normalized, e.Host);
    }

    [Theory]
    [InlineData("myorg.myaccount", "myorg-myaccount.snowflakecomputing.com", "MYORG-MYACCOUNT")]
    [InlineData("myorg.myaccount.privatelink", "myorg-myaccount.privatelink.snowflakecomputing.com", "MYORG-MYACCOUNT")]
    [InlineData("xy12345.west-europe.azure.privatelink", "xy12345.west-europe.azure.privatelink.snowflakecomputing.com", "XY12345")]
    [InlineData("MYORG-MYACCOUNT", "myorg-myaccount.snowflakecomputing.com", "MYORG-MYACCOUNT")]
    [InlineData("myorg-my_account", "myorg-my-account.snowflakecomputing.com", "MYORG-MY_ACCOUNT")]
    [InlineData("xy12345.eu-central-1", "xy12345.eu-central-1.snowflakecomputing.com", "XY12345")]
    public void Account_forms_map_to_the_api_host_and_the_jwt_account(string account, string host, string jwt)
    {
        var e = Endpoint();
        e.Host = account;
        Assert.Empty(new SnowflakeEngine().Validate(e));
        Assert.Equal(host, SnowflakeEngine.ApiHost(e.Host));
        Assert.Equal(jwt, SnowflakeEngine.JwtAccount(e.Host));
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

    /// <summary>1.15: the values a statement was bound to are never read; the text only when the customer sends it.</summary>
    [Fact]
    public void Events_windows_on_end_time_never_reads_bind_values_and_drops_text_when_the_customer_said_so()
    {
        Assert.Equal("SELECT * EXCLUDE (BIND_VALUES) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE END_TIME > TO_TIMESTAMP_LTZ(1000, 6) AND END_TIME <= TO_TIMESTAMP_LTZ(2000, 6)",
            SfRawSelects.Events(Endpoint(), 1000, 2000));
        Assert.StartsWith("SELECT * EXCLUDE (QUERY_TEXT, BIND_VALUES) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY", SfRawSelects.Events(Endpoint(text: false), 1000, 2000));
    }

    private const long Start = 1_790_000_000_000_000;
    private const long End = Start + 3_600_000_000;
    private const string WindowStatementsSql = "WITH wq AS (SELECT {0} FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY " +
        "WHERE END_TIME > TO_TIMESTAMP_LTZ(1790000000000000, 6) AND END_TIME <= TO_TIMESTAMP_LTZ(1790003600000000, 6)) ";

    /// <summary>
    /// 1.15: access history is the rows of the statements that ENDED in the window, so a statement that ran for hours has
    /// them once it is complete, from a literal start.
    /// </summary>
    [Fact]
    public void Access_history_is_the_rows_of_the_statements_that_ended_in_the_window()
    {
        Assert.Equal(
            string.Format(WindowStatementsSql, "QUERY_ID") +
            "SELECT a.* FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a " +
            "WHERE a.QUERY_START_TIME >= TO_TIMESTAMP_LTZ(1789999000000000, 6) AND a.QUERY_START_TIME <= TO_TIMESTAMP_LTZ(1790003600000000, 6) " +
            "AND a.QUERY_ID IN (SELECT wq.QUERY_ID FROM wq)",
            SfRawSelects.AccessHistory(Start, End, 1_789_999_000_000_000));
        Assert.Equal("SELECT COALESCE(DATE_PART(epoch_microsecond, MIN(START_TIME)), 0) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY " +
                     "WHERE END_TIME > TO_TIMESTAMP_LTZ(1790000000000000, 6) AND END_TIME <= TO_TIMESTAMP_LTZ(1790003600000000, 6)",
            SfRawSelects.StatementsEarliestStart(Start, End));
    }

    /// <summary>
    /// The read starts a minute before the window's first statement, a day before the window at most, at its end when it
    /// has none, and a day before it when the first start is not known.
    /// </summary>
    [Theory]
    [InlineData(Start + 600_000_000, Start + 540_000_000)]                    // short statements only: about the window itself
    [InlineData(Start - 600_000_000, Start - 660_000_000)]                    // a 10-minute statement: a minute before it
    [InlineData(Start - 5 * 3_600_000_000L, Start - 5 * 3_600_000_000L - 60_000_000)]   // a 5-hour one
    [InlineData(Start - 24 * 3_600_000_000L + 60_000_001, Start - 24 * 3_600_000_000L + 1)]   // just inside the day
    [InlineData(Start - 30 * 3_600_000_000L, Start - 24 * 3_600_000_000L)]   // 30 hours: a day at most
    [InlineData(0L, End)]                                                     // no statement: nothing to read
    [InlineData(null, Start - 24 * 3_600_000_000L)]                           // not known: the whole day, never nothing
    public void The_access_read_starts_at_the_windows_first_statement(long? earliestStart, long from)
    {
        Assert.Equal(from, SfRawSelects.AccessFrom(Start, End, earliestStart));
    }

    /// <summary>An access history task first asks for the window's first statement start, then reads from that literal.</summary>
    [Theory]
    [InlineData("1789999400000000", Start - 660_000_000)]          // a minute before the first statement
    [InlineData("0", End)]                                         // no statement: reads nothing
    [InlineData("1.7899994E+15", Start - 24 * 3_600_000_000L)]     // not a whole number: the whole day, never nothing
    public async Task An_access_history_task_reads_from_its_first_statement_start(string earliest, long from)
    {
        var api = new FakeSqlApi(sql => sql.Contains("MIN(START_TIME)") ? Rows(earliest) : Rows("x"));
        var source = Source(api, TimeProvider.System);

        await using var rows = await source.OpenAsync(new AgentTask { Stream = ProtocolInfo.Streams.AccessHistory, StartMicros = Start, EndMicros = End }, CancellationToken.None);

        Assert.Equal(2, api.Statements.Count);
        Assert.Equal(SfRawSelects.StatementsEarliestStart(Start, End), api.Statements[0]);
        Assert.Equal(SfRawSelects.AccessHistory(Start, End, from), api.Statements[1]);
    }

    /// <summary>
    /// 1.15: the sessions created in the window, and the older sessions of the statements that ended in it, whatever
    /// their age: a session opened before the first window, or a pooled one kept for weeks.
    /// </summary>
    [Fact]
    public void Sessions_are_those_created_in_the_window_and_those_of_its_statements()
    {
        Assert.Equal(
            string.Format(WindowStatementsSql, "DISTINCT SESSION_ID") +
            "SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS " +
            "WHERE CREATED_ON > TO_TIMESTAMP_LTZ(1790000000000000, 6) AND CREATED_ON <= TO_TIMESTAMP_LTZ(1790003600000000, 6) " +
            "UNION ALL SELECT s.* FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS s WHERE s.CREATED_ON <= TO_TIMESTAMP_LTZ(1790000000000000, 6) " +
            "AND s.SESSION_ID IN (SELECT wq.SESSION_ID FROM wq)",
            SfRawSelects.Sessions(Start, End));
        Assert.Equal(SfRawSelects.Sessions(Start, End),
            SfRawSelects.ForTask(Endpoint(), new AgentTask { Stream = ProtocolInfo.Streams.Sessions, StartMicros = Start, EndMicros = End }));
    }

    /// <summary>
    /// 1.15: the oldest statement and sign-in are each a scan of a whole view, read at most once a day rather than at
    /// every probe. A failed read waits for the next day as well, and keeps the last value read.
    /// </summary>
    [Fact]
    public async Task The_oldest_rows_are_read_at_most_once_a_day_and_a_failed_read_keeps_the_last_value()
    {
        var (failEvent, failSession) = (false, true);
        var api = new FakeSqlApi(sql => sql switch
        {
            _ when sql.Contains("CURRENT_VERSION") => Rows("9.30.1"),
            _ when sql.Contains("MIN(END_TIME)") && failEvent => Refused("000630", "Statement reached its statement or warehouse timeout"),
            _ when sql.Contains("MIN(END_TIME)") => Rows("1700000000000000"),
            _ when sql.Contains("MIN(EVENT_TIMESTAMP)") && failSession => Refused("000630", "Statement reached its statement or warehouse timeout"),
            _ when sql.Contains("MIN(EVENT_TIMESTAMP)") => Rows("1690000000000000"),
            _ => Rows("5"),
        });
        var clock = new ManualClock(DateTimeOffset.Parse("2030-01-01T00:00:00Z"));
        var source = Source(api, clock);
        async Task<(long, long)> Probe()
        {
            var caps = await source.ProbeAsync(CancellationToken.None);
            return (caps.EarliestEventMicros, caps.EarliestSessionMicros);
        }

        var first = await Probe();
        failSession = false;
        clock.Now += TimeSpan.FromHours(1);
        var anHourLater = await Probe();
        clock.Now += TimeSpan.FromDays(1);
        var aDayLater = await Probe();
        failEvent = true;
        clock.Now += TimeSpan.FromDays(1);
        var failedNext = await Probe();

        Assert.Equal((1700000000000000L, 0L), first);
        Assert.Equal((1700000000000000L, 0L), anHourLater);                              // nothing read again within the day
        Assert.Equal((1700000000000000L, 1690000000000000L), aDayLater);
        Assert.Equal((1700000000000000L, 1690000000000000L), failedNext);                // the last value read is kept
        Assert.Equal(3, api.Statements.Count(s => s.Contains("MIN(END_TIME)")));
        Assert.Equal(3, api.Statements.Count(s => s.Contains("MIN(EVENT_TIMESTAMP)")));
        Assert.Equal(4, api.Statements.Count(s => s.Contains("CURRENT_VERSION")));
    }

    /// <summary>1.15: a 500 or 502 is retried like the other transient answers, and the resubmit says it is one.</summary>
    [Theory]
    [InlineData(HttpStatusCode.InternalServerError)]
    [InlineData(HttpStatusCode.BadGateway)]
    public async Task A_transient_failure_is_resubmitted_under_the_same_request_id_with_retry_set(HttpStatusCode transient)
    {
        var calls = 0;
        var api = new FakeSqlApi(_ => ++calls == 1 ? new HttpResponseMessage(transient) { Content = new StringContent("try again") } : Rows("1"));

        Assert.Equal("1", await Statements(api).ScalarAsync("SELECT 1", CancellationToken.None));

        Assert.Equal(2, api.Posts.Count);
        var (first, resubmit) = (api.Posts[0].Query, api.Posts[1].Query);
        Assert.StartsWith("?requestId=", first);
        Assert.DoesNotContain("retry=true", first);
        Assert.Equal(first + "&retry=true", resubmit);
    }

    /// <summary>
    /// 1.15: Snowflake stopping a statement at a time limit marks the failure, so a shorter window is asked for: the API's
    /// 408 for the request's own timeout, and the timeout's error code for a warehouse's, user's or account's. A proxy's
    /// own 408 page and any other refusal are not.
    /// </summary>
    [Theory]
    [InlineData(HttpStatusCode.RequestTimeout, "{\"code\":\"000630\",\"message\":\"Statement reached its statement or warehouse timeout of 1,800 second(s) and was canceled.\",\"statementHandle\":\"01c7-0000\"}", true)]
    [InlineData(HttpStatusCode.RequestTimeout, "{\"message\":\"timeout\",\"statementHandle\":\"01c7-0000\"}", true)]
    [InlineData(HttpStatusCode.UnprocessableEntity, "{\"code\":\"000630\",\"message\":\"Statement reached its statement or warehouse timeout of 600 second(s) and was canceled.\"}", true)]
    [InlineData(HttpStatusCode.RequestTimeout, "<html><body>408 Request Time-out</body></html>", false)]
    [InlineData(HttpStatusCode.UnprocessableEntity, "{\"code\":\"002003\",\"message\":\"SQL compilation error: Object does not exist\"}", false)]
    public async Task A_statement_stopped_at_its_time_limit_says_the_window_was_too_long(HttpStatusCode status, string body, bool tooLong)
    {
        var ex = await Assert.ThrowsAsync<SfException>(() => Statements(new FakeSqlApi(_ =>
            new HttpResponseMessage(status) { Content = new StringContent(body) })).ExecuteAsync("SELECT 1", CancellationToken.None));

        Assert.Equal(tooLong, ex.WindowTooLong);
    }

    /// <summary>A statement still running (202) is polled by GET: a timeout there is flagged too, and a GET never says retry.</summary>
    [Fact]
    public async Task A_statement_that_times_out_while_polled_says_so_and_a_poll_never_says_retry()
    {
        var api = new FakeSqlApi(_ => new HttpResponseMessage(HttpStatusCode.Accepted)
        {
            Content = new StringContent("{\"statementHandle\":\"01c7-0000\",\"message\":\"Asynchronous execution in progress.\"}"),
        })
        {
            OnGet = () => Refused("000630", "Statement reached its statement or warehouse timeout of 1,800 second(s) and was canceled."),
        };

        var ex = await Assert.ThrowsAsync<SfException>(() => Statements(api).ExecuteAsync("SELECT 1", CancellationToken.None));

        Assert.True(ex.WindowTooLong);
        Assert.Equal("/api/v2/statements/01c7-0000", Assert.Single(api.Gets));
    }

    [Fact]
    public void Every_stream_reads_only_its_own_views_and_nothing_is_joined_flattened_or_classified()
    {
        var streams = new[]
        {
            ProtocolInfo.Streams.Events, ProtocolInfo.Streams.AccessHistory, ProtocolInfo.Streams.Logins, ProtocolInfo.Streams.Sessions,
            ProtocolInfo.Streams.CatalogTables, ProtocolInfo.Streams.CatalogColumns, ProtocolInfo.Streams.CatalogUsers, ProtocolInfo.Streams.CatalogGrants,
            ProtocolInfo.Streams.CatalogCredentials,
        };
        foreach (var stream in streams)
        {
            var sql = SfRawSelects.ForTask(Endpoint(), new AgentTask { Stream = stream, StartMicros = 1, EndMicros = 2 });
            Assert.NotNull(sql);
            Assert.Matches(@"^(SELECT |WITH wq AS \(SELECT )", sql);
            // The window's statements are the only second view a read may name: access history and sessions keep to them.
            var views = Regex.Matches(sql!, @"ACCOUNT_USAGE\.([A-Z_]+)").Select(m => m.Groups[1].Value).Distinct().Order().ToArray();
            var expected = stream switch
            {
                ProtocolInfo.Streams.Events => new[] { "QUERY_HISTORY" },
                ProtocolInfo.Streams.AccessHistory => new[] { "ACCESS_HISTORY", "QUERY_HISTORY" },
                ProtocolInfo.Streams.Logins => new[] { "LOGIN_HISTORY" },
                ProtocolInfo.Streams.Sessions => new[] { "QUERY_HISTORY", "SESSIONS" },
                _ => views.Length == 1 ? views : new[] { "one view" },
            };
            Assert.Equal(expected, views);
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

    /// <summary>
    /// 1.11: the credential list and four more user columns. They are sent by these lower-cased names, as
    /// docs/PROTOCOL.md lists them, so the names are the contract.
    /// </summary>
    [Fact]
    public void Credentials_and_the_new_user_columns_are_read_by_name_and_never_a_secret()
    {
        Assert.Equal(
            "SELECT TO_VARCHAR(CURRENT_DATE(), 'YYYY-MM-DD') AS SNAPSHOT_DATE, CREDENTIAL_ID, NAME, USER_NAME, TYPE, DOMAIN, COMMENT, STATUS, " +
            "ADDITIONAL_DETAILS, CREATED_BY, LAST_ALTERED_BY, CREATED_ON, LAST_USED_ON, LAST_ALTERED, EXPIRATION_DATE " +
            "FROM SNOWFLAKE.ACCOUNT_USAGE.CREDENTIALS",
            SfRawSelects.CatalogCredentials);
        Assert.Equal(SfRawSelects.CatalogCredentials,
            SfRawSelects.ForTask(Endpoint(), new AgentTask { Stream = ProtocolInfo.Streams.CatalogCredentials }));
        Assert.Equal("catalog_credentials", ProtocolInfo.Streams.CatalogCredentials);

        Assert.Contains(", OWNER, HAS_WORKLOAD_IDENTITY, PASSWORD_LAST_SET_TIME, DISPLAY_NAME, EMAIL FROM ", SfRawSelects.CatalogUsers);
        // Leading ", " so HAS_PASSWORD and HAS_RSA_PUBLIC_KEY (flags, not secrets) do not match.
        foreach (var never in new[] { "FIRST_NAME", "LAST_NAME", ", RSA_PUBLIC_KEY", ", PASSWORD," })
        {
            Assert.DoesNotContain(never, SfRawSelects.CatalogUsers);
        }
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

    private static string Pem()
    {
        using var rsa = RSA.Create(2048);
        return rsa.ExportPkcs8PrivateKeyPem();
    }

    private static SfSource Source(FakeSqlApi api, TimeProvider clock)
    {
        var e = Endpoint();
        e.Password = Pem();
        return new SfSource(new HttpClient(api), e, clock);
    }

    private sealed class ManualClock(DateTimeOffset start) : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = start;

        public override DateTimeOffset GetUtcNow() => Now;
    }

    private static SfStatements Statements(FakeSqlApi api) =>
        new(new HttpClient(api), new SfAuth("myorg-myaccount", "trailox_agent", Pem()), "myorg-myaccount.snowflakecomputing.com", "TRAILOX_WH", "TRAILOX_MONITOR");

    /// <summary>A settled one-column result whose single row holds <paramref name="value"/>.</summary>
    private static HttpResponseMessage Rows(string value) => new(HttpStatusCode.OK)
    {
        Content = new StringContent($$"""{"resultSetMetaData":{"numRows":1,"rowType":[{"name":"V"}]},"data":[["{{value}}"]],"statementHandle":"01c7-0000"}""",
            Encoding.UTF8, "application/json"),
    };

    private static HttpResponseMessage Refused(string code, string message) => new(HttpStatusCode.UnprocessableEntity)
    {
        Content = new StringContent($$"""{"code":"{{code}}","message":"{{message}}","sqlState":"57014"}""", Encoding.UTF8, "application/json"),
    };

    /// <summary>Plays the SQL API: answers each submitted statement as a test says, and records what was sent.</summary>
    private sealed class FakeSqlApi : HttpMessageHandler
    {
        private readonly Func<string, HttpResponseMessage> _answer;
        public FakeSqlApi(Func<string, HttpResponseMessage> answer) => _answer = answer;

        public List<string> Statements { get; } = new();
        public List<(string Query, string Statement)> Posts { get; } = new();
        public List<string> Gets { get; } = new();

        /// <summary>The answer to a poll of a running statement (a GET of its handle).</summary>
        public Func<HttpResponseMessage>? OnGet { get; init; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Method == HttpMethod.Get)
            {
                Gets.Add(request.RequestUri!.PathAndQuery);
                return OnGet!();
            }
            using var body = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(cancellationToken));
            var statement = body.RootElement.GetProperty("statement").GetString()!;
            Statements.Add(statement);
            Posts.Add((request.RequestUri!.Query, statement));
            return _answer(statement);
        }
    }
}
