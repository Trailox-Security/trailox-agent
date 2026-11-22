using System.Net;
using System.Security.Cryptography;
using System.Text;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.ClickHouse;
using Trailox.Agent.Engines.Databricks;
using Trailox.Agent.Engines.Snowflake;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests;

/// <summary>
/// An answer the agent cannot use - a proxy's page answered with 200, a request that ran out of time, a
/// reply without a field it needs - ends as a source error that says which, where it used to surface as
/// a bare parser or cancellation error. The agent stopping is still left as it is.
/// </summary>
public class UnexpectedAnswerTests
{
    private const string Workspace = "dbc-00000000-0000.cloud.example.test";
    private const string ProxyPage = "<html><body>Access to this site is blocked by your network policy</body></html>";
    private const string TimeoutMessage = "The request was canceled due to the configured HttpClient.Timeout of 1800 seconds elapsing.";
    private const string Token = """{"access_token":"a-token-for-tests","expires_in":3600}""";

    /// <summary>Plays a host.</summary>
    private sealed class FakeHost : HttpMessageHandler
    {
        private readonly Func<HttpMethod, Uri, HttpResponseMessage> _answer;
        public FakeHost(Func<HttpMethod, Uri, HttpResponseMessage> answer) => _answer = answer;

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(_answer(request.Method, request.RequestUri!));
        }
    }

    private static HttpResponseMessage Answer(string body, string mediaType = "application/json") =>
        new(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, mediaType) };

    // ---- Databricks: the SQL statement API ----

    private static DbxStatements Databricks(Func<HttpResponseMessage> statements)
    {
        var workspace = new FakeHost((_, uri) => uri.AbsolutePath == "/oidc/v1/token" ? Answer(Token) : statements());
        return new DbxStatements(new HttpClient(workspace), new HttpClient(new FakeHost((_, _) => Answer("[]"))),
            new DbxAuth(new HttpClient(workspace), Workspace, "00000000-0000-0000-0000-000000000000", "not-a-real-secret"),
            Workspace, "0123456789abcdef");
    }

    [Fact]
    public async Task Databricks_a_page_answered_with_200_that_is_not_JSON_says_so()
    {
        var statements = Databricks(() => Answer(ProxyPage, "text/html"));

        var ex = await Assert.ThrowsAsync<SourceException>(() => statements.ExecuteAsync("SELECT 1", CancellationToken.None));

        Assert.Contains("not JSON", ex.Message);
        Assert.Contains("blocked by your network policy", ex.Message);
    }

    [Fact]
    public async Task Databricks_a_request_that_ran_out_of_time_says_so()
    {
        var statements = Databricks(() => throw new TaskCanceledException(TimeoutMessage));

        var ex = await Assert.ThrowsAsync<SourceException>(() => statements.ExecuteAsync("SELECT 1", CancellationToken.None));

        Assert.Contains("did not answer a SQL statement request in time", ex.Message);
        Assert.Contains("1800 seconds", ex.Message);
    }

    [Fact]
    public async Task Databricks_an_answer_without_a_field_it_needs_names_the_field()
    {
        var statements = Databricks(() => Answer("""{"statement_id":"stmt-1"}"""));

        var ex = await Assert.ThrowsAsync<SourceException>(() => statements.ExecuteAsync("SELECT 1", CancellationToken.None));

        Assert.Contains("without 'status'", ex.Message);
    }

    [Fact]
    public async Task Databricks_the_agent_stopping_is_left_as_it_is()
    {
        var statements = Databricks(() => Answer(ProxyPage, "text/html"));
        using var stopping = new CancellationTokenSource();
        stopping.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => statements.ExecuteAsync("SELECT 1", stopping.Token));
    }

    // ---- Databricks: the token exchange ----

    private static DbxAuth Auth(Func<HttpResponseMessage> answer) =>
        new(new HttpClient(new FakeHost((_, _) => answer())), Workspace, "00000000-0000-0000-0000-000000000000", "not-a-real-secret");

    /// <summary>A successful token answer is a credential, so an unreadable one is described, never quoted.</summary>
    [Fact]
    public async Task Databricks_a_token_answer_that_is_not_JSON_is_described_and_never_quoted()
    {
        const string body = "<html>Sign in to the proxy. Session reference do-not-echo-this</html>";

        var ex = await Assert.ThrowsAsync<SourceException>(() => Auth(() => Answer(body, "text/html")).TokenAsync(CancellationToken.None));

        Assert.Contains("not JSON", ex.Message);
        Assert.Contains("text/html", ex.Message);
        Assert.DoesNotContain("do-not-echo-this", ex.Message);
    }

    [Fact]
    public async Task Databricks_a_token_answer_without_a_token_says_so()
    {
        var ex = await Assert.ThrowsAsync<DbxException>(() => Auth(() => Answer("""{"token_type":"Bearer"}""")).TokenAsync(CancellationToken.None));

        Assert.Contains("without access_token", ex.Message);
    }

    [Fact]
    public async Task Databricks_a_token_request_that_ran_out_of_time_says_so()
    {
        var ex = await Assert.ThrowsAsync<SourceException>(() => Auth(() => throw new TaskCanceledException(TimeoutMessage)).TokenAsync(CancellationToken.None));

        Assert.Contains("did not answer the token request in time", ex.Message);
    }

    // ---- Snowflake: the SQL API ----

    private static async Task<SfException> SnowflakeFails(Func<HttpResponseMessage> answer)
    {
        // The key pair is made here: a private key written into a test is flagged by the secret scan.
        using var rsa = RSA.Create(2048);
        using var auth = new SfAuth("myorg-myaccount", "trailox_agent", rsa.ExportPkcs8PrivateKeyPem());
        var statements = new SfStatements(new HttpClient(new FakeHost((_, _) => answer())), auth,
            "myorg-myaccount.snowflakecomputing.example.test", "TRAILOX_WH", "TRAILOX_MONITOR");
        return await Assert.ThrowsAsync<SfException>(() => statements.ExecuteAsync("SELECT 1", CancellationToken.None));
    }

    [Fact]
    public async Task Snowflake_a_page_answered_with_200_that_is_not_JSON_says_so()
    {
        var ex = await SnowflakeFails(() => Answer(ProxyPage, "text/html"));

        Assert.Contains("not JSON", ex.Message);
        Assert.Contains("blocked by your network policy", ex.Message);
    }

    [Fact]
    public async Task Snowflake_a_request_that_ran_out_of_time_says_so()
    {
        var ex = await SnowflakeFails(() => throw new TaskCanceledException(TimeoutMessage));

        Assert.Contains("did not answer in time", ex.Message);
    }

    [Fact]
    public async Task Snowflake_an_answer_without_a_field_it_needs_names_the_field()
    {
        var ex = await SnowflakeFails(() => Answer("""{"statementHandle":"01b2-0000"}"""));

        Assert.Contains("without 'resultSetMetaData'", ex.Message);
    }

    // ---- ClickHouse: the row stream ----

    [Fact]
    public async Task ClickHouse_a_row_stream_that_ran_out_of_time_says_so()
    {
        var source = new ChSource(new HttpClient(new FakeHost((_, _) => throw new TaskCanceledException(TimeoutMessage))), new EndpointConfig
        {
            Alias = "prod-cluster", Engine = "clickhouse", Host = "clickhouse.example.test", Port = 8443, Tls = true,
            Username = "trailox_monitor", Password = "not-a-real-password",
        });

        var ex = await Assert.ThrowsAsync<SourceException>(() =>
            source.OpenAsync(new AgentTask { ChunkId = "chunk-a", Stream = ProtocolInfo.Streams.CatalogTables }, CancellationToken.None));

        Assert.Contains("ClickHouse did not answer in time", ex.Message);
    }
}
