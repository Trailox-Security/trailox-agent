using System.Net;
using System.Text;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.Databricks;

namespace Trailox.Agent.Tests;

/// <summary>
/// The statement executor against a workspace and a result-storage host that the test plays.
/// </summary>
/// <remarks>
/// Result pages are presigned links on a SECOND host, fetched with no credentials of ours. That loop
/// is where a strict outbound policy shows up, and until these tests nothing drove it: the parser
/// and the row stream were tested, the fetching was not. 1.3.1 restructured it to name the storage
/// host in its errors, and the first run of the new shape would otherwise have been a customer's.
/// </remarks>
public class DbxStatementsFetchTests
{
    private const string Workspace = "dbc-00000000-0000.cloud.example.test";
    private const string Storage = "results.storage.example.test";
    private const string Signature = "X-Sig=do-not-log-this";

    /// <summary>Plays a host. Requests are disposed by their sender, so what matters is copied out.</summary>
    private sealed class FakeHost : HttpMessageHandler
    {
        private readonly Func<HttpMethod, Uri, HttpResponseMessage> _answer;
        public FakeHost(Func<HttpMethod, Uri, HttpResponseMessage> answer) => _answer = answer;

        public List<(HttpMethod Method, string Path, string? AuthorizationScheme)> Seen { get; } = new();

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Seen.Add((request.Method, request.RequestUri!.AbsolutePath, request.Headers.Authorization?.Scheme));
            return Task.FromResult(_answer(request.Method, request.RequestUri));
        }
    }

    private static HttpResponseMessage Json(string body, HttpStatusCode status = HttpStatusCode.OK) =>
        new(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    private const string Token = """{"access_token":"a-token-for-tests","expires_in":3600}""";

    /// <summary>A finished statement of two pages: the first page's link rides in the answer, the second is asked for.</summary>
    private static readonly string Succeeded = $$"""
        {
          "statement_id": "stmt-1",
          "status": { "state": "SUCCEEDED" },
          "manifest": {
            "schema": { "columns": [ { "name": "user_name" }, { "name": "statements" } ] },
            "total_row_count": 3,
            "total_chunk_count": 2
          },
          "result": { "external_links": [ { "chunk_index": 0, "row_count": 2, "external_link": "https://{{Storage}}/page0?{{Signature}}" } ] }
        }
        """;

    private static readonly string SecondPageLinks = $$"""
        { "external_links": [ { "chunk_index": 1, "row_count": 1, "external_link": "https://{{Storage}}/page1?{{Signature}}" } ] }
        """;

    private static FakeHost HealthyWorkspace() => new((method, uri) =>
        uri.AbsolutePath == "/oidc/v1/token" ? Json(Token)
        : method == HttpMethod.Post && uri.AbsolutePath == "/api/2.0/sql/statements/" ? Json(Succeeded)
        : uri.AbsolutePath == "/api/2.0/sql/statements/stmt-1/result/chunks/1" ? Json(SecondPageLinks)
        : Json("{}", HttpStatusCode.NotFound));

    private static DbxStatements Statements(FakeHost workspace, FakeHost storage) =>
        new(new HttpClient(workspace), new HttpClient(storage),
            new DbxAuth(new HttpClient(workspace), Workspace, "00000000-0000-0000-0000-000000000000", "not-a-real-secret"),
            Workspace, "0123456789abcdef");

    [Fact]
    public async Task Every_page_of_a_result_is_fetched_and_its_rows_come_back_in_order()
    {
        var workspace = HealthyWorkspace();
        var storage = new FakeHost((_, uri) => Json(uri.AbsolutePath == "/page0" ? """[["ana",12],["ben",7]]""" : """[["cyd",3]]"""));
        var statements = Statements(workspace, storage);

        var result = await statements.ExecuteAsync("SELECT user_name, count(*) FROM t GROUP BY 1", CancellationToken.None);
        var rows = new List<string>();
        await foreach (var row in statements.RowsAsync(result, CancellationToken.None))
        {
            rows.Add(row[0].GetString() + ":" + row[1].GetInt32());
        }

        Assert.Equal(new[] { "ana:12", "ben:7", "cyd:3" }, rows);
        Assert.Equal(new[] { "user_name", "statements" }, result.Columns);

        // The workspace is asked as us; the storage host is asked as nobody. A presigned link
        // carries its own authority, and our token has no business travelling to a second host.
        Assert.Equal(new[] { "/page0", "/page1" }, storage.Seen.Select(s => s.Path).ToArray());
        Assert.All(storage.Seen, s => Assert.Null(s.AuthorizationScheme));
        Assert.All(workspace.Seen.Where(s => s.Path.StartsWith("/api/", StringComparison.Ordinal)), s => Assert.Equal("Bearer", s.AuthorizationScheme));
    }

    [Fact]
    public async Task A_result_page_the_network_will_not_let_through_names_the_host_and_never_the_link()
    {
        var storage = new FakeHost((_, _) => throw new HttpRequestException("No route to host"));
        var statements = Statements(HealthyWorkspace(), storage);
        var result = await statements.ExecuteAsync("SELECT 1", CancellationToken.None);

        var ex = await Assert.ThrowsAsync<SourceException>(async () =>
        {
            await foreach (var _ in statements.RowsAsync(result, CancellationToken.None)) { }
        });

        Assert.Contains(Storage, ex.Message);
        Assert.Contains("allow HTTPS to " + Storage, ex.Message);
        Assert.Contains("No route to host", ex.Message);
        Assert.DoesNotContain("X-Sig", ex.Message);              // the link is a bearer credential
        Assert.DoesNotContain("do-not-log-this", ex.Message);
    }

    [Fact]
    public async Task A_result_page_the_storage_refuses_keeps_its_status_and_names_the_host()
    {
        var storage = new FakeHost((_, _) => Json("<Error>AccessDenied</Error>", HttpStatusCode.Forbidden));
        var statements = Statements(HealthyWorkspace(), storage);
        var result = await statements.ExecuteAsync("SELECT 1", CancellationToken.None);

        var ex = await Assert.ThrowsAsync<DbxException>(async () =>
        {
            await foreach (var _ in statements.RowsAsync(result, CancellationToken.None)) { }
        });

        Assert.Equal(403, ex.StatusCode);
        Assert.Contains(Storage, ex.Message);
        Assert.DoesNotContain("do-not-log-this", ex.Message);
    }

    /// <summary>
    /// What a workspace says when it will not start its warehouse, word for word as one did. The
    /// agent reports the warehouse's own sentence, because it is the only thing that says why.
    /// </summary>
    [Fact]
    public async Task A_statement_the_warehouse_will_not_run_reports_the_warehouses_own_words()
    {
        const string refusal = """
            {"error_code":"BAD_REQUEST","message":"Cannot start warehouse 'Serverless Starter Warehouse' with Serverless Compute since it is disabled in global warehouse config. To use the warehouse, please contact your administrator."}
            """;
        var workspace = new FakeHost((method, uri) =>
            uri.AbsolutePath == "/oidc/v1/token" ? Json(Token) : Json(refusal, HttpStatusCode.BadRequest));
        var statements = Statements(workspace, new FakeHost((_, _) => Json("[]")));

        var ex = await Assert.ThrowsAsync<DbxException>(() => statements.ExecuteAsync("SELECT 1", CancellationToken.None));

        Assert.Equal(400, ex.StatusCode);
        Assert.Contains("Serverless Compute", ex.Message);
        Assert.Contains("disabled in global warehouse config", ex.Message);
    }
}
