using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.Databricks;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests;

/// <summary>
/// The service principal directory (agent 1.6): the one Databricks read that is not a statement.
/// It is pinned as closely as the SQL is, because it is part of what the agent does on a workspace.
/// </summary>
public class DbxDirectoryTests
{
    private const string Workspace = "dbc-00000000-0000.cloud.example.test";
    private const string Token = """{"access_token":"a-token-for-tests","expires_in":3600}""";

    private const string AppA = "00000000-0000-0000-0000-00000000000a";
    private const string AppB = "00000000-0000-0000-0000-00000000000b";
    private const string AppC = "00000000-0000-0000-0000-00000000000c";

    private const string Day = "2026-09-26";
    private static readonly DateTime LastMinute = new(2026, 9, 26, 23, 59, 30, DateTimeKind.Utc);

    /// <summary>Plays the workspace. Requests are disposed by their sender, so what matters is copied out.</summary>
    private sealed class FakeHost : HttpMessageHandler
    {
        private readonly Func<HttpMethod, Uri, HttpResponseMessage> _answer;
        public FakeHost(Func<HttpMethod, Uri, HttpResponseMessage> answer) => _answer = answer;

        public List<(HttpMethod Method, string PathAndQuery, string? AuthorizationScheme)> Seen { get; } = new();

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Seen.Add((request.Method, request.RequestUri!.PathAndQuery, request.Headers.Authorization?.Scheme));
            return Task.FromResult(_answer(request.Method, request.RequestUri));
        }
    }

    private static HttpResponseMessage Json(string body, HttpStatusCode status = HttpStatusCode.OK) =>
        new(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    private static JsonElement Parse(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }

    private static string[] Cells(JsonElement row) => row.EnumerateArray().Select(c => c.GetString() ?? "").ToArray();

    /// <summary>A listing run on the real clock is dated today, or yesterday if it straddled midnight.</summary>
    private static void AssertDatedNow(string date) =>
        Assert.InRange(DateTime.ParseExact(date, "yyyy-MM-dd", CultureInfo.InvariantCulture),
            DateTime.UtcNow.Date.AddDays(-1), DateTime.UtcNow.Date);

    private static EndpointConfig Endpoint() => new()
    {
        Alias = "lakehouse", Engine = "databricks", Host = Workspace,
        ClusterName = "0123456789abcdef", Username = "00000000-1111-2222-3333-444444444444", Password = "not-a-real-secret",
    };

    [Fact]
    public void The_listing_asks_for_three_attributes_a_page_at_a_time_and_dates_each_row()
    {
        Assert.Equal("/api/2.0/preview/scim/v2/ServicePrincipals?attributes=applicationId,displayName,active&startIndex=1&count=100",
            DbxDirectory.PageQuery(1));
        Assert.Equal(new[] { "snapshot_date", "application_id", "display_name", "active" }, DbxDirectory.Columns);
    }

    [Fact]
    public void A_page_becomes_rows_of_text_and_an_entry_without_an_application_id_is_dropped()
    {
        var page = DbxDirectory.ParsePage(Parse($$"""
            { "totalResults": 5, "Resources": [
                { "applicationId": "{{AppA}}", "displayName": "etl-nightly", "active": true },
                { "applicationId": "{{AppB}}", "active": false },
                { "applicationId": "{{AppC}}", "displayName": "reporting", "active": "false" },
                { "displayName": "no id, cannot be joined to anything" },
                null ] }
            """), Day);

        Assert.Equal(5, page.TotalResults);
        Assert.Equal(5, page.ResourceCount);          // every entry, usable or not
        Assert.Equal(new[] { Day, AppA, "etl-nightly", "true" }, Cells(page.Rows[0]));
        Assert.Equal(new[] { Day, AppB, "", "false" }, Cells(page.Rows[1]));
        Assert.Equal(new[] { Day, AppC, "reporting", "false" }, Cells(page.Rows[2]));
        Assert.Equal(3, page.Rows.Count);
    }

    [Fact]
    public void An_empty_directory_leaves_out_Resources_and_reads_as_an_empty_page()
    {
        var page = DbxDirectory.ParsePage(Parse("""{ "totalResults": 0, "itemsPerPage": 0, "startIndex": 1 }"""), Day);

        Assert.Empty(page.Rows);
        Assert.Equal(0, page.ResourceCount);
        Assert.Equal(0, page.TotalResults);
    }

    [Fact]
    public void An_absent_active_flag_reads_as_active()
    {
        var page = DbxDirectory.ParsePage(Parse($$"""{ "Resources": [ { "applicationId": "{{AppA}}", "displayName": "etl" } ] }"""), Day);

        Assert.Equal("true", Cells(page.Rows[0])[3]);
        Assert.Null(page.TotalResults);
    }

    private static Func<string, CancellationToken, Task<JsonElement>> Directory(int total, int pageSize, List<int> asked, bool statesTotal = true) =>
        (path, _) =>
        {
            var start = int.Parse(path.Split("startIndex=")[1].Split('&')[0]);
            asked.Add(start);
            var entries = Enumerable.Range(start, Math.Max(0, Math.Min(pageSize, total - start + 1)))
                .Select(i => $$"""{ "applicationId": "00000000-0000-0000-0000-{{i:D12}}", "displayName": "sp-{{i}}", "active": true }""");
            var totalPart = statesTotal ? $"\"totalResults\": {total}, " : "";
            return Task.FromResult(Parse($"{{ {totalPart}\"Resources\": [ {string.Join(", ", entries)} ] }}"));
        };

    [Fact]
    public async Task Pages_are_read_until_the_stated_total()
    {
        var asked = new List<int>();

        var rows = await DbxDirectory.ListAsync(Directory(total: 250, pageSize: 100, asked), LastMinute, CancellationToken.None);

        Assert.Equal(250, rows.Count);
        Assert.Equal(new[] { 1, 101, 201 }, asked);
        Assert.Equal(250, rows.Select(r => r[1].GetString()).Distinct().Count());
    }

    /// <summary>A listing that runs past midnight is still one day's snapshot.</summary>
    [Fact]
    public async Task Every_row_of_a_listing_carries_the_date_it_started()
    {
        var rows = await DbxDirectory.ListAsync(Directory(total: 250, pageSize: 100, new List<int>()), LastMinute, CancellationToken.None);

        Assert.All(rows, r => Assert.Equal(Day, r[0].GetString()));
    }

    [Fact]
    public async Task Without_a_total_a_short_page_is_the_last()
    {
        var asked = new List<int>();

        var rows = await DbxDirectory.ListAsync(Directory(total: 150, pageSize: 100, asked, statesTotal: false), LastMinute, CancellationToken.None);

        Assert.Equal(150, rows.Count);
        Assert.Equal(new[] { 1, 101 }, asked);
    }

    /// <summary>A directory that ignores startIndex would otherwise return its first page forever.</summary>
    [Fact]
    public async Task A_page_with_nothing_new_ends_the_listing()
    {
        var calls = 0;
        var firstPage = Parse($$"""
            { "totalResults": 1000, "Resources": [
                { "applicationId": "{{AppA}}", "displayName": "a" }, { "applicationId": "{{AppB}}", "displayName": "b" } ] }
            """);

        var rows = await DbxDirectory.ListAsync((_, _) => { calls++; return Task.FromResult(firstPage); }, LastMinute, CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Equal(2, calls);
    }

    /// <summary>
    /// Paging advances on the page's full size: counted after dropping an unusable entry, a full
    /// page would look like the short last one and end the listing early.
    /// </summary>
    [Fact]
    public async Task A_dropped_entry_does_not_end_the_listing_early()
    {
        var asked = new List<int>();
        Func<string, CancellationToken, Task<JsonElement>> get = (path, _) =>
        {
            var start = int.Parse(path.Split("startIndex=")[1].Split('&')[0]);
            asked.Add(start);
            var entries = Enumerable.Range(start, start == 1 ? 100 : 1)
                .Select(i => i == 1
                    ? """{ "displayName": "no id" }"""
                    : $$"""{ "applicationId": "00000000-0000-0000-0000-{{i:D12}}", "displayName": "sp-{{i}}" }""");
            return Task.FromResult(Parse($"{{ \"Resources\": [ {string.Join(", ", entries)} ] }}"));
        };

        var rows = await DbxDirectory.ListAsync(get, LastMinute, CancellationToken.None);

        Assert.Equal(new[] { 1, 101 }, asked);
        Assert.Equal(100, rows.Count);
    }

    [Fact]
    public async Task The_directory_is_asked_with_the_principals_own_token()
    {
        var workspace = new FakeHost((_, uri) =>
            uri.AbsolutePath == "/oidc/v1/token" ? Json(Token)
            : Json($$"""{ "totalResults": 1, "Resources": [ { "applicationId": "{{AppA}}", "displayName": "etl-nightly", "active": true } ] }"""));
        var directory = new DbxDirectory(new HttpClient(workspace),
            new DbxAuth(new HttpClient(workspace), Workspace, "00000000-0000-0000-0000-000000000000", "not-a-real-secret"), Workspace);

        var rows = await directory.ListAsync(CancellationToken.None);

        var cells = Cells(Assert.Single(rows));
        AssertDatedNow(cells[0]);
        Assert.Equal(new[] { AppA, "etl-nightly", "true" }, cells[1..]);
        var listing = Assert.Single(workspace.Seen, s => s.PathAndQuery.StartsWith(DbxDirectory.ListPath, StringComparison.Ordinal));
        Assert.Equal(HttpMethod.Get, listing.Method);
        Assert.Equal("Bearer", listing.AuthorizationScheme);
    }

    /// <summary>
    /// A workspace that will not let the principal list its directory answers 403. The failure
    /// carries the status in the same words as every other Databricks failure of the agent.
    /// </summary>
    [Theory]
    [InlineData(HttpStatusCode.Forbidden, 403)]
    [InlineData(HttpStatusCode.NotFound, 404)]
    public async Task A_refused_listing_is_a_source_error_that_keeps_its_status(HttpStatusCode status, int code)
    {
        var workspace = new FakeHost((_, uri) =>
            uri.AbsolutePath == "/oidc/v1/token" ? Json(Token) : Json("""{"error_code":"PERMISSION_DENIED","message":"no"}""", status));
        var directory = new DbxDirectory(new HttpClient(workspace),
            new DbxAuth(new HttpClient(workspace), Workspace, "00000000-0000-0000-0000-000000000000", "not-a-real-secret"), Workspace);

        var ex = await Assert.ThrowsAsync<DbxException>(() => directory.ListAsync(CancellationToken.None));

        Assert.Equal(code, ex.StatusCode);
        Assert.StartsWith($"Databricks answered HTTP {code}: ", ex.Message);
        Assert.IsAssignableFrom<SourceException>(ex);
    }

    /// <summary>
    /// A proxy's HTML page with a 200, or a request that runs out of time, must fail the task like
    /// any other source failure. Escaping the task runner instead, it would leave the task
    /// unanswered - handed back at every check-in - and stop the rest of that cycle's tasks.
    /// </summary>
    [Fact]
    public async Task An_answer_that_is_not_json_is_a_source_failure()
    {
        var workspace = new FakeHost((_, uri) =>
            uri.AbsolutePath == "/oidc/v1/token" ? Json(Token)
            : new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("<html>Access denied by proxy</html>", Encoding.UTF8, "text/html") });
        var directory = new DbxDirectory(new HttpClient(workspace),
            new DbxAuth(new HttpClient(workspace), Workspace, "00000000-0000-0000-0000-000000000000", "not-a-real-secret"), Workspace);

        var ex = await Assert.ThrowsAsync<SourceException>(() => directory.ListAsync(CancellationToken.None));

        Assert.Contains("not JSON", ex.Message);
        Assert.Contains("Access denied by proxy", ex.Message);
    }

    [Fact]
    public async Task A_listing_that_runs_out_of_time_is_a_source_failure()
    {
        var workspace = new FakeHost((_, uri) =>
            uri.AbsolutePath == "/oidc/v1/token" ? Json(Token)
            : throw new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout of 100 seconds elapsing."));
        var directory = new DbxDirectory(new HttpClient(workspace),
            new DbxAuth(new HttpClient(workspace), Workspace, "00000000-0000-0000-0000-000000000000", "not-a-real-secret"), Workspace);

        var ex = await Assert.ThrowsAsync<SourceException>(() => directory.ListAsync(CancellationToken.None));

        Assert.Contains("did not answer the service principal listing in time", ex.Message);
    }

    /// <summary>A shutdown is not a failure to report: the agent's own cancellation still escapes.</summary>
    [Fact]
    public async Task The_agents_own_shutdown_is_not_reported_as_a_failure()
    {
        using var shutdown = new CancellationTokenSource();
        var workspace = new FakeHost((_, uri) =>
        {
            if (uri.AbsolutePath == "/oidc/v1/token")
            {
                return Json(Token);
            }
            shutdown.Cancel();
            throw new TaskCanceledException();
        });
        var directory = new DbxDirectory(new HttpClient(workspace),
            new DbxAuth(new HttpClient(workspace), Workspace, "00000000-0000-0000-0000-000000000000", "not-a-real-secret"), Workspace);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => directory.ListAsync(shutdown.Token));
    }

    /// <summary>
    /// The task turns into one JSON object per principal, and asks nothing of the SQL warehouse:
    /// the directory is read even when the warehouse is asleep or refuses statements.
    /// </summary>
    [Fact]
    public async Task The_directory_task_uploads_one_line_per_principal_and_runs_no_statement()
    {
        var listing = $$"""
            { "totalResults": 2, "Resources": [
                { "applicationId": "{{AppA}}", "displayName": "etl \"nightly\"", "active": true },
                { "applicationId": "{{AppB}}", "displayName": "retired", "active": false } ] }
            """;
        var workspace = new FakeHost((_, uri) =>
            uri.AbsolutePath == "/oidc/v1/token" ? Json(Token)
            : uri.AbsolutePath == DbxDirectory.ListPath ? Json(listing)
            : Json("""{"message":"no statements in this test"}""", HttpStatusCode.BadRequest));
        var source = new DbxSource(new HttpClient(workspace), new HttpClient(new FakeHost((_, _) => Json("[]"))), Endpoint());

        await using var rows = await source.OpenAsync(new AgentTask { ChunkId = "c1", Stream = ProtocolInfo.Streams.CatalogServicePrincipals }, CancellationToken.None);
        Assert.NotNull(rows);
        using var reader = new StreamReader(rows!.Body, Encoding.UTF8);
        var lines = (await reader.ReadToEndAsync()).Split('\n', StringSplitOptions.RemoveEmptyEntries);

        Assert.Equal(2, lines.Length);
        var first = Parse(lines[0]);
        Assert.Equal(DbxDirectory.Columns, first.EnumerateObject().Select(p => p.Name));
        AssertDatedNow(first.GetProperty("snapshot_date").GetString()!);
        Assert.Equal(AppA, first.GetProperty("application_id").GetString());
        Assert.Equal("etl \"nightly\"", first.GetProperty("display_name").GetString());
        Assert.Equal("true", first.GetProperty("active").GetString());
        Assert.EndsWith($$"""
            "application_id":"{{AppB}}","display_name":"retired","active":"false"}
            """, lines[1]);
        Assert.DoesNotContain(workspace.Seen, s => s.PathAndQuery.StartsWith("/api/2.0/sql/", StringComparison.Ordinal));
    }

    [Fact]
    public void The_directory_stream_is_not_sql()
    {
        Assert.Null(DbxRawSelects.ForTask(Endpoint(), new AgentTask { Stream = ProtocolInfo.Streams.CatalogServicePrincipals }));
        Assert.Equal("catalog_service_principals", ProtocolInfo.Streams.CatalogServicePrincipals);
    }
}
