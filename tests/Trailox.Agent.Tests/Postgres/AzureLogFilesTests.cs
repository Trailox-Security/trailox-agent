using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using Azure.Core;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.Postgres;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests.Postgres;

/// <summary>
/// <c>log_source: azure_files</c>: the server's log files listed through Azure Resource Manager and read in ranges,
/// end to end from the list to the rows of a window.
/// </summary>
public class AzureLogFilesTests
{
    private const string FileName = "postgresql_2026_01_15_10_00_00.log";

    private static long Micros(string utc) =>
        (DateTime.SpecifyKind(DateTime.Parse(utc, CultureInfo.InvariantCulture), DateTimeKind.Utc) - DateTime.UnixEpoch).Ticks / 10;

    /// <summary>Serves files as Azure does: a list, and ranges of each file by its link; a link can be made to expire.</summary>
    private sealed class FakeAzure : IAzureLogFiles
    {
        private readonly Dictionary<string, byte[]> _files = new();
        private int _version = 1;

        public int Lists { get; private set; }
        public int Reads { get; private set; }
        public long BytesRead { get; private set; }
        public bool ExpireNextRead { get; set; }

        public void Add(string name, byte[] content) => _files[name] = content;

        public Task<IReadOnlyList<AzureLogFile>> ListAsync(CancellationToken ct)
        {
            Lists++;
            IReadOnlyList<AzureLogFile> list = _files.Select(f => new AzureLogFile(f.Key, Micros("2026-01-15 11:00:00"),
                (f.Value.Length + 1023) / 1024 * 1024, $"https://logs.example/{f.Key}?v={_version}")).ToList();
            return Task.FromResult(list);
        }

        public Task<byte[]> ReadAsync(AzureLogFile file, long offset, int count, CancellationToken ct)
        {
            if (ExpireNextRead || !file.Url.EndsWith($"v={_version}", StringComparison.Ordinal))
            {
                ExpireNextRead = false;
                _version++;
                throw new AzureLogFileLinkException("read: its link answered 403");
            }
            Reads++;
            var content = _files[file.Name];
            var bytes = offset >= content.Length ? Array.Empty<byte>() : content.AsSpan((int)offset, (int)Math.Min(count, content.Length - offset)).ToArray();
            BytesRead += bytes.Length;
            return Task.FromResult(bytes);
        }
    }

    private static FakeAzure Fixture()
    {
        var azure = new FakeAzure();
        azure.Add(FileName, File.ReadAllBytes(PgFixtures.AzureDownload));
        return azure;
    }

    private static EndpointConfig Endpoint() => new()
    {
        Alias = "orders",
        Engine = "postgres",
        Host = "orders.postgres.database.azure.com",
        Port = 5432,
        Username = "monitor",
        StoreRawQueryText = true,
        Options = new Dictionary<string, string>
        {
            ["log_source"] = "azure_files",
            ["azure_subscription"] = "00000000-0000-0000-0000-000000000000",
            ["azure_resource_group"] = "rg-databases",
            ["azure_server"] = "orders",
        },
    };

    private static async Task<List<JsonElement>> RowsAsync(RowStream stream)
    {
        await using (stream)
        {
            using var text = new StreamReader(stream.Body);
            return (await text.ReadToEndAsync()).Split('\n', StringSplitOptions.RemoveEmptyEntries)
                .Select(l => JsonDocument.Parse(l).RootElement.Clone()).ToList();
        }
    }

    private static AgentTask Window(string from, string to) =>
        new() { ChunkId = "c1", EndpointId = 1, Stream = ProtocolInfo.Streams.PgLog, StartMicros = Micros(from), EndMicros = Micros(to) };

    private static string S(JsonElement row, string column) => row.GetProperty(column).ToString();

    /// <summary>🔴 What the collector needs of a window: a user on every row, from the download's sessions.</summary>
    [Fact]
    public async Task A_window_of_the_download_ships_every_row_with_its_user()
    {
        var azure = Fixture();
        var source = new PgSource(Endpoint(), utcNow: () => new DateTime(2026, 1, 20), azure: azure);

        var rows = await RowsAsync((await source.OpenAsync(Window("2026-01-15 10:57:00", "2026-01-15 10:58:00"), CancellationToken.None))!);

        var shipped = rows.Where(r => S(r, "level") != "summary").ToList();
        Assert.NotEmpty(shipped);
        Assert.All(shipped, r => Assert.NotEqual("", S(r, "user")));
        Assert.All(rows, r => Assert.Equal("orders", S(r, "instance")));
        Assert.Contains(shipped, r => S(r, "message") == "statement: CREATE SCHEMA IF NOT EXISTS shop" && S(r, "user") == "dbadmin");
        var counts = JsonDocument.Parse(S(rows.Single(r => S(r, "level") == "summary"), "message")).RootElement;
        Assert.Equal(1, counts.GetProperty("files").GetInt32());
        Assert.Equal(1, azure.Lists);
    }

    /// <summary>A window inside an hour's file reads its own part of it: the offset is found by halving the file.</summary>
    [Fact]
    public async Task A_window_reads_its_part_of_the_hours_file_not_the_whole_file()
    {
        var azure = Fixture();
        var reader = new AzureLogReader(azure, 1000, probeBytes: 1024);
        var format = PgLineFormat.Of("%t-%c-");
        var lines = new List<string>();

        await foreach (var line in reader.LinesAsync(Micros("2026-01-15 10:58:30"), Micros("2026-01-15 10:58:40"), format.TimeOf, CancellationToken.None))
        {
            lines.Add(line.Text);
        }

        var times = lines.Select(format.TimeOf).OfType<long>().ToList();
        Assert.True(times.First() <= Micros("2026-01-15 10:58:25"));
        Assert.True(times.First() >= Micros("2026-01-15 10:57:00"));
        Assert.True(times.Last() > Micros("2026-01-15 10:58:45"));
        Assert.True(azure.BytesRead < new FileInfo(PgFixtures.AzureDownload).Length);
    }

    [Fact]
    public async Task An_expired_link_is_listed_again_and_the_read_goes_on()
    {
        var azure = Fixture();
        var reader = new AzureLogReader(azure, 1000);
        azure.ExpireNextRead = true;
        var count = 0;

        await foreach (var _ in reader.LinesAsync(Micros("2026-01-15 10:52:00"), Micros("2026-01-15 11:00:00"), PgLineFormat.Of("%t-%c-").TimeOf, CancellationToken.None))
        {
            count++;
        }

        Assert.Equal(479, count);
        Assert.Equal(2, azure.Lists);
    }

    /// <summary>Azure adds to the newest file every few minutes: a window it has not written past waits.</summary>
    [Fact]
    public async Task A_window_azure_has_not_written_past_is_not_yet_published()
    {
        var source = new PgSource(Endpoint(), utcNow: () => new DateTime(2026, 1, 15, 11, 5, 0, DateTimeKind.Utc), azure: Fixture());

        var ex = await Assert.ThrowsAsync<SourceException>(async () =>
            await RowsAsync((await source.OpenAsync(Window("2026-01-15 10:59:00", "2026-01-15 11:04:00"), CancellationToken.None))!));

        Assert.True(ex.NotYetPublished);
    }

    [Fact]
    public async Task A_window_that_ended_an_hour_ago_is_read_as_it_is()
    {
        var source = new PgSource(Endpoint(), utcNow: () => new DateTime(2026, 1, 15, 12, 30, 0, DateTimeKind.Utc), azure: Fixture());

        var rows = await RowsAsync((await source.OpenAsync(Window("2026-01-15 10:59:00", "2026-01-15 11:04:00"), CancellationToken.None))!);

        Assert.Contains(rows, r => S(r, "level") == "summary");
    }

    private sealed class StaticToken : TokenCredential
    {
        public int Asked { get; private set; }

        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            new("token", DateTimeOffset.UtcNow.AddHours(1));

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            Asked++;
            return new ValueTask<AccessToken>(GetToken(requestContext, cancellationToken));
        }
    }

    private sealed class Answers : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _answer;

        public Answers(Func<HttpRequestMessage, HttpResponseMessage> answer) => _answer = answer;

        public List<HttpRequestMessage> Requests { get; } = new();

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request);
            return Task.FromResult(_answer(request));
        }
    }

    private static HttpResponseMessage Json(HttpStatusCode status, string body) =>
        new(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    private const string Page1 = """
        {"value":[{"properties":{"createdTime":"2026-01-15T10:00:00+00:00","lastModifiedTime":"2026-01-15T10:59:58+00:00","sizeInKb":70,
          "type":"LOG","url":"https://logs.blob.example/serverlogs/postgresql_2026_01_15_10_00_00.log?sig=x"},
          "id":"/subscriptions/s/resourceGroups/rg/providers/Microsoft.DBforPostgreSQL/flexibleServers/orders/logFiles/postgresql_2026_01_15_10_00_00.log",
          "name":"postgresql_2026_01_15_10_00_00.log","type":"Microsoft.DBforPostgreSQL/flexibleServers/logFiles"}],
         "nextLink":"https://management.azure.com/subscriptions/s/next?page=2"}
        """;

    private const string Page2 = """
        {"value":[{"properties":{"lastModifiedTime":"2026-01-15T11:09:58Z","sizeInKb":3,"url":"https://logs.blob.example/serverlogs/postgresql_2026_01_15_11_00_00.log?sig=y"},
          "name":"postgresql_2026_01_15_11_00_00.log"},
          {"properties":{"lastModifiedTime":"2026-01-15T11:09:58Z","sizeInKb":3},"name":"no-link.log"}]}
        """;

    [Fact]
    public async Task The_list_follows_its_pages_with_the_agents_token()
    {
        var token = new StaticToken();
        var http = new Answers(r => Json(HttpStatusCode.OK, r.RequestUri!.Query.Contains("page=2") ? Page2 : Page1));
        var api = new ArmAzureLogFiles(new HttpClient(http), "00000000-0000-0000-0000-000000000000", "rg-databases", "orders", token);

        var files = await api.ListAsync(CancellationToken.None);

        Assert.Equal(new[] { "postgresql_2026_01_15_10_00_00.log", "postgresql_2026_01_15_11_00_00.log" }, files.Select(f => f.Name));
        Assert.Equal((70 * 1024L, Micros("2026-01-15 10:59:58")), (files[0].SizeBytes, files[0].LastModifiedMicros));
        Assert.Equal(Micros("2026-01-15 10:00:00"), files[0].StartMicros);
        Assert.Equal("https://management.azure.com/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-databases/providers/"
            + "Microsoft.DBforPostgreSQL/flexibleServers/orders/logFiles?api-version=2024-08-01", http.Requests[0].RequestUri!.ToString());
        Assert.All(http.Requests, r => Assert.Equal("Bearer token", r.Headers.Authorization!.ToString()));
        Assert.Equal(1, token.Asked);   // kept until it nearly expires
    }

    /// <summary>🔴 The token goes to Resource Manager only: a next page on any other host is refused, not followed.</summary>
    [Fact]
    public async Task A_next_page_on_another_host_is_refused()
    {
        var http = new Answers(_ => Json(HttpStatusCode.OK, Page1.Replace("https://management.azure.com/subscriptions/s/next", "https://elsewhere.example/next")));
        var api = new ArmAzureLogFiles(new HttpClient(http), "00000000-0000-0000-0000-000000000000", "rg", "orders", new StaticToken());

        var ex = await Assert.ThrowsAsync<SourceException>(() => api.ListAsync(CancellationToken.None));

        Assert.Contains("not on management.azure.com", ex.Message);
        Assert.Single(http.Requests);
    }

    [Theory]
    [InlineData(HttpStatusCode.Forbidden, "AuthorizationFailed", "needs Microsoft.DBforPostgreSQL/flexibleServers/logFiles/read on the server")]
    [InlineData(HttpStatusCode.NotFound, "ResourceNotFound", "no such server (ResourceNotFound)")]
    [InlineData(HttpStatusCode.InternalServerError, "InternalServerError", "failed: 500 InternalServerError")]
    public async Task A_refused_list_says_what_to_change(HttpStatusCode status, string code, string said)
    {
        var http = new Answers(_ => Json(status, "{\"error\":{\"code\":\"" + code + "\",\"message\":\"m\"}}"));
        var api = new ArmAzureLogFiles(new HttpClient(http), "00000000-0000-0000-0000-000000000000", "rg", "orders", new StaticToken());

        var ex = await Assert.ThrowsAsync<SourceException>(() => api.ListAsync(CancellationToken.None));

        Assert.Contains(said, ex.Message);
    }

    [Fact]
    public async Task A_file_is_read_by_range_and_an_expired_link_says_so()
    {
        var http = new Answers(r => r.RequestUri!.Query.Contains("old")
            ? new HttpResponseMessage(HttpStatusCode.Forbidden)
            : new HttpResponseMessage(HttpStatusCode.PartialContent) { Content = new ByteArrayContent(Encoding.UTF8.GetBytes("abc")) });
        var api = new ArmAzureLogFiles(new HttpClient(http), "00000000-0000-0000-0000-000000000000", "rg", "orders", new StaticToken());

        var bytes = await api.ReadAsync(new AzureLogFile(FileName, 0, 3, "https://logs.blob.example/f?sig=new"), 100, 3, CancellationToken.None);
        await Assert.ThrowsAsync<AzureLogFileLinkException>(() =>
            api.ReadAsync(new AzureLogFile(FileName, 0, 3, "https://logs.blob.example/f?sig=old"), 0, 3, CancellationToken.None));

        Assert.Equal("abc", Encoding.UTF8.GetString(bytes));
        Assert.Equal("bytes=100-102", http.Requests[0].Headers.Range!.ToString());
        Assert.Null(http.Requests[0].Headers.Authorization);   // the link is signed: no token goes with it
    }

    [Fact]
    public void Azures_file_names_say_their_hour()
    {
        Assert.Equal(Micros("2026-01-15 10:00:00"), PgLogFileNames.StartMicros("postgresql_2026_01_15_10_00_00.log"));
        Assert.Equal(PgLogFormat.Stderr, PgLogFileNames.FormatOf("postgresql_2026_01_15_10_00_00.log"));
    }

    [Fact]
    public void An_azure_files_source_is_valid()
    {
        var e = Endpoint();

        Assert.Empty(new PostgresEngine().Validate(e));
        Assert.Equal("cloud", e.Kind);
    }

    [Theory]
    [InlineData("azure_subscription", "not-a-guid", "options.azure_subscription: the server's subscription id")]
    [InlineData("azure_resource_group", "", "options.azure_resource_group: the server's resource group")]
    [InlineData("azure_resource_group", "ends-with-dot.", "options.azure_resource_group")]
    [InlineData("azure_server", "Orders", "options.azure_server: the flexible server's name")]
    [InlineData("azure_server", "orders.postgres.database.azure.com", "options.azure_server")]
    [InlineData("rds_region", "eu-west-1", "options.rds_region: only with log_source: rds_api")]
    [InlineData("log_directory", "/var/log/pg", "options.log_directory: only with log_source: file")]
    public void An_azure_files_source_says_what_is_wrong(string key, string value, string problem)
    {
        var e = Endpoint();
        e.Options[key] = value;

        Assert.Contains(new PostgresEngine().Validate(e), p => p.StartsWith(problem, StringComparison.Ordinal));
    }

    [Fact]
    public void Azure_options_belong_to_azure_files_only()
    {
        var file = new EndpointConfig
        {
            Alias = "vm", Engine = "postgres", Host = "10.0.0.5", Username = "monitor",
            Options = new Dictionary<string, string> { ["log_source"] = "file", ["log_directory"] = "/var/log/postgresql", ["azure_server"] = "orders" },
        };

        Assert.Contains(new PostgresEngine().Validate(file), p => p == "options.azure_server: only with log_source: azure_files");
    }

    [Theory]
    [InlineData(0, "fail", "set the server parameter logfiles.download_enable = on")]
    [InlineData(72, "ok", "72 log files, the oldest from 2026-01-15 10:00 UTC")]
    public void The_probe_says_how_many_files_azure_keeps(int files, string status, string said)
    {
        var check = PgProbeChecks.AzureLogFiles("orders", files, Micros("2026-01-15 10:00:00"));

        Assert.Equal(("log_files", status), (check.Name, check.Status));
        Assert.Contains(said, check.Message);
    }
}
