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
/// <c>log_source: azure_log_analytics</c>: the server's rows in Log Analytics, queried in pages and read as the lines
/// of the server log download, end to end from the query API's answer to the rows of a window.
/// </summary>
public class AzureLogAnalyticsTests
{
    private const string Subscription = "00000000-0000-0000-0000-000000000000";
    private const string Workspace = "11111111-2222-3333-4444-555555555555";
    private static readonly DateTime Later = new(2026, 1, 20, 0, 0, 0, DateTimeKind.Utc);

    private static long Micros(string utc) =>
        (DateTime.SpecifyKind(DateTime.Parse(utc, CultureInfo.InvariantCulture), DateTimeKind.Utc) - DateTime.UnixEpoch).Ticks / 10;

    private static IReadOnlyList<AzureLogRow> FixtureRows(string name)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(PgFixtures.AzureBoth(name, "json")));
        return ArmLogAnalyticsQuery.Page(doc, 0).Rows;
    }

    private const string WorkspaceA = "aaaaaaaa-0000-0000-0000-000000000001";
    private const string WorkspaceB = "bbbbbbbb-0000-0000-0000-000000000002";

    /// <summary>
    /// Answers as the query API does: rows by time then id, a page at a time; a page can be cut short. Without a
    /// workspace named, every workspace's rows answer, as the server's own query does.
    /// </summary>
    private sealed class FakeLogs : IAzureLogQuery
    {
        private readonly List<(string Workspace, AzureLogRow Row)> _rows;

        /// <param name="workspaces">
        /// The workspaces that hold the fixture: each the same records with ids of its own, as two diagnostic settings write them.
        /// </param>
        public FakeLogs(string fixture, params string[] workspaces)
        {
            var rows = FixtureRows(fixture);
            _rows = (workspaces.Length == 0 ? new[] { WorkspaceA } : workspaces)
                .SelectMany((w, i) => rows.Select(r => (w, i == 0 ? r : r with { ItemId = r.ItemId[..^12] + i.ToString("x12", CultureInfo.InvariantCulture) })))
                .ToList();
        }

        public FakeLogs(IEnumerable<AzureLogRow> rows) => _rows = rows.Select(r => (WorkspaceA, r)).ToList();

        public List<int> Pages { get; } = new();
        public List<AzureLogTarget> Targets { get; } = new();
        public Func<int, bool> CutShort { get; set; } = _ => false;

        private IEnumerable<AzureLogRow> Of(AzureLogTarget target)
        {
            Assert.Equal(AzureLogTables.ResourceSpecific, target.Table);
            return _rows.Where(x => target.Workspace == null || x.Workspace == target.Workspace).Select(x => x.Row);
        }

        public Task<AzureLogPage> RowsAsync(AzureLogTarget target, long fromMicros, long toMicros, AzureLogPosition? after, int max, CancellationToken ct)
        {
            Targets.Add(target);
            Pages.Add(max);
            var rows = Of(target).Where(r => r.Micros >= fromMicros && r.Micros < toMicros)
                .Where(r => after is not { } p || r.Ticks > p.Ticks || (r.Ticks == p.Ticks && string.CompareOrdinal(r.ItemId, p.ItemId) > 0))
                .OrderBy(r => r.Ticks).ThenBy(r => r.ItemId, StringComparer.Ordinal).Take(max).ToList();
            return Task.FromResult(CutShort(max)
                ? new AzureLogPage(Array.Empty<AzureLogRow>(), 10, Partial: true)
                : new AzureLogPage(rows, rows.Sum(r => r.Message.Length), Partial: false));
        }

        public Task<bool> AnyFromAsync(AzureLogTarget target, long micros, CancellationToken ct) => Task.FromResult(Of(target).Any(r => r.Micros >= micros));

        public Task<IReadOnlyList<AzureLogTableRows>> TablesAsync(CancellationToken ct) => Task.FromResult<IReadOnlyList<AzureLogTableRows>>(
            _rows.GroupBy(x => x.Workspace).Select(g => new AzureLogTableRows(AzureLogTables.ResourceSpecific, g.Key, g.Count(), g.Max(x => x.Row.Micros))).ToList());

        public Task<long?> OldestMicrosAsync(AzureLogTarget target, CancellationToken ct) => Task.FromResult<long?>(Of(target).Min(r => r.Micros));
    }

    /// <summary>The same stretch as one hourly file of the download.</summary>
    private sealed class OneFile : IAzureLogFiles
    {
        private readonly string _name;
        private readonly byte[] _content;

        public OneFile(string name, byte[] content) => (_name, _content) = (name, content);

        public Task<IReadOnlyList<AzureLogFile>> ListAsync(CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<AzureLogFile>>(new[] { new AzureLogFile(_name, Micros("2026-01-15 17:00:00"), _content.Length, "https://logs.example/f") });

        public Task<byte[]> ReadAsync(AzureLogFile file, long offset, int count, CancellationToken ct) =>
            Task.FromResult(offset >= _content.Length ? Array.Empty<byte>() : _content.AsSpan((int)offset, (int)Math.Min(count, _content.Length - offset)).ToArray());
    }

    private static EndpointConfig Endpoint(string source = PostgresEngine.LogSourceAzureLogAnalytics) => new()
    {
        Alias = "orders",
        Engine = "postgres",
        Host = "orders.postgres.database.azure.com",
        Port = 5432,
        Username = "monitor",
        StoreRawQueryText = true,
        Options = new Dictionary<string, string>
        {
            ["log_source"] = source,
            ["azure_subscription"] = Subscription,
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

    private static readonly AzureLogTarget Rs = new(AzureLogTables.ResourceSpecific);

    private static async Task<List<JsonElement>> ShippedAsync(PgSource source, string from, string to) =>
        (await RowsAsync((await source.OpenAsync(Window(from, to), CancellationToken.None))!)).Where(r => S(r, "level") != "summary").ToList();

    private static async Task<List<string>> LinesAsync(AzureLogAnalyticsReader reader, string from, string to)
    {
        var lines = new List<string>();
        await foreach (var line in reader.LinesAsync(Micros(from), Micros(to), PgLineFormat.Of(PgSource.AzureDefaultPrefix).TimeOf, CancellationToken.None))
        {
            lines.Add(line.Text);
        }
        return lines;
    }

    /// <summary>
    /// 🔴 One stretch of a server read from both of Azure's sources: every Log Analytics row, rebuilt as a line, is the
    ///    download's line to the character, in the same order (measured on a real server), so a window ships the same
    ///    rows either way. The download's extra lines are Azure's own (<c>[Microsoft Entra]</c>, <c>[pg-availability]</c>,
    ///    <c>pg_qs</c>), none of them a row.
    /// </summary>
    [Theory]
    [InlineData("traffic", "15", "2026-01-15 15:32:00", "2026-01-15 15:36:00")]
    [InlineData("traffic", "15", "2026-01-15 15:30:00", "2026-01-15 15:39:00")]   // the fixture's last row is 15:39:59
    [InlineData("odd", "16", "2026-01-15 16:51:00", "2026-01-15 16:54:00")]
    public async Task A_window_ships_the_same_rows_from_log_analytics_as_from_the_download(string fixture, string hour, string from, string to)
    {
        var download = new PgSource(Endpoint(PostgresEngine.LogSourceAzureFiles), utcNow: () => Later,
            azure: new OneFile($"postgresql_2026_01_15_{hour}_00_00.log", File.ReadAllBytes(PgFixtures.AzureBoth(fixture, "log"))));
        var logAnalytics = new PgSource(Endpoint(), utcNow: () => Later, logAnalytics: new FakeLogs(fixture));

        var fromFiles = await ShippedAsync(download, from, to);
        var fromRows = await ShippedAsync(logAnalytics, from, to);

        Assert.NotEmpty(fromRows);
        Assert.Equal(fromFiles.Select(r => Same(r, from)), fromRows.Select(r => Same(r, from)));
    }

    /// <summary>
    /// A row as both sources must give it. One field may differ, for a reason that is not the source's: the download is
    /// read from a little before the window (where halving the file lands), so it can see the sign-in of a session that
    /// began before the window, and Log Analytics, read from the window, does not. A disconnection names its user and
    /// database, so that session needs no look-back, and its application name stays unknown (null) rather than none ("").
    /// </summary>
    private static string Same(JsonElement row, string windowStart)
    {
        var began = PgSessionId.StartMicros(S(row, "session_id"));
        return began < Micros(windowStart) && S(row, "message").StartsWith("disconnection:", StringComparison.Ordinal)
            ? row.GetRawText().Replace("\"application_name\":\"\"", "\"application_name\":null", StringComparison.Ordinal)
            : row.GetRawText();
    }

    /// <summary>The test traffic: each sign-in, refusal, DDL statement and pgaudit row carries its user from its session.</summary>
    [Fact]
    public async Task A_window_of_log_analytics_ships_the_traffic_with_its_users()
    {
        var source = new PgSource(Endpoint(), utcNow: () => Later, logAnalytics: new FakeLogs("traffic"));

        var rows = await RowsAsync((await source.OpenAsync(Window("2026-01-15 15:33:00", "2026-01-15 15:35:00"), CancellationToken.None))!);

        var shipped = rows.Where(r => S(r, "level") != "summary").ToList();
        var traffic = shipped.Where(r => S(r, "remote_host") == "192.0.2.10").ToList();
        Assert.Contains(traffic, r => S(r, "message").StartsWith("connection authorized: user=app_user", StringComparison.Ordinal)
            && S(r, "application_name") == "shop-app");
        Assert.Contains(traffic, r => S(r, "level") == "FATAL" && S(r, "user") == "app_user");
        Assert.Contains(traffic, r => S(r, "level") == "FATAL" && S(r, "user") == "no_such_user");
        // pgaudit's READ and WRITE rows, shipped as their statements.
        Assert.Contains(traffic, r => S(r, "command") == "SELECT" && S(r, "message") == "SELECT holder, pan FROM shop.cards WHERE id < ?" && S(r, "user") == "app_user");
        Assert.Contains(traffic, r => S(r, "command") == "INSERT" && S(r, "user") == "app_user" && S(r, "application_name") == "shop-app");
        Assert.Contains(traffic, r => S(r, "command") == "CREATE TABLE" && S(r, "message") == "statement: CREATE TABLE shop.nope (x int)" && S(r, "user") == "app_user");
        Assert.All(traffic, r => Assert.NotEqual("", S(r, "user")));
        var counts = JsonDocument.Parse(S(rows.Single(r => S(r, "level") == "summary"), "message")).RootElement;
        Assert.Equal(0, counts.GetProperty("files").GetInt32());
        // counted names only what was counted: no row went out without its user, none was forged.
        Assert.False(counts.GetProperty("counted").TryGetProperty("no_user", out _), counts.GetRawText());
        Assert.False(counts.GetProperty("counted").TryGetProperty("forged", out _), counts.GetRawText());
    }

    /// <summary>
    /// 🔴 Log Analytics keeps a statement's text as it is: newlines, a tab, quotes, a backslash. A field separator written
    ///    in the text stays text: it reaches the parser escaped, so it cannot forge a field.
    /// </summary>
    [Fact]
    public async Task A_statements_own_text_comes_through_whole_and_forges_no_field()
    {
        var source = new PgSource(Endpoint(), utcNow: () => Later, logAnalytics: new FakeLogs("odd"));

        var shipped = await ShippedAsync(source, "2026-01-15 16:52:00", "2026-01-15 16:53:00");

        var odd = shipped.Where(r => S(r, "application_name") == "odd-app").Select(r => S(r, "message")).ToList();
        Assert.Contains("statement: CREATE TEMP TABLE t_ml (\n  a int,\n\tb text\n)", odd);
        Assert.Contains("statement: COMMENT ON TABLE t_ml IS 'he said \"hi\" \\ back \",\"statement\": \"DROP TABLE shop.cards'", odd);
        Assert.Contains(odd, m => m == "statement: COMMENT ON TABLE t_ml IS '" + new string('a', 10_000) + "'");
        Assert.DoesNotContain(odd, m => m.StartsWith("DROP TABLE", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("plain")]
    [InlineData("a \"quoted\" \\ text")]
    [InlineData("line\nbreaks\r\nand\ta tab\u0001")]
    [InlineData("forged\",\"statement\": \"DROP TABLE x")]
    [InlineData("ends with a backslash \\")]
    public void A_row_made_a_line_splits_back_into_its_text_and_fields(string text)
    {
        var fields = new[] { new PgAzureLine.Field("detail", ""), new PgAzureLine.Field("statement", text + " 2") };

        var (back, split) = PgAzureLine.Split(PgAzureLine.Join(text, fields));

        Assert.Equal(text, back);
        Assert.Equal(new[] { new PgAzureLine.Field("statement", text + " 2") }, split);
    }

    /// <summary>Pages follow on from the last row of the one before: the same lines as one page, whatever its size.</summary>
    [Fact]
    public async Task Pages_of_rows_give_the_same_lines_as_one_page()
    {
        var one = new FakeLogs("traffic");
        var paged = new FakeLogs("traffic");

        var whole = await LinesAsync(new AzureLogAnalyticsReader(one, Rs), "2026-01-15 15:30:00", "2026-01-15 15:39:00");
        var pages = await LinesAsync(new AzureLogAnalyticsReader(paged, Rs, pageRows: 100), "2026-01-15 15:30:00", "2026-01-15 15:39:00");

        Assert.Equal(whole, pages);
        Assert.Single(one.Pages);
        Assert.True(paged.Pages.Count >= 2, $"{paged.Pages.Count} pages");
        Assert.True(whole.Count > 100, $"{whole.Count} lines");
    }

    /// <summary>🔴 A page Azure cuts short is never read as whole: it is asked again with half as many rows.</summary>
    [Fact]
    public async Task A_page_cut_short_is_asked_again_with_fewer_rows()
    {
        var expected = await LinesAsync(new AzureLogAnalyticsReader(new FakeLogs("traffic"), Rs), "2026-01-15 15:30:00", "2026-01-15 15:39:00");
        var logs = new FakeLogs("traffic") { CutShort = max => max > 150 };

        var lines = await LinesAsync(new AzureLogAnalyticsReader(logs, Rs), "2026-01-15 15:30:00", "2026-01-15 15:39:00");

        Assert.Equal(expected, lines);
        Assert.Equal(new[] { 10_000, 5_000, 2_500, 1_250, 625, 312, 156, 100 }, logs.Pages.Take(8));
    }

    [Fact]
    public async Task A_window_whose_rows_are_always_cut_short_fails()
    {
        var logs = new FakeLogs("traffic") { CutShort = _ => true };

        var ex = await Assert.ThrowsAsync<SourceException>(() =>
            LinesAsync(new AzureLogAnalyticsReader(logs, Rs), "2026-01-15 15:30:00", "2026-01-15 15:39:00"));

        Assert.Contains("cut short even 100 rows", ex.Message);
    }

    /// <summary>
    /// Rows reach Log Analytics 2 to 6 minutes after they are written, a batch at times after a newer one: a recent
    /// window is read once a row 5 minutes past its end has come. The traffic fixture's newest row is 15:39:59.
    /// </summary>
    [Theory]
    [InlineData("2026-01-15 15:32:00", "2026-01-15 15:36:00", "2026-01-15 15:45:00", false)]
    [InlineData("2026-01-15 15:30:00", "2026-01-15 15:34:00", "2026-01-15 15:45:00", true)]
    [InlineData("2026-01-15 15:32:00", "2026-01-15 15:36:00", "2026-01-15 16:37:00", true)]
    public async Task A_recent_window_waits_for_a_row_past_its_end(string from, string to, string now, bool read)
    {
        var source = new PgSource(Endpoint(), utcNow: () => DateTime.SpecifyKind(DateTime.Parse(now, CultureInfo.InvariantCulture), DateTimeKind.Utc),
            logAnalytics: new FakeLogs("traffic"));

        var open = async () => await RowsAsync((await source.OpenAsync(Window(from, to), CancellationToken.None))!);

        if (read)
        {
            Assert.Contains(await open(), r => S(r, "level") == "summary");
        }
        else
        {
            Assert.True((await Assert.ThrowsAsync<SourceException>(open)).NotYetPublished);
        }
    }

    /// <summary>
    /// 🔴 No row after a window is never read as an empty window: on Azure the platform's own sign-ins write rows every
    ///    minute, so silence means the server stopped or its diagnostic setting no longer sends there. The traffic
    ///    fixture's newest row is 15:39:59.
    /// </summary>
    [Fact]
    public async Task An_old_window_with_no_row_after_it_fails_and_says_why()
    {
        var source = new PgSource(Endpoint(), utcNow: () => new DateTime(2026, 1, 15, 17, 0, 0, DateTimeKind.Utc), logAnalytics: new FakeLogs("traffic"));

        var ex = await Assert.ThrowsAsync<SourceException>(async () =>
            await RowsAsync((await source.OpenAsync(Window("2026-01-15 15:39:00", "2026-01-15 15:40:00"), CancellationToken.None))!));

        Assert.False(ex.NotYetPublished);
        Assert.Contains("Log Analytics has no row of orders in PGSQLServerLogs after the window: the server is stopped, or nothing sends "
            + "its PostgreSQLLogs there any more (check its diagnostic setting)", ex.Message);
    }

    /// <summary>
    /// 🔴 Two diagnostic settings send each record to two workspaces, and the server's own query answers it from both:
    ///    only one workspace is read, so every row ships once.
    /// </summary>
    [Fact]
    public async Task Rows_that_reach_two_workspaces_ship_once()
    {
        var once = new PgSource(Endpoint(), utcNow: () => Later, logAnalytics: new FakeLogs("traffic"));
        var twice = new FakeLogs("traffic", WorkspaceA, WorkspaceB);
        var both = new PgSource(Endpoint(), utcNow: () => Later, logAnalytics: twice);

        var expected = await ShippedAsync(once, "2026-01-15 15:32:00", "2026-01-15 15:36:00");
        var shipped = await ShippedAsync(both, "2026-01-15 15:32:00", "2026-01-15 15:36:00");

        Assert.Equal(expected.Select(r => r.GetRawText()), shipped.Select(r => r.GetRawText()));
        Assert.All(twice.Targets, t => Assert.Equal(WorkspaceA, t.Workspace));
    }

    [Fact]
    public void The_target_is_the_table_that_holds_the_rows_and_one_workspace_of_them()
    {
        AzureLogTableRows Rows(string table, long n, string workspace = WorkspaceA) => new(table, workspace, n, null);

        Assert.Equal(new AzureLogTarget("PGSQLServerLogs"), PgSource.LogTargetOf(new[] { Rows("PGSQLServerLogs", 5), Rows("AzureDiagnostics", 9) }));
        Assert.Equal(new AzureLogTarget("AzureDiagnostics"), PgSource.LogTargetOf(new[] { Rows("PGSQLServerLogs", 0), Rows("AzureDiagnostics", 9) }));
        Assert.Equal(new AzureLogTarget("PGSQLServerLogs"), PgSource.LogTargetOf(new[] { Rows("AzureDiagnostics", 0) }));
        Assert.Equal(new AzureLogTarget("PGSQLServerLogs"), PgSource.LogTargetOf(Array.Empty<AzureLogTableRows>()));
        Assert.Equal(new AzureLogTarget("AzureDiagnostics"), PgSource.LogTargetOf(new[] { Rows("PGSQLServerLogs", 5) }, given: "AzureDiagnostics"));
        // Two workspaces: the one with the most rows, the first by id on a tie.
        Assert.Equal(new AzureLogTarget("PGSQLServerLogs", WorkspaceB),
            PgSource.LogTargetOf(new[] { Rows("PGSQLServerLogs", 5, WorkspaceA), Rows("PGSQLServerLogs", 7, WorkspaceB) }));
        Assert.Equal(new AzureLogTarget("PGSQLServerLogs", WorkspaceA),
            PgSource.LogTargetOf(new[] { Rows("PGSQLServerLogs", 7, WorkspaceB), Rows("PGSQLServerLogs", 7, WorkspaceA) }));
    }

    [Theory]
    [InlineData(0, "16:39", new string[0], 1, "fail", "no row of its server log came to Log Analytics in the last day in PGSQLServerLogs. Send its PostgreSQLLogs category there")]
    [InlineData(0, "16:39", new[] { "AzureDiagnostics" }, 1, "fail", "no row of its server log came to PGSQLServerLogs in the last day, but AzureDiagnostics has them: set options.azure_log_table")]
    [InlineData(6480, "15:20", new string[0], 1, "fail", "the newest row of its server log in PGSQLServerLogs is from 2026-01-15 15:20 UTC: nothing has sent it there since")]
    [InlineData(6480, "16:39", new string[0], 1, "ok", "6480 rows of its server log came to PGSQLServerLogs in the last day, the newest from 2026-01-15 16:39 UTC, the oldest kept from 2026-01-15 11:13 UTC")]
    [InlineData(6480, "16:39", new[] { "AzureDiagnostics" }, 1, "warn", "AzureDiagnostics also holds them: the agent reads PGSQLServerLogs only")]
    [InlineData(6480, "16:39", new string[0], 2, "warn", "its rows reach 2 workspaces (" + WorkspaceA + ", " + WorkspaceB + "), each with the same rows: the agent reads " + WorkspaceA + "'s only")]
    public void The_probe_says_whether_log_analytics_receives_the_servers_log(long rows, string newest, string[] tables, int workspaces, string status, string said)
    {
        var check = PgProbeChecks.AzureLogAnalytics("orders", new AzureLogTarget("PGSQLServerLogs", workspaces > 1 ? WorkspaceA : null), rows,
            Micros("2026-01-15 11:13:00"), Micros("2026-01-15 " + newest), Micros("2026-01-15 16:40:00"), tables,
            new[] { WorkspaceA, WorkspaceB }.Take(workspaces).ToList());

        Assert.Equal(("log_analytics", status), (check.Name, check.Status));
        Assert.Contains(said, check.Message);
    }

    /// <summary>A row with no message is no record: it is counted as unreadable, and joins no record before it.</summary>
    [Fact]
    public async Task A_row_with_no_message_is_counted_not_joined_to_the_record_before()
    {
        var rows = FixtureRows("traffic").ToList();
        var at = rows.FindIndex(r => r.Message.Contains("connection authorized: user=app_user", StringComparison.Ordinal));
        rows.Insert(at + 1, rows[at] with { ItemId = rows[at].ItemId[..^1] + "f", Message = "",
            Fields = new[] { new PgAzureLine.Field("statement", "DROP TABLE shop.cards") } });
        var source = new PgSource(Endpoint(), utcNow: () => Later, logAnalytics: new FakeLogs(rows));

        var all = await RowsAsync((await source.OpenAsync(Window("2026-01-15 15:33:00", "2026-01-15 15:35:00"), CancellationToken.None))!);

        Assert.Contains(all, r => S(r, "message").StartsWith("connection authorized: user=app_user", StringComparison.Ordinal) && !S(r, "message").Contains('\n'));
        Assert.DoesNotContain(all, r => S(r, "message").Contains("DROP TABLE shop.cards", StringComparison.Ordinal));
        var counts = JsonDocument.Parse(S(all.Single(r => S(r, "level") == "summary"), "message")).RootElement;
        Assert.Equal(1, counts.GetProperty("unreadable").GetInt32());
    }

    /// <summary>🔴 A page that ends inside one millisecond goes on after the last row's id, not its time: none lost, none twice.</summary>
    [Fact]
    public async Task A_page_that_ends_inside_one_millisecond_goes_on_by_id()
    {
        var at = ArmLogAnalyticsQuery.Ticks("2026-01-15T15:30:00.123Z");
        var rows = Enumerable.Range(1, 150).Select(i => new AzureLogRow(at, $"{i:x8}-0000-1000-8000-000000000000",
            $"2026-01-15 15:30:00 UTC-6968fe48.{i:x}-LOG:  connection received: host=127.0.0.1 port={i}", Array.Empty<PgAzureLine.Field>())).ToList();
        var logs = new FakeLogs(rows);

        var lines = await LinesAsync(new AzureLogAnalyticsReader(logs, Rs, pageRows: 100), "2026-01-15 15:30:00", "2026-01-15 15:31:00");

        Assert.Equal(rows.Select(r => r.Message), lines);
        Assert.Equal(2, logs.Pages.Count);
    }

    /// <summary>A page is sized by the page before: long statements make smaller pages, never past the first size.</summary>
    [Theory]
    [InlineData(10_000, 3_000_000, 10_000)]   // 300 bytes a row: as many as the first page
    [InlineData(1_000, 10_000_000, 838)]      // 10 KB a row: about 8 MB
    [InlineData(100, 6_500_000, 129)]         // 65 KB a row
    [InlineData(100, 100_000_000, 100)]       // never fewer than 100
    public void The_next_page_is_sized_by_the_bytes_of_the_last(int rows, long bytes, int next)
    {
        Assert.Equal(next, new AzureLogAnalyticsReader(new FakeLogs("traffic"), Rs).NextPageRows(rows, bytes));
    }

    private sealed class Token : TokenCredential
    {
        public List<string> Scopes { get; } = new();

        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            new("token", DateTimeOffset.UtcNow.AddHours(1));

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            Scopes.AddRange(requestContext.Scopes);
            return new ValueTask<AccessToken>(GetToken(requestContext, cancellationToken));
        }
    }

    /// <summary>
    /// Answers each request in turn, the last answer from then on, and keeps what was sent (the body is read before the
    /// request is disposed). Each answer is made afresh: the client disposes the one it read.
    /// </summary>
    private sealed class Api : HttpMessageHandler
    {
        private readonly Queue<Func<HttpResponseMessage>> _answers;

        public Api(params Func<HttpResponseMessage>[] answers) => _answers = new Queue<Func<HttpResponseMessage>>(answers);

        public List<(string Url, string? Prefer, string? Auth, string? CacheControl, JsonElement Body)> Sent { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(cancellationToken)).RootElement.Clone();
            Sent.Add((request.RequestUri!.ToString(), request.Headers.TryGetValues("Prefer", out var p) ? p.Single() : null,
                request.Headers.Authorization?.ToString(), request.Headers.CacheControl?.ToString(), body));
            return (_answers.Count > 1 ? _answers.Dequeue() : _answers.Peek())();
        }
    }

    private static HttpResponseMessage Json(HttpStatusCode status, string body) =>
        new(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    private static Func<HttpResponseMessage> Fixture() => () => Json(HttpStatusCode.OK, File.ReadAllText(PgFixtures.AzureBoth("traffic", "json")));

    private static ArmLogAnalyticsQuery Query(Api api, string? workspace = null, List<TimeSpan>? waits = null, Token? token = null) =>
        new(new HttpClient(api), Subscription, "rg-databases", "orders", workspace, token ?? new Token(),
            (wait, _) => { waits?.Add(wait); return Task.CompletedTask; });

    /// <summary>
    /// Without a workspace the query is the server's own, so Azure answers with its rows only; the token is for
    /// api.loganalytics.io, the API's older name (Entra ID knows no other).
    /// </summary>
    [Fact]
    public async Task The_query_is_the_servers_own_with_the_agents_token()
    {
        var api = new Api(Fixture());
        var token = new Token();

        var page = await Query(api, token: token).RowsAsync(Rs, Micros("2026-01-15 15:30:00"), Micros("2026-01-15 15:40:00"), null, 20_000, CancellationToken.None);

        Assert.Equal(208, page.Rows.Count);
        Assert.False(page.Partial);
        var sent = api.Sent.Single();
        Assert.Equal($"https://api.loganalytics.azure.com/v1/subscriptions/{Subscription}/resourceGroups/rg-databases/providers/"
            + "Microsoft.DBforPostgreSQL/flexibleServers/orders/query", sent.Url);
        Assert.Equal(("Bearer token", "wait=600"), (sent.Auth, sent.Prefer));
        Assert.Equal(new[] { "https://api.loganalytics.io/.default" }, token.Scopes);
        Assert.Equal("2026-01-15T15:30:00.0000000Z/2026-01-15T15:40:00.0000000Z", sent.Body.GetProperty("timespan").GetString());
        var kql = sent.Body.GetProperty("query").GetString()!;
        Assert.StartsWith("PGSQLServerLogs\n| where TimeGenerated >= datetime(2026-01-15T15:30:00.0000000Z) and TimeGenerated < datetime(2026-01-15T15:40:00.0000000Z)", kql);
        Assert.EndsWith("| sort by TimeGenerated asc, _ItemId asc\n| take 20000", kql);
        Assert.Equal("no-cache", sent.CacheControl);
        Assert.DoesNotContain("_ResourceId", kql);
    }

    /// <summary>A workspace holds every resource's rows: only the server's are asked for.</summary>
    [Fact]
    public async Task A_workspace_query_reads_the_servers_rows_only()
    {
        var api = new Api(Fixture());

        await Query(api, Workspace).RowsAsync(Rs, Micros("2026-01-15 15:30:00"), Micros("2026-01-15 15:40:00"), null, 100, CancellationToken.None);

        Assert.Equal($"https://api.loganalytics.azure.com/v1/workspaces/{Workspace}/query", api.Sent.Single().Url);
        Assert.Contains($"| where _ResourceId =~ '/subscriptions/{Subscription}/resourceGroups/rg-databases/providers/Microsoft.DBforPostgreSQL/flexibleServers/orders'",
            api.Sent.Single().Body.GetProperty("query").GetString());
    }

    [Fact]
    public void A_target_with_a_workspace_reads_that_workspaces_rows_only()
    {
        var kql = ArmLogAnalyticsQuery.RowsQuery(new AzureLogTarget(AzureLogTables.ResourceSpecific, WorkspaceA), null, 0, 1, null, 50);

        Assert.StartsWith($"PGSQLServerLogs\n| where TenantId == '{WorkspaceA}'\n| where TimeGenerated", kql);
    }

    [Fact]
    public void The_next_page_starts_after_the_last_rows_time_and_id()
    {
        var after = new AzureLogPosition(ArmLogAnalyticsQuery.Ticks("2026-01-15T15:30:02.29Z"), "0000003c-0000-1000-8000-000000000000");

        var kql = ArmLogAnalyticsQuery.RowsQuery(Rs, null, 0, 1, after, 50);

        Assert.Contains("| where TimeGenerated > datetime(2026-01-15T15:30:02.2900000Z) or (TimeGenerated == datetime(2026-01-15T15:30:02.2900000Z) "
            + "and strcmp(_ItemId, '0000003c-0000-1000-8000-000000000000') > 0)", kql);
    }

    /// <summary>🔴 Azure drops a time's trailing zeros, so its text does not sort: <c>20.29Z</c> is before <c>20.291Z</c>.</summary>
    [Fact]
    public void Times_are_compared_as_times_not_as_text()
    {
        Assert.True(ArmLogAnalyticsQuery.Ticks("2026-01-15T11:14:20.29Z") < ArmLogAnalyticsQuery.Ticks("2026-01-15T11:14:20.291Z"));
        Assert.True(string.CompareOrdinal("2026-01-15T11:14:20.29Z", "2026-01-15T11:14:20.291Z") > 0);
    }

    [Fact]
    public void The_legacy_table_is_read_by_its_category()
    {
        var kql = ArmLogAnalyticsQuery.RowsQuery(new AzureLogTarget(AzureLogTables.AzureDiagnostics), "/subscriptions/x", 0, 1, null, 50);

        Assert.StartsWith("AzureDiagnostics\n| where Category == 'PostgreSQLLogs'\n| where _ResourceId =~ '/subscriptions/x'", kql);
        Assert.Contains("Statement = tostring(column_ifexists('statement_s', ''))", kql);
    }

    [Fact]
    public void A_string_in_a_query_stays_a_string()
    {
        Assert.Equal(@"'it\'s \\ ok'", ArmLogAnalyticsQuery.Literal(@"it's \ ok"));
    }

    [Fact]
    public async Task An_answer_cut_short_says_so()
    {
        var cut = File.ReadAllText(PgFixtures.AzureBoth("traffic", "json")).TrimEnd()[..^1]
            + ",\"error\":{\"code\":\"PartialError\",\"message\":\"There were some errors when processing your query.\"}}";

        var page = await Query(new Api(() => Json(HttpStatusCode.OK, cut))).RowsAsync(Rs, 0, 1, null, 20_000, CancellationToken.None);

        Assert.True(page.Partial);
        Assert.Empty(page.Rows);   // never read, so never built: a cut answer is up to 64 MB
    }

    [Theory]
    [InlineData("""{"tables":[{"name":"PrimaryResult","rows":[]}]}""", "no columns or rows")]
    [InlineData("""{"tables":[{"name":"PrimaryResult","columns":[{"name":"TimeGenerated"}],"rows":[{"a":1}]}]}""", "not a list of values")]
    [InlineData("""{"tables":[{"name":"PrimaryResult","columns":[{"name":"TimeGenerated"},{"name":"_ItemId"}],"rows":[[null,"x"]]}]}""", "no TimeGenerated or _ItemId")]
    public async Task An_answer_of_another_shape_fails_as_the_sources_error(string body, string said)
    {
        var ex = await Assert.ThrowsAsync<SourceException>(() =>
            Query(new Api(() => Json(HttpStatusCode.OK, body))).RowsAsync(Rs, 0, 1, null, 50, CancellationToken.None));

        Assert.Contains(said, ex.Message);
    }

    private const string NoTable = """
        {"error":{"message":"The request had some invalid properties","code":"BadArgumentError","correlationId":"c",
          "innererror":{"code":"SemanticError","message":"A semantic error occurred.",
            "innererror":{"code":"SEM0100","message":"'where' operator: Failed to resolve table or column expression named 'PGSQLServerLogs'"}}}}
        """;

    private const string Denied = """
        {"error":{"message":"The provided credentials have insufficient access to perform the requested operation","code":"InsufficientAccessError",
          "correlationId":"c","innererror":{"code":"AuthorizationFailedError","message":"m"}}}
        """;

    [Theory]
    [InlineData(HttpStatusCode.Forbidden, Denied, null, "access denied (AuthorizationFailedError). The agent's Azure identity needs Microsoft.Insights/logs/*/read on the server")]
    [InlineData(HttpStatusCode.Forbidden, Denied, Workspace, "needs, on workspace " + Workspace + ", a role with Microsoft.OperationalInsights/workspaces/read, "
        + "workspaces/query/read and workspaces/query/PGSQLServerLogs/read (or Log Analytics Reader)")]
    [InlineData(HttpStatusCode.Forbidden, """{"error":{"code":5,"message":["not","text"]}}""", null, "access denied (). The agent's Azure identity needs")]
    [InlineData(HttpStatusCode.NotFound, """{"error":{"code":"WorkspaceNotFoundError","message":"m"}}""", Workspace, "no such workspace (WorkspaceNotFoundError)")]
    [InlineData(HttpStatusCode.BadRequest, NoTable, null, "no table PGSQLServerLogs there. Send the server's PostgreSQLLogs there with a diagnostic setting")]
    [InlineData(HttpStatusCode.GatewayTimeout, "", null, "the query ran out of its 600 s")]
    [InlineData(HttpStatusCode.InternalServerError, """{"error":{"code":"InternalServerError","message":"boom"}}""", null, "failed: 500 InternalServerError: boom")]
    public async Task A_refused_query_says_what_to_change(HttpStatusCode status, string body, string? workspace, string said)
    {
        var ex = await Assert.ThrowsAsync<SourceException>(() =>
            Query(new Api(() => Json(status, body)), workspace).RowsAsync(Rs, 0, 1, null, 50, CancellationToken.None));

        Assert.Contains(said, ex.Message);
    }

    [Fact]
    public async Task A_throttled_query_waits_and_goes_on()
    {
        HttpResponseMessage Throttled()
        {
            var answer = Json(HttpStatusCode.TooManyRequests, """{"error":{"code":"ThrottledError","message":"m"}}""");
            answer.Headers.RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromSeconds(7));
            return answer;
        }
        var api = new Api(Throttled, () => Json(HttpStatusCode.ServiceUnavailable, ""), Fixture());
        var waits = new List<TimeSpan>();

        var page = await Query(api, waits: waits).RowsAsync(Rs, 0, 1, null, 50, CancellationToken.None);

        Assert.Equal(208, page.Rows.Count);
        Assert.Equal(new[] { TimeSpan.FromSeconds(7), TimeSpan.FromSeconds(20) }, waits);
    }

    [Fact]
    public async Task A_query_throttled_every_time_fails()
    {
        var api = new Api(() => Json(HttpStatusCode.TooManyRequests, """{"error":{"code":"ThrottledError","message":"m"}}"""));

        var ex = await Assert.ThrowsAsync<SourceException>(() => Query(api).RowsAsync(Rs, 0, 1, null, 50, CancellationToken.None));

        Assert.Contains("throttled (ThrottledError), 4 times", ex.Message);
        Assert.Equal(4, api.Sent.Count);
    }

    [Fact]
    public async Task The_tables_are_counted_together_by_workspace_a_missing_one_left_out()
    {
        var api = new Api(() => Json(HttpStatusCode.OK, """
            {"tables":[{"name":"PrimaryResult","columns":[{"name":"Table","type":"string"},{"name":"TenantId","type":"string"},
              {"name":"Rows","type":"long"},{"name":"Newest","type":"datetime"}],
              "rows":[["PGSQLServerLogs","aaaaaaaa-0000-0000-0000-000000000001",6480,"2026-01-15T16:39:57.181Z"],
                      ["PGSQLServerLogs","bbbbbbbb-0000-0000-0000-000000000002",6470,"2026-01-15T16:39:50Z"]]}]}
            """));

        var tables = await Query(api).TablesAsync(CancellationToken.None);

        Assert.Equal(new[] { ("PGSQLServerLogs", WorkspaceA, 6480L), ("PGSQLServerLogs", WorkspaceB, 6470L) },
            tables.Select(t => (t.Table, t.Workspace, t.Rows)));
        Assert.Equal(Micros("2026-01-15 16:39:57.181"), tables[0].NewestMicros);
        var kql = api.Sent.Single().Body.GetProperty("query").GetString()!;
        Assert.StartsWith("union isfuzzy=true\n(PGSQLServerLogs\n", kql);
        Assert.Contains("| summarize Rows = count(), Newest = max(TimeGenerated) by TenantId", kql);
    }

    [Fact]
    public void A_log_analytics_source_is_valid()
    {
        var e = Endpoint();
        e.Options["azure_workspace_id"] = Workspace;
        e.Options["azure_log_table"] = "AzureDiagnostics";

        Assert.Empty(new PostgresEngine().Validate(e));
        Assert.Equal("cloud", e.Kind);
    }

    [Theory]
    [InlineData("azure_workspace_id", "my-workspace", "options.azure_workspace_id: the Log Analytics workspace's id")]
    [InlineData("azure_log_table", "pgsqlserverlogs", "options.azure_log_table: PGSQLServerLogs or AzureDiagnostics")]
    [InlineData("azure_server", "", "options.azure_server: the flexible server's name")]
    [InlineData("log_directory", "/var/log/pg", "options.log_directory: only with log_source: file")]
    public void A_log_analytics_source_says_what_is_wrong(string key, string value, string problem)
    {
        var e = Endpoint();
        e.Options[key] = value;

        Assert.Contains(new PostgresEngine().Validate(e), p => p.StartsWith(problem, StringComparison.Ordinal));
    }

    [Fact]
    public void Log_analytics_options_belong_to_it_only()
    {
        var e = Endpoint(PostgresEngine.LogSourceAzureFiles);
        e.Options["azure_log_table"] = "PGSQLServerLogs";

        Assert.Contains(new PostgresEngine().Validate(e), p => p == "options.azure_log_table: only with log_source: azure_log_analytics");
    }
}
