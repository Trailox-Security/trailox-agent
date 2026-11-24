using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Azure.Core;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// A server log Azure keeps for the agent to read: the server log download (<see cref="AzureLogReader"/>) or the rows a
/// diagnostic setting sends to Log Analytics (<see cref="AzureLogAnalyticsReader"/>).
/// </summary>
public interface IAzureLog : IPgLogLines
{
    /// <summary>The time of the oldest line Azure still keeps; null when it keeps none.</summary>
    Task<long?> OldestMicrosAsync(CancellationToken ct);

    AzureReadStats Stats { get; }
}

/// <summary>The tables a flexible server's <c>PostgreSQLLogs</c> reach in Log Analytics.</summary>
public static class AzureLogTables
{
    /// <summary>A diagnostic setting's "Resource specific" destination, the one Microsoft recommends.</summary>
    public const string ResourceSpecific = "PGSQLServerLogs";

    /// <summary>The legacy "Azure diagnostics" destination, shared with other resources' logs (category <c>PostgreSQLLogs</c>).</summary>
    public const string AzureDiagnostics = "AzureDiagnostics";

    public static readonly IReadOnlyList<string> All = new[] { ResourceSpecific, AzureDiagnostics };
}

/// <summary>Where the server's rows are read: a table, and the one workspace they are read from when they reach several.</summary>
/// <param name="Workspace">A workspace id (<c>TenantId</c> of its rows), or null for every workspace the query reaches.</param>
public readonly record struct AzureLogTarget(string Table, string? Workspace = null);

/// <summary>One row of the server's log in Log Analytics: its time (100 ns ticks), its id, the record's text and fields.</summary>
public sealed record AzureLogRow(long Ticks, string ItemId, string Message, IReadOnlyList<PgAzureLine.Field> Fields)
{
    public long Micros => (Ticks - DateTime.UnixEpoch.Ticks) / 10;

    public AzureLogPosition Position => new(Ticks, ItemId);

    /// <summary>The record as the server log download writes it, so one parser reads both (<see cref="PgAzureLine"/>).</summary>
    public string Line() => PgAzureLine.Join(Message, Fields);
}

/// <summary>Where a page of rows ended. Rows are read in the order of their time, then of their id.</summary>
public readonly record struct AzureLogPosition(long Ticks, string ItemId);

/// <summary>One answer of the query API.</summary>
/// <param name="Partial">
/// Azure cut the answer short (past 500,000 rows or about 64 MB) and said so beside the rows; its rows are not read.
/// </param>
public sealed record AzureLogPage(IReadOnlyList<AzureLogRow> Rows, long Bytes, bool Partial);

/// <summary>How many of the server's rows a table received in one workspace in the last day, and the newest.</summary>
public sealed record AzureLogTableRows(string Table, string Workspace, long Rows, long? NewestMicros);

/// <summary>The reads the agent makes for <c>log_source: azure_log_analytics</c>. A seam, so tests run without Azure.</summary>
public interface IAzureLogQuery
{
    /// <summary>
    /// Up to <paramref name="max"/> of the server's rows of <paramref name="target"/> stamped from
    /// <paramref name="fromMicros"/> to before <paramref name="toMicros"/>, after <paramref name="after"/>, in order.
    /// </summary>
    Task<AzureLogPage> RowsAsync(AzureLogTarget target, long fromMicros, long toMicros, AzureLogPosition? after, int max, CancellationToken ct);

    /// <summary>Whether <paramref name="target"/> holds a row of the server stamped at or after <paramref name="micros"/>.</summary>
    Task<bool> AnyFromAsync(AzureLogTarget target, long micros, CancellationToken ct);

    /// <summary>The server's rows in each of <see cref="AzureLogTables.All"/>, by workspace, over the last day.</summary>
    Task<IReadOnlyList<AzureLogTableRows>> TablesAsync(CancellationToken ct);

    /// <summary>The time of the server's oldest row of <paramref name="target"/>; null when it has none.</summary>
    Task<long?> OldestMicrosAsync(AzureLogTarget target, CancellationToken ct);
}

/// <summary>
/// The server's log in Azure Monitor Logs, through the Log Analytics query API, as the agent's own Azure identity
/// (<see cref="ArmAzureLogFiles.Credential"/>).
/// </summary>
/// <remarks>
/// <para>
/// Without a workspace the query is the server's own (<c>/v1/{the server's resource id}/query</c>): Azure answers it with
/// the server's rows only, in whatever workspace they are, and it needs <c>Microsoft.Insights/logs/*/read</c> on the
/// server, nothing else (measured with that role alone). Azure refuses a role that names one table
/// (<c>Microsoft.Insights/logs/PGSQLServerLogs/read</c>: "does not match any of the actions supported", measured). The
/// workspace must allow resource permissions, as every workspace made since 2019 does by default. With
/// <c>azure_workspace_id</c> the workspace is queried and its rows filtered on the server's <c>_ResourceId</c>: that
/// needs, on the workspace, a custom role with <c>Microsoft.OperationalInsights/workspaces/read</c>,
/// <c>.../workspaces/query/read</c> and <c>.../workspaces/query/PGSQLServerLogs/read</c>, or Log Analytics Reader
/// (both measured). Without <c>query/read</c> the role is refused. With it, <c>search *</c> over the workspace reaches
/// <c>PGSQLServerLogs</c> only (the server's sessions table answered no rows), but every server's rows in it: the
/// server's own query, which reaches this server's logs only, is the narrower grant.
/// </para>
/// <para>
/// Measured: an answer stops at 500,000 rows or about 64 MB of JSON, still with 200 and the rows it has, and a
/// <c>PartialError</c> beside them; 850 small queries in 35 s were not throttled. 🔴 Azure answers a query whose text
/// it saw in the last 2 minutes or so from a cache, without rows that came since: every query asks for a fresh answer
/// (<c>Cache-Control: no-cache</c>, measured to work).
/// </para>
/// </remarks>
public sealed class ArmLogAnalyticsQuery : IAzureLogQuery
{
    public const string Api = "https://api.loganalytics.azure.com/v1";

    /// <summary>The longest a query may run (<c>Prefer: wait</c>); Azure's own default is 3 minutes, its limit 10.</summary>
    public const int WaitSeconds = 600;

    /// <summary>How often a throttled query (429, 503) is sent again before the window fails.</summary>
    internal const int Retries = 3;

    // Entra ID has no resource api.loganalytics.azure.com (AADSTS500011, measured): the token is for the older name.
    private static readonly TokenRequestContext Scope = new(new[] { "https://api.loganalytics.io/.default" });

    /// <summary>How long the oldest row of a target is kept before it is asked again.</summary>
    private static readonly TimeSpan OldestFor = TimeSpan.FromHours(1);

    private readonly HttpClient _http;
    private readonly TokenCredential _credential;
    private readonly AzureCallBudget _budget;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly Func<DateTime> _utcNow;
    private readonly string _server;
    private readonly string? _workspace;
    private readonly string? _resourceFilter;
    private readonly string _url;
    private readonly Dictionary<AzureLogTarget, (DateTime At, long? Micros)> _oldest = new();
    private AccessToken _token;

    public ArmLogAnalyticsQuery(HttpClient http, string subscription, string resourceGroup, string server, string? workspaceId = null,
        TokenCredential? credential = null, Func<TimeSpan, CancellationToken, Task>? delay = null, Func<DateTime>? utcNow = null)
    {
        _http = http;
        _credential = credential ?? ArmAzureLogFiles.Credential();
        _budget = AzureCallBudget.For("api.loganalytics.azure.com");
        _delay = delay ?? Task.Delay;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _server = server;
        _workspace = workspaceId;
        var resource = $"/subscriptions/{Uri.EscapeDataString(subscription)}/resourceGroups/{Uri.EscapeDataString(resourceGroup)}"
            + $"/providers/Microsoft.DBforPostgreSQL/flexibleServers/{Uri.EscapeDataString(server)}";
        _url = workspaceId == null ? $"{Api}{resource}/query" : $"{Api}/workspaces/{Uri.EscapeDataString(workspaceId)}/query";
        // A workspace holds every resource's rows: only the server's are read.
        _resourceFilter = workspaceId == null ? null : $"/subscriptions/{subscription}/resourceGroups/{resourceGroup}"
            + $"/providers/Microsoft.DBforPostgreSQL/flexibleServers/{server}";
    }

    public async Task<AzureLogPage> RowsAsync(AzureLogTarget target, long fromMicros, long toMicros, AzureLogPosition? after, int max, CancellationToken ct)
    {
        var (doc, bytes) = await QueryAsync(RowsQuery(target, _resourceFilter, fromMicros, toMicros, after, max), (fromMicros, toMicros), target.Table, ct);
        using (doc)
        {
            return Page(doc, bytes);
        }
    }

    public async Task<bool> AnyFromAsync(AzureLogTarget target, long micros, CancellationToken ct)
    {
        var (doc, _) = await QueryAsync($"{Source(target, _resourceFilter)}\n| where TimeGenerated >= {Time(micros)}\n| take 1\n| project TimeGenerated",
            (micros, Micros(_utcNow()) + 86_400_000_000L), target.Table, ct);
        using (doc)
        {
            return Rows(doc.RootElement).Any();
        }
    }

    public async Task<IReadOnlyList<AzureLogTableRows>> TablesAsync(CancellationToken ct)
    {
        var parts = AzureLogTables.All.Select(t => $"({Source(new AzureLogTarget(t), _resourceFilter)}\n| where TimeGenerated > ago(1d)\n"
            + $"| summarize Rows = count(), Newest = max(TimeGenerated) by TenantId\n| extend Table = {Literal(t)})");
        // isfuzzy: a workspace that never received the legacy table has none; the other table is still counted.
        var (doc, _) = await QueryAsync($"union isfuzzy=true\n{string.Join(",\n", parts)}\n| project Table, TenantId, Rows, Newest", null, "", ct);
        using (doc)
        {
            var found = new List<AzureLogTableRows>();
            foreach (var row in Rows(doc.RootElement))
            {
                var rows = row.TryGetValue("Rows", out var r) && r.ValueKind == JsonValueKind.Number && r.TryGetInt64(out var n) ? n : 0;
                found.Add(new AzureLogTableRows(Str(row, "Table") ?? "", Str(row, "TenantId") ?? "", rows,
                    Str(row, "Newest") is { } at ? Micros(Ticks(at)) : null));
            }
            return found;
        }
    }

    public async Task<long?> OldestMicrosAsync(AzureLogTarget target, CancellationToken ct)
    {
        if (_oldest.TryGetValue(target, out var known) && _utcNow() - known.At < OldestFor)
        {
            return known.Micros;
        }
        var (doc, _) = await QueryAsync($"{Source(target, _resourceFilter)}\n| summarize Oldest = min(TimeGenerated)", null, target.Table, ct);
        using (doc)
        {
            long? oldest = Rows(doc.RootElement).Select(r => Str(r, "Oldest")).FirstOrDefault(s => s != null) is { } at ? Micros(Ticks(at)) : null;
            _oldest[target] = (_utcNow(), oldest);
            return oldest;
        }
    }

    /// <summary>
    /// The server's rows of <paramref name="target"/>: in a workspace filtered on the server's resource id, and from one
    /// workspace only when it names one.
    /// </summary>
    internal static string Source(AzureLogTarget target, string? resourceFilter)
    {
        var q = new StringBuilder(target.Table == AzureLogTables.AzureDiagnostics
            ? "AzureDiagnostics\n| where Category == 'PostgreSQLLogs'"
            : AzureLogTables.ResourceSpecific);
        if (resourceFilter != null)
        {
            q.Append("\n| where _ResourceId =~ ").Append(Literal(resourceFilter));
        }
        if (target.Workspace != null)
        {
            q.Append("\n| where TenantId == ").Append(Literal(target.Workspace));
        }
        return q.ToString();
    }

    /// <summary>
    /// One page of rows, in order of time then id: with a page's last row as <paramref name="after"/>, the next page
    /// starts right after it, whatever else arrived meanwhile.
    /// </summary>
    internal static string RowsQuery(AzureLogTarget target, string? resourceFilter, long fromMicros, long toMicros, AzureLogPosition? after, int max)
    {
        var q = new StringBuilder(Source(target, resourceFilter));
        q.Append($"\n| where TimeGenerated >= {Time(fromMicros)} and TimeGenerated < {Time(toMicros)}");
        if (after is { } p)
        {
            var at = TimeOfTicks(p.Ticks);
            q.Append($"\n| where TimeGenerated > {at} or (TimeGenerated == {at} and strcmp(_ItemId, {Literal(p.ItemId)}) > 0)");
        }
        // The legacy table's names follow its _s suffixes; only Message is documented. Not measured: the legacy
        // table is read as Microsoft documents it, the resource-specific one as measured.
        q.Append(target.Table == AzureLogTables.AzureDiagnostics
            ? "\n| project TimeGenerated, _ItemId, Message, Statement = tostring(column_ifexists('statement_s', '')), "
                + "DetailLog = tostring(column_ifexists('detail_log_s', '')), Detail = tostring(column_ifexists('Detail', '')), "
                + "Context = tostring(column_ifexists('context_s', ''))"
            : "\n| project TimeGenerated, _ItemId, Message, Statement, DetailLog, Detail, Context");
        // top takes one key only (SYN0002 at the second, measured): sort, then take, which keeps the order.
        q.Append($"\n| sort by TimeGenerated asc, _ItemId asc\n| take {max}");
        return q.ToString();
    }

    /// <summary>A page of the query's answer. An answer cut short gives no rows: it is asked again with fewer.</summary>
    internal static AzureLogPage Page(JsonDocument doc, long bytes)
    {
        if (doc.RootElement.ValueKind == JsonValueKind.Object && doc.RootElement.TryGetProperty("error", out var e) && e.ValueKind == JsonValueKind.Object)
        {
            return new AzureLogPage(Array.Empty<AzureLogRow>(), bytes, Partial: true);
        }
        var rows = new List<AzureLogRow>();
        foreach (var row in Rows(doc.RootElement))
        {
            if (Str(row, "TimeGenerated") is not { } at || Str(row, "_ItemId") is not { } id)
            {
                throw new SourceException("Log Analytics answered a row with no TimeGenerated or _ItemId");
            }
            rows.Add(new AzureLogRow(Ticks(at), id, Str(row, "Message") ?? "", new[]
            {
                new PgAzureLine.Field("detail", Str(row, "Detail") ?? ""),
                new PgAzureLine.Field("detail_log", Str(row, "DetailLog") ?? ""),
                new PgAzureLine.Field("context", Str(row, "Context") ?? ""),
                new PgAzureLine.Field("statement", Str(row, "Statement") ?? ""),
            }));
        }
        return new AzureLogPage(rows, bytes, Partial: false);
    }

    /// <summary>The first table's rows, each as its columns by name.</summary>
    private static IEnumerable<Dictionary<string, JsonElement>> Rows(JsonElement root)
    {
        if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("tables", out var tables)
            || tables.ValueKind != JsonValueKind.Array || tables.GetArrayLength() == 0)
        {
            yield break;
        }
        var table = tables[0];
        if (table.ValueKind != JsonValueKind.Object || !table.TryGetProperty("columns", out var columns) || columns.ValueKind != JsonValueKind.Array
            || !table.TryGetProperty("rows", out var rows) || rows.ValueKind != JsonValueKind.Array)
        {
            throw new SourceException("Log Analytics answered a table with no columns or rows");
        }
        var names = columns.EnumerateArray()
            .Select(c => c.ValueKind == JsonValueKind.Object && c.TryGetProperty("name", out var n) && n.ValueKind == JsonValueKind.String ? n.GetString()! : "")
            .ToList();
        foreach (var row in rows.EnumerateArray())
        {
            if (row.ValueKind != JsonValueKind.Array)
            {
                throw new SourceException("Log Analytics answered a row that is not a list of values");
            }
            var values = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
            var i = 0;
            foreach (var v in row.EnumerateArray())
            {
                if (i < names.Count)
                {
                    values[names[i]] = v;
                }
                i++;
            }
            yield return values;
        }
    }

    private static string? Str(Dictionary<string, JsonElement> row, string name) =>
        row.TryGetValue(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

    private static string? Str(JsonElement o, string name) =>
        o.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

    /// <summary>
    /// A time as Azure writes it. 🔴 Compare times, never their text: Azure drops trailing zeros (<c>20.29Z</c> is after
    /// <c>20.291Z</c> as text, before it as a time).
    /// </summary>
    internal static long Ticks(string at) =>
        DateTimeOffset.TryParse(at, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var t)
            ? t.UtcTicks
            : throw new SourceException($"Log Analytics answered a time that does not read: {Runner.EndpointSession.Trim(at)}");

    private static long Micros(long ticks) => (ticks - DateTime.UnixEpoch.Ticks) / 10;

    private static long Micros(DateTime utc) => Micros(utc.Ticks);

    internal static string Time(long micros) => TimeOfTicks(DateTime.UnixEpoch.Ticks + micros * 10);

    private static string TimeOfTicks(long ticks) =>
        "datetime(" + new DateTime(ticks, DateTimeKind.Utc).ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture) + ")";

    /// <summary>A KQL string literal. Every value in a query is the agent's own or validated; this keeps it so.</summary>
    internal static string Literal(string s) => "'" + s.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("'", "\\'", StringComparison.Ordinal) + "'";

    /// <summary>
    /// One query. The answer is parsed from its bytes, never turned into a string first: a page can be 64 MB of JSON.
    /// </summary>
    private async Task<(JsonDocument Doc, long Bytes)> QueryAsync(string kql, (long From, long To)? span, string table, CancellationToken ct)
    {
        for (var attempt = 0; ; attempt++)
        {
            await _budget.WaitAsync(ct);
            var body = new Dictionary<string, string> { ["query"] = kql };
            if (span is { } s)
            {
                // The time span narrows what Azure scans; the query's own filter is the one that counts.
                body["timespan"] = Time(s.From)[9..^1] + "/" + Time(s.To)[9..^1];
            }
            using var request = new HttpRequestMessage(HttpMethod.Post, _url)
            {
                Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json"),
            };
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await TokenAsync(ct));
            request.Headers.TryAddWithoutValidation("Prefer", $"wait={WaitSeconds}");
            request.Headers.CacheControl = new CacheControlHeaderValue { NoCache = true };
            using var response = await SendAsync(request, ct);
            var bytes = await response.Content.ReadAsByteArrayAsync(ct);
            if (response.IsSuccessStatusCode)
            {
                try
                {
                    return (JsonDocument.Parse(bytes), bytes.Length);
                }
                catch (JsonException)
                {
                    throw new SourceException($"query {_server}'s log in Log Analytics: the answer is not JSON");
                }
            }
            if (response.StatusCode is HttpStatusCode.TooManyRequests or HttpStatusCode.ServiceUnavailable && attempt < Retries)
            {
                await _delay(response.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(10 * (attempt + 1)), ct);
                continue;
            }
            throw Worded(response.StatusCode, bytes, table);
        }
    }

    private async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        try
        {
            return await _http.SendAsync(request, HttpCompletionOption.ResponseContentRead, ct);
        }
        catch (HttpRequestException ex)
        {
            throw new SourceException($"Azure {request.RequestUri?.Host}: {ex.Message}");
        }
    }

    private async Task<string> TokenAsync(CancellationToken ct)
    {
        if (_token.ExpiresOn > DateTimeOffset.UtcNow.AddMinutes(5))
        {
            return _token.Token;
        }
        try
        {
            _token = await _credential.GetTokenAsync(Scope, ct);
            return _token.Token;
        }
        catch (Azure.Identity.AuthenticationFailedException ex)
        {
            throw new SourceException($"query {_server}'s log in Log Analytics: no Azure identity ({Runner.EndpointSession.Trim(ex.Message)}). "
                + "Give the agent a managed identity, or a service principal in AZURE_TENANT_ID, AZURE_CLIENT_ID and AZURE_CLIENT_SECRET");
        }
    }

    /// <summary>
    /// A failed query as what to change. Azure nests its errors: a missing table is <c>BadArgumentError</c>, in it
    /// <c>SemanticError</c>, in that <c>SEM0100</c> "Failed to resolve table or column expression named '...'" (measured).
    /// </summary>
    private SourceException Worded(HttpStatusCode status, byte[] body, string table)
    {
        var codes = new List<string>();
        var inner = "";
        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.ValueKind == JsonValueKind.Object && doc.RootElement.TryGetProperty("error", out var e) && e.ValueKind == JsonValueKind.Object)
            {
                // The innermost error says what is wrong; the outer ones say the request was invalid.
                for (var x = e; ; )
                {
                    if (Str(x, "code") is { Length: > 0 } name)
                    {
                        codes.Add(name);
                    }
                    if (Str(x, "message") is { Length: > 0 } said)
                    {
                        inner = said;
                    }
                    if (!x.TryGetProperty("innererror", out var i) || i.ValueKind != JsonValueKind.Object)
                    {
                        break;
                    }
                    x = i;
                }
            }
        }
        catch (JsonException)
        {
        }
        var code = codes.LastOrDefault() ?? "";
        var what = $"query {_server}'s log in Log Analytics";
        var detail = inner.Length > 0 ? $": {Runner.EndpointSession.Trim(inner)}" : "";
        var noTable = table.Length > 0 && codes.Contains("SemanticError") && inner.Contains($"named '{table}'", StringComparison.Ordinal);
        return status switch
        {
            HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden => new SourceException(_workspace == null
                ? $"{what}: access denied ({code}). The agent's Azure identity needs Microsoft.Insights/logs/*/read on the server "
                    + "(a custom role assigned on the server), or set options.azure_workspace_id and give it read access there"
                : $"{what}: access denied ({code}). The agent's Azure identity needs, on workspace {_workspace}, a role with "
                    + "Microsoft.OperationalInsights/workspaces/read, workspaces/query/read and "
                    + $"workspaces/query/{(table.Length > 0 ? table : AzureLogTables.ResourceSpecific)}/read (or Log Analytics Reader); "
                    + "or leave out options.azure_workspace_id and give it Microsoft.Insights/logs/*/read on the server"),
            HttpStatusCode.NotFound => new SourceException(_workspace == null
                ? $"{what}: no such server ({code}): check options.azure_subscription, azure_resource_group and azure_server"
                : $"{what}: no such workspace ({code}): check options.azure_workspace_id, the workspace's id (a GUID, not its name)"),
            HttpStatusCode.BadRequest when noTable => new SourceException(
                $"{what}: no table {table} there. Send the server's PostgreSQLLogs there with a diagnostic setting, "
                + $"or set options.azure_log_table to the table it uses ({string.Join(" or ", AzureLogTables.All)})"),
            HttpStatusCode.GatewayTimeout => new SourceException($"{what}: the query ran out of its {WaitSeconds} s"),
            HttpStatusCode.TooManyRequests => new SourceException($"{what}: throttled ({code}), {Retries + 1} times"),
            _ => new SourceException($"{what} failed: {(int)status} {code}{detail}".TrimEnd()),
        };
    }
}

/// <summary>
/// Reads the lines of an Azure server's log for a stretch of time from Log Analytics (<see cref="IAzureLogQuery"/>):
/// pages of rows in order of time, each made the line the server log download would hold.
/// </summary>
/// <remarks>
/// <para>
/// The order is the rows' time to the millisecond, then their id. 🔴 A sign-in's lines can share a millisecond
/// (<c>connection authorized</c> before <c>connection authenticated</c> in 87 of 981 sessions by time alone, measured);
/// by id, none of the 6,480 rows measured was out of order. The id is a version-1 UUID whose text begins with the low
/// bits of its time, which wrap about every 7 minutes: two lines of one millisecond can still swap at a wrap. That is
/// rare, and swaps two lines of one sign-in, which the session map reads in either order.
/// </para>
/// <para>
/// A page that Azure cuts short is read again with half as many rows: a page counts only when it is whole. Pages are
/// sized to about <see cref="PageBytes"/> by the rows of the page before, so long statements make smaller pages.
/// </para>
/// </remarks>
public sealed class AzureLogAnalyticsReader : IAzureLog
{
    public const int PageRows = 10_000;
    public const int MinPageRows = 100;

    /// <summary>About how large a page's answer is meant to be: well inside the agent's memory, far below Azure's 64 MB.</summary>
    public const long PageBytes = 8 << 20;

    /// <summary>Rows from a second before the stretch: records of one second can be out of order (<see cref="IPgLogLines"/>).</summary>
    private const long BeforeMicros = 1_000_000;

    /// <summary>How far past the stretch rows are asked for, to give the first line past it.</summary>
    private const long AfterMicros = 60_000_000;

    private readonly IAzureLogQuery _query;
    private readonly int _pageRows;

    public AzureLogAnalyticsReader(IAzureLogQuery query, AzureLogTarget target, int pageRows = PageRows)
    {
        _query = query;
        Target = target;
        _pageRows = Math.Max(MinPageRows, pageRows);
    }

    /// <summary>The table, and workspace, the rows are read from.</summary>
    public AzureLogTarget Target { get; }

    /// <summary>The queries made and the bytes of their answers (no files: <see cref="AzureReadStats.Files"/> stays 0).</summary>
    public AzureReadStats Stats { get; } = new();

    public Task<long?> OldestMicrosAsync(CancellationToken ct) => _query.OldestMicrosAsync(Target, ct);

    /// <summary>Whether Log Analytics has received a row stamped at or after <paramref name="micros"/>.</summary>
    public Task<bool> WrittenPastAsync(long micros, CancellationToken ct) => _query.AnyFromAsync(Target, micros, ct);

    public async IAsyncEnumerable<PgFileLine> LinesAsync(long fromMicros, long toMicros, Func<string, long?> timeOf,
        [EnumeratorCancellation] CancellationToken ct)
    {
        AzureLogPosition? after = null;
        var max = _pageRows;
        while (true)
        {
            var page = await _query.RowsAsync(Target, fromMicros - BeforeMicros, toMicros + AfterMicros, after, max, ct);
            Stats.Reads++;
            Stats.Bytes += page.Bytes;
            if (page.Partial)
            {
                if (max == MinPageRows)
                {
                    throw new SourceException($"Log Analytics cut short even {MinPageRows} rows of the server's log (its answers stop at about 64 MB)");
                }
                max = Math.Max(MinPageRows, max / 2);
                continue;
            }
            foreach (var row in page.Rows)
            {
                if (row.Message.Length == 0)
                {
                    // No record to read: a line of nothing would join the record before it.
                    Stats.Unreadable++;
                    continue;
                }
                var line = row.Line();
                yield return new PgFileLine(Target.Table, line);
                if (timeOf(line) is { } at && at > toMicros)
                {
                    yield break;
                }
            }
            if (page.Rows.Count < max)
            {
                yield break;
            }
            after = page.Rows[^1].Position;
            max = NextPageRows(page.Rows.Count, page.Bytes);
        }
    }

    /// <summary>The next page's rows: as many as make about <see cref="PageBytes"/> at the last page's bytes a row.</summary>
    internal int NextPageRows(int rows, long bytes) =>
        (int)Math.Clamp(PageBytes / Math.Max(1, bytes / Math.Max(1, rows)), MinPageRows, _pageRows);
}
