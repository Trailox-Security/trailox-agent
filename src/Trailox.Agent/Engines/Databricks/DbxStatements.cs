using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace Trailox.Agent.Engines.Databricks;

/// <summary>A settled statement: its column names and how to page its result.</summary>
public sealed record DbxResult(string StatementId, IReadOnlyList<string> Columns, int TotalChunks, long TotalRows, IReadOnlyList<DbxChunkLink> FirstLinks);

public sealed record DbxChunkLink(int ChunkIndex, long RowCount, string Url);

/// <summary>
/// The SQL Statement Execution API: submit, poll to a terminal state, page the result through
/// EXTERNAL_LINKS. INLINE is not used: it caps at 25 MiB and fails outright above it, which
/// would leave one oversized window failing forever.
/// </summary>
public sealed class DbxStatements
{
    private const string Statements = "/api/2.0/sql/statements/";
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(2);

    private readonly HttpClient _api;
    private readonly HttpClient _links;
    private readonly DbxAuth _auth;
    private readonly string _host;
    private readonly string _warehouseId;

    public DbxStatements(HttpClient api, HttpClient links, DbxAuth auth, string host, string warehouseId)
    {
        _api = api;
        _links = links;
        _auth = auth;
        _host = host;
        _warehouseId = warehouseId;
    }

    /// <summary>
    /// Runs a statement to completion. A warehouse that is asleep pays its cold start inside the
    /// 50-second server-side wait; anything longer is polled. Terminal failures carry the
    /// workspace's own message, which holds the SQLSTATE.
    /// </summary>
    public async Task<DbxResult> ExecuteAsync(string sql, CancellationToken ct)
    {
        var body = JsonSerializer.Serialize(new
        {
            warehouse_id = _warehouseId,
            statement = sql,
            wait_timeout = "50s",
            disposition = "EXTERNAL_LINKS",
            format = "JSON_ARRAY",
        });
        var root = await SendAsync(HttpMethod.Post, Statements, body, ct);
        var statementId = Get(root, "statement_id").GetString() ?? "";

        while (true)
        {
            var status = Get(root, "status");
            var state = Get(status, "state").GetString();
            switch (state)
            {
                case "SUCCEEDED":
                    return Parse(root, statementId);
                case "PENDING":
                case "RUNNING":
                    await Task.Delay(PollInterval, ct);
                    root = await SendAsync(HttpMethod.Get, Statements + statementId, null, ct);
                    continue;
                default:
                    var message = status.TryGetProperty("error", out var err) && err.ValueKind == JsonValueKind.Object && err.TryGetProperty("message", out var m)
                        ? m.GetString() : null;
                    throw new DbxException(200, $"statement {statementId} ended {state}: {message ?? "(no message)"}");
            }
        }
    }

    /// <summary>A single value (first column of the first row), or null for an empty result.</summary>
    public async Task<string?> ScalarAsync(string sql, CancellationToken ct)
    {
        var result = await ExecuteAsync(sql, ct);
        await foreach (var row in RowsAsync(result, ct))
        {
            return row.GetArrayLength() > 0 && row[0].ValueKind != JsonValueKind.Null ? row[0].ToString() : null;
        }
        return null;
    }

    /// <summary>A result page that could not be fetched, naming the storage HOST.</summary>
    /// <remarks>
    /// Never the URL: its query string is a signature that grants the page to whoever holds it,
    /// for fifteen minutes. The host is all an allow-list needs.
    /// </remarks>
    internal static string StorageFailure(string url, int chunkIndex, string reason)
    {
        var host = Uri.TryCreate(url, UriKind.Absolute, out var uri) ? uri.Host : "(unreadable link)";
        return $"result page {chunkIndex} could not be fetched from the workspace's result storage at {host} ({reason}). "
             + $"If outbound traffic is restricted where the agent runs, allow HTTPS to {host}.";
    }

    /// <summary>
    /// The rows of every page in order, one JSON array each, streamed: a page is parsed element
    /// by element as it downloads and is never held whole. Pages after the first are addressed by
    /// chunk index on the statement, which also sidesteps the fifteen-minute link expiry.
    /// </summary>
    public async IAsyncEnumerable<JsonElement> RowsAsync(DbxResult result, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        for (var chunk = 0; chunk < result.TotalChunks; chunk++)
        {
            var links = chunk == 0 && result.FirstLinks.Count > 0
                ? result.FirstLinks
                : await ChunkLinksAsync(result.StatementId, chunk, ct);
            foreach (var link in links.Where(l => l.ChunkIndex == chunk))
            {
                // A strict outbound policy shows up HERE, not on the workspace: result pages are
                // presigned links on the workspace's result storage, a second host. The error names
                // that host so the allow-list can be fixed from the log.
                HttpResponseMessage response;
                try
                {
                    response = await _links.GetAsync(link.Url, HttpCompletionOption.ResponseHeadersRead, ct);
                }
                catch (Exception ex) when (ex is HttpRequestException || (ex is TaskCanceledException && !ct.IsCancellationRequested))
                {
                    throw new SourceException(StorageFailure(link.Url, link.ChunkIndex, ex.Message));
                }

                using (response)
                {
                    if (!response.IsSuccessStatusCode)
                    {
                        throw new DbxException((int)response.StatusCode, StorageFailure(link.Url, link.ChunkIndex, "the request was refused"));
                    }
                    await using var stream = await response.Content.ReadAsStreamAsync(ct);
                    await foreach (var row in JsonSerializer.DeserializeAsyncEnumerable<JsonElement>(stream, cancellationToken: ct))
                    {
                        yield return row;
                    }
                }
            }
        }
    }

    private async Task<IReadOnlyList<DbxChunkLink>> ChunkLinksAsync(string statementId, int chunkIndex, CancellationToken ct)
    {
        var root = await SendAsync(HttpMethod.Get, $"{Statements}{statementId}/result/chunks/{chunkIndex}", null, ct);
        return ParseLinks(root);
    }

    /// <remarks>
    /// Every way a request can fail ends as a <see cref="SourceException"/>, as in DbxDirectory: a
    /// request that ran out of time, or an answer that is not JSON - a proxy's page answered with
    /// 200, say - is named as such. Only the caller's own cancellation, the agent stopping, is left
    /// as it is.
    /// </remarks>
    private async Task<JsonElement> SendAsync(HttpMethod method, string path, string? body, CancellationToken ct)
    {
        string payload;
        try
        {
            using var request = new HttpRequestMessage(method, $"https://{_host}{path}");
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await _auth.TokenAsync(ct));
            if (body != null)
            {
                request.Content = new StringContent(body, Encoding.UTF8, "application/json");
            }
            using var response = await _api.SendAsync(request, ct);
            payload = await response.Content.ReadAsStringAsync(ct);
            if (!response.IsSuccessStatusCode)
            {
                // An expired token answers with the bare text "Invalid Token", not JSON: the status
                // is checked before anything is parsed.
                throw new DbxException((int)response.StatusCode, Trim(payload));
            }
        }
        catch (TaskCanceledException ex) when (!ct.IsCancellationRequested)
        {
            throw new SourceException("the workspace did not answer a SQL statement request in time (" + ex.Message + ")");
        }

        try
        {
            using var doc = JsonDocument.Parse(payload);
            return doc.RootElement.Clone();
        }
        catch (JsonException)
        {
            throw new SourceException("the workspace answered a SQL statement request with something that is not JSON: " + Trim(payload));
        }
    }

    private static string Trim(string payload) => payload.Length > 600 ? payload[..600] : payload;

    /// <summary>
    /// A field the answer must have. Missing, it ends as a <see cref="SourceException"/> that names
    /// it, where the plain lookup would say only that some key was not there.
    /// </summary>
    private static JsonElement Get(JsonElement holder, string name) =>
        holder.ValueKind == JsonValueKind.Object && holder.TryGetProperty(name, out var value)
            ? value
            : throw new SourceException($"the workspace answered a SQL statement request without '{name}'");

    internal static DbxResult Parse(JsonElement root, string statementId)
    {
        var columns = new List<string>();
        var manifest = Get(root, "manifest");
        foreach (var column in Get(Get(manifest, "schema"), "columns").EnumerateArray())
        {
            columns.Add(Get(column, "name").GetString() ?? "");
        }
        var totalRows = manifest.TryGetProperty("total_row_count", out var t) && t.TryGetInt64(out var n) ? n : 0;
        var totalChunks = manifest.TryGetProperty("total_chunk_count", out var c) && c.TryGetInt32(out var k) ? k : 0;
        var first = root.TryGetProperty("result", out var result) ? ParseLinks(result) : Array.Empty<DbxChunkLink>();
        return new DbxResult(statementId, columns, totalChunks, totalRows, first);
    }

    private static IReadOnlyList<DbxChunkLink> ParseLinks(JsonElement holder)
    {
        var links = new List<DbxChunkLink>();
        if (holder.TryGetProperty("external_links", out var external))
        {
            foreach (var link in external.EnumerateArray())
            {
                links.Add(new DbxChunkLink(
                    Get(link, "chunk_index").GetInt32(),
                    link.TryGetProperty("row_count", out var rc) ? rc.GetInt64() : 0,
                    Get(link, "external_link").GetString() ?? ""));
            }
        }
        return links;
    }
}
