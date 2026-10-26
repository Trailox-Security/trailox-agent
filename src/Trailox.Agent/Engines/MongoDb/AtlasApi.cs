using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace Trailox.Agent.Engines.MongoDb;

/// <summary>
/// The Atlas Administration API as a service account: OAuth client credentials for a one-hour token,
/// reused until shortly before it expires (every token request mints a new token, and the token
/// endpoint allows 50 requests), JSON reads, paged lists and log-file downloads. Only GETs, and the
/// secret never leaves this process.
/// </summary>
/// <remarks>
/// The answers below were measured with a service account holding Project Read Only and
/// Project Cluster Log Viewer: a wrong secret is 401 invalid_client at the token step; a missing role is
/// 401 USER_UNAUTHORIZED (not 403); an unknown project is 404 GROUP_NOT_FOUND. A real project the account
/// is not a member of is 401 USER_CANNOT_ACCESS_GROUP on every read under it, the project itself included,
/// not the 404.
/// </remarks>
public sealed class AtlasApi
{
    public const string DefaultBaseUrl = "https://cloud.mongodb.com";
    internal const string JsonAccept = "application/vnd.atlas.2023-01-01+json";
    internal const string GzipAccept = "application/vnd.atlas.2023-02-01+gzip";
    private const int PageSize = 500;
    private const int MaxRetries = 4;
    private static readonly TimeSpan RefreshMargin = TimeSpan.FromMinutes(2);

    private readonly HttpClient _http;
    private readonly string _baseUrl;
    private readonly string _basic;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private string? _token;
    private DateTime _expiresAtUtc = DateTime.MinValue;

    public string ProjectId { get; }

    public AtlasApi(HttpClient http, string projectId, string clientId, string secret, string baseUrl = DefaultBaseUrl)
    {
        _http = http;
        ProjectId = projectId;
        _baseUrl = baseUrl.TrimEnd('/');
        _basic = Convert.ToBase64String(Encoding.UTF8.GetBytes(clientId + ":" + secret));
    }

    /// <summary>GET /api/atlas/v2/groups/{project}{path} as JSON. The caller disposes the document.</summary>
    public async Task<JsonDocument> GetJsonAsync(string path, string what, CancellationToken ct)
    {
        using var response = await SendAsync(() => Request(path, JsonAccept), what, ct);
        await using var body = await response.Content.ReadAsStreamAsync(ct);
        try
        {
            return await JsonDocument.ParseAsync(body, cancellationToken: ct);
        }
        catch (JsonException)
        {
            throw new AtlasException(200, "", $"Atlas answered {what} with something that is not JSON");
        }
    }

    /// <summary>Every item of a paged list ({results, totalCount}), page by page, cloned out of each page.</summary>
    public async Task<List<JsonElement>> ListAsync(string path, string what, CancellationToken ct)
    {
        var items = new List<JsonElement>();
        var separator = path.Contains('?') ? '&' : '?';
        for (var page = 1; page <= 1000; page++)
        {
            using var doc = await GetJsonAsync($"{path}{separator}itemsPerPage={PageSize}&pageNum={page}", what, ct);
            var root = doc.RootElement;
            if (root.ValueKind == JsonValueKind.Array)
            {
                // A few endpoints (custom roles) answer with a bare array and no paging.
                items.AddRange(root.EnumerateArray().Select(e => e.Clone()));
                return items;
            }
            if (!root.TryGetProperty("results", out var results) || results.ValueKind != JsonValueKind.Array)
            {
                throw new AtlasException(200, "", $"Atlas answered {what} without a results list");
            }
            var count = 0;
            foreach (var item in results.EnumerateArray())
            {
                items.Add(item.Clone());
                count++;
            }
            var total = root.TryGetProperty("totalCount", out var t) && t.TryGetInt32(out var n) ? n : items.Count;
            if (count < PageSize || items.Count >= total)
            {
                return items;
            }
        }
        return items;
    }

    /// <summary>
    /// The gzip log file of one process for [start, end] (epoch seconds), as the response stream. Atlas
    /// returns whole published chunks around the window (minutes early and late) and, with auditing off,
    /// the last audit file again: the caller keeps only the lines whose own time is in its window.
    /// </summary>
    /// <remarks>
    /// The <c>x-uncompressed-content-length</c> header carries the gzip length, measured: it is not used.
    /// </remarks>
    public async Task<HttpResponseMessage> DownloadLogAsync(string host, string logName, long startSeconds, long endSeconds, CancellationToken ct)
    {
        var path = $"/clusters/{Uri.EscapeDataString(host)}/logs/{logName}.gz?startDate={startSeconds}&endDate={endSeconds}";
        return await SendAsync(() => Request(path, GzipAccept), $"the {logName} log of {host}", ct, HttpCompletionOption.ResponseHeadersRead);
    }

    private HttpRequestMessage Request(string path, string accept)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, $"{_baseUrl}/api/atlas/v2/groups/{ProjectId}{path}");
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(accept));
        return request;
    }

    /// <summary>Sends with the bearer token; 429 and 5xx wait (Retry-After, else backoff with jitter) and retry.</summary>
    private async Task<HttpResponseMessage> SendAsync(Func<HttpRequestMessage> build, string what, CancellationToken ct,
        HttpCompletionOption completion = HttpCompletionOption.ResponseContentRead)
    {
        for (var attempt = 0; ; attempt++)
        {
            using var request = build();
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await TokenAsync(ct));
            HttpResponseMessage response;
            try
            {
                response = await _http.SendAsync(request, completion, ct);
            }
            catch (TaskCanceledException ex) when (!ct.IsCancellationRequested)
            {
                throw new SourceException($"Atlas did not answer {what} in time ({ex.Message})");
            }
            if (response.IsSuccessStatusCode)
            {
                return response;
            }
            var status = (int)response.StatusCode;
            var retryable = status == 429 || status >= 500;
            if (retryable && attempt < MaxRetries)
            {
                var wait = response.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(Math.Pow(2, attempt + 1));
                response.Dispose();
                await Task.Delay(wait + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 500)), ct);
                continue;
            }
            using (response)
            {
                throw await ErrorAsync(response, what, ct);
            }
        }
    }

    internal async Task<string> TokenAsync(CancellationToken ct)
    {
        if (_token != null && DateTime.UtcNow < _expiresAtUtc - RefreshMargin)
        {
            return _token;
        }
        await _gate.WaitAsync(ct);
        try
        {
            if (_token != null && DateTime.UtcNow < _expiresAtUtc - RefreshMargin)
            {
                return _token;
            }
            using var request = new HttpRequestMessage(HttpMethod.Post, $"{_baseUrl}/api/oauth/token")
            {
                Content = new FormUrlEncodedContent(new Dictionary<string, string> { ["grant_type"] = "client_credentials" }),
            };
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", _basic);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            HttpResponseMessage response;
            try
            {
                response = await _http.SendAsync(request, ct);
            }
            catch (TaskCanceledException ex) when (!ct.IsCancellationRequested)
            {
                throw new SourceException("Atlas did not answer the token request in time (" + ex.Message + ")");
            }
            using (response)
            {
                if (!response.IsSuccessStatusCode)
                {
                    // 🔴 The error body of the token endpoint names no secret, but a success body IS one:
                    //    only failures are ever described.
                    throw await ErrorAsync(response, "the service account's token request", ct);
                }
                var body = await response.Content.ReadAsStringAsync(ct);
                JsonDocument doc;
                try
                {
                    doc = JsonDocument.Parse(body);
                }
                catch (JsonException)
                {
                    throw new SourceException($"Atlas answered the token request with something that is not JSON ({body.Length} characters)");
                }
                using (doc)
                {
                    var root = doc.RootElement;
                    var token = root.ValueKind == JsonValueKind.Object && root.TryGetProperty("access_token", out var t) && t.ValueKind == JsonValueKind.String
                        ? t.GetString()
                        : null;
                    if (string.IsNullOrEmpty(token))
                    {
                        throw new SourceException("Atlas answered the token request without an access_token");
                    }
                    var seconds = root.TryGetProperty("expires_in", out var e) && e.TryGetInt32(out var n) ? n : 3600;
                    _token = token;
                    _expiresAtUtc = DateTime.UtcNow.AddSeconds(seconds);
                    return _token;
                }
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>The measured Atlas answers, each turned into what the customer has to fix.</summary>
    private static async Task<AtlasException> ErrorAsync(HttpResponseMessage response, string what, CancellationToken ct)
    {
        var status = (int)response.StatusCode;
        string body;
        try
        {
            body = await response.Content.ReadAsStringAsync(ct);
        }
        catch (HttpRequestException)
        {
            body = "";
        }
        string code = "", detail = "";
        try
        {
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            if (root.ValueKind == JsonValueKind.Object)
            {
                code = root.TryGetProperty("errorCode", out var c) && c.ValueKind == JsonValueKind.String ? c.GetString()!
                    : root.TryGetProperty("error", out var err) && err.ValueKind == JsonValueKind.String ? err.GetString()! : "";
                detail = root.TryGetProperty("detail", out var d) && d.ValueKind == JsonValueKind.String ? d.GetString()!
                    : root.TryGetProperty("error_description", out var ed) && ed.ValueKind == JsonValueKind.String ? ed.GetString()! : "";
            }
        }
        catch (JsonException)
        {
        }
        var message = (status, code) switch
        {
            (401, "invalid_client") => "Atlas rejected the service account's credentials: the client id in username or the secret is wrong, deleted or expired",
            (401, "USER_UNAUTHORIZED") => $"the service account may not read {what}: give it Project Read Only and Project Cluster Log Viewer on this project",
            (401, "USER_CANNOT_ACCESS_GROUP") => "the service account is not a member of the project in host: add it to that project with Project Read Only and Project Cluster Log Viewer",
            // Measured: Atlas checks the service account's own API access list even when the organization
            // does not require one. The token request still succeeds; every call after it answers this 403, and its
            // detail (appended below) names the refused address.
            (403, "IP_ADDRESS_NOT_ON_ACCESS_LIST") => $"Atlas refused {what} from this agent's address: add the agent's outbound address to the service account's API access list",
            (403, _) => $"Atlas refused {what} ({code})",
            (404, "GROUP_NOT_FOUND") => "Atlas knows no project with the id in host",
            (404, _) => $"Atlas found no {what} ({code})",
            _ => $"Atlas answered {what} with HTTP {status} {code}",
        };
        if (detail.Length > 0 && status != 401)
        {
            message += ": " + (detail.Length > 200 ? detail[..200] : detail);
        }
        return new AtlasException(status, code, message);
    }
}

/// <summary>Atlas refused or failed a request. A source failure for the runner.</summary>
public sealed class AtlasException : SourceException
{
    public int StatusCode { get; }
    public string ErrorCode { get; }

    public AtlasException(int statusCode, string errorCode, string message) : base(message)
    {
        StatusCode = statusCode;
        ErrorCode = errorCode;
    }

    public bool IsNotFound => StatusCode == (int)HttpStatusCode.NotFound;
}
