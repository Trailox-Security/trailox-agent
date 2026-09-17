using System.Text;
using System.Text.Json;
using Trailox.Agent.Config;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Engines.ClickHouse;

/// <summary>
/// The ClickHouse HTTP(S) interface, with the monitoring user's credentials in headers (never
/// in the URL) and <c>log_comment=trailox-agent</c> on every request. Streams results; never
/// holds a chunk in memory. Owns the two version-dependent facts (which columns exist, whether
/// system.users.auth_type is an array) so the runner does not have to.
/// </summary>
public sealed class ChSource : IEngineSource
{
    private readonly HttpClient _http;
    private readonly EndpointConfig _endpoint;
    private IReadOnlySet<string> _presentColumns = new HashSet<string>();
    private bool _usersAuthTypeIsArray = true;

    public ChSource(HttpClient http, EndpointConfig endpoint)
    {
        _http = http;
        _endpoint = endpoint;
    }

    public async Task<EndpointCaps> ProbeAsync(CancellationToken ct)
    {
        var version = await ScalarStringAsync(RawSelects.ProbeVersion, ct);
        var columnRows = await QueryRowsAsync(RawSelects.ProbeColumns, ct);
        var columns = columnRows.Count > 0
            ? columnRows[0][0].EnumerateArray().Select(e => e.GetString() ?? "").Where(s => s.Length > 0).ToHashSet(StringComparer.OrdinalIgnoreCase)
            : new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var missing = RawSelects.RequiredColumns.Where(c => !columns.Contains(c)).ToList();
        if (missing.Count > 0)
        {
            throw new SourceException($"server {version} lacks system.query_log columns {string.Join(", ", missing)}; ClickHouse 23.8 or newer is required");
        }

        var hasSessionLog = await ScalarLongAsync(RawSelects.ProbeSessionLog, ct) > 0;
        _presentColumns = columns;
        return new EndpointCaps
        {
            Version = version,
            Columns = RawSelects.ProbedColumns.Where(columns.Contains).ToList(),
            HasSessionLog = hasSessionLog,
            EarliestEventMicros = await SoftScalarAsync(RawSelects.ProbeEarliestEvent(_endpoint), ct),
            EarliestSessionMicros = hasSessionLog ? await SoftScalarAsync(RawSelects.ProbeEarliestSession(_endpoint), ct) : 0,
        };
    }

    public async Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct)
    {
        var sql = RawSelects.ForTask(_endpoint, _presentColumns, task, _usersAuthTypeIsArray);
        if (sql == null)
        {
            return null;
        }
        try
        {
            return await StreamAsync(sql, ct);
        }
        catch (ChException) when (task.Stream == ProtocolInfo.Streams.CatalogUsers && _usersAuthTypeIsArray)
        {
            // Older servers expose system.users.auth_type as a scalar; remember and retry once.
            _usersAuthTypeIsArray = false;
            return await StreamAsync(RawSelects.ForTask(_endpoint, _presentColumns, task, usersAuthTypeIsArray: false)!, ct);
        }
    }

    // ---- transport ----

    private Uri BuildUri(IReadOnlyDictionary<string, string>? parameters = null)
    {
        var query = new List<string> { "log_comment=" + Uri.EscapeDataString(RawSelects.LogComment) };
        if (parameters != null)
        {
            query.AddRange(parameters.Select(p => Uri.EscapeDataString(p.Key) + "=" + Uri.EscapeDataString(p.Value)));
        }
        return new Uri($"{(_endpoint.Tls ? "https" : "http")}://{_endpoint.Host}:{_endpoint.Port}/?{string.Join("&", query)}");
    }

    private HttpRequestMessage Request(string sql, IReadOnlyDictionary<string, string>? parameters = null)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, BuildUri(parameters)) { Content = new StringContent(sql, Encoding.UTF8) };
        request.Headers.Add("X-ClickHouse-User", _endpoint.Username);
        request.Headers.Add("X-ClickHouse-Key", _endpoint.Password);
        return request;
    }

    private static async Task ThrowOnError(HttpResponseMessage response, CancellationToken ct)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }
        var body = await response.Content.ReadAsStringAsync(ct);
        throw new ChException((int)response.StatusCode, body.Length > 600 ? body[..600] : body);
    }

    /// <summary>Probe queries are small and must fail fast; only the row streams get the long client timeout.</summary>
    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(60);

    internal async Task<List<JsonElement[]>> QueryRowsAsync(string sql, CancellationToken outer)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(outer);
        timeout.CancelAfter(ProbeTimeout);
        var ct = timeout.Token;
        using var request = Request(sql, new Dictionary<string, string> { ["default_format"] = "JSONCompact" });
        using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
        await ThrowOnError(response, ct);
        await using var stream = await response.Content.ReadAsStreamAsync(ct);
        using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
        return doc.RootElement.GetProperty("data").EnumerateArray()
            .Select(row => row.EnumerateArray().Select(e => e.Clone()).ToArray())
            .ToList();
    }

    private async Task<string> ScalarStringAsync(string sql, CancellationToken ct)
    {
        var rows = await QueryRowsAsync(sql, ct);
        return rows.Count > 0 && rows[0].Length > 0 ? rows[0][0].ToString() : "";
    }

    private async Task<long> ScalarLongAsync(string sql, CancellationToken ct)
    {
        var rows = await QueryRowsAsync(sql, ct);
        if (rows.Count == 0 || rows[0].Length == 0)
        {
            return 0;
        }
        var e = rows[0][0];
        return e.ValueKind == JsonValueKind.Number ? e.GetInt64() : long.TryParse(e.GetString(), out var n) ? n : 0;
    }

    /// <summary>Fails soft to 0: the caller treats it as "unknown" and keeps the full backfill window.</summary>
    private async Task<long> SoftScalarAsync(string sql, CancellationToken ct)
    {
        try
        {
            return await ScalarLongAsync(sql, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return 0;
        }
    }

    private async Task<RowStream> StreamAsync(string sql, CancellationToken ct)
    {
        var request = Request(sql);
        var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
        try
        {
            await ThrowOnError(response, ct);
            return new RowStream(await response.Content.ReadAsStreamAsync(ct), response);
        }
        catch
        {
            response.Dispose();
            throw;
        }
    }
}

public sealed class ChException : SourceException
{
    public int StatusCode { get; }

    public ChException(int statusCode, string body) : base($"ClickHouse answered HTTP {statusCode}: {body}")
    {
        StatusCode = statusCode;
    }
}
