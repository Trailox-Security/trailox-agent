using System.Net;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

namespace Trailox.Agent.Engines.Snowflake;

/// <summary>A settled statement: its column names, its first partition's rows and how many partitions follow.</summary>
public sealed record SfResult(string Handle, IReadOnlyList<string> Columns, int Partitions, long TotalRows, JsonElement FirstRows);

/// <summary>
/// The SQL API: submit, poll to completion, page the result by partition. Partition 0 arrives
/// with the settled statement; the rest are fetched one at a time (gzip, decompressed by the
/// handler) and held only while they are emitted. Measured partitions are 0.1-1 MB uncompressed.
/// </summary>
public sealed class SfStatements
{
    private const string Statements = "/api/v2/statements";
    private const int StatementTimeoutSeconds = 1800;
    private const int MaxRetries = 4;
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(2);

    private readonly HttpClient _http;
    private readonly SfAuth _auth;
    private readonly string _host;
    private readonly string _warehouse;
    private readonly string _role;

    public SfStatements(HttpClient http, SfAuth auth, string host, string warehouse, string role)
    {
        _http = http;
        _auth = auth;
        _host = host;
        _warehouse = warehouse;
        _role = role;
    }

    /// <summary>
    /// Runs one statement to completion. 202 means still running (a suspended warehouse resumes
    /// inside it); 422 carries Snowflake's own error text, which names the object on a missing
    /// grant. The requestId makes a retried submit idempotent on Snowflake's side.
    /// </summary>
    public async Task<SfResult> ExecuteAsync(string sql, CancellationToken ct)
    {
        var body = JsonSerializer.Serialize(new
        {
            statement = sql,
            timeout = StatementTimeoutSeconds,
            warehouse = _warehouse,
            role = _role,
        });
        var (status, root) = await SendAsync(HttpMethod.Post, $"{Statements}?requestId={Guid.NewGuid()}", body, ct);
        while (status == HttpStatusCode.Accepted)
        {
            var handle = root.GetProperty("statementHandle").GetString() ?? "";
            await Task.Delay(PollInterval, ct);
            (status, root) = await SendAsync(HttpMethod.Get, $"{Statements}/{handle}", null, ct);
        }
        return Parse(root);
    }

    /// <summary>A single value (first column of the first row), or null for an empty result.</summary>
    public async Task<string?> ScalarAsync(string sql, CancellationToken ct)
    {
        var result = await ExecuteAsync(sql, ct);
        await foreach (var row in RowsAsync(result, ct))
        {
            return row.GetArrayLength() > 0 && row[0].ValueKind != JsonValueKind.Null ? row[0].GetString() : null;
        }
        return null;
    }

    /// <summary>Every row of every partition, in order, one JSON array of strings/nulls each.</summary>
    public async IAsyncEnumerable<JsonElement> RowsAsync(SfResult result, [EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var row in Data(result.FirstRows))
        {
            yield return row;
        }
        for (var partition = 1; partition < result.Partitions; partition++)
        {
            var (_, page) = await SendAsync(HttpMethod.Get, $"{Statements}/{result.Handle}?partition={partition}", null, ct);
            foreach (var row in Data(page.ValueKind == JsonValueKind.Object && page.TryGetProperty("data", out var d) ? d : page))
            {
                yield return row;
            }
        }
    }

    private static IEnumerable<JsonElement> Data(JsonElement rows) =>
        rows.ValueKind == JsonValueKind.Array ? rows.EnumerateArray() : Enumerable.Empty<JsonElement>();

    /// <summary>
    /// One request, with the documented transient answers (429, 503, 504) retried with backoff.
    /// Anything else that is not 200/202 becomes an <see cref="SfException"/> with Snowflake's message.
    /// </summary>
    private async Task<(HttpStatusCode Status, JsonElement Root)> SendAsync(HttpMethod method, string pathAndQuery, string? body, CancellationToken ct)
    {
        for (var attempt = 0; ; attempt++)
        {
            using var request = new HttpRequestMessage(method, $"https://{_host}{pathAndQuery}");
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _auth.Token());
            request.Headers.Add("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT");
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            request.Headers.UserAgent.ParseAdd("trailox-agent");
            if (body != null)
            {
                request.Content = new StringContent(body, Encoding.UTF8, "application/json");
            }
            using var response = await _http.SendAsync(request, ct);
            var payload = await response.Content.ReadAsStringAsync(ct);
            var status = response.StatusCode;

            if (status is HttpStatusCode.TooManyRequests or HttpStatusCode.ServiceUnavailable or HttpStatusCode.GatewayTimeout
                && attempt < MaxRetries)
            {
                await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt + 1)), ct);
                continue;
            }
            if (status is not (HttpStatusCode.OK or HttpStatusCode.Accepted))
            {
                throw new SfException((int)status, ErrorText(payload));
            }
            using var doc = JsonDocument.Parse(payload);
            return (status, doc.RootElement.Clone());
        }
    }

    /// <summary>Snowflake's code and message when the body is its JSON error; otherwise the start of the body.</summary>
    internal static string ErrorText(string payload)
    {
        try
        {
            using var doc = JsonDocument.Parse(payload);
            var root = doc.RootElement;
            var code = root.TryGetProperty("code", out var c) ? c.ToString() : null;
            var message = root.TryGetProperty("message", out var m) ? m.GetString() : null;
            if (message != null)
            {
                return code != null ? $"{code} {message}" : message;
            }
        }
        catch (JsonException)
        {
        }
        return payload.Length > 600 ? payload[..600] : payload;
    }

    internal static SfResult Parse(JsonElement root)
    {
        var meta = root.GetProperty("resultSetMetaData");
        var columns = meta.GetProperty("rowType").EnumerateArray()
            .Select(c => c.GetProperty("name").GetString() ?? "")
            .ToList();
        var partitions = meta.TryGetProperty("partitionInfo", out var p) && p.ValueKind == JsonValueKind.Array ? p.GetArrayLength() : 1;
        var totalRows = meta.TryGetProperty("numRows", out var n) && n.TryGetInt64(out var rows) ? rows : 0;
        var handle = root.TryGetProperty("statementHandle", out var h) ? h.GetString() ?? "" : "";
        var first = root.TryGetProperty("data", out var d) ? d : default;
        return new SfResult(handle, columns, Math.Max(partitions, 1), totalRows, first);
    }
}
