using System.IO.Compression;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Gateway;

/// <summary>
/// The agent's client for the Trailox gateway: one hostname, HTTPS, outbound only. Proxies come
/// from HTTPS_PROXY / NO_PROXY and a private CA from SSL_CERT_FILE / SSL_CERT_DIR, both honoured
/// by the .NET runtime on Linux without any code here.
/// </summary>
public sealed class GatewayClient
{
    private readonly HttpClient _http;
    private readonly string _agentVersion;

    public GatewayClient(HttpClient http, Uri gateway, string agentKey, string agentVersion)
    {
        _http = http;
        _http.BaseAddress = gateway;
        _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", agentKey);
        _http.DefaultRequestHeaders.Add(ProtocolInfo.ProtocolHeader, ProtocolInfo.Version.ToString());
        _http.DefaultRequestHeaders.Add(ProtocolInfo.AgentVersionHeader, agentVersion);
        _agentVersion = agentVersion;
    }

    /// <summary>
    /// A check-in is a small request and must fail fast: with the network gone, a connect can hang
    /// far longer than the check-in cadence, and the loop would sit silent for the client's
    /// 30-minute timeout. Uploads keep the long timeout; this does not.
    /// </summary>
    public static readonly TimeSpan CheckinTimeout = TimeSpan.FromSeconds(30);

    public async Task<CheckinResponse> CheckinAsync(CheckinRequest request, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(CheckinTimeout);
        using var response = await _http.PostAsJsonAsync("v1/agent/checkin", request, ProtocolInfo.Json, timeout.Token);
        await ThrowOnError(response, ct);
        return await response.Content.ReadFromJsonAsync<CheckinResponse>(ProtocolInfo.Json, ct)
               ?? throw new GatewayException(200, "empty", "the gateway returned an empty check-in response");
    }

    /// <summary>
    /// Uploads a chunk: the source's response stream is gzip-compressed on the fly into the
    /// request body. Nothing is buffered beyond the compressor's window.
    /// </summary>
    public async Task<ChunkAccepted> UploadChunkAsync(string chunkId, Stream rows, CancellationToken ct)
    {
        using var content = new GzipStreamContent(rows);
        using var request = new HttpRequestMessage(HttpMethod.Post, $"v1/agent/chunks/{chunkId}") { Content = content };
        using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
        await ThrowOnError(response, ct);
        return await response.Content.ReadFromJsonAsync<ChunkAccepted>(ProtocolInfo.Json, ct) ?? new ChunkAccepted();
    }

    public async Task FailChunkAsync(string chunkId, string message, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(CheckinTimeout);
        using var response = await _http.PostAsJsonAsync($"v1/agent/chunks/{chunkId}/failed", new ChunkFailed { Message = message }, ProtocolInfo.Json, timeout.Token);
        await ThrowOnError(response, ct);
    }

    private static async Task ThrowOnError(HttpResponseMessage response, CancellationToken ct)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }
        var body = await response.Content.ReadAsStringAsync(ct);
        GatewayError? error = null;
        try
        {
            error = JsonSerializer.Deserialize<GatewayError>(body, ProtocolInfo.Json);
        }
        catch (JsonException)
        {
        }
        var retryAfter = response.Headers.RetryAfter?.Delta;
        throw new GatewayException((int)response.StatusCode, error?.Error ?? "http_" + (int)response.StatusCode,
            error?.Message ?? (body.Length > 300 ? body[..300] : body), retryAfter);
    }

    /// <summary>HttpContent that gzips a source stream as it is sent; length unknown, so the request is chunked.</summary>
    private sealed class GzipStreamContent : HttpContent
    {
        private readonly Stream _source;

        public GzipStreamContent(Stream source)
        {
            _source = source;
            Headers.ContentType = new MediaTypeHeaderValue("application/x-ndjson");
            Headers.ContentEncoding.Add("gzip");
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            await using var gzip = new GZipStream(stream, CompressionLevel.Fastest, leaveOpen: true);
            await _source.CopyToAsync(gzip);
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken cancellationToken)
        {
            await using var gzip = new GZipStream(stream, CompressionLevel.Fastest, leaveOpen: true);
            await _source.CopyToAsync(gzip, cancellationToken);
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }
    }
}

public sealed class GatewayException : Exception
{
    public int StatusCode { get; }
    public string Code { get; }
    public TimeSpan? RetryAfter { get; }

    public GatewayException(int statusCode, string code, string message, TimeSpan? retryAfter = null)
        : base($"gateway {statusCode} {code}: {message}")
    {
        StatusCode = statusCode;
        Code = code;
        RetryAfter = retryAfter;
    }

    public bool IsUnauthorized => StatusCode == (int)HttpStatusCode.Unauthorized;
}
