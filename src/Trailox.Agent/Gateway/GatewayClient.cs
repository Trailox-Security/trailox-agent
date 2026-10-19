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
    /// How often an upload with keep-alive puts bytes on the wire while its rows are quiet: well under the
    /// idle timeouts of the proxies, firewalls and load balancers on the path (often 60 s).
    /// </summary>
    internal TimeSpan KeepAliveEvery { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Uploads a chunk: the source's response stream is gzip-compressed on the fly into the
    /// request body. Nothing is buffered beyond the compressor's window.
    /// </summary>
    /// <param name="keepAlive">The rows can go quiet for minutes (<see cref="Engines.RowStream.KeepAlive"/>).</param>
    public async Task<ChunkAccepted> UploadChunkAsync(string chunkId, Stream rows, CancellationToken ct, bool keepAlive = false)
    {
        using var content = new GzipStreamContent(rows, keepAlive ? KeepAliveEvery : null);
        using var request = new HttpRequestMessage(HttpMethod.Post, $"v1/agent/chunks/{chunkId}") { Content = content };
        using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
        await ThrowOnError(response, ct);
        return await response.Content.ReadFromJsonAsync<ChunkAccepted>(ProtocolInfo.Json, ct) ?? new ChunkAccepted();
    }

    public async Task FailChunkAsync(string chunkId, string message, CancellationToken ct, string? reason = null)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(CheckinTimeout);
        using var response = await _http.PostAsJsonAsync($"v1/agent/chunks/{chunkId}/failed", new ChunkFailed { Message = message, Reason = reason }, ProtocolInfo.Json, timeout.Token);
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
        private static readonly byte[] BlankLine = { (byte)'\n' };

        private readonly Stream _source;
        private readonly TimeSpan? _keepAlive;

        public GzipStreamContent(Stream source, TimeSpan? keepAlive)
        {
            _source = source;
            _keepAlive = keepAlive;
            Headers.ContentType = new MediaTypeHeaderValue("application/x-ndjson");
            Headers.ContentEncoding.Add("gzip");
        }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) =>
            SerializeToStreamAsync(stream, context, CancellationToken.None);

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken cancellationToken)
        {
            await using var gzip = new GZipStream(stream, CompressionLevel.Fastest, leaveOpen: true);
            if (_keepAlive is { } every)
            {
                await CopyWithKeepAliveAsync(_source, gzip, every, cancellationToken);
            }
            else
            {
                await _source.CopyToAsync(gzip, cancellationToken);
            }
        }

        /// <summary>
        /// Copies the rows, and flushes the compressor at least every <paramref name="every"/>, so the connection
        /// carries bytes while the source is quiet: gzip turns a quiet stretch into nothing, and a proxy, firewall
        /// or load balancer on the path closes a connection that sends nothing for long. When nothing was copied
        /// since the last flush, a blank line is written first, because a flush with no new input sends nothing.
        /// </summary>
        /// <remarks>
        /// 🔴 NEVER A NEWLINE INSIDE A ROW: it corrupts the row, so a blank line is written only when the last byte
        ///    copied ended a row. The flush alone is harmless anywhere, but sends nothing when nothing was copied,
        ///    so a source that stalls in the middle of a row is not kept alive (MongoSource hands the pipe only
        ///    whole rows).
        /// ⚠ A fresh buffer, not a pooled one: a read can still be pending when a write fails. That read ends when
        ///   the rows are disposed, and nobody awaits it then, so its failure is observed here.
        /// </remarks>
        private static async Task CopyWithKeepAliveAsync(Stream source, Stream target, TimeSpan every, CancellationToken ct)
        {
            var buffer = new byte[81920];
            var atRowEnd = true;
            var copiedSinceFlush = false;
            var read = source.ReadAsync(buffer, ct).AsTask();
            var tick = Task.Delay(every, ct);
            try
            {
                while (true)
                {
                    if (!read.IsCompleted)
                    {
                        await Task.WhenAny(read, tick);
                    }
                    if (read.IsCompleted)
                    {
                        var n = await read;
                        if (n == 0)
                        {
                            return;
                        }
                        await target.WriteAsync(buffer.AsMemory(0, n), ct);
                        atRowEnd = buffer[n - 1] == (byte)'\n';
                        copiedSinceFlush = true;
                        read = source.ReadAsync(buffer, ct).AsTask();
                    }
                    if (tick.IsCompleted)
                    {
                        ct.ThrowIfCancellationRequested();
                        if (!copiedSinceFlush && atRowEnd)
                        {
                            await target.WriteAsync(BlankLine, ct);
                        }
                        await target.FlushAsync(ct);
                        copiedSinceFlush = false;
                        tick = Task.Delay(every, ct);
                    }
                }
            }
            catch
            {
                _ = read.ContinueWith(static t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
                throw;
            }
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
