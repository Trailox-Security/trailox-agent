using System.IO.Compression;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Gateway;
using Trailox.Agent.Protocol;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Tests;

/// <summary>
/// An upload whose rows go quiet still puts bytes on the wire (agent 1.8, MongoDB): a blank line, never inside a
/// row, and a flush of the compressor at least every keep-alive interval, so no proxy, firewall or load balancer
/// on the path sees an idle connection to close.
/// </summary>
/// <remarks>
/// Before 1.8 an upload was a plain copy into gzip: a quiet stretch was compressed to nothing and sent nothing,
/// and a MongoDB window of a busy cluster with few shapes is quiet for minutes while the audit files are read.
/// The tests wait for what reached the wire, never for a number of flushes in a row, so a slow runner that
/// misses ticks cannot fail them.
/// </remarks>
public class UploadKeepAliveTests
{
    private static readonly TimeSpan Every = TimeSpan.FromMilliseconds(50);

    /// <summary>Records what reaches the wire: every byte, and every flush.</summary>
    private sealed class Wire : Stream
    {
        private readonly MemoryStream _bytes = new();
        private int _flushes;

        public int Flushes => Volatile.Read(ref _flushes);

        /// <summary>The connection is gone: every flush fails.</summary>
        public bool Broken { get; set; }

        public byte[] Received()
        {
            lock (_bytes)
            {
                return _bytes.ToArray();
            }
        }

        public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            lock (_bytes)
            {
                _bytes.Write(buffer);
            }
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Write(buffer.Span);
            return ValueTask.CompletedTask;
        }

        public override void Flush()
        {
            if (Broken)
            {
                throw new IOException("the connection was reset");
            }
            Interlocked.Increment(ref _flushes);
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            Flush();
            return Task.CompletedTask;
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }

    /// <summary>Plays the gateway: reads the upload onto the wire as it is sent, then accepts it.</summary>
    private sealed class Gateway : HttpMessageHandler
    {
        public Wire Wire { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            await request.Content!.CopyToAsync(Wire, cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("""{"rowsAccepted":1}""", Encoding.UTF8, "application/json") };
        }
    }

    private sealed class OneStream : IEngineSource
    {
        private readonly RowStream _rows;

        public OneStream(RowStream rows) => _rows = rows;

        public Task<EndpointCaps> ProbeAsync(CancellationToken ct) => Task.FromResult(new EndpointCaps { Version = "1" });

        public Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct) => Task.FromResult<RowStream?>(_rows);
    }

    private static (GatewayClient Client, Gateway Gateway) Client()
    {
        var gateway = new Gateway();
        var client = new GatewayClient(new HttpClient(gateway), new Uri("https://gateway.example.test/"), "tlx_not-a-real-key", "0.0.0-test")
        {
            KeepAliveEvery = Every,
        };
        return (client, gateway);
    }

    private static async Task WaitFor(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "timed out waiting for the upload");
            await Task.Delay(10);
        }
    }

    /// <summary>All of a finished body.</summary>
    private static string Gunzip(byte[] body)
    {
        using var gzip = new GZipStream(new MemoryStream(body), CompressionMode.Decompress);
        using var reader = new StreamReader(gzip, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    /// <summary>What a body still being sent decompresses to so far: everything up to its last flush.</summary>
    private static string GunzipSoFar(byte[] body)
    {
        using var gzip = new GZipStream(new MemoryStream(body), CompressionMode.Decompress);
        var text = new MemoryStream();
        var buffer = new byte[4096];
        try
        {
            int n;
            while ((n = gzip.Read(buffer)) > 0)
            {
                text.Write(buffer, 0, n);
            }
        }
        catch (InvalidDataException)
        {
            // The body has no end yet.
        }
        return Encoding.UTF8.GetString(text.ToArray());
    }

    private static string[] Rows(string body) => body.Split('\n', StringSplitOptions.RemoveEmptyEntries);

    [Fact]
    public async Task A_quiet_body_still_reaches_the_wire_before_it_ends()
    {
        var (client, gateway) = Client();
        var pipe = new Pipe();
        var upload = client.UploadChunkAsync("c", pipe.Reader.AsStream(), CancellationToken.None, keepAlive: true);

        await pipe.Writer.WriteAsync("{\"a\":1}\n"u8.ToArray());
        // The row, then blank lines, while the body is still open.
        await WaitFor(() => GunzipSoFar(gateway.Wire.Received()).Contains("{\"a\":1}\n\n", StringComparison.Ordinal));

        Assert.False(upload.IsCompleted);
        await pipe.Writer.CompleteAsync();
        await upload;
        Assert.Equal(new[] { "{\"a\":1}" }, Rows(Gunzip(gateway.Wire.Received())));
    }

    [Fact]
    public async Task A_blank_line_is_never_written_inside_a_row()
    {
        var (client, gateway) = Client();
        var pipe = new Pipe();
        var upload = client.UploadChunkAsync("c", pipe.Reader.AsStream(), CancellationToken.None, keepAlive: true);

        // Quiet in the middle of a row, for several intervals after that half has reached the wire.
        await pipe.Writer.WriteAsync("{\"a\":"u8.ToArray());
        await WaitFor(() => GunzipSoFar(gateway.Wire.Received()).TrimStart('\n') == "{\"a\":");
        var flushes = gateway.Wire.Flushes;
        await WaitFor(() => gateway.Wire.Flushes >= flushes + 3);
        await pipe.Writer.WriteAsync("1}\n{\"b\":2}\n"u8.ToArray());
        await WaitFor(() => GunzipSoFar(gateway.Wire.Received()).Contains("{\"b\":2}\n\n", StringComparison.Ordinal));
        await pipe.Writer.CompleteAsync();
        await upload;

        // Blank lines may come before the first row (a tick before it arrived), never inside one.
        var body = Gunzip(gateway.Wire.Received());
        Assert.StartsWith("{\"a\":1}\n{\"b\":2}\n", body.TrimStart('\n'), StringComparison.Ordinal);
        Assert.Equal(new[] { "{\"a\":1}", "{\"b\":2}" }, Rows(body));
    }

    [Fact]
    public async Task Over_a_real_connection_a_quiet_body_keeps_arriving()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            var received = new MemoryStream();
            var ended = false;
            byte[] Received()
            {
                lock (received)
                {
                    return received.ToArray();
                }
            }
            long BodyBytes()
            {
                var text = Encoding.ASCII.GetString(Received());
                var headersEnd = text.IndexOf("\r\n\r\n", StringComparison.Ordinal);
                return headersEnd < 0 ? 0 : text.Length - headersEnd - 4;
            }
            var server = Task.Run(async () =>
            {
                using var socket = await listener.AcceptSocketAsync();
                var buffer = new byte[65536];
                int n;
                while ((n = await socket.ReceiveAsync(buffer, SocketFlags.None)) > 0)
                {
                    lock (received)
                    {
                        received.Write(buffer, 0, n);
                    }
                    // The last chunk of a chunked body, once the rows have ended.
                    if (Volatile.Read(ref ended) && Encoding.ASCII.GetString(Received()).EndsWith("0\r\n\r\n", StringComparison.Ordinal))
                    {
                        break;
                    }
                }
                await socket.SendAsync("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 18\r\nConnection: close\r\n\r\n{\"rowsAccepted\":1}"u8.ToArray(), SocketFlags.None);
            });

            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            using var http = new HttpClient(new SocketsHttpHandler { UseProxy = false });
            var client = new GatewayClient(http, new Uri($"http://127.0.0.1:{port}/"), "tlx_not-a-real-key", "0.0.0-test")
            {
                KeepAliveEvery = Every,
            };
            var pipe = new Pipe();
            var upload = client.UploadChunkAsync("c", pipe.Reader.AsStream(), CancellationToken.None, keepAlive: true);

            await pipe.Writer.WriteAsync("{\"a\":1}\n"u8.ToArray());
            await WaitFor(() => BodyBytes() > 0);
            var first = BodyBytes();
            // More of the body arrives though no row was written.
            await WaitFor(() => BodyBytes() > first);

            Assert.False(upload.IsCompleted);
            Volatile.Write(ref ended, true);
            await pipe.Writer.CompleteAsync();
            Assert.Equal(1, (await upload).RowsAccepted);
            await server;
        }
        finally
        {
            listener.Stop();
        }
    }

    [Fact]
    public async Task Without_keep_alive_the_body_is_the_rows_alone()
    {
        var (client, gateway) = Client();
        var pipe = new Pipe();
        var upload = client.UploadChunkAsync("c", pipe.Reader.AsStream(), CancellationToken.None);

        await pipe.Writer.WriteAsync("{\"a\":1}\n"u8.ToArray());
        await Task.Delay(Every * 6);
        await pipe.Writer.CompleteAsync();
        await upload;

        Assert.Equal("{\"a\":1}\n", Gunzip(gateway.Wire.Received()));
    }

    [Fact]
    public async Task A_connection_lost_while_the_rows_are_quiet_fails_the_upload_and_the_rows_still_release()
    {
        var (client, gateway) = Client();
        gateway.Wire.Broken = true;
        var pipe = new Pipe();
        var rows = new RowStream(pipe.Reader.AsStream(), new MemoryStream()) { KeepAlive = true };
        var upload = client.UploadChunkAsync("c", rows.Body, CancellationToken.None, keepAlive: true);

        await pipe.Writer.WriteAsync("{\"a\":1}\n"u8.ToArray());
        var failure = await Assert.ThrowsAnyAsync<Exception>(() => upload.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Contains("the connection was reset", failure.ToString(), StringComparison.Ordinal);

        // A read of the rows was still pending when the flush failed: releasing them must not hang.
        var release = Task.Run(async () =>
        {
            await rows.DisposeAsync();
            await pipe.Writer.CompleteAsync();
        });
        Assert.Same(release, await Task.WhenAny(release, Task.Delay(TimeSpan.FromSeconds(10))));
        await release;
    }

    [Fact]
    public async Task The_task_runner_keeps_alive_the_uploads_of_a_source_that_asks()
    {
        var (client, gateway) = Client();
        var executor = new TaskExecutor(client, new ErrorRing(), NullLogger.Instance);
        var pipe = new Pipe();
        var session = new EndpointSession(new EndpointConfig { Alias = "atlas", Engine = "mongodb", Kind = "cloud" },
            new OneStream(new RowStream(pipe.Reader.AsStream(), new MemoryStream()) { KeepAlive = true }), TimeSpan.FromMinutes(60));
        var task = new AgentTask { ChunkId = "c", EndpointId = 7, Stream = ProtocolInfo.Streams.MongoOps, StartMicros = 0, EndMicros = 60_000_000 };

        var run = executor.ExecuteAsync(task, session, CancellationToken.None);
        await pipe.Writer.WriteAsync("{\"a\":1}\n"u8.ToArray());
        await WaitFor(() => GunzipSoFar(gateway.Wire.Received()).Contains("{\"a\":1}\n\n", StringComparison.Ordinal));
        await pipe.Writer.CompleteAsync();

        Assert.True(await run);
        Assert.Equal(new[] { "{\"a\":1}" }, Rows(Gunzip(gateway.Wire.Received())));
    }
}
