using System.IO.Compression;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Trailox.Agent.Bench.Pipeline;

/// <summary>
/// One stream's upload as the agent sends it: NDJSON rows, gzip at CompressionLevel.Fastest
/// (GatewayClient.GzipStreamContent). The bytes go nowhere; their count is the network out.
/// </summary>
internal sealed class RowSink : IDisposable
{
    private static readonly JsonWriterOptions Options = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping, SkipValidation = true };

    private readonly CountingStream _wire = new();
    private readonly GZipStream _gzip;
    private readonly CountingStream _raw;

    public string Stream { get; }

    public Utf8JsonWriter Writer { get; }

    public long Rows { get; private set; }

    public long EventCount { get; private set; }

    /// <summary>When set, every row's bytes are also kept, for the literal check of a self-test.</summary>
    public MemoryStream? Capture { get; }

    public RowSink(string stream, bool capture = false)
    {
        Stream = stream;
        _gzip = new GZipStream(_wire, CompressionLevel.Fastest, leaveOpen: true);
        Capture = capture ? new MemoryStream() : null;
        _raw = new CountingStream(_gzip, Capture);
        Writer = new Utf8JsonWriter(_raw, Options);
    }

    public long RawBytes => _raw.Count;

    public long WireBytes => _wire.Count;

    public void EndRow(long eventCount)
    {
        Writer.Flush();
        _raw.WriteByte((byte)'\n');
        Writer.Reset();
        Rows++;
        EventCount += eventCount;
    }

    public void Dispose()
    {
        Writer.Dispose();
        _gzip.Dispose();
    }

    private sealed class CountingStream : System.IO.Stream
    {
        private readonly System.IO.Stream? _next;
        private readonly MemoryStream? _copy;

        public CountingStream(System.IO.Stream? next = null, MemoryStream? copy = null)
        {
            _next = next;
            _copy = copy;
        }

        public long Count { get; private set; }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            Count += buffer.Length;
            _next?.Write(buffer);
            _copy?.Write(buffer);
        }

        public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

        public override void WriteByte(byte value) => Write([value]);

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => Count;
        public override long Position { get => Count; set => throw new NotSupportedException(); }

        // Not forwarded: Utf8JsonWriter flushes its stream after every row, and a gzip flush per row would
        // shrink the compressor's blocks and cost CPU the agent does not spend (its rows reach gzip through
        // a CopyToAsync, GatewayClient.GzipStreamContent). The gzip stream is completed on Dispose.
        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }
}
