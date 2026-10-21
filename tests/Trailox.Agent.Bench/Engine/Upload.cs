using System.Buffers;
using System.Buffers.Text;
using System.IO.Compression;

namespace Trailox.Agent.Bench.Engine;

/// <summary>
/// One task's upload as GatewayClient makes it: the NDJSON body gzipped at <see cref="CompressionLevel.Fastest"/>.
/// The wire bytes are counted and dropped. Every row is tallied on the way, so a pass can check what was shipped.
/// </summary>
internal sealed class Upload : IDisposable
{
    private readonly CountingStream _wire = new();
    private readonly GZipStream _gzip;
    private readonly bool _tally;

    public long Rows { get; private set; }

    /// <summary>The sum of every row's event_count (rows without one: mongo_connections, a row per line).</summary>
    public long EventCount { get; private set; }

    public long Nreturned { get; private set; }

    public long DurationMsSum { get; private set; }

    public long RawBytes { get; private set; }

    public long WireBytes => _wire.Length;

    /// <summary>Every shipped byte, for the self-test; null in a measured run.</summary>
    public MemoryStream? Captured { get; }

    /// <param name="tally">False skips the row scan, to measure what the verification itself costs.</param>
    public Upload(bool capture, bool tally = true)
    {
        _gzip = new GZipStream(_wire, CompressionLevel.Fastest, leaveOpen: true);
        _tally = tally;
        Captured = capture ? new MemoryStream() : null;
    }

    /// <summary>Whole rows, each ending in a newline.</summary>
    public void Write(ReadOnlySpan<byte> rows)
    {
        RawBytes += rows.Length;
        _gzip.Write(rows);
        Captured?.Write(rows);
        if (!_tally)
        {
            return;
        }
        while (!rows.IsEmpty)
        {
            var end = rows.IndexOf((byte)'\n');
            var row = end < 0 ? rows : rows[..end];
            rows = end < 0 ? default : rows[(end + 1)..];
            Rows++;
            // The metrics are written after the row's text fields, and a quote inside a text value is escaped,
            // so the last match of a quoted key is the row's own field.
            EventCount += Number(row, "\"event_count\":"u8);
            Nreturned += Number(row, "\"nreturned\":"u8);
            DurationMsSum += Number(row, "\"duration_ms_sum\":"u8);
        }
    }

    /// <summary>Ends the gzip stream, as the upload's last write does.</summary>
    public void Complete() => _gzip.Dispose();

    public void Dispose() => _gzip.Dispose();

    private static long Number(ReadOnlySpan<byte> row, ReadOnlySpan<byte> key)
    {
        var at = row.LastIndexOf(key);
        return at >= 0 && Utf8Parser.TryParse(row[(at + key.Length)..], out long value, out _) ? value : 0;
    }

    private sealed class CountingStream : Stream
    {
        private long _length;

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => _length;

        public override long Position { get => _length; set => throw new NotSupportedException(); }

        public override void Write(byte[] buffer, int offset, int count) => _length += count;

        public override void Write(ReadOnlySpan<byte> buffer) => _length += buffer.Length;

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();
    }
}

/// <summary>
/// The rows of a streaming task (mongo_ops), passed to the upload a whole row at a time as the processor writes
/// them, as MongoSource's pipe hands them to the upload while the file is still being read.
/// </summary>
internal sealed class StreamingRows : IBufferWriter<byte>
{
    private readonly Upload _upload;
    private byte[] _buffer = new byte[1 << 16];
    private int _length;

    public StreamingRows(Upload upload)
    {
        _upload = upload;
    }

    public void Advance(int count)
    {
        var from = _length;
        _length += count;
        if (_buffer.AsSpan(from, count).IndexOf((byte)'\n') >= 0)
        {
            Drain();
        }
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer.AsMemory(_length);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer.AsSpan(_length);
    }

    /// <summary>Passes on what is left (nothing, once the last row has ended).</summary>
    public void Flush() => Drain();

    private void Drain()
    {
        var last = _buffer.AsSpan(0, _length).LastIndexOf((byte)'\n');
        if (last < 0)
        {
            return;
        }
        _upload.Write(_buffer.AsSpan(0, last + 1));
        var rest = _length - (last + 1);
        Buffer.BlockCopy(_buffer, last + 1, _buffer, 0, rest);
        _length = rest;
    }

    private void Ensure(int sizeHint)
    {
        var need = Math.Max(sizeHint, 256);
        if (_buffer.Length - _length >= need)
        {
            return;
        }
        var bigger = new byte[Math.Max(_buffer.Length * 2, _length + need)];
        Buffer.BlockCopy(_buffer, 0, bigger, 0, _length);
        _buffer = bigger;
    }
}
