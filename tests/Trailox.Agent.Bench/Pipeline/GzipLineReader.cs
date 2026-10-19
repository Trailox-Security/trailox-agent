using System.Buffers;
using System.IO.Compression;

namespace Trailox.Agent.Bench.Pipeline;

/// <summary>
/// Streams the lines of a gzip file (any number of members, as Atlas concatenates its chunks) as
/// spans over one pooled buffer: no string and no array per line. A line is valid only until the
/// next call.
/// </summary>
internal sealed class GzipLineReader : IDisposable
{
    private const int ReadSize = 1 << 18;

    private readonly Stream _file;
    private readonly GZipStream _gzip;
    private byte[] _buffer = ArrayPool<byte>.Shared.Rent(1 << 20);
    private int _start;
    private int _end;
    private bool _eof;

    public long CompressedBytes => _file.Length;

    public long RawBytes { get; private set; }

    public GzipLineReader(string path)
    {
        _file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 16, FileOptions.SequentialScan);
        _gzip = new GZipStream(_file, CompressionMode.Decompress);
    }

    public bool TryReadLine(out ReadOnlySpan<byte> line)
    {
        while (true)
        {
            var pending = _buffer.AsSpan(_start, _end - _start);
            var newline = pending.IndexOf((byte)'\n');
            if (newline >= 0)
            {
                line = pending[..newline];
                _start += newline + 1;
                return true;
            }
            if (_eof)
            {
                line = pending;
                _start = _end;
                return !line.IsEmpty;
            }
            Fill();
        }
    }

    private void Fill()
    {
        // Move the unfinished line to the front; grow only for a line longer than the buffer
        // (mongod truncates at 10 KB by default, so this is rare).
        var pending = _end - _start;
        if (_start > 0)
        {
            Buffer.BlockCopy(_buffer, _start, _buffer, 0, pending);
            _start = 0;
            _end = pending;
        }
        if (_buffer.Length - _end < ReadSize)
        {
            var bigger = ArrayPool<byte>.Shared.Rent(_buffer.Length * 2);
            Buffer.BlockCopy(_buffer, 0, bigger, 0, _end);
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = bigger;
        }
        var read = _gzip.Read(_buffer, _end, _buffer.Length - _end);
        if (read == 0)
        {
            _eof = true;
        }
        _end += read;
        RawBytes += read;
    }

    public void Dispose()
    {
        _gzip.Dispose();
        _file.Dispose();
        ArrayPool<byte>.Shared.Return(_buffer);
    }
}
