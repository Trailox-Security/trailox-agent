using System.Buffers;
using System.Text.Json;

namespace Trailox.Agent.Bench.Engine;

/// <summary>
/// The lines of one Atlas S3 export object: plain JSON lines, not gzip (Phase 0 facts §18.3). The agent has no S3
/// reader yet (1.8.x refuses log_source s3), so this is bench code standing in for one: the same pooled-buffer
/// loop as the engine's <c>LogLines</c>, without the gunzip.
/// </summary>
internal sealed class PlainLines : IDisposable
{
    private const int ReadSize = 1 << 18;

    private readonly Stream _source;
    private byte[] _buffer = ArrayPool<byte>.Shared.Rent(1 << 20);
    private int _start;
    private int _end;
    private bool _eof;

    public PlainLines(Stream source)
    {
        _source = source;
    }

    public async ValueTask<bool> FillAsync(CancellationToken ct)
    {
        if (_eof)
        {
            return _start < _end;
        }
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
        var read = await _source.ReadAsync(_buffer.AsMemory(_end), ct);
        if (read == 0)
        {
            _eof = true;
        }
        _end += read;
        return true;
    }

    public bool TryReadLine(out ReadOnlySpan<byte> line)
    {
        var pending = _buffer.AsSpan(_start, _end - _start);
        var newline = pending.IndexOf((byte)'\n');
        if (newline >= 0)
        {
            line = pending[..newline];
            _start += newline + 1;
            return true;
        }
        if (_eof && !pending.IsEmpty)
        {
            line = pending;
            _start = _end;
            return true;
        }
        line = default;
        return false;
    }

    public void Dispose()
    {
        _source.Dispose();
        ArrayPool<byte>.Shared.Return(_buffer);
    }
}

/// <summary>
/// One export record's inner log line. The record is an envelope whose <c>log</c> member is the line as a JSON
/// STRING, so the line is unescaped out of it before the engine sees it. The envelope's <c>timestamp</c> is never
/// read: on backfilled audit records it is the export time (§18.3); the engine reads the line's own time.
/// </summary>
internal sealed class S3Record
{
    private byte[] _inner = new byte[1 << 14];

    /// <summary>The inner line, valid until the next call; empty when the record has no string <c>log</c>.</summary>
    public ReadOnlySpan<byte> Inner(ReadOnlySpan<byte> record)
    {
        try
        {
            var r = new Utf8JsonReader(record);
            if (!r.Read() || r.TokenType != JsonTokenType.StartObject)
            {
                return default;
            }
            while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
            {
                var isLog = r.ValueTextEquals("log"u8);
                r.Read();
                if (!isLog || r.TokenType != JsonTokenType.String)
                {
                    r.Skip();
                    continue;
                }
                if (!r.ValueIsEscaped)
                {
                    return r.ValueSpan;
                }
                if (_inner.Length < r.ValueSpan.Length)
                {
                    _inner = new byte[Math.Max(_inner.Length * 2, r.ValueSpan.Length)];
                }
                var written = r.CopyString(_inner);
                return _inner.AsSpan(0, written);
            }
        }
        catch (JsonException)
        {
        }
        return default;
    }
}
