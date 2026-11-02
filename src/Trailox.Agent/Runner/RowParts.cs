using System.Diagnostics;

namespace Trailox.Agent.Runner;

/// <summary>
/// One read's rows as several upload bodies, for a read that one upload cannot take (PROTOCOL.md section 4). Each body is at
/// most so many bytes, ends so long after it began, and holds whole rows.
/// </summary>
/// <remarks>
/// 🔴 A BODY ENDS ONLY BETWEEN ROWS. What was read past that waits here for the next body: the rows come from one read, and
///    nothing can be asked for again.
/// 🔴 THE TIME COUNTS WHILE THE ROWS ARE QUIET TOO. A fetch can send nothing for many minutes, and a body must end on time all
///    the same. The read stays pending for the next body.
/// ⚠ NO BODY STARTS UNTIL IT HAS A ROW, so the end of the rows is seen between bodies and no upload stands open through a
///   quiet read. A body cannot say that it is the last: that is only known once the rows have ended.
/// </remarks>
internal sealed class RowParts
{
    /// <summary>The longest row a read in parts can hold. Only <c>ora_audit</c> goes in parts, and its rows stay well under it.</summary>
    internal const int MaxRowBytes = 81920;

    private readonly Stream _rows;
    private readonly long _maxBytes;
    private readonly TimeSpan _maxTime;
    private readonly CancellationToken _ct;
    private readonly byte[] _buffer = new byte[MaxRowBytes];

    private int _start;
    private int _end;
    private Task<int>? _read;
    private bool _ended;

    /// <param name="ct">The task's token: a read outlives the request of the body that began it.</param>
    public RowParts(Stream rows, long maxBytes, TimeSpan maxTime, CancellationToken ct)
    {
        _rows = rows;
        _maxBytes = maxBytes;
        _maxTime = maxTime;
        _ct = ct;
    }

    /// <summary>Waits until the next body has a row; false when the rows have ended. A read that failed throws here.</summary>
    public async Task<bool> MoreAsync()
    {
        while (NextRow() == 0 && !_ended)
        {
            await FillAsync(Timeout.InfiniteTimeSpan, _ct);
        }
        return _start < _end;
    }

    /// <summary>The next body, for one upload. Its rows are read as the upload asks for them.</summary>
    public Stream Next() => new Body(this);

    /// <summary>The bytes of the next row; 0 until all of it is here. The end of the rows ends a row.</summary>
    private int NextRow() => _ended ? _end - _start : _buffer.AsSpan(_start, _end - _start).IndexOf((byte)'\n') + 1;

    /// <summary>The bytes of the whole rows that fit in <paramref name="most"/>.</summary>
    private int WholeRows(long most)
    {
        var seen = (int)Math.Min(_end - _start, Math.Max(0, most));
        return _ended && seen == _end - _start ? seen : _buffer.AsSpan(_start, seen).LastIndexOf((byte)'\n') + 1;
    }

    /// <summary>Waits for more rows; false when <paramref name="wait"/> passed first, with the read still pending.</summary>
    private async Task<bool> FillAsync(TimeSpan wait, CancellationToken ct)
    {
        if (_read == null)
        {
            _buffer.AsSpan(_start, _end - _start).CopyTo(_buffer);
            _end -= _start;
            _start = 0;
            if (_end == _buffer.Length)
            {
                throw new InvalidDataException($"a row is longer than the {MaxRowBytes} bytes a part can hold");
            }
            _read = _rows.ReadAsync(_buffer.AsMemory(_end), _ct).AsTask();
            _ = _read.ContinueWith(static t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
        }
        if (await Task.WhenAny(_read, Task.Delay(wait, ct)) != _read)
        {
            ct.ThrowIfCancellationRequested();
            return false;
        }
        var read = await _read;
        _read = null;
        _end += read;
        _ended = read == 0;
        return true;
    }

    private sealed class Body : Stream
    {
        private readonly RowParts _parts;
        private readonly Stopwatch _clock = Stopwatch.StartNew();
        private long _sent;
        private int _owed;

        public Body(RowParts parts) => _parts = parts;

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            while (_owed == 0)
            {
                var time = _parts._maxTime - _clock.Elapsed;
                if (_sent > 0 && (time <= TimeSpan.Zero || _sent >= _parts._maxBytes))
                {
                    return 0;
                }
                var rows = _parts.WholeRows(_parts._maxBytes - _sent);
                if (rows == 0 && _sent == 0)
                {
                    // A body always carries a row, even one longer than the size asked for: it could go nowhere else.
                    rows = _parts.NextRow();
                }
                if (rows > 0)
                {
                    _owed = rows;
                    _sent += rows;
                }
                else if (_parts.NextRow() > 0 || _parts._ended || !await _parts.FillAsync(time, ct))
                {
                    return 0;
                }
            }
            var copied = Math.Min(_owed, buffer.Length);
            _parts._buffer.AsMemory(_parts._start, copied).CopyTo(buffer);
            _parts._start += copied;
            _owed -= copied;
            return copied;
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
