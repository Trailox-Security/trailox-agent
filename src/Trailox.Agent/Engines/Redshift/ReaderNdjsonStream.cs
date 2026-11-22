using System.Data.Common;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Trailox.Agent.Engines.Redshift;

/// <summary>
/// Re-emits a <see cref="DbDataReader"/> as NDJSON, one object per row keyed by the column
/// names, as a read-only stream the gateway upload pulls from. A row is read only as the reader
/// consumes it; the buffer holds one row at a time. Values keep the driver's types: numbers as
/// JSON numbers, booleans as true/false, timestamps as ISO text with microseconds (UTC), text
/// as-is (including Redshift's blank padding), NULL as null.
/// </summary>
public sealed class ReaderNdjsonStream : Stream
{
    private static readonly JsonWriterOptions WriterOptions = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    private readonly DbDataReader _reader;
    private readonly string[] _columns;
    private readonly MemoryStream _buffer = new();
    private bool _exhausted;

    public ReaderNdjsonStream(DbDataReader reader)
    {
        _reader = reader;
        _columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
    }

    public long RowsEmitted { get; private set; }

    public override async ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken ct = default)
    {
        while (_buffer.Position == _buffer.Length)
        {
            if (_exhausted)
            {
                return 0;
            }
            if (!await _reader.ReadAsync(ct))
            {
                _exhausted = true;
                return 0;
            }
            _buffer.SetLength(0);
            WriteRow();
            _buffer.Position = 0;
            RowsEmitted++;
        }
        return await _buffer.ReadAsync(destination, ct);
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct) =>
        ReadAsync(buffer.AsMemory(offset, count), ct).AsTask();

    public override int Read(byte[] buffer, int offset, int count) =>
        ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();

    private void WriteRow()
    {
        using var writer = new Utf8JsonWriter(_buffer, WriterOptions);
        writer.WriteStartObject();
        for (var i = 0; i < _columns.Length; i++)
        {
            writer.WritePropertyName(_columns[i]);
            if (_reader.IsDBNull(i))
            {
                writer.WriteNullValue();
                continue;
            }
            WriteValue(writer, _reader.GetValue(i));
        }
        writer.WriteEndObject();
        writer.Flush();
        _buffer.WriteByte((byte)'\n');
    }

    /// <summary>The driver's CLR value to JSON, without re-typing anything the Trailox side could not undo.</summary>
    internal static void WriteValue(Utf8JsonWriter writer, object value)
    {
        switch (value)
        {
            case string s: writer.WriteStringValue(s); break;
            case bool b: writer.WriteBooleanValue(b); break;
            case int n: writer.WriteNumberValue(n); break;
            case long n: writer.WriteNumberValue(n); break;
            case short n: writer.WriteNumberValue(n); break;
            case byte n: writer.WriteNumberValue(n); break;
            case uint n: writer.WriteNumberValue(n); break;
            case ulong n: writer.WriteNumberValue(n); break;
            case decimal n: writer.WriteNumberValue(n); break;
            case double n: writer.WriteNumberValue(n); break;
            case float n: writer.WriteNumberValue(n); break;
            // Redshift's `timestamp without time zone` is UTC; the driver hands it back Unspecified.
            case DateTime dt: writer.WriteStringValue(dt.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'")); break;
            case DateTimeOffset dto: writer.WriteStringValue(dto.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'")); break;
            case DateOnly d: writer.WriteStringValue(d.ToString("yyyy-MM-dd")); break;
            case Guid g: writer.WriteStringValue(g.ToString("D")); break;
            case byte[] bytes: writer.WriteBase64StringValue(bytes); break;
            default: writer.WriteStringValue(value.ToString()); break;
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _reader.Dispose();
            _buffer.Dispose();
        }
        base.Dispose(disposing);
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
