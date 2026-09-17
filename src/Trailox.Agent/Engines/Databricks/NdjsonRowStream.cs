using System.Text.Encodings.Web;
using System.Text.Json;

namespace Trailox.Agent.Engines.Databricks;

/// <summary>
/// Re-emits rows that arrive as JSON arrays (the statement API's JSON_ARRAY pages) as NDJSON
/// objects keyed by the column names, as a read-only stream the gateway upload pulls from.
/// Rows are pulled from the source only as the reader consumes them; the buffer holds one row
/// at a time plus whatever the reader has not drained yet. Values are forwarded as they came
/// (strings, or null): nothing is re-typed here.
/// </summary>
public sealed class NdjsonRowStream : Stream
{
    private readonly IReadOnlyList<string> _columns;
    private readonly IAsyncEnumerator<JsonElement> _rows;
    private readonly MemoryStream _buffer = new();
    private bool _exhausted;
    private static readonly JsonWriterOptions WriterOptions = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    public NdjsonRowStream(IReadOnlyList<string> columns, IAsyncEnumerable<JsonElement> rows, CancellationToken ct)
    {
        _columns = columns;
        _rows = rows.GetAsyncEnumerator(ct);
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
            if (!await _rows.MoveNextAsync())
            {
                _exhausted = true;
                return 0;
            }
            _buffer.SetLength(0);
            WriteRow(_rows.Current);
            _buffer.Position = 0;
            RowsEmitted++;
        }
        return await _buffer.ReadAsync(destination, ct);
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct) =>
        ReadAsync(buffer.AsMemory(offset, count), ct).AsTask();

    public override int Read(byte[] buffer, int offset, int count) =>
        ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();

    /// <summary>One row array to one JSON object line. Unnamed trailing cells (never seen) are dropped, missing cells are null.</summary>
    private void WriteRow(JsonElement row)
    {
        // Relaxed escaping: a quote inside a value (every STRUCT/MAP cell is JSON text) is written
        // as \" rather than ", so the raw rows stay readable; there is no HTML context here.
        using var writer = new Utf8JsonWriter(_buffer, WriterOptions);
        writer.WriteStartObject();
        var cells = row.ValueKind == JsonValueKind.Array ? row.GetArrayLength() : 0;
        for (var i = 0; i < _columns.Count; i++)
        {
            writer.WritePropertyName(_columns[i]);
            if (i < cells)
            {
                row[i].WriteTo(writer);
            }
            else
            {
                writer.WriteNullValue();
            }
        }
        writer.WriteEndObject();
        writer.Flush();
        _buffer.WriteByte((byte)'\n');
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _rows.DisposeAsync().AsTask().GetAwaiter().GetResult();
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
