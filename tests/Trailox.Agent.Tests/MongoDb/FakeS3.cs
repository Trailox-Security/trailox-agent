using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.MongoDb;

namespace Trailox.Agent.Tests.MongoDb;

/// <summary>
/// A bucket in memory, answering as S3 does: keys in byte order, pages of <see cref="PageSize"/>, StartAfter strictly
/// after, folders by delimiter, a range read of the first bytes. Counts what was asked of it.
/// </summary>
internal sealed class FakeS3 : IS3Objects
{
    public int PageSize { get; set; } = 1000;

    public SortedDictionary<string, byte[]> Objects { get; } = new(StringComparer.Ordinal);

    /// <summary>A failure for every call, as the SDK wrapper raises it (null: none).</summary>
    public SourceException? Refuse { get; set; }

    public List<string> Opened { get; } = new();

    public int Lists { get; private set; }

    public int Heads { get; private set; }

    public void Put(string key, IEnumerable<string> records) =>
        Objects[key] = Encoding.UTF8.GetBytes(string.Concat(records.Select(r => r + "\n")));

    public async IAsyncEnumerable<string> KeysAsync(string prefix, string? startAfter, [EnumeratorCancellation] CancellationToken ct)
    {
        var keys = Objects.Keys.Where(k => k.StartsWith(prefix, StringComparison.Ordinal)
            && (startAfter is null || string.CompareOrdinal(k, startAfter) > 0)).ToList();
        for (var i = 0; i < keys.Count || i == 0; i += PageSize)
        {
            Lists++;
            Check();
            await Task.Yield();
            foreach (var key in keys.Skip(i).Take(PageSize))
            {
                yield return key;
            }
        }
    }

    public Task<IReadOnlyList<string>> FoldersAsync(string prefix, CancellationToken ct)
    {
        Lists++;
        Check();
        IReadOnlyList<string> folders = Objects.Keys.Where(k => k.StartsWith(prefix, StringComparison.Ordinal) && k.IndexOf('/', prefix.Length) > 0)
            .Select(k => k[..(k.IndexOf('/', prefix.Length) + 1)])
            .Distinct().Order(StringComparer.Ordinal).ToList();
        return Task.FromResult(folders);
    }

    public Task<Stream> OpenAsync(string key, CancellationToken ct)
    {
        Check();
        Opened.Add(key);
        return Task.FromResult<Stream>(new MemoryStream(Objects[key], writable: false));
    }

    public Task<byte[]> HeadAsync(string key, int bytes, CancellationToken ct)
    {
        Heads++;
        Check();
        var body = Objects[key];
        return Task.FromResult(body.AsSpan(0, Math.Min(bytes, body.Length)).ToArray());
    }

    private void Check()
    {
        if (Refuse is not null)
        {
            throw Refuse;
        }
    }
}

/// <summary>Lines as the Atlas S3 export rewrites them (Phase 0 facts §18.3), for the readers' tests.</summary>
internal static class S3Form
{
    private static readonly JsonWriterOptions Writer = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    /// <summary>
    /// The line re-encoded as the export does: no padding, keys in a random order at every depth, every array an object
    /// keyed "1".."n", null members dropped. Then wrapped in the envelope, with <c>log</c> the line as a JSON string.
    /// </summary>
    public static string Record(string line, Random rng, string logType = "mongod")
    {
        using var doc = JsonDocument.Parse(line);
        var inner = new MemoryStream();
        using (var w = new Utf8JsonWriter(inner, Writer))
        {
            Value(w, doc.RootElement, rng);
        }
        var outer = new MemoryStream();
        using (var w = new Utf8JsonWriter(outer, Writer))
        {
            w.WriteStartObject();
            w.WriteString("service.name", "mongod");
            w.WriteString("log", Encoding.UTF8.GetString(inner.ToArray()));
            w.WriteString("host.name", "atlas-abcdef-shard-00-00.example.mongodb.net");
            w.WriteString("mongodb.log.type", logType);
            w.WriteString("timestamp", "2026-10-19T13:30:31.545600Z");
            w.WriteEndObject();
        }
        return Encoding.UTF8.GetString(outer.ToArray());
    }

    private static void Value(Utf8JsonWriter w, JsonElement e, Random rng)
    {
        switch (e.ValueKind)
        {
            case JsonValueKind.Object:
            {
                var members = e.EnumerateObject().Where(p => p.Value.ValueKind != JsonValueKind.Null).OrderBy(_ => rng.Next()).ToList();
                w.WriteStartObject();
                foreach (var p in members)
                {
                    w.WritePropertyName(p.Name);
                    Value(w, p.Value, rng);
                }
                w.WriteEndObject();
                break;
            }
            case JsonValueKind.Array:
            {
                w.WriteStartObject();
                var i = 0;
                foreach (var item in e.EnumerateArray())
                {
                    w.WritePropertyName((++i).ToString(System.Globalization.CultureInfo.InvariantCulture));
                    Value(w, item, rng);
                }
                w.WriteEndObject();
                break;
            }
            default:
                e.WriteTo(w);
                break;
        }
    }
}
