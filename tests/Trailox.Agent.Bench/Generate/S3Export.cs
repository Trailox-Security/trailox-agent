using System.Buffers;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Encodings.Web;
using System.Text.Json;
using Trailox.Agent.Engines.MongoDb;

namespace Trailox.Agent.Bench.Generate;

/// <summary>
/// Rewrites a generated corpus as Atlas's S3 log export delivers it, line for line, so
/// the same manifest holds and a pass over it is verified the same way.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>Objects are plain JSON lines, not gzip: at most 10,000 records, and an object over 10 MiB is split into
/// <c>partNNNNN-of-NNNNN</c> files at line boundaries. Named <c>&lt;seq&gt;-&lt;dir&gt;.log</c> by order of writing
/// (the export names them by time), under <c>node&lt;n&gt;/mongod</c> and <c>node&lt;n&gt;/mongod_audit</c>.</item>
/// <item>A record is the documented envelope (service.name, log, host.name, mongodb.group.id, mongodb.cluster.name,
/// mongodb.customer.cluster.name, mongodb.log.type, timestamp) with placeholder values, and <c>log</c> is the line
/// as a JSON string. <c>timestamp</c> is the line's own time, as on live records.</item>
/// <item>The line is re-encoded as measured: no padding; keys in a new random order at every depth; every array
/// an object keyed "1".."n" (<c>[]</c> becomes <c>{}</c>); null members dropped; an integer outside the 32-bit
/// range written as a double of 15 significant digits (1792402961205004 arrived as 1792402961205000.0). Which
/// fields the export treats as int64 is not measured, so smaller integers stay as they are.</item>
/// </list>
/// </remarks>
internal static class S3Export
{
    public const int MaxRecords = 10_000;
    public const long SplitBytes = 10L * 1024 * 1024;

    private static readonly JsonWriterOptions Writer = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping, SkipValidation = true };
    private static readonly JsonDocumentOptions Reader = new() { MaxDepth = 256 };

    public static Manifest Write(string from, string outDir, int seed)
    {
        var api = JsonSerializer.Deserialize<Manifest>(File.ReadAllText(Path.Combine(from, "manifest.json")))!;
        if (api.Form != "api")
        {
            throw new ArgumentException($"{from} is already an export corpus");
        }
        Directory.CreateDirectory(outDir);
        var written = new ConcurrentBag<(string Name, long Bytes)>();
        foreach (var name in api.Files.Order(StringComparer.Ordinal))
        {
            var node = int.Parse(name.AsSpan(4, name.IndexOf('.') - 4), CultureInfo.InvariantCulture);
            var audit = name.Contains(".audit.", StringComparison.Ordinal);
            var dir = audit ? "mongod_audit" : "mongod";
            Directory.CreateDirectory(Path.Combine(outDir, $"node{node}", dir));
            var batches = Batches(Path.Combine(from, name));
            Parallel.ForEach(System.Collections.Concurrent.Partitioner.Create(batches, EnumerablePartitionerOptions.NoBuffering),
                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, batch =>
                {
                    var rng = new Random(seed * 1_000_003 + node * 7919 + (audit ? 104_729 : 0) + batch.Seq * 31);
                    foreach (var file in WriteObject(outDir, node, dir, audit, batch.Seq, batch.Lines, rng))
                    {
                        written.Add(file);
                    }
                });
        }
        var manifest = JsonSerializer.Deserialize<Manifest>(JsonSerializer.Serialize(api))!;
        manifest.Form = "s3";
        manifest.Files = written.Select(f => f.Name).Order(StringComparer.Ordinal).ToList();
        manifest.RawBytes = written.Sum(f => f.Bytes);
        manifest.GzipBytes = 0;
        File.WriteAllText(Path.Combine(outDir, "manifest.json"), JsonSerializer.Serialize(manifest, Manifest.Json));
        return manifest;
    }

    /// <summary>The file's lines, 10,000 to a batch: one export object each.</summary>
    private static IEnumerable<(int Seq, List<byte[]> Lines)> Batches(string path)
    {
        using var lines = new LogLines(File.OpenRead(path));
        var batch = new List<byte[]>(MaxRecords);
        var seq = 0;
        while (lines.FillAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult())
        {
            while (lines.TryReadLine(out var line))
            {
                batch.Add(line.ToArray());
                if (batch.Count == MaxRecords)
                {
                    yield return (++seq, batch);
                    batch = new List<byte[]>(MaxRecords);
                }
            }
        }
        if (batch.Count > 0)
        {
            yield return (++seq, batch);
        }
    }

    private static List<(string, long)> WriteObject(string outDir, int node, string dir, bool audit, int seq, List<byte[]> lines, Random rng)
    {
        var body = new ArrayBufferWriter<byte>(lines.Count * 1600);
        var cuts = new List<int>(lines.Count + 1) { 0 };
        var inner = new ArrayBufferWriter<byte>(4096);
        using var innerWriter = new Utf8JsonWriter(inner, Writer);
        using var outerWriter = new Utf8JsonWriter(body, Writer);
        foreach (var line in lines)
        {
            Record(body, outerWriter, inner, innerWriter, line, audit, node, dir, rng);
            cuts.Add(body.WrittenCount);
        }

        // Parts: whole records, each part at most 10 MiB.
        var parts = new List<(int From, int To)>();
        var partStart = 0;
        for (var i = 1; i < cuts.Count; i++)
        {
            if (cuts[i] - partStart > SplitBytes && cuts[i - 1] > partStart)
            {
                parts.Add((partStart, cuts[i - 1]));
                partStart = cuts[i - 1];
            }
        }
        parts.Add((partStart, cuts[^1]));

        var files = new List<(string, long)>();
        for (var p = 0; p < parts.Count; p++)
        {
            var name = parts.Count == 1
                ? $"node{node}/{dir}/{seq:D6}-{dir}.log"
                : $"node{node}/{dir}/{seq:D6}-{dir}.part{p + 1:D5}-of-{parts.Count:D5}.log";
            var (fromByte, toByte) = parts[p];
            using (var file = File.Create(Path.Combine(outDir, name)))
            {
                file.Write(body.WrittenSpan[fromByte..toByte]);
            }
            files.Add((name, toByte - fromByte));
        }
        return files;
    }

    private static void Record(ArrayBufferWriter<byte> body, Utf8JsonWriter w, ArrayBufferWriter<byte> inner, Utf8JsonWriter iw,
        byte[] line, bool audit, int node, string dir, Random rng)
    {
        using (var doc = JsonDocument.Parse(line, Reader))
        {
            inner.Clear();
            iw.Reset(inner);
            Value(iw, doc.RootElement, rng);
            iw.Flush();
        }
        LineScan.TryTimestampMicros(line, audit, out var micros);
        w.Reset(body);
        w.WriteStartObject();
        w.WriteString("service.name"u8, "mongod"u8);
        w.WriteString("log"u8, inner.WrittenSpan);
        w.WriteString("host.name"u8, $"atlas-example-shard-00-{node:D2}.example.mongodb.net");
        w.WriteString("mongodb.group.id"u8, "000000000000000000000000"u8);
        w.WriteString("mongodb.cluster.name"u8, "atlas-example"u8);
        w.WriteString("mongodb.customer.cluster.name"u8, "Cluster0"u8);
        w.WriteString("mongodb.log.type"u8, dir);
        w.WriteString("timestamp"u8, DateTime.UnixEpoch.AddTicks(micros * 10).ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture));
        w.WriteEndObject();
        w.Flush();
        body.Write("\n"u8);
    }

    private static void Value(Utf8JsonWriter w, JsonElement e, Random rng)
    {
        switch (e.ValueKind)
        {
            case JsonValueKind.Object:
            {
                var members = new List<(string Name, JsonElement Value)>();
                foreach (var p in e.EnumerateObject())
                {
                    if (p.Value.ValueKind != JsonValueKind.Null)
                    {
                        members.Add((p.Name, p.Value));
                    }
                }
                Shuffle(members, rng);
                w.WriteStartObject();
                foreach (var (name, value) in members)
                {
                    w.WritePropertyName(name);
                    Value(w, value, rng);
                }
                w.WriteEndObject();
                break;
            }
            case JsonValueKind.Array:
            {
                var members = new List<(string Name, JsonElement Value)>();
                var index = 0;
                foreach (var item in e.EnumerateArray())
                {
                    index++;
                    if (item.ValueKind != JsonValueKind.Null)
                    {
                        members.Add((index.ToString(CultureInfo.InvariantCulture), item));
                    }
                }
                Shuffle(members, rng);
                w.WriteStartObject();
                foreach (var (name, value) in members)
                {
                    w.WritePropertyName(name);
                    Value(w, value, rng);
                }
                w.WriteEndObject();
                break;
            }
            case JsonValueKind.Number:
                w.WriteRawValue(e.TryGetInt64(out var v) && (v > int.MaxValue || v < int.MinValue) ? Int64AsDouble(v) : e.GetRawText(), skipInputValidation: true);
                break;
            case JsonValueKind.String:
                w.WriteStringValue(e.GetString());
                break;
            case JsonValueKind.True:
            case JsonValueKind.False:
                w.WriteBooleanValue(e.ValueKind == JsonValueKind.True);
                break;
            default:
                w.WriteNullValue();
                break;
        }
    }

    /// <summary>1792402961205004 → "1792402961205000.0": the double the export writes, to 15 significant digits.</summary>
    internal static string Int64AsDouble(long v)
    {
        var d = double.Parse(((double)v).ToString("G15", CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
        return Math.Abs(d) < 1e16 ? d.ToString("F0", CultureInfo.InvariantCulture) + ".0" : d.ToString("R", CultureInfo.InvariantCulture);
    }

    private static void Shuffle<T>(List<T> list, Random rng)
    {
        for (var i = list.Count - 1; i > 0; i--)
        {
            var j = rng.Next(i + 1);
            (list[i], list[j]) = (list[j], list[i]);
        }
    }
}
