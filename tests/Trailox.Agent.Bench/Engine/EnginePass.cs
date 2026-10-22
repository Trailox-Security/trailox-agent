using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using Trailox.Agent.Bench.Generate;
using Trailox.Agent.Engines.MongoDb;

namespace Trailox.Agent.Bench.Engine;

/// <summary>What one stream's task did with its files, and what it shipped.</summary>
internal sealed class StreamResult
{
    public long LinesIn, OutOfWindow, Filtered, Malformed, Kept, OverflowLines, LateLines;
    public long Rows, EventCount, Nreturned, DurationMsSum, RawBytesOut, WireBytesOut, BytesIn;

    /// <summary>A server-log task holds all its rows before the upload starts: the most it held.</summary>
    public long PeakBufferedBytes;

    public double Seconds;

    public void Add(StreamResult o)
    {
        LinesIn += o.LinesIn; OutOfWindow += o.OutOfWindow; Filtered += o.Filtered; Malformed += o.Malformed; Kept += o.Kept;
        OverflowLines += o.OverflowLines; LateLines += o.LateLines; Rows += o.Rows; EventCount += o.EventCount;
        Nreturned += o.Nreturned; DurationMsSum += o.DurationMsSum; RawBytesOut += o.RawBytesOut; WireBytesOut += o.WireBytesOut;
        BytesIn += o.BytesIn; PeakBufferedBytes = Math.Max(PeakBufferedBytes, o.PeakBufferedBytes); Seconds += o.Seconds;
    }
}

/// <summary>One pass: the four tasks of one window, each stream's result, and (self-test) every byte shipped.</summary>
internal sealed class PassResult
{
    public Dictionary<MongoLogStream, StreamResult> Streams { get; } = EnginePass.Order.ToDictionary(s => s, _ => new StreamResult());

    public Dictionary<MongoLogStream, byte[]> Shipped { get; } = new();

    public long LinesIn => Streams.Values.Sum(s => s.LinesIn);

    public void Add(PassResult o)
    {
        foreach (var (stream, r) in o.Streams)
        {
            Streams[stream].Add(r);
        }
    }
}

internal sealed record PassOptions(int GroupCap, Pacer? Pacer, bool Capture, bool Tally);

/// <summary>
/// One task window read the way the agent reads it (MongoSource, 1.8.x), through the shipped classes:
/// <c>LogLines</c> (gunzip into one pooled buffer), <c>MongoLogProcessor</c> (window check and filter on the raw
/// bytes, <c>Utf8JsonReader</c> extraction, <c>MongoShape</c>, <c>MinuteAggregator</c>) and <c>RowSink</c>.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>Four tasks, one after the other, as TaskExecutor runs them: mongo_ops reads every node's audit file;
/// mongo_slow_ops, mongo_connections and mongo_signin_failures each read every node's server log whole, so a
/// server log is read three times, as the agent downloads it three times.</item>
/// <item>mongo_ops streams its rows to the upload as they are made (MongoSource's pipe); the server-log tasks
/// hold every row in memory and upload after (MongoSource.ServerLogAsync), so their peak is reported.</item>
/// <item>Not reproduced: the HTTP download (its TLS and socket cost), and the server-log tail mongo_ops reads per
/// node to check the window is published (a 3-minute slice), which Atlas only needs for recent windows.</item>
/// </list>
/// </remarks>
internal static class EnginePass
{
    // MongoSource: how often a file's closed minutes are shipped.
    private const int FlushLines = 4096;

    public static readonly MongoLogStream[] Order =
        [MongoLogStream.Ops, MongoLogStream.SlowOps, MongoLogStream.Connections, MongoLogStream.SigninFailures];

    public static string StreamName(MongoLogStream s) => s switch
    {
        MongoLogStream.Ops => "mongo_ops",
        MongoLogStream.SlowOps => "mongo_slow_ops",
        MongoLogStream.Connections => "mongo_connections",
        _ => "mongo_signin_failures",
    };

    public static async Task<PassResult> RunAsync(Manifest manifest, string corpus, PassOptions o, CancellationToken ct)
    {
        var files = CorpusFile.List(manifest, corpus);
        var nodes = files.Select(f => f.Node).Distinct().Order().ToArray();
        var s3 = manifest.Form == "s3";
        var start = manifest.WindowStartMs * 1000;
        var end = manifest.WindowEndMs * 1000;
        var pass = new PassResult();
        foreach (var stream in Order)
        {
            var clock = Stopwatch.StartNew();
            var result = pass.Streams[stream];
            var audit = stream == MongoLogStream.Ops;
            using var upload = new Upload(o.Capture, o.Tally);
            var streaming = audit ? new StreamingRows(upload) : null;
            var buffered = audit ? null : new ArrayBufferWriter<byte>(64 * 1024);
            var sink = new RowSink(audit ? streaming! : buffered!);
            foreach (var node in nodes)
            {
                var processor = new MongoLogProcessor(stream, Host(node), node == 0 ? "REPLICA_PRIMARY" : "REPLICA_SECONDARY",
                    start, end, sink, o.GroupCap, s3 ? LineForm.Rewritten : LineForm.Api);
                foreach (var file in files.Where(f => f.Node == node && f.Audit == audit))
                {
                    result.BytesIn += s3 ? await ReadS3Async(file.Path, processor, o.Pacer, ct) : await ReadApiAsync(file.Path, processor, o.Pacer, ct);
                }
                processor.Finish();
                streaming?.Flush();
                var c = processor.Counts;
                result.LinesIn += c.LinesIn;
                result.OutOfWindow += c.OutOfWindow;
                result.Filtered += c.Filtered;
                result.Malformed += c.Malformed;
                result.Kept += c.Kept;
                result.OverflowLines += processor.OverflowLines;
                result.LateLines += processor.LateLines;
            }
            if (buffered != null)
            {
                result.PeakBufferedBytes = buffered.Capacity;
                upload.Write(buffered.WrittenSpan);
            }
            upload.Complete();
            result.Rows = upload.Rows;
            result.EventCount = upload.EventCount;
            result.Nreturned = upload.Nreturned;
            result.DurationMsSum = upload.DurationMsSum;
            result.RawBytesOut = upload.RawBytes;
            result.WireBytesOut = upload.WireBytes;
            result.Seconds = clock.Elapsed.TotalSeconds;
            if (upload.Captured != null)
            {
                pass.Shipped[stream] = upload.Captured.ToArray();
            }
        }
        return pass;
    }

    public static string Host(int node) => $"cluster0-shard-00-{node:D2}.example.mongodb.net";

    /// <summary>MongoSource.ReadAsync's loop over one gzip file, as the Admin API serves it.</summary>
    private static async Task<long> ReadApiAsync(string path, MongoLogProcessor processor, Pacer? pacer, CancellationToken ct)
    {
        var bytes = new FileInfo(path).Length;
        using var lines = new LogLines(File.OpenRead(path));
        var since = 0;
        while (await lines.FillAsync(ct))
        {
            while (lines.TryReadLine(out var line))
            {
                processor.Process(line);
                if (++since == FlushLines)
                {
                    since = 0;
                    processor.FlushClosedMinutes();
                    Progress.Add(FlushLines);
                    pacer?.Wait();
                }
            }
        }
        Progress.Add(since);
        return bytes;
    }

    /// <summary>
    /// The same loop over one export object, with the engine's own S3 classes (1.10.0): the plain-file reader and the
    /// envelope's unwrap, so what is measured is what ships. Each record's inner line goes to the engine, rewritten form.
    /// </summary>
    private static async Task<long> ReadS3Async(string path, MongoLogProcessor processor, Pacer? pacer, CancellationToken ct)
    {
        var bytes = new FileInfo(path).Length;
        using var lines = new LogLines(File.OpenRead(path), gzip: false);
        var record = new S3Envelope();
        var since = 0;
        while (await lines.FillAsync(ct))
        {
            while (lines.TryReadLine(out var line))
            {
                // A record that is not an envelope with a string log reaches the processor empty: malformed.
                processor.Process(record.Inner(line));
                if (++since == FlushLines)
                {
                    since = 0;
                    processor.FlushClosedMinutes();
                    Progress.Add(FlushLines);
                    pacer?.Wait();
                }
            }
        }
        Progress.Add(since);
        return bytes;
    }
}

/// <summary>A corpus file: its node, whether it is an audit log, and its path.</summary>
internal sealed record CorpusFile(int Node, bool Audit, string Path)
{
    /// <summary>
    /// API files are "node0.audit.log.gz" / "node0.mongodb.log.gz"; export objects are
    /// "node0/mongod_audit/000001-mongod_audit.log", in name order (the order they were written).
    /// </summary>
    public static IReadOnlyList<CorpusFile> List(Manifest manifest, string corpus) =>
        manifest.Files.Order(StringComparer.Ordinal).Select(name =>
        {
            var node = int.Parse(name.AsSpan(4, name.IndexOfAny(['.', '/']) - 4), CultureInfo.InvariantCulture);
            var audit = manifest.Form == "s3" ? name.Contains("/mongod_audit/", StringComparison.Ordinal) : name.Contains(".audit.", StringComparison.Ordinal);
            return new CorpusFile(node, audit, System.IO.Path.Combine(corpus, name));
        }).ToList();
}
