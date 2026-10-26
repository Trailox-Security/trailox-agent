using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Trailox.Agent.Bench.Engine;
using Trailox.Agent.Bench.Generate;
using Trailox.Agent.Bench.Measure;
using Trailox.Agent.Engines.MongoDb;

namespace Trailox.Agent.Bench;

/// <summary>
/// The MongoDB throughput gate, on the agent's own engine classes. See README.md.
///   generate --out DIR [--profile typical|hostile] [--ops N] [--window-minutes N] [--nodes N] [--audit true|false] [--seed N]
///   s3 --from DIR --out DIR [--seed N]
///   run --corpus DIR [--seconds N | --passes N] [--rate LINES_PER_SECOND] [--group-cap N] [--verify true|false]
///       [--neighbour-cgroup DIR] [--report FILE]
///   selftest
/// </summary>
public static class Program
{
    public static int Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.Error.WriteLine("usage: trailox-agent-bench generate|s3|run|selftest [options]  (see README.md)");
            return 2;
        }
        var options = Options.Parse(args.Skip(1));
        return args[0] switch
        {
            "generate" => Generate(options),
            "s3" => Export(options),
            "run" => Run(options).GetAwaiter().GetResult(),
            "selftest" => SelfTest().GetAwaiter().GetResult(),
            _ => Usage(args[0]),
        };
    }

    private static int Usage(string verb)
    {
        Console.Error.WriteLine($"unknown command '{verb}'");
        return 2;
    }

    private static string Fixtures => Path.Combine(AppContext.BaseDirectory, "Fixtures");

    private static int Generate(Options o)
    {
        var spec = new CorpusSpec
        {
            Profile = o.Get("profile", "typical"),
            OpsPerSecond = o.Int("ops", 100_000),
            WindowMinutes = o.Int("window-minutes", 2),
            Nodes = o.Int("nodes", 3),
            Audit = o.Get("audit", "true") == "true",
            Seed = o.Int("seed", 1),
            MemberSeconds = o.Int("member-seconds", 20),
        };
        var clock = Stopwatch.StartNew();
        var manifest = new CorpusGenerator(spec, Fixtures).Write(o.Required("out"));
        Console.WriteLine($"generated {manifest.Lines:N0} lines ({manifest.RawBytes / 1048576.0:N0} MiB raw, {manifest.GzipBytes / 1048576.0:N0} MiB gzip, " +
                          $"{(double)manifest.RawBytes / manifest.GzipBytes:N1}x) in {clock.Elapsed.TotalSeconds:N0} s; in window: {manifest.Ops:N0} ops, " +
                          $"{manifest.SlowOps:N0} slow, {manifest.Connections:N0} connection lines, {manifest.SigninFailures:N0} failed sign-in lines");
        return 0;
    }

    private static int Export(Options o)
    {
        var clock = Stopwatch.StartNew();
        var manifest = S3Export.Write(o.Required("from"), o.Required("out"), o.Int("seed", 1));
        Console.WriteLine($"exported {manifest.Lines:N0} records into {manifest.Files.Count:N0} objects ({manifest.RawBytes / 1048576.0:N0} MiB, plain JSON lines) " +
                          $"in {clock.Elapsed.TotalSeconds:N0} s");
        return 0;
    }

    private static async Task<int> Run(Options o)
    {
        var corpus = o.Required("corpus");
        var manifest = JsonSerializer.Deserialize<Manifest>(File.ReadAllText(Path.Combine(corpus, "manifest.json")))!;
        var seconds = o.Int("seconds", 0);
        var passes = o.Int("passes", seconds > 0 ? int.MaxValue : 1);
        var rate = o.Int("rate", 0);
        var groupCap = o.Int("group-cap", MongoLogProcessor.DefaultGroupCap);
        var verify = o.Get("verify", "true") == "true";
        var neighbour = o.Get("neighbour-cgroup", "");
        var linesPerPass = manifest.AuditLines + 3 * manifest.ServerLines;

        Console.WriteLine($"corpus {manifest.Spec.Profile} ({manifest.Form}), {linesPerPass:N0} lines processed a pass ({manifest.Lines:N0} in its files), " +
                          $"{manifest.Files.Count} files; engine classes, group cap {groupCap:N0}, " +
                          (rate > 0 ? $"paced at {rate:N0} lines/s" : "as fast as possible") + (seconds > 0 ? $" for {seconds} s" : $" for {passes} pass(es)"));
        var pacer = rate > 0 ? new Pacer(rate) : null;
        var total = new PassResult();
        var failures = new List<string>();
        var done = 0;
        var clock = Stopwatch.StartNew();
        using var meter = new Meter(neighbour.Length > 0 ? neighbour : null);
        while (done < passes && (seconds == 0 || clock.Elapsed.TotalSeconds < seconds))
        {
            var pass = await EnginePass.RunAsync(manifest, corpus, new PassOptions(groupCap, pacer, Capture: false, Tally: verify), CancellationToken.None);
            if (verify)
            {
                failures.AddRange(Verify(manifest, pass).Select(f => $"pass {done + 1}: {f}"));
            }
            total.Add(pass);
            done++;
        }
        var measured = meter.Stop();
        var report = new Dictionary<string, object>
        {
            ["profile"] = manifest.Spec.Profile,
            ["form"] = manifest.Form,
            ["pipeline"] = "engine classes (Trailox.Agent.Engines.MongoDb)",
            ["opsPerSecondInCorpus"] = manifest.Spec.OpsPerSecond,
            ["windowSeconds"] = (manifest.WindowEndMs - manifest.WindowStartMs) / 1000,
            ["linesProcessedPerPass"] = linesPerPass,
            ["linesNeededPerSecond"] = Math.Round(linesPerPass / ((manifest.WindowEndMs - manifest.WindowStartMs) / 1000.0), 0),
            ["passes"] = done,
            ["groupCap"] = groupCap,
            ["pacedLinesPerSecond"] = rate,
            ["measured"] = measured,
            ["perPass"] = Describe(total, done),
            ["lostOrMiscounted"] = failures.Count,
            ["verification"] = !verify
                ? "none (--verify false: the shipped rows were not tallied)"
                : failures.Count == 0
                    ? "every pass, every stream: lines in = the generator's, out of window + filtered + kept = lines in, malformed 0, kept = the generator's, " +
                      "shipped event_count (rows for mongo_connections) = kept, slow nreturned and durationMillis shipped = the generator's"
                    : string.Join("; ", failures.Take(20)),
        };
        var json = JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true });
        Console.WriteLine(json);
        var reportPath = o.Get("report", "");
        if (reportPath.Length > 0)
        {
            File.WriteAllText(reportPath, json);
        }
        return failures.Count == 0 ? 0 : 1;
    }

    /// <summary>Every line of every stream accounted for, every kept line shipped, all equal to what the generator wrote.</summary>
    private static List<string> Verify(Manifest m, PassResult pass)
    {
        var problems = new List<string>();
        void Check(string what, long actual, long expected)
        {
            if (actual != expected)
            {
                problems.Add($"{what} {actual:N0} != {expected:N0}");
            }
        }
        foreach (var (stream, r) in pass.Streams)
        {
            var name = EnginePass.StreamName(stream);
            var audit = stream == MongoLogStream.Ops;
            var expectedKept = stream switch
            {
                MongoLogStream.Ops => m.AuditOps,
                MongoLogStream.SlowOps => m.ServerSlowOps,
                MongoLogStream.Connections => m.ServerConnections,
                _ => m.ServerSigninFailures,
            };
            Check($"{name} lines in", r.LinesIn, audit ? m.AuditLines : m.ServerLines);
            Check($"{name} lines accounted", r.OutOfWindow + r.Filtered + r.Malformed + r.Kept, r.LinesIn);
            Check($"{name} malformed", r.Malformed, 0);
            Check($"{name} out of window", r.OutOfWindow, audit ? m.AuditOutOfWindow : m.ServerOutOfWindow);
            Check($"{name} kept", r.Kept, expectedKept);
            Check(stream == MongoLogStream.Connections ? $"{name} rows shipped" : $"{name} event_count shipped",
                stream == MongoLogStream.Connections ? r.Rows : r.EventCount, r.Kept);
        }
        var slow = pass.Streams[MongoLogStream.SlowOps];
        Check("slow nreturned shipped", slow.Nreturned, m.SlowNreturned);
        Check("slow durationMillis shipped", slow.DurationMsSum, m.SlowDurationMs);
        return problems;
    }

    private static Dictionary<string, object> Describe(PassResult t, int passes)
    {
        long P(long v) => passes == 0 ? 0 : v / passes;
        var streams = new Dictionary<string, object>();
        foreach (var (stream, r) in t.Streams)
        {
            streams[EnginePass.StreamName(stream)] = new Dictionary<string, object>
            {
                ["linesIn"] = P(r.LinesIn),
                ["outOfWindow"] = P(r.OutOfWindow),
                ["filtered"] = P(r.Filtered),
                ["malformed"] = P(r.Malformed),
                ["kept"] = P(r.Kept),
                ["rows"] = P(r.Rows),
                ["eventsPerRow"] = r.Rows == 0 ? 0 : Math.Round((double)(stream == MongoLogStream.Connections ? r.Rows : r.EventCount) / r.Rows, 1),
                ["linesInOverflowGroup"] = P(r.OverflowLines),
                ["lateLines"] = P(r.LateLines),
                ["bytesIn"] = P(r.BytesIn),
                ["rawBytesOut"] = P(r.RawBytesOut),
                ["wireBytesOut"] = P(r.WireBytesOut),
                ["peakRowsHeldMiB"] = Math.Round(r.PeakBufferedBytes / 1048576.0, 1),
                ["secondsPerPass"] = passes == 0 ? 0 : Math.Round(r.Seconds / passes, 2),
            };
        }
        return new Dictionary<string, object>
        {
            ["linesIn"] = P(t.LinesIn),
            ["wireBytesOut"] = P(t.Streams.Values.Sum(s => s.WireBytesOut)),
            ["bytesIn"] = P(t.Streams.Values.Sum(s => s.BytesIn)),
            ["streams"] = streams,
        };
    }

    /// <summary>
    /// Small corpora, typical and hostile, read through the engine as the API serves them and as the S3 export
    /// rewrites them, with every shipped row kept: both must match the manifest, both must ship the same rows,
    /// and no literal value may appear in any row.
    /// </summary>
    private static async Task<int> SelfTest()
    {
        var failed = false;
        foreach (var profile in new[] { "typical", "hostile" })
        {
            var dir = Path.Combine(Path.GetTempPath(), $"trailox-bench-selftest-{profile}-{Environment.ProcessId}");
            var s3Dir = dir + "-s3";
            try
            {
                var spec = new CorpusSpec { Profile = profile, OpsPerSecond = 3000, WindowMinutes = 2, Nodes = 3, MemberSeconds = 30, Seed = 7 };
                var manifest = new CorpusGenerator(spec, Fixtures).Write(dir);
                var s3 = S3Export.Write(dir, s3Dir, seed: 7);
                // A small cap, so the hostile corpus exercises the __other__ group.
                var options = new PassOptions(GroupCap: 5000, Pacer: null, Capture: true, Tally: true);
                var api = await EnginePass.RunAsync(manifest, dir, options, CancellationToken.None);
                var exported = await EnginePass.RunAsync(s3, s3Dir, options, CancellationToken.None);
                var problems = Verify(manifest, api).Select(p => "api: " + p).Concat(Verify(s3, exported).Select(p => "s3: " + p)).ToList();
                // Every stream ships the same rows from both forms: until 1.10.0 a slow findAndModify could ship as update
                // from the S3 form, and was tolerated here as a known difference.
                foreach (var stream in EnginePass.Order)
                {
                    if (Difference(api.Shipped[stream], exported.Shipped[stream]) is { } difference)
                    {
                        problems.Add($"{EnginePass.StreamName(stream)}: the S3 form shipped other rows than the API form: {difference}");
                    }
                }
                var marker = Encoding.UTF8.GetBytes(Commands.LiteralMarker);
                var leaked = api.Shipped.Values.Concat(exported.Shipped.Values).Sum(rows => Count(rows, marker));
                if (leaked > 0)
                {
                    problems.Add($"{leaked} literal value(s) reached the shipped rows");
                }
                var overflow = api.Streams.Values.Sum(s => s.OverflowLines);
                if (profile == "hostile" && overflow == 0)
                {
                    problems.Add("the hostile corpus never reached the group cap");
                }
                Console.WriteLine($"{profile}: {manifest.Lines:N0} lines in {manifest.Files.Count} files and {s3.Files.Count} export objects, " +
                                  $"{api.Streams.Values.Sum(s => s.Rows):N0} rows, {overflow:N0} lines over the cap: " +
                                  (problems.Count == 0 ? "PASS" : "FAIL " + string.Join("; ", problems)));
                failed |= problems.Count > 0;
            }
            finally
            {
                foreach (var d in new[] { dir, s3Dir })
                {
                    if (Directory.Exists(d))
                    {
                        Directory.Delete(d, recursive: true);
                    }
                }
            }
        }
        return failed ? 1 : 0;
    }

    /// <summary>Null when both hold the same rows (in any order); else how they differ, with one row of each side.</summary>
    private static string? Difference(byte[] a, byte[] b)
    {
        var left = Rows(a);
        var right = Rows(b);
        if (left.SequenceEqual(right))
        {
            return null;
        }
        var onlyLeft = left.Except(right).ToList();
        var onlyRight = right.Except(left).ToList();
        string Cut(string s) => s.Length > 400 ? s[..400] + "..." : s;
        return $"{left.Count:N0} rows vs {right.Count:N0}; {onlyLeft.Count:N0} only in the API form" +
               (onlyLeft.Count > 0 ? $" (e.g. {Cut(onlyLeft[0])})" : "") + $", {onlyRight.Count:N0} only in the S3 form" +
               (onlyRight.Count > 0 ? $" (e.g. {Cut(onlyRight[0])})" : "");
    }

    private static List<string> Rows(byte[] ndjson) =>
        Encoding.UTF8.GetString(ndjson).Split('\n', StringSplitOptions.RemoveEmptyEntries).Order(StringComparer.Ordinal).ToList();

    private static int Count(byte[] haystack, byte[] needle)
    {
        var n = 0;
        var span = haystack.AsSpan();
        for (var at = span.IndexOf(needle); at >= 0; at = span.IndexOf(needle))
        {
            n++;
            span = span[(at + needle.Length)..];
        }
        return n;
    }

    private sealed class Options
    {
        private readonly Dictionary<string, string> _values = new(StringComparer.Ordinal);

        public static Options Parse(IEnumerable<string> args)
        {
            var o = new Options();
            string? key = null;
            foreach (var a in args)
            {
                if (a.StartsWith("--", StringComparison.Ordinal))
                {
                    key = a[2..];
                    o._values[key] = "true";
                }
                else if (key != null)
                {
                    o._values[key] = a;
                    key = null;
                }
            }
            return o;
        }

        public string Get(string key, string fallback) => _values.TryGetValue(key, out var v) ? v : fallback;

        public int Int(string key, int fallback) => _values.TryGetValue(key, out var v) ? int.Parse(v, CultureInfo.InvariantCulture) : fallback;

        public string Required(string key) => _values.TryGetValue(key, out var v) ? v : throw new ArgumentException($"--{key} is required");
    }
}
