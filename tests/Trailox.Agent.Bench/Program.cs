using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Trailox.Agent.Bench.Generate;
using Trailox.Agent.Bench.Measure;
using Trailox.Agent.Bench.Pipeline;

namespace Trailox.Agent.Bench;

/// <summary>
/// The MongoDB throughput gate (PLAN D12, §6.4). See README.md.
///   generate --out DIR [--profile typical|hostile] [--ops N] [--window-minutes N] [--nodes N] [--audit true|false] [--seed N]
///   run --corpus DIR [--seconds N | --passes N] [--rate LINES_PER_SECOND] [--group-cap N] [--readers N] [--report FILE]
///   selftest
/// </summary>
public static class Program
{
    public static int Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.Error.WriteLine("usage: trailox-agent-bench generate|run|selftest [options]  (see README.md)");
            return 2;
        }
        var options = Options.Parse(args.Skip(1));
        return args[0] switch
        {
            "generate" => Generate(options),
            "run" => Run(options),
            "selftest" => SelfTest(),
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

    private static int Run(Options o)
    {
        var corpus = o.Required("corpus");
        var manifest = JsonSerializer.Deserialize<Manifest>(File.ReadAllText(Path.Combine(corpus, "manifest.json")))!;
        var files = manifest.Files.Select(f => Path.Combine(corpus, f)).ToArray();
        var seconds = o.Int("seconds", 0);
        var passes = o.Int("passes", seconds > 0 ? int.MaxValue : 1);
        var rate = o.Int("rate", 0);
        var groupCap = o.Int("group-cap", 20_000);
        var readers = o.Int("readers", Environment.ProcessorCount);
        var stage = o.Get("stage", "full");

        Console.WriteLine($"corpus {manifest.Spec.Profile}, {manifest.Lines:N0} lines/pass, {files.Length} files; readers {readers}, group cap {groupCap:N0}, " +
                          (rate > 0 ? $"paced at {rate:N0} lines/s" : "as fast as possible") + (seconds > 0 ? $" for {seconds} s" : $" for {passes} pass(es)"));
        var pacer = rate > 0 ? new Pacer(rate) : null;
        var total = new Counts();
        var failures = new List<string>();
        var done = 0;
        var clock = Stopwatch.StartNew();
        using var meter = new Meter();
        while (done < passes && (seconds == 0 || clock.Elapsed.TotalSeconds < seconds))
        {
            var pass = RunPass(manifest, files, groupCap, readers, pacer, capture: false, stage: stage);
            if (stage == "full")
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
            ["opsPerSecondInCorpus"] = manifest.Spec.OpsPerSecond,
            ["audit"] = manifest.Spec.Audit,
            ["passes"] = done,
            ["groupCap"] = groupCap,
            ["readers"] = readers,
            ["pacedLinesPerSecond"] = rate,
            ["stage"] = stage,
            ["measured"] = measured,
            ["perPass"] = Describe(total, done),
            ["lostOrMiscounted"] = failures.Count,
            ["verification"] = stage != "full" ? $"none (stage {stage} stops before the rows)" : failures.Count == 0 ? "every pass: lines in = lines accounted, shipped eventCount = lines aggregated, all equal to the generator's manifest" : string.Join("; ", failures.Take(20)),
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

    private static Counts RunPass(Manifest manifest, string[] files, int groupCap, int readers, Pacer? pacer, bool capture, List<RowSink>? captured = null, string stage = "full")
    {
        var results = new Counts[files.Length];
        Parallel.For(0, files.Length, new ParallelOptions { MaxDegreeOfParallelism = readers }, i =>
        {
            var name = Path.GetFileName(files[i]);
            var processor = new FileProcessor(name[..name.IndexOf('.')], manifest.WindowStartMs, manifest.WindowEndMs, groupCap, pacer, capture) { Stage = stage };
            processor.Read(files[i]);
            results[i] = processor.Finish();
            if (captured != null)
            {
                lock (captured)
                {
                    captured.AddRange(processor.Sinks);
                }
            }
        });
        var pass = new Counts();
        foreach (var r in results)
        {
            pass.Add(r);
        }
        return pass;
    }

    /// <summary>Every line accounted for, every aggregated line shipped, all equal to what the generator wrote.</summary>
    private static List<string> Verify(Manifest m, Counts c)
    {
        var problems = new List<string>();
        void Check(string what, long actual, long expected)
        {
            if (actual != expected)
            {
                problems.Add($"{what} {actual:N0} != {expected:N0}");
            }
        }
        Check("lines in", c.LinesIn, m.Lines);
        Check("lines accounted", c.Accounted, c.LinesIn);
        Check("malformed", c.Malformed, 0);
        Check("out of window", c.OutOfWindow, m.OutOfWindow);
        Check("filtered", c.Filtered, m.Filtered);
        Check("ops", c.Ops, m.Ops);
        Check("slow ops", c.SlowOps, m.SlowOps);
        Check("connection lines", c.Connections, m.Connections);
        Check("failed sign-in lines", c.SigninFailures, m.SigninFailures);
        Check("mongo_ops eventCount shipped", c.OpsEvents, c.Ops);
        Check("mongo_slow_ops eventCount shipped", c.SlowEvents, c.SlowOps);
        Check("mongo_signin_failures eventCount shipped", c.FailureEvents, c.SigninFailures);
        Check("mongo_connections rows shipped", c.ConnectionRows, c.Connections);
        Check("slow nreturned shipped", c.SlowNreturned, m.SlowNreturned);
        Check("slow durationMillis shipped", c.SlowDurationMs, m.SlowDurationMs);
        return problems;
    }

    private static Dictionary<string, object> Describe(Counts t, int passes)
    {
        long P(long v) => passes == 0 ? 0 : v / passes;
        return new Dictionary<string, object>
        {
            ["linesIn"] = P(t.LinesIn),
            ["outOfWindow"] = P(t.OutOfWindow),
            ["filtered"] = P(t.Filtered),
            ["malformed"] = P(t.Malformed),
            ["ops"] = P(t.Ops),
            ["slowOps"] = P(t.SlowOps),
            ["connectionLines"] = P(t.Connections),
            ["signinFailureLines"] = P(t.SigninFailures),
            ["mongoOpsRows"] = P(t.OpsRows),
            ["mongoSlowOpsRows"] = P(t.SlowRows),
            ["mongoConnectionsRows"] = P(t.ConnectionRows),
            ["mongoSigninFailuresRows"] = P(t.FailureRows),
            ["opsPerRow"] = t.OpsRows == 0 ? 0 : Math.Round((double)t.OpsEvents / t.OpsRows, 1),
            ["linesInOverflowGroup"] = P(t.OverflowLines),
            ["lateLines"] = P(t.LateLines),
            ["peakGroupsHeld"] = P(t.PeakGroups),
            ["gzipBytesIn"] = P(t.GzipBytesIn),
            ["rawBytesIn"] = P(t.RawBytesIn),
            ["rawBytesOut"] = P(t.RawBytesOut),
            ["wireBytesOut"] = P(t.WireBytesOut),
            ["wireOutPerGzipIn"] = t.GzipBytesIn == 0 ? 0 : Math.Round((double)t.WireBytesOut / t.GzipBytesIn, 4),
        };
    }

    /// <summary>
    /// Small corpora, typical and hostile, one pass each with every shipped row kept: the counts
    /// must match the manifest and no literal value may appear in any row.
    /// </summary>
    private static int SelfTest()
    {
        var failed = false;
        foreach (var profile in new[] { "typical", "hostile" })
        {
            var dir = Path.Combine(Path.GetTempPath(), $"trailox-bench-selftest-{profile}-{Environment.ProcessId}");
            try
            {
                var spec = new CorpusSpec { Profile = profile, OpsPerSecond = 3000, WindowMinutes = 2, Nodes = 3, MemberSeconds = 30, Seed = 7 };
                var manifest = new CorpusGenerator(spec, Fixtures).Write(dir);
                var files = manifest.Files.Select(f => Path.Combine(dir, f)).ToArray();
                var sinks = new List<RowSink>();
                // A small cap, so the hostile corpus exercises the __other__ group.
                var counts = RunPass(manifest, files, groupCap: 5000, readers: 2, pacer: null, capture: true, captured: sinks);
                var problems = Verify(manifest, counts);
                var leaked = sinks.Sum(s => Count(s.Capture!.ToArray(), Encoding.UTF8.GetBytes(Commands.LiteralMarker)));
                if (leaked > 0)
                {
                    problems.Add($"{leaked} literal value(s) reached the shipped rows");
                }
                if (profile == "hostile" && counts.OverflowLines == 0)
                {
                    problems.Add("the hostile corpus never reached the group cap");
                }
                Console.WriteLine($"{profile}: {manifest.Lines:N0} lines, {counts.OpsRows + counts.SlowRows:N0} op rows, " +
                                  $"{counts.OverflowLines:N0} lines over the cap, {counts.LateLines:N0} late lines: " +
                                  (problems.Count == 0 ? "PASS" : "FAIL " + string.Join("; ", problems)));
                failed |= problems.Count > 0;
            }
            finally
            {
                if (Directory.Exists(dir))
                {
                    Directory.Delete(dir, recursive: true);
                }
            }
        }
        return failed ? 1 : 0;
    }

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
