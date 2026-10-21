using System.Buffers;
using System.Globalization;
using System.IO.Compression;
using System.Text.Json;

namespace Trailox.Agent.Bench.Generate;

/// <summary>What to generate. One corpus is one task window of one replica set, plus the margin
/// Atlas returns around it (its files start and end on chunk boundaries, not on the window).</summary>
internal sealed record CorpusSpec
{
    public string Profile { get; init; } = "typical";
    public int OpsPerSecond { get; init; } = 100_000;
    public int WindowMinutes { get; init; } = 2;
    public int MarginSeconds { get; init; } = 10;
    public int Nodes { get; init; } = 3;
    public double PrimaryShare { get; init; } = 0.9;
    public bool Audit { get; init; } = true;
    public int MemberSeconds { get; init; } = 20;
    public int Seed { get; init; } = 1;
    public DateTime WindowStartUtc { get; init; } = new(2026, 10, 19, 9, 0, 0, DateTimeKind.Utc);

    public bool Hostile => Profile == "hostile";
}

/// <summary>Counts the generator knows to be true, which a run must reproduce exactly.</summary>
internal sealed class Manifest
{
    public CorpusSpec Spec { get; set; } = new();
    public long WindowStartMs { get; set; }
    public long WindowEndMs { get; set; }
    public long Lines { get; set; }
    public long OutOfWindow { get; set; }
    public long Filtered { get; set; }
    public long Ops { get; set; }
    public long SlowOps { get; set; }
    public long Connections { get; set; }
    public long SigninFailures { get; set; }
    public long SlowNreturned { get; set; }
    public long SlowDurationMs { get; set; }
    public long BackwardsLines { get; set; }
    public long RawBytes { get; set; }
    public long GzipBytes { get; set; }

    // The same lines per file kind, as the engine reads them: the audit file feeds mongo_ops, and every server
    // log is read whole by each of the three other streams. In window unless named otherwise.
    public long ServerLines { get; set; }
    public long ServerOutOfWindow { get; set; }
    public long ServerSlowOps { get; set; }
    public long ServerConnections { get; set; }
    public long ServerSigninFailures { get; set; }
    public long AuditLines { get; set; }
    public long AuditOutOfWindow { get; set; }
    public long AuditOps { get; set; }

    /// <summary>"api": gzip files as the Admin API serves them. "s3": the export's plain JSON-line objects (S3Export).</summary>
    public string Form { get; set; } = "api";

    public List<string> Files { get; set; } = new();

    public void Add(Manifest o)
    {
        Lines += o.Lines; OutOfWindow += o.OutOfWindow; Filtered += o.Filtered; Ops += o.Ops; SlowOps += o.SlowOps;
        Connections += o.Connections; SigninFailures += o.SigninFailures; SlowNreturned += o.SlowNreturned;
        SlowDurationMs += o.SlowDurationMs; BackwardsLines += o.BackwardsLines; RawBytes += o.RawBytes;
        ServerLines += o.ServerLines; ServerOutOfWindow += o.ServerOutOfWindow; ServerSlowOps += o.ServerSlowOps;
        ServerConnections += o.ServerConnections; ServerSigninFailures += o.ServerSigninFailures;
        AuditLines += o.AuditLines; AuditOutOfWindow += o.AuditOutOfWindow; AuditOps += o.AuditOps;
    }

    public static readonly JsonSerializerOptions Json = new() { WriteIndented = true };
}

/// <summary>
/// Writes, per node, a server-log file and (with audit on) an audit-log file, both gzip with one
/// member per chunk as Atlas publishes them (measured: one member per ~5-minute chunk). Slices of
/// <see cref="CorpusSpec.MemberSeconds"/> are generated in parallel and concatenated.
/// </summary>
internal sealed class CorpusGenerator
{
    // Measured on the test cluster under load, audit off (Phase 0 facts §10, §13).
    private const double SlowShare = 0.0005;
    private const int NoisePerNodeSecond = 10;
    private const double BackwardsShare = 0.002;
    private const int MaxBackwardsMs = 90;

    private readonly CorpusSpec _spec;
    private readonly IReadOnlyDictionary<string, LineTemplate[]> _server;
    private readonly IReadOnlyDictionary<string, LineTemplate[]> _audit;

    public CorpusGenerator(CorpusSpec spec, string fixtures)
    {
        _spec = spec;
        _server = LineTemplate.Load(Path.Combine(fixtures, "server-log.tmpl")).GroupBy(t => t.Name).ToDictionary(g => g.Key, g => g.ToArray());
        _audit = LineTemplate.Load(Path.Combine(fixtures, "audit-log.tmpl")).GroupBy(t => t.Name).ToDictionary(g => g.Key, g => g.ToArray());
    }

    public Manifest Write(string outDir)
    {
        Directory.CreateDirectory(outDir);
        var windowStart = new DateTimeOffset(_spec.WindowStartUtc).ToUnixTimeMilliseconds();
        var windowEnd = windowStart + _spec.WindowMinutes * 60_000L;
        var spanStart = windowStart - _spec.MarginSeconds * 1000L;
        var spanEnd = windowEnd + _spec.MarginSeconds * 1000L;
        var slices = (int)Math.Ceiling((spanEnd - spanStart) / (_spec.MemberSeconds * 1000.0));

        var jobs = (from node in Enumerable.Range(0, _spec.Nodes) from slice in Enumerable.Range(0, slices) select (node, slice)).ToArray();
        var results = new Manifest[jobs.Length];
        Parallel.For(0, jobs.Length, i =>
        {
            var (node, slice) = jobs[i];
            var from = spanStart + slice * _spec.MemberSeconds * 1000L;
            var to = Math.Min(spanEnd, from + _spec.MemberSeconds * 1000L);
            results[i] = WriteSlice(outDir, node, slice, from, to, windowStart, windowEnd);
        });

        var manifest = new Manifest { Spec = _spec, WindowStartMs = windowStart, WindowEndMs = windowEnd };
        foreach (var r in results)
        {
            manifest.Add(r);
        }
        for (var node = 0; node < _spec.Nodes; node++)
        {
            foreach (var kind in _spec.Audit ? new[] { "mongodb", "audit" } : new[] { "mongodb" })
            {
                var target = Path.Combine(outDir, $"node{node}.{kind}.log.gz");
                using (var output = File.Create(target))
                {
                    for (var slice = 0; slice < slices; slice++)
                    {
                        var part = SlicePath(outDir, node, slice, kind);
                        using (var input = File.OpenRead(part))
                        {
                            input.CopyTo(output);
                        }
                        File.Delete(part);
                    }
                }
                manifest.GzipBytes += new FileInfo(target).Length;
                manifest.Files.Add(Path.GetFileName(target));
            }
        }
        File.WriteAllText(Path.Combine(outDir, "manifest.json"), JsonSerializer.Serialize(manifest, Manifest.Json));
        return manifest;
    }

    private static string SlicePath(string dir, int node, int slice, string kind) => Path.Combine(dir, $".node{node}.{kind}.{slice:D4}.part");

    /// <summary>The lines of one node for [from, to), as one gzip member per file.</summary>
    private Manifest WriteSlice(string dir, int node, int slice, long from, long to, long windowStart, long windowEnd)
    {
        var rng = new Random(_spec.Seed * 1_000_003 + node * 7919 + slice * 104_729 + 1);
        var m = new Manifest();
        var share = node == 0 ? _spec.PrimaryShare : (1 - _spec.PrimaryShare) / Math.Max(1, _spec.Nodes - 1);
        var opsPerSecond = _spec.OpsPerSecond * share;
        var population = new Population(_spec, node, slice, rng);

        using var serverFile = new SliceWriter(SlicePath(dir, node, slice, "mongodb"));
        using var auditFile = _spec.Audit ? new SliceWriter(SlicePath(dir, node, slice, "audit")) : null;
        var values = new LineValues();
        values.Set(Slot.Nip, $"10.0.0.{node + 10}");
        values.Set(Slot.Host, $"cluster0-shard-00-{node:D2}.example.mongodb.net");
        var cmd = new ArrayBufferWriter<byte>(1024);
        var keyId = 7_000_000_000_000_000_000L + _spec.Seed;
        var increment = 0;
        var signature = "";

        // Lines are generated a second at a time, sorted by their timestamp, then a few are moved
        // back in time as the real files show (threads writing at the same moment).
        var events = new List<(long Ts, Action<long> Write)>();
        var carry = 0.0;
        for (var second = from; second < to; second += 1000)
        {
            events.Clear();
            var span = (int)Math.Min(1000, to - second);
            // One cluster time signature per second, shared by the operations of that second.
            signature = Commands.Base64(rng);
            increment = 0;
            var clusterSecond = second / 1000;
            carry += opsPerSecond * span / 1000.0;
            var ops = (int)carry;
            carry -= ops;

            if (slice == 0 && second == from)
            {
                events.Add((second, ts => Emit(serverFile, Server("process_start"), ts, v => v.Set(Slot.Pid, 2000 + node), Kind.Connection)));
            }
            for (var i = 0; i < ops; i++)
            {
                var conn = population.ConnectionFor(rng);
                var (template, f1, f2, db, coll) = population.QueryFor(conn, rng);
                var slow = rng.NextDouble() < SlowShare;
                events.Add((second + rng.Next(span), ts =>
                {
                    var envelope = new Envelope(conn.Lsid, clusterSecond, ++increment, signature, keyId, ++conn.TxnNumber);
                    values.Set(Slot.Cmdname, Commands.WireName(template));
                    values.Set(Slot.Ns, $"{db}.{coll}");
                    if (auditFile != null)
                    {
                        Commands.Write(cmd, audit: true, template, coll, db, f1, f2, envelope, rng);
                        values[Slot.Cmd] = cmd.WrittenSpan.ToArray();
                        Emit(auditFile, Audit("authCheck"), ts, v => { conn.Fill(v); v.Set(Slot.Db, db); v.Set(Slot.Result, 0); }, Kind.Op);
                    }
                    if (slow)
                    {
                        Commands.Write(cmd, audit: false, template, coll, db, f1, f2, envelope, rng);
                        values[Slot.Cmd] = cmd.WrittenSpan.ToArray();
                        Emit(serverFile, Server("slow"), ts, v =>
                        {
                            conn.Fill(v);
                            var nret = rng.Next(0, 200);
                            var dur = rng.Next(100, 2000);
                            v.Set(Slot.Nret, nret);
                            v.Set(Slot.Docs, nret + rng.Next(0, 5000));
                            v.Set(Slot.Keys, nret);
                            v.Set(Slot.Reslen, 200 + nret * 300);
                            v.Set(Slot.Dur, dur);
                            v.Set(Slot.Cpu, dur * 900_000L);
                            v.Set(Slot.Qsh, Commands.Hex(rng, 32).ToUpperInvariant());
                            if (InWindow(ts))
                            {
                                m.SlowNreturned += nret;
                                m.SlowDurationMs += dur;
                            }
                        }, Kind.Slow);
                    }
                }));
            }

            // Connection churn: a short-lived client connects, signs in and leaves.
            var churn = population.ChurnPerSecond(share) * span / 1000.0;
            for (var c = (int)churn + (rng.NextDouble() < churn % 1 ? 1 : 0); c > 0; c--)
            {
                var fresh = population.Churn();
                var at = second + rng.Next(span);
                events.Add((at + 1, ts => Emit(serverFile, Server("conn_accepted"), ts, fresh.Fill, Kind.Connection)));
                events.Add((at + 400, ts => Emit(serverFile, Server("conn_ended"), ts, fresh.Fill, Kind.Connection)));
                events.Add((at + 2, ts => Emit(serverFile, ServerAll("noise")[0], ts, fresh.Fill, Kind.Noise)));
                events.Add((at + 3, ts => Emit(serverFile, Server("signin"), ts, fresh.Fill, Kind.Connection)));
                events.Add((at + 3, ts => Emit(serverFile, ServerAll("noise")[3], ts, fresh.Fill, Kind.Noise)));
                if (auditFile != null)
                {
                    events.Add((at + 3, ts => Emit(auditFile, Audit("authenticate"), ts, v => { fresh.Fill(v); v.Set(Slot.Result, 0); }, Kind.Connection)));
                    events.Add((at + 3, ts => Emit(auditFile, Audit("clientMetadata"), ts, fresh.Fill, Kind.Connection)));
                    events.Add((at + 400, ts => Emit(auditFile, Audit("noise"), ts, fresh.Fill, Kind.Noise)));
                }
            }

            // Failed sign-ins: an unknown user writes two lines per attempt (mechanism fallback).
            var failures = population.FailuresPerSecond(node) * span / 1000.0;
            for (var f = (int)failures + (rng.NextDouble() < failures % 1 ? 1 : 0); f > 0; f--)
            {
                var attacker = population.Attacker(rng);
                var at = second + rng.Next(span);
                events.Add((at, ts => Emit(serverFile, Server("signin_failed"), ts, attacker.Fill, Kind.Failure)));
                events.Add((at + 1, ts => Emit(serverFile, Server("signin_failed"), ts, attacker.Fill, Kind.Failure)));
                if (auditFile != null)
                {
                    events.Add((at + 1, ts => Emit(auditFile, Audit("authenticate"), ts, v => { attacker.Fill(v); v.Set(Slot.Result, 18); }, Kind.Failure)));
                }
            }

            for (var n = NoisePerNodeSecond * span / 1000; n > 0; n--)
            {
                var noise = ServerAll("noise");
                var template = noise[rng.Next(noise.Length)];
                var who = population.ConnectionFor(rng);
                events.Add((second + rng.Next(span), ts => Emit(serverFile, template, ts, who.Fill, Kind.Noise)));
            }
            if (auditFile != null && rng.Next(60) == 0)
            {
                var who = population.ConnectionFor(rng);
                var (db, coll) = population.Namespace(rng);
                events.Add((second + rng.Next(span), ts => Emit(auditFile, Audit("createIndex"), ts, v => { who.Fill(v); v.Set(Slot.Ns, $"{db}.{coll}"); }, Kind.Op)));
            }

            events.Sort((a, b) => a.Ts.CompareTo(b.Ts));
            foreach (var (ts, write) in events)
            {
                var written = ts;
                if (rng.NextDouble() < BackwardsShare && ts - MaxBackwardsMs > from)
                {
                    written = ts - rng.Next(1, MaxBackwardsMs);
                    m.BackwardsLines++;
                }
                write(written);
            }
        }

        m.RawBytes = serverFile.RawBytes + (auditFile?.RawBytes ?? 0);
        return m;

        bool InWindow(long ts) => ts >= windowStart && ts < windowEnd;

        void Emit(SliceWriter file, LineTemplate template, long ts, Action<LineValues> fill, Kind kind)
        {
            values.Set(Slot.T, DateTimeOffset.FromUnixTimeMilliseconds(ts).ToString("yyyy-MM-dd'T'HH:mm:ss.fff'+00:00'", CultureInfo.InvariantCulture));
            fill(values);
            template.Render(file.Buffer, values);
            file.LineDone();
            m.Lines++;
            var server = ReferenceEquals(file, serverFile);
            if (server)
            {
                m.ServerLines++;
            }
            else
            {
                m.AuditLines++;
            }
            if (!InWindow(ts))
            {
                m.OutOfWindow++;
                if (server)
                {
                    m.ServerOutOfWindow++;
                }
                else
                {
                    m.AuditOutOfWindow++;
                }
                return;
            }
            switch (kind)
            {
                case Kind.Op: m.Ops++; break;
                case Kind.Slow: m.SlowOps++; break;
                case Kind.Connection: m.Connections++; break;
                case Kind.Failure: m.SigninFailures++; break;
                default: m.Filtered++; break;
            }
            switch (kind)
            {
                case Kind.Op when !server: m.AuditOps++; break;
                case Kind.Slow when server: m.ServerSlowOps++; break;
                case Kind.Connection when server: m.ServerConnections++; break;
                case Kind.Failure when server: m.ServerSigninFailures++; break;
            }
        }
    }

    private LineTemplate Server(string name) => _server[name][0];

    private LineTemplate[] ServerAll(string name) => _server[name];

    private LineTemplate Audit(string name) => _audit[name][0];

    private enum Kind { Op, Slow, Connection, Failure, Noise }

    /// <summary>A gzip member written to its own file; the slices are concatenated afterwards.</summary>
    private sealed class SliceWriter : IDisposable
    {
        private readonly FileStream _file;
        private readonly GZipStream _gzip;

        public ArrayBufferWriter<byte> Buffer { get; } = new(1 << 20);

        public long RawBytes { get; private set; }

        public SliceWriter(string path)
        {
            _file = File.Create(path);
            _gzip = new GZipStream(_file, CompressionLevel.Optimal);
        }

        public void LineDone()
        {
            if (Buffer.WrittenCount >= 1 << 20)
            {
                Flush();
            }
        }

        private void Flush()
        {
            RawBytes += Buffer.WrittenCount;
            _gzip.Write(Buffer.WrittenSpan);
            Buffer.Clear();
        }

        public void Dispose()
        {
            Flush();
            _gzip.Dispose();
            _file.Dispose();
        }
    }
}
