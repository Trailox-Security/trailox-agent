using System.Diagnostics;
using System.Globalization;
using Trailox.Agent.Bench.Pipeline;

namespace Trailox.Agent.Bench.Measure;

/// <summary>One second of the run.</summary>
internal readonly record struct Sample(double Seconds, long Lines, double CpuSeconds, long WorkingSet, long ManagedHeap, long CgroupMemory);

/// <summary>
/// Samples the process once a second: lines, CPU, working set, managed heap and, inside a cgroup
/// (a container, or a systemd scope with limits), the cgroup's own memory and CPU throttling.
/// </summary>
internal sealed class Meter : IDisposable
{
    private readonly List<Sample> _samples = new();
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private readonly Timer _timer;
    private readonly string? _cgroup = CgroupDirectory();
    private readonly (long Throttled, long ThrottledUsec) _throttleAtStart;
    private readonly int[] _gcAtStart = [GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2)];
    private readonly TimeSpan _pauseAtStart = GC.GetTotalPauseDuration();
    private readonly long _allocatedAtStart = GC.GetTotalAllocatedBytes();
    private readonly TimeSpan _cpuAtStart = Environment.CpuUsage.TotalTime;
    private readonly long _linesAtStart = Progress.Lines;
    private TimeSpan _maxPause;

    public Meter()
    {
        _throttleAtStart = Throttling();
        _timer = new Timer(_ => Take(), null, 1000, 1000);
    }

    private void Take()
    {
        var info = GC.GetGCMemoryInfo(GCKind.Any);
        foreach (var pause in info.PauseDurations)
        {
            if (pause > _maxPause)
            {
                _maxPause = pause;
            }
        }
        var sample = new Sample(
            _clock.Elapsed.TotalSeconds,
            Progress.Lines - _linesAtStart,
            (Environment.CpuUsage.TotalTime - _cpuAtStart).TotalSeconds,
            Environment.WorkingSet,
            GC.GetTotalMemory(false),
            ReadLong("memory.current"));
        lock (_samples)
        {
            _samples.Add(sample);
        }
    }

    public Dictionary<string, object> Stop()
    {
        _timer.Dispose();
        Take();
        List<Sample> s;
        lock (_samples)
        {
            s = _samples.ToList();
        }
        var last = s[^1];
        var lines = last.Lines;
        // The sustained rate is the slowest 10-second stretch, not the mean.
        var floor = double.NaN;
        for (int i = 0, j = 0; j < s.Count; j++)
        {
            while (s[j].Seconds - s[i].Seconds > 10)
            {
                i++;
            }
            if (s[j].Seconds - s[i].Seconds >= 9.5)
            {
                var rate = (s[j].Lines - s[i].Lines) / (s[j].Seconds - s[i].Seconds);
                floor = double.IsNaN(floor) ? rate : Math.Min(floor, rate);
            }
        }
        // Lines per second in 10-second steps, and the rate once the JIT has settled (after 30 s).
        var series = new List<double>();
        for (int i = 0, j = 0; j < s.Count; j++)
        {
            if (s[j].Seconds - s[i].Seconds >= 10)
            {
                series.Add(Round((s[j].Lines - s[i].Lines) / (s[j].Seconds - s[i].Seconds), 0));
                i = j;
            }
        }
        var settled = s.FirstOrDefault(x => x.Seconds >= 30);
        var afterWarmup = settled.Seconds > 0 && last.Seconds - settled.Seconds >= 10
            ? (object)Round((last.Lines - settled.Lines) / (last.Seconds - settled.Seconds))
            : "n/a (run shorter than 40 s)";
        var (throttled, throttledUsec) = Throttling();
        return new Dictionary<string, object>
        {
            ["seconds"] = Round(last.Seconds),
            ["lines"] = lines,
            ["linesPerSecond"] = Round(lines / last.Seconds),
            ["linesPerSecondSlowest10s"] = double.IsNaN(floor) ? "n/a (run shorter than 10 s)" : Round(floor),
            ["linesPerSecondAfter30s"] = afterWarmup,
            ["linesPerSecondEvery10s"] = series,
            ["cpuCoresUsed"] = Round(last.CpuSeconds / last.Seconds, 3),
            ["cpuMicrosPerLine"] = Round(last.CpuSeconds * 1e6 / Math.Max(1, lines), 3),
            ["processorCount"] = Environment.ProcessorCount,
            ["cgroupCpuMax"] = ReadText("cpu.max") ?? "none",
            ["cgroupMemoryMax"] = ReadText("memory.max") ?? "none",
            ["cgroupThrottledPeriods"] = throttled - _throttleAtStart.Throttled,
            ["cgroupThrottledSeconds"] = Round((throttledUsec - _throttleAtStart.ThrottledUsec) / 1e6),
            // The process's own peak resident memory (VmHWM). The cgroup's peak also counts the page cache
            // of the corpus files the bench reads; the agent streams its input over HTTP and has none.
            ["peakResidentMiB"] = Mib(Process.GetCurrentProcess().PeakWorkingSet64),
            ["peakManagedHeapMiB"] = Mib(s.Max(x => x.ManagedHeap)),
            ["cgroupMemoryPeakMiBInclPageCache"] = _cgroup == null ? "none" : Mib(ReadLong("memory.peak")),
            ["gcHeapHardLimitMiB"] = Mib(GC.GetGCMemoryInfo().TotalAvailableMemoryBytes),
            ["gen0"] = GC.CollectionCount(0) - _gcAtStart[0],
            ["gen1"] = GC.CollectionCount(1) - _gcAtStart[1],
            ["gen2"] = GC.CollectionCount(2) - _gcAtStart[2],
            ["gcPauseTotalMs"] = Round((GC.GetTotalPauseDuration() - _pauseAtStart).TotalMilliseconds),
            ["gcPauseMaxMs"] = Round(_maxPause.TotalMilliseconds),
            ["gcPauseShare"] = Round((GC.GetTotalPauseDuration() - _pauseAtStart).TotalSeconds / last.Seconds, 4),
            ["allocatedBytesPerLine"] = Round((GC.GetTotalAllocatedBytes() - _allocatedAtStart) / (double)Math.Max(1, lines), 1),
            ["serverGc"] = System.Runtime.GCSettings.IsServerGC,
            ["runtime"] = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
            ["os"] = System.Runtime.InteropServices.RuntimeInformation.OSDescription,
        };
    }

    public void Dispose() => _timer.Dispose();

    private static double Round(double v, int digits = 1) => Math.Round(v, digits);

    private static double Mib(long bytes) => Math.Round(bytes / 1048576.0, 1);

    private (long, long) Throttling()
    {
        var text = ReadText("cpu.stat");
        long periods = 0, usec = 0;
        foreach (var line in text?.Split('\n') ?? [])
        {
            var parts = line.Split(' ');
            if (parts.Length == 2 && parts[0] == "nr_throttled") { periods = long.Parse(parts[1], CultureInfo.InvariantCulture); }
            if (parts.Length == 2 && parts[0] == "throttled_usec") { usec = long.Parse(parts[1], CultureInfo.InvariantCulture); }
        }
        return (periods, usec);
    }

    private long ReadLong(string file) =>
        long.TryParse(ReadText(file), NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : 0;

    private string? ReadText(string file)
    {
        try
        {
            return _cgroup == null ? null : File.ReadAllText(Path.Combine(_cgroup, file)).Trim();
        }
        catch (IOException)
        {
            return null;
        }
    }

    /// <summary>This process's cgroup v2 directory ("0::/path" in /proc/self/cgroup), or null.</summary>
    private static string? CgroupDirectory()
    {
        try
        {
            var line = File.ReadLines("/proc/self/cgroup").FirstOrDefault(l => l.StartsWith("0::", StringComparison.Ordinal));
            var dir = line == null ? null : Path.Combine("/sys/fs/cgroup", line[3..].TrimStart('/'));
            return dir != null && Directory.Exists(dir) ? dir : null;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }
}
