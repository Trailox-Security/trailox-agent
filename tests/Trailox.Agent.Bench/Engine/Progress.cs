namespace Trailox.Agent.Bench.Engine;

/// <summary>Lines processed so far, for the meter and the pacer.</summary>
internal static class Progress
{
    private static long _lines;

    public static long Lines => Interlocked.Read(ref _lines);

    public static void Add(long lines) => Interlocked.Add(ref _lines, lines);
}

/// <summary>Holds the reader to a line rate, for the sustained run (null = as fast as possible).</summary>
internal sealed class Pacer
{
    private readonly double _linesPerSecond;
    private readonly long _startLines;
    private readonly long _startTicks = Environment.TickCount64;

    public Pacer(double linesPerSecond)
    {
        _linesPerSecond = linesPerSecond;
        _startLines = Progress.Lines;
    }

    public void Wait()
    {
        var due = (Progress.Lines - _startLines) / _linesPerSecond * 1000;
        var ahead = due - (Environment.TickCount64 - _startTicks);
        if (ahead > 1)
        {
            Thread.Sleep((int)ahead);
        }
    }
}
