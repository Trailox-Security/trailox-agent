using System.Diagnostics;
using Trailox.Agent.Engines.Oracle;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests.Oracle;

/// <summary>What the limit on a call does beyond stopping it: the agent stays alive while a call waits, and only while it does.</summary>
public class OraBoundedSessionTests
{
    private const string Sql = "SELECT 1 FROM SYS.DUAL";

    private static readonly TimeSpan BeatEvery = TimeSpan.FromMilliseconds(20);

    private static async Task<OraBoundedSession> SessionAsync(FakeOra fake, TimeSpan limit, Action? beat = null, ReadTime? time = null) =>
        new(await fake.OpenAsync(new OraTarget("192.0.2.10", 1521, "LEDGER", false, "U", "p"), CancellationToken.None), limit, time, beat ?? (() => { }), BeatEvery);

    /// <summary>A quiet fetch sends no row: without this the liveness probe restarts the agent in the middle of a long read.</summary>
    [Fact]
    public async Task A_call_that_waits_marks_the_agent_alive()
    {
        var beats = 0;
        using var secondBeat = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var fake = new FakeOra();
        fake.Answer(Sql, _ => new FakeRows(Array.Empty<object?[]>()) { HangsAtEnd = true });
        await using var session = await SessionAsync(fake, TimeSpan.FromSeconds(30), () =>
        {
            if (Interlocked.Increment(ref beats) == 2)
            {
                secondBeat.Cancel();
            }
        });

        await using var rows = await session.QueryAsync(Sql, Array.Empty<OraBind>(), CancellationToken.None);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => rows.ReadAsync(secondBeat.Token));

        Assert.True(Volatile.Read(ref beats) >= 2, $"{beats} beats");
    }

    /// <summary>The beats end with the limit: a call that hangs for good must not keep a stuck agent looking alive.</summary>
    [Fact]
    public async Task A_call_past_its_limit_stops_marking_the_agent_alive()
    {
        var beats = 0;
        var fake = new FakeOra();
        fake.Answer(Sql, _ => new FakeRows(Array.Empty<object?[]>()) { HangsAtEnd = true, DeafToToken = true });
        await using var session = await SessionAsync(fake, TimeSpan.FromMilliseconds(100), () => Interlocked.Increment(ref beats));

        await using var rows = await session.QueryAsync(Sql, Array.Empty<OraBind>(), CancellationToken.None);
        var stopped = await Assert.ThrowsAsync<OraDriverException>(() => rows.ReadAsync(CancellationToken.None));
        Assert.True(stopped.TimedOut);

        var atStop = Volatile.Read(ref beats);
        await Task.Delay(100);
        Assert.Equal(atStop, Volatile.Read(ref beats));
    }

    /// <summary>A driver's async call may block its caller, and then only a timer of the session's own can beat.</summary>
    [Fact]
    public async Task A_call_that_blocks_its_caller_marks_the_agent_alive_too()
    {
        var beats = 0;
        var fake = new FakeOra();
        fake.Answer(Sql, _ => new FakeRows(UntilTwo()));
        await using var session = await SessionAsync(fake, TimeSpan.FromSeconds(30), () => Interlocked.Increment(ref beats));

        await using var rows = await session.QueryAsync(Sql, Array.Empty<OraBind>(), CancellationToken.None);
        Assert.False(await rows.ReadAsync(CancellationToken.None));

        Assert.True(Volatile.Read(ref beats) >= 2, $"{beats} beats");

        IEnumerable<object?[]> UntilTwo()
        {
            var started = Stopwatch.StartNew();
            while (Volatile.Read(ref beats) < 2 && started.Elapsed < TimeSpan.FromSeconds(20))
            {
                Thread.Sleep(5);
            }
            yield break;
        }
    }

    /// <summary>The beats end at the limit even for a call that never returns to anything that could stop them.</summary>
    [Fact]
    public async Task A_call_that_blocks_past_its_limit_stops_marking_the_agent_alive()
    {
        var beats = 0;
        var atLimit = -1;
        var fake = new FakeOra();
        fake.Answer(Sql, _ => new FakeRows(PastTheLimit()));
        await using var session = await SessionAsync(fake, TimeSpan.FromMilliseconds(100), () => Interlocked.Increment(ref beats));

        await using var rows = await session.QueryAsync(Sql, Array.Empty<OraBind>(), CancellationToken.None);
        await rows.ReadAsync(CancellationToken.None);

        Assert.Equal(atLimit, Volatile.Read(ref beats));

        IEnumerable<object?[]> PastTheLimit()
        {
            Thread.Sleep(200);
            atLimit = Volatile.Read(ref beats);
            Thread.Sleep(200);
            yield break;
        }
    }

    /// <summary>The calls are what counts, whatever the upload did around them.</summary>
    [Fact]
    public async Task The_time_counts_the_calls_and_not_the_wait_between_them()
    {
        var time = new ReadTime();
        var fake = new FakeOra();
        fake.Answer(Sql, _ => new FakeRows(Array.Empty<object?[]>()) { EndsAfter = TimeSpan.FromMilliseconds(120) });
        await using var session = await SessionAsync(fake, TimeSpan.FromSeconds(30), time: time);
        await using var rows = await session.QueryAsync(Sql, Array.Empty<OraBind>(), CancellationToken.None);
        var before = time.Millis ?? 0;

        await Task.Delay(300);
        Assert.Equal(before, time.Millis ?? 0);

        await rows.ReadAsync(CancellationToken.None);
        Assert.True(time.Millis >= before + 100, $"{time.Millis} ms");
    }

    /// <summary>A quiet read stopped at its limit has sent nothing, and its time is all there is to say how slow it was.</summary>
    [Fact]
    public async Task A_call_stopped_at_its_limit_counts()
    {
        var time = new ReadTime();
        var fake = new FakeOra();
        fake.Answer(Sql, _ => new FakeRows(Array.Empty<object?[]>()) { HangsAtEnd = true });
        await using var session = await SessionAsync(fake, TimeSpan.FromMilliseconds(100), time: time);
        await using var rows = await session.QueryAsync(Sql, Array.Empty<OraBind>(), CancellationToken.None);

        await Assert.ThrowsAsync<OraDriverException>(() => rows.ReadAsync(CancellationToken.None));

        Assert.True(time.Millis >= 90, $"{time.Millis} ms");
    }

    [Fact]
    public async Task A_call_that_will_not_stop_fails_the_sessions_next_calls_at_once_as_timed_out()
    {
        var fake = new FakeOra();
        fake.Answer(Sql, _ => new FakeRows(Array.Empty<object?[]>()) { HangsAtEnd = true, DeafToToken = true });
        await using var session = await SessionAsync(fake, TimeSpan.FromMilliseconds(50));
        await using var rows = await session.QueryAsync(Sql, Array.Empty<OraBind>(), CancellationToken.None);
        await Assert.ThrowsAsync<OraDriverException>(() => rows.ReadAsync(CancellationToken.None));
        var asked = fake.Statements.Count;

        var next = await Assert.ThrowsAsync<OraDriverException>(() => session.QueryAsync("SELECT 2 FROM SYS.DUAL", Array.Empty<OraBind>(), CancellationToken.None));

        Assert.True(next.TimedOut);
        Assert.Equal(asked, fake.Statements.Count);
    }

    /// <summary>The agent stopping is no time-out, and must not read as one.</summary>
    [Fact]
    public async Task A_cancelled_call_stays_a_cancellation()
    {
        var fake = new FakeOra();
        fake.Answer(Sql, _ => new FakeRows(Array.Empty<object?[]>()) { HangsAtEnd = true });
        await using var session = await SessionAsync(fake, TimeSpan.FromSeconds(30));
        await using var rows = await session.QueryAsync(Sql, Array.Empty<OraBind>(), CancellationToken.None);
        using var stop = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => rows.ReadAsync(stop.Token));
    }
}
