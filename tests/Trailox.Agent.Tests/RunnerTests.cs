using Trailox.Agent.Runner;

namespace Trailox.Agent.Tests;

public class RunnerTests
{
    [Theory]
    [InlineData("1.0.3", "1.0.0", true)]
    [InlineData("1.0.0", "1.0.0", true)]
    [InlineData("0.9.9", "1.0.0", false)]
    [InlineData("1.2.0-rc.1", "1.1.0", true)]
    [InlineData("garbage", "1.0.0", true)]      // unparsable is never fatal
    [InlineData("1.0.0", "", true)]
    public void Version_gate(string agent, string minimum, bool ok)
    {
        Assert.Equal(ok, AgentLoop.VersionOk(agent, minimum));
    }

    [Fact]
    public void Backoff_doubles_to_a_cap_with_jitter_and_honours_retry_after()
    {
        var b = new Backoff();
        var first = b.Next();
        Assert.InRange(first.TotalSeconds, 5, 6.3);
        var second = b.Next();
        Assert.InRange(second.TotalSeconds, 10, 12.6);
        for (var i = 0; i < 10; i++) b.Next();
        Assert.InRange(b.Next().TotalSeconds, 300, 375);

        b.Reset();
        Assert.InRange(b.Next(TimeSpan.FromSeconds(42)).TotalSeconds, 42, 52.6);
    }

    [Fact]
    public void Health_file_reports_fresh_touches_only()
    {
        var path = Path.Combine(Path.GetTempPath(), "trailox-agent-test-" + Guid.NewGuid().ToString("N"));
        var health = new HealthFile(path);
        try
        {
            Assert.False(health.IsHealthy());
            health.Touch();
            Assert.True(health.IsHealthy());
            File.WriteAllText(path, DateTime.UtcNow.AddMinutes(-10).ToString("O"));
            Assert.False(health.IsHealthy());
            File.WriteAllText(path, "not a date");
            Assert.False(health.IsHealthy());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Error_ring_keeps_the_newest_twenty()
    {
        var ring = new ErrorRing(3);
        ring.Add("a", "1");
        ring.Add("a", "2");
        ring.Add("a", "3");
        ring.Add("a", "4");
        Assert.Equal(new[] { "2", "3", "4" }, ring.Snapshot().Select(e => e.Message));
    }
}
