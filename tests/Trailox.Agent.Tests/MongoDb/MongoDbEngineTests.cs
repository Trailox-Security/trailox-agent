using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.MongoDb;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Tests.MongoDb;

/// <summary>agent.yaml for engine mongodb: the Atlas project, the cluster and the service account, and nothing else.</summary>
public class MongoDbEngineTests
{
    private static EndpointConfig Endpoint() => new()
    {
        Alias = "atlas", Engine = "mongodb", Host = "0123456789ABCDEF01234567", ClusterName = "Cluster0",
        Username = "mdb_sa_id_0123456789abcdef01234567",
    };

    [Fact]
    public void Registry_knows_mongodb()
    {
        Assert.Equal("mongodb", EngineRegistry.Require("MongoDB").Name);
    }

    [Fact]
    public void A_project_cluster_and_client_id_are_valid_and_https_is_pinned()
    {
        var e = Endpoint();
        e.Port = 27017;
        e.Tls = false;

        Assert.Empty(new MongoDbEngine().Validate(e));
        Assert.Equal("0123456789abcdef01234567", e.Host);
        Assert.Equal(443, e.Port);
        Assert.True(e.Tls);
        Assert.Equal("cloud", e.Kind);
    }

    [Theory]
    [InlineData("cluster0.abcde.mongodb.net", "Cluster0", "host: the Atlas project id")]
    [InlineData("0123456789abcdef01234567", "default", "clusterName")]
    [InlineData("0123456789abcdef01234567", "Cluster 0", "clusterName")]
    public void A_wrong_coordinate_says_what_it_should_be(string host, string cluster, string expected)
    {
        var e = Endpoint();
        e.Host = host;
        e.ClusterName = cluster;

        Assert.Contains(new MongoDbEngine().Validate(e), p => p.StartsWith(expected, StringComparison.Ordinal));
    }

    [Fact]
    public void Options_take_no_secret_and_no_unknown_key()
    {
        var e = Endpoint();
        e.Options["secret"] = "not-a-real-secret";

        var problems = new MongoDbEngine().Validate(e);

        var problem = Assert.Single(problems);
        Assert.StartsWith("options.secret", problem, StringComparison.Ordinal);
        Assert.DoesNotContain("not-a-real-secret", problem, StringComparison.Ordinal);
    }

    [Fact]
    public void The_S3_log_source_is_refused_until_an_agent_reads_it()
    {
        var e = Endpoint();
        e.Options["log_source"] = "s3";

        Assert.Contains(new MongoDbEngine().Validate(e), p => p.Contains("s3 is not supported by this agent version yet", StringComparison.Ordinal));
    }

    [Fact]
    public void The_heartbeat_touches_the_liveness_file_at_most_every_30_seconds()
    {
        var path = Path.Combine(Path.GetTempPath(), $"trailox-heartbeat-{Guid.NewGuid():N}");
        try
        {
            TaskHeartbeat.Attach(new HealthFile(path));
            TaskHeartbeat.Beat();
            Assert.True(File.Exists(path));
            var first = File.ReadAllText(path);
            File.Delete(path);
            TaskHeartbeat.Beat();
            Assert.False(File.Exists(path));
            Assert.NotEmpty(first);
        }
        finally
        {
            TaskHeartbeat.Attach(null);
            File.Delete(path);
        }
    }
}
