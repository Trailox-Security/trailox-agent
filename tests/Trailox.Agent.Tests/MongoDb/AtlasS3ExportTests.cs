using Trailox.Agent.Engines.MongoDb;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests.MongoDb;

/// <summary>
/// The Atlas S3 export's layout in the customer's bucket, as measured on Cluster1 (Phase 0 facts §18): which objects
/// hold a window, the "Daily folders" path, and where the live objects start after the first backfill.
/// </summary>
public class AtlasS3ExportTests
{
    private static readonly DateTime T0 = new(2026, 10, 19, 13, 0, 0, DateTimeKind.Utc);
    private static readonly AtlasProcess Node = new() { Host = "cluster0-shard-00-00.example.mongodb.net", Type = "REPLICA_PRIMARY", Id = "atlas-abcdef-shard-00-00.example.mongodb.net:27017" };
    private const string Folder = "atlas-logs/Cluster0/atlas-abcdef-shard-00-00.example.mongodb.net/mongod/";

    private static long Micros(DateTime utc) => (utc - DateTime.UnixEpoch).Ticks / 10;

    private static string Key(DateTime named, string dir = "mongod", string folder = Folder, string part = "") =>
        $"{folder}{named:yyyyMMdd'T'HHmmss.fff'Z'}-{dir}{part}.log";

    private static string ServerLine(DateTime t) =>
        """{"t":{"$date":"@0@+00:00"},"s":"I","c":"NETWORK","id":6723804,"ctx":"conn1","msg":"m","attr":{}}""".Replace("@0@", t.ToString("yyyy-MM-dd'T'HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture));

    /// <summary>An object named <paramref name="named"/> whose first line is <paramref name="firstLine"/>.</summary>
    private static void Put(FakeS3 s3, DateTime named, DateTime firstLine, string folder = Folder) =>
        s3.Put(Key(named, folder: folder), [S3Form.Record(ServerLine(firstLine), new Random(1)), S3Form.Record(ServerLine(named.AddSeconds(-5)), new Random(2))]);

    [Fact]
    public void A_process_folder_is_named_by_its_internal_host_and_log_type()
    {
        var export = new AtlasS3Export(new FakeS3(), "atlas-logs", "Cluster0");
        var router = new AtlasProcess { Host = "cluster0-shard-00-00.example.mongodb.net", Type = "SHARD_MONGOS", Id = "atlas-abcdef-shard-00-00.example.mongodb.net:27016" };

        Assert.Equal(Folder, export.Folder(Node, AtlasS3Export.ServerDir(Node)));
        Assert.Equal("atlas-logs/Cluster0/atlas-abcdef-shard-00-00.example.mongodb.net/mongod_audit/", export.Folder(Node, AtlasS3Export.AuditDir(Node)));
        Assert.Equal("atlas-logs/Cluster0/atlas-abcdef-shard-00-00.example.mongodb.net/mongos_audit/", export.Folder(router, AtlasS3Export.AuditDir(router)));
        Assert.Equal("Cluster0/atlas-abcdef-shard-00-00.example.mongodb.net/mongod/", new AtlasS3Export(new FakeS3(), "", "Cluster0").Folder(Node, "mongod"));
    }

    [Theory]
    [InlineData("a/b/20261019T132813.090Z-mongod.log", "2026-10-19T13:28:13.090Z")]
    [InlineData("a/b/2026-10-19/20261019T132813.090Z-mongod_audit.part00001-of-00002.log", "2026-10-19T13:28:13.090Z")]
    public void An_objects_name_carries_its_time(string key, string expected)
    {
        Assert.True(AtlasS3Export.TryNameMicros(key, out var micros));
        Assert.Equal(Micros(DateTime.Parse(expected, null, System.Globalization.DateTimeStyles.AdjustToUniversal)), micros);
        Assert.False(AtlasS3Export.TryNameMicros("atlas-logs/atlas-test-20261019T130657.802Z", out _));
    }

    /// <summary>
    /// 🔴 A window's lines are in the objects named from its start to its end plus 5 minutes: an object is named after its
    ///    newest line, and at most 173.6 s after its oldest (§20.2). Parts of one batch are named alike.
    /// </summary>
    [Fact]
    public async Task A_window_selects_the_objects_named_from_its_start_to_five_minutes_past_its_end()
    {
        var s3 = new FakeS3 { PageSize = 2 };
        foreach (var minutes in new[] { -1, 0, 3, 9, 14, 15, 16 })
        {
            s3.Objects[Key(T0.AddMinutes(10).AddMinutes(minutes))] = [];
        }
        s3.Objects[Key(T0.AddMinutes(12), "mongod", Folder, ".part00001-of-00002")] = [];
        s3.Objects[Key(T0.AddMinutes(12), "mongod", Folder, ".part00002-of-00002")] = [];
        var export = new AtlasS3Export(s3, "atlas-logs", "Cluster0");

        var keys = await export.NamedBetweenAsync(Folder, Micros(T0.AddMinutes(10)), Micros(T0.AddMinutes(20)) + AtlasS3Export.SelectAfterEndMicros, CancellationToken.None);

        Assert.Equal(new[] { 0, 3, 9 }.Select(m => Key(T0.AddMinutes(10 + m)))
            .Concat([Key(T0.AddMinutes(12), "mongod", Folder, ".part00001-of-00002"), Key(T0.AddMinutes(12), "mongod", Folder, ".part00002-of-00002")])
            .Append(Key(T0.AddMinutes(24))).Append(Key(T0.AddMinutes(25))).Order(StringComparer.Ordinal), keys);
    }

    /// <summary>Atlas's "Daily folders" path choice puts a date folder before the file name (Phase 0 facts §18.1).</summary>
    [Fact]
    public async Task The_daily_folders_layout_is_read_through_the_dates_a_window_touches()
    {
        var s3 = new FakeS3();
        var midnight = new DateTime(2026, 10, 20, 0, 0, 0, DateTimeKind.Utc);
        foreach (var named in new[] { midnight.AddMinutes(-20), midnight.AddMinutes(-3), midnight.AddMinutes(2), midnight.AddMinutes(30) })
        {
            s3.Objects[Key(named, folder: Folder + named.ToString("yyyy-MM-dd'/'", System.Globalization.CultureInfo.InvariantCulture))] = [];
        }
        var export = new AtlasS3Export(s3, "atlas-logs", "Cluster0");

        var keys = await export.NamedBetweenAsync(Folder, Micros(midnight.AddMinutes(-5)), Micros(midnight.AddMinutes(10)), CancellationToken.None);

        Assert.Equal([Folder + "2026-10-19/" + Key(midnight.AddMinutes(-3), folder: ""), Folder + "2026-10-20/" + Key(midnight.AddMinutes(2), folder: "")], keys);
        Assert.True(await export.AnyNamedSinceAsync(Folder, Micros(midnight.AddMinutes(20)), CancellationToken.None));
        Assert.False(await export.AnyNamedSinceAsync(Folder, Micros(midnight.AddMinutes(31)), CancellationToken.None));
    }

    /// <summary>
    /// S1: the export's first backfill, objects named at its start that hold hours of older lines, comes first; the live
    /// start is the first object named less than 10 minutes after its first line. Found in a few reads of first bytes.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(300)]
    public async Task The_live_start_is_the_first_object_after_the_backfill(int backfillObjects)
    {
        var s3 = new FakeS3 { PageSize = 50 };
        for (var i = 0; i < backfillObjects; i++)
        {
            // Named at the export's start, holding lines from hours before it.
            Put(s3, T0.AddSeconds(20 + i), T0.AddHours(-7).AddMinutes(i % 400));
        }
        var live = T0.AddMinutes(30);
        for (var i = 0; i < 400; i++)
        {
            Put(s3, live.AddMinutes(i), live.AddMinutes(i).AddSeconds(-70));
        }
        var export = new AtlasS3Export(s3, "atlas-logs", "Cluster0");

        var start = await export.LiveStartAsync(Folder, audit: false, CancellationToken.None);

        Assert.Equal(Micros(live), start);
        Assert.True(s3.Heads <= 2 * Math.Ceiling(Math.Log2(backfillObjects + 2)) + 2, $"{s3.Heads} first-byte reads");
    }

    [Fact]
    public async Task An_empty_folder_has_no_live_start()
    {
        Assert.Null(await new AtlasS3Export(new FakeS3(), "atlas-logs", "Cluster0").LiveStartAsync(Folder, audit: false, CancellationToken.None));
    }
}
