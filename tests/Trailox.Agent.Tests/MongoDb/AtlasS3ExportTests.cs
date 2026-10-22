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
        Assert.True(await export.AnyNamedSinceAsync(Folder, Micros(midnight.AddMinutes(20)), Micros(midnight.AddMinutes(40)), CancellationToken.None));
        Assert.False(await export.AnyNamedSinceAsync(Folder, Micros(midnight.AddMinutes(31)), Micros(midnight.AddMinutes(40)), CancellationToken.None));
    }

    /// <summary>
    /// ⚠ A date folder need not be the name's date (an object named 00:00:30 can be filed under the day before), so the
    ///   day before a window is listed too. Measured layouts never showed it; listing one more prefix costs one call.
    /// </summary>
    [Fact]
    public async Task An_object_filed_under_the_day_before_its_name_is_found()
    {
        var s3 = new FakeS3();
        var named = new DateTime(2026, 10, 20, 0, 0, 30, DateTimeKind.Utc);
        s3.Objects[Key(named, folder: Folder + "2026-10-19/")] = [];
        var export = new AtlasS3Export(s3, "atlas-logs", "Cluster0");

        var keys = await export.NamedBetweenAsync(Folder, Micros(named.AddSeconds(-30)), Micros(named.AddMinutes(10)), CancellationToken.None);

        Assert.Equal([Key(named, folder: Folder + "2026-10-19/")], keys);
    }

    /// <summary>
    /// The integration's path can change at any time: both layouts are listed at every call, and nothing about a folder is
    /// kept. Before, a folder first seen plain was plain for the life of the process, and new daily keys went unread.
    /// </summary>
    [Fact]
    public async Task A_folder_that_changes_layout_is_read_in_both()
    {
        var s3 = new FakeS3();
        var export = new AtlasS3Export(s3, "atlas-logs", "Cluster0");
        s3.Objects[Key(T0)] = [];
        Assert.Equal([Key(T0)], await export.NamedBetweenAsync(Folder, Micros(T0), Micros(T0.AddMinutes(10)), CancellationToken.None));

        s3.Objects[Key(T0.AddMinutes(1), folder: Folder + "2026-10-19/")] = [];

        Assert.Equal([Key(T0), Key(T0.AddMinutes(1), folder: Folder + "2026-10-19/")],
            await export.NamedBetweenAsync(Folder, Micros(T0), Micros(T0.AddMinutes(10)), CancellationToken.None));
    }

    /// <summary>
    /// The folder's own listing from a 2026 stamp also returns the date folders of 2027 (they sort after the 2026 names),
    /// which the day listing returns again: an object is selected once.
    /// </summary>
    [Fact]
    public async Task An_object_two_listings_find_is_read_once()
    {
        var s3 = new FakeS3();
        var named = new DateTime(2027, 1, 1, 0, 1, 0, DateTimeKind.Utc);
        s3.Objects[Key(named, folder: Folder + "2027-01-01/")] = [];
        var export = new AtlasS3Export(s3, "atlas-logs", "Cluster0");

        var keys = await export.NamedBetweenAsync(Folder, Micros(named.AddMinutes(-10)), Micros(named.AddMinutes(5)), CancellationToken.None);

        Assert.Equal([Key(named, folder: Folder + "2027-01-01/")], keys);
    }

    /// <summary>
    /// 🔴 After an export stall, the first object holds the stalled lines under a late name, past the 5-minute margin:
    ///    it is read while its first record is still the window's (before its end plus the 60-second grace), and the
    ///    first object that is not ends the read. Measured stalls never reached the margin; this keeps one from losing lines.
    /// </summary>
    [Fact]
    public async Task Objects_past_the_margin_that_still_hold_the_window_are_read()
    {
        var s3 = new FakeS3();
        var end = T0.AddMinutes(10);
        Put(s3, end.AddMinutes(8), end.AddMinutes(-2));    // the stall: lines of the window, named 8 minutes after its end
        Put(s3, end.AddMinutes(9), end.AddSeconds(40));    // still within the grace: read
        Put(s3, end.AddMinutes(10), end.AddMinutes(3));    // past it: ends the read
        Put(s3, end.AddMinutes(11), end.AddMinutes(-1));   // never reached
        var export = new AtlasS3Export(s3, "atlas-logs", "Cluster0");

        var keys = await export.HoldingWindowAfterAsync(Folder, Micros(end.AddMinutes(5)), Micros(end), audit: false, CancellationToken.None);

        Assert.Equal([Key(end.AddMinutes(8)), Key(end.AddMinutes(9))], keys);
    }

    [Fact]
    public async Task Without_a_stall_one_first_record_ends_the_read_past_the_margin()
    {
        var s3 = new FakeS3();
        var end = T0.AddMinutes(10);
        for (var i = 6; i < 20; i++)
        {
            Put(s3, end.AddMinutes(i), end.AddMinutes(i).AddSeconds(-70));
        }
        var export = new AtlasS3Export(s3, "atlas-logs", "Cluster0");

        Assert.Empty(await export.HoldingWindowAfterAsync(Folder, Micros(end.AddMinutes(5)), Micros(end), audit: false, CancellationToken.None));
        Assert.Equal(1, s3.Heads);
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

        Assert.Equal(new AtlasS3Export.LiveStart(Micros(live), Found: true), start);
        Assert.True(s3.Heads <= 2 * Math.Ceiling(Math.Log2(backfillObjects + 2)) + 2, $"{s3.Heads} first-byte reads");
    }

    /// <summary>
    /// Right after the export is enabled every object is still its first backfill: the newest one read is returned as not
    /// found, so the caller does not keep it (before, it was kept for the life of the process).
    /// </summary>
    [Fact]
    public async Task A_folder_that_is_all_backfill_has_no_live_start_yet()
    {
        var s3 = new FakeS3();
        for (var i = 0; i < 5; i++)
        {
            Put(s3, T0.AddSeconds(20 + i), T0.AddHours(-7));
        }
        var export = new AtlasS3Export(s3, "atlas-logs", "Cluster0");

        Assert.Equal(new AtlasS3Export.LiveStart(Micros(T0.AddSeconds(24)), Found: false),
            await export.LiveStartAsync(Folder, audit: false, CancellationToken.None));
    }

    [Fact]
    public async Task An_empty_object_has_no_first_record_and_is_said_so()
    {
        var s3 = new FakeS3();
        s3.Objects[Key(T0)] = [];

        Assert.Equal((true, (long?)null), await new AtlasS3Export(s3, "atlas-logs", "Cluster0").FirstRecordAsync(Key(T0), audit: false, CancellationToken.None));
    }

    [Fact]
    public async Task An_empty_folder_has_no_live_start()
    {
        Assert.Null(await new AtlasS3Export(new FakeS3(), "atlas-logs", "Cluster0").LiveStartAsync(Folder, audit: false, CancellationToken.None));
    }
}
