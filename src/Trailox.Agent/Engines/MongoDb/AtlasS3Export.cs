using System.Globalization;
using System.Runtime.CompilerServices;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Engines.MongoDb;

/// <summary>The reads the S3 log reader makes on the customer's bucket: list, read, and read the first bytes.</summary>
public interface IS3Objects
{
    /// <summary>The keys under <paramref name="prefix"/> after <paramref name="startAfter"/>, in key order, a page at a time.</summary>
    IAsyncEnumerable<string> KeysAsync(string prefix, string? startAfter, CancellationToken ct);

    /// <summary>The object, streamed; the caller disposes it.</summary>
    Task<Stream> OpenAsync(string key, CancellationToken ct);

    /// <summary>The object's first <paramref name="bytes"/> bytes (fewer when it is shorter; none when it is empty).</summary>
    Task<byte[]> HeadAsync(string key, int bytes, CancellationToken ct);
}

/// <summary>
/// The customer's bucket through the AWS SDK, as the agent's own AWS identity: the SDK's default credential chain
/// (IRSA, EKS Pod Identity, ECS, the instance profile, or AWS_* variables). No AWS secret is in agent.yaml.
/// </summary>
public sealed class AwsS3Objects : IS3Objects
{
    private readonly string _bucket;
    private readonly string _region;
    private readonly Lazy<IAmazonS3> _client;

    public AwsS3Objects(string bucket, string region)
    {
        _bucket = bucket;
        _region = region;
        // Lazy: building the client resolves nothing, but a config error should surface as the probe's, not the constructor's.
        _client = new Lazy<IAmazonS3>(() => new AmazonS3Client(new AmazonS3Config { RegionEndpoint = RegionEndpoint.GetBySystemName(region) }));
    }

    public async IAsyncEnumerable<string> KeysAsync(string prefix, string? startAfter, [EnumeratorCancellation] CancellationToken ct)
    {
        var request = new ListObjectsV2Request { BucketName = _bucket, Prefix = prefix, StartAfter = startAfter };
        while (true)
        {
            var page = await Call(() => _client.Value.ListObjectsV2Async(request, ct), $"list s3://{_bucket}/{prefix}");
            foreach (var o in page.S3Objects ?? new List<S3Object>())
            {
                yield return o.Key;
            }
            if (page.IsTruncated != true || string.IsNullOrEmpty(page.NextContinuationToken))
            {
                yield break;
            }
            request = new ListObjectsV2Request { BucketName = _bucket, Prefix = prefix, ContinuationToken = page.NextContinuationToken };
        }
    }

    public async Task<Stream> OpenAsync(string key, CancellationToken ct)
    {
        var response = await Call(() => _client.Value.GetObjectAsync(new GetObjectRequest { BucketName = _bucket, Key = key }, ct), $"read s3://{_bucket}/{key}");
        return response.ResponseStream;
    }

    public async Task<byte[]> HeadAsync(string key, int bytes, CancellationToken ct)
    {
        GetObjectResponse response;
        try
        {
            response = await _client.Value.GetObjectAsync(
                new GetObjectRequest { BucketName = _bucket, Key = key, ByteRange = new ByteRange(0, bytes - 1) }, ct);
        }
        catch (AmazonS3Exception ex) when (ex.ErrorCode == "InvalidRange")
        {
            // A range of an empty object is unsatisfiable (416): there is nothing to read.
            return Array.Empty<byte>();
        }
        catch (Exception ex) when (Worded(ex, $"read s3://{_bucket}/{key}") is { } worded)
        {
            throw worded;
        }
        using (response)
        {
            using var buffer = new MemoryStream();
            await response.ResponseStream.CopyToAsync(buffer, ct);
            return buffer.ToArray();
        }
    }

    /// <summary>One S3 call, its failure worded as what to fix.</summary>
    private async Task<T> Call<T>(Func<Task<T>> call, string what)
    {
        try
        {
            return await call();
        }
        catch (Exception ex) when (Worded(ex, what) is { } worded)
        {
            throw worded;
        }
    }

    /// <summary>An AWS failure as what to fix; null for anything else, which keeps its own message.</summary>
    private SourceException? Worded(Exception ex, string what) => ex switch
    {
        AmazonS3Exception s3 when s3.ErrorCode is "AccessDenied" or "AllAccessDisabled" || s3.StatusCode == System.Net.HttpStatusCode.Forbidden =>
            new SourceException($"{what}: access denied. The agent's AWS identity needs s3:ListBucket on the bucket for the "
                + "cluster's prefix and s3:GetObject on its objects (and kms:Decrypt when the bucket uses SSE-KMS)"),
        AmazonS3Exception s3 when s3.ErrorCode == "NoSuchBucket" =>
            new SourceException($"{what}: the bucket does not exist (options.s3_bucket)"),
        AmazonS3Exception s3 when s3.ErrorCode is "PermanentRedirect" or "AuthorizationHeaderMalformed" or "IllegalLocationConstraintException"
            || s3.StatusCode == System.Net.HttpStatusCode.MovedPermanently =>
            new SourceException($"{what}: the bucket is not in {_region} (options.s3_region is the bucket's region)"),
        // S3's other answers, and the credential chain's own services (STS for IRSA) refusing the agent's identity.
        AmazonServiceException service => new SourceException($"{what} failed: {service.ErrorCode} {service.Message}".TrimEnd()),
        // Raised on the client side. Only the credential chain's failure gets the credential hint: a client error can be
        // anything else too, and naming credentials for it would send the customer the wrong way.
        AmazonClientException client when client.Message.Contains("credential", StringComparison.OrdinalIgnoreCase) =>
            new SourceException($"{what}: no AWS credentials ({client.Message}). Give the agent an AWS identity that can read "
                + "the bucket: IRSA or EKS Pod Identity on Kubernetes, an instance profile, or AWS_* variables"),
        AmazonClientException client => new SourceException($"{what}: AWS client error ({client.Message})"),
        _ => null,
    };
}

/// <summary>
/// The Atlas S3 log export of one cluster in the customer's bucket: its key layout, and which objects hold a window.
/// </summary>
/// <remarks>
/// <para>Keys: <c>{prefix}/{cluster}/{host}/{dir}/[{yyyy-MM-dd}/]{yyyyMMddTHHmmss.fffZ}-{dir}[.partNNNNN-of-NNNNN].log</c>.
/// The host is the process's internal name (its /processes id without the port), and dir is mongod, mongod_audit,
/// mongos or mongos_audit. The date folder is Atlas's "Daily folders" path choice.</para>
/// <para>🔴 AN OBJECT IS NAMED AFTER ITS NEWEST LINE, AND AT MOST 173.6 s AFTER ITS OLDEST. So a window's
///    lines are in the objects named from its start to its end plus <see cref="SelectAfterEndMicros"/>, and an object
///    named before the start holds none of them. An export that stalls breaks the second half: the first object after
///    the stall holds the stalled lines under a late name, so the objects after that margin are read too while their
///    first line is still the window's (<see cref="HoldingWindowAfterAsync"/>).</para>
/// <para>🔴 EXCEPT THE EXPORT'S FIRST BACKFILL: each node's current log file at the moment the export was enabled, in
///    objects named then, holding hours of older lines. Name-based selection never reads it for the hours it
///    holds (a new S3 source starts at the export's live start, <see cref="LiveStartAsync"/>).</para>
/// <para>⚠ BOTH LAYOUTS ARE LISTED EVERY TIME, and nothing about a folder is cached: the folder itself (the plain layout)
///   and each date folder from the day before a range to its last day (the daily one, whose folder date need not be
///   the name's). A customer can change the integration's path at any time, and a date folder never listed is a day
///   lost.</para>
/// </remarks>
public sealed class AtlasS3Export
{
    /// <summary>How long after a window's end its last object can be named: the worst measured was 173.6 s.</summary>
    public const long SelectAfterEndMicros = 5 * 60_000_000L;

    /// <summary>An object named this long after its first line is the export's first backfill; live ones: at most 174 s.</summary>
    internal const long BackfillAfterMicros = 10 * 60_000_000L;

    /// <summary>The bytes read from an object to find its first record, then more when that record is longer.</summary>
    internal const int HeadBytes = 64 * 1024;

    internal const int LongHeadBytes = 1024 * 1024;

    /// <summary>The most keys the live-start search walks before it settles for the last one it checked.</summary>
    internal const int LiveStartMaxKeys = 100_000;

    /// <summary>The most objects past the margin one window reads for one folder after a stall.</summary>
    internal const int MaxStalledObjects = 1_000;

    private const string ShardMongos = "SHARD_MONGOS";

    private readonly IS3Objects _objects;
    private readonly string _root;

    /// <param name="prefix">The integration's prefix as typed in Atlas, without slashes; empty when it has none.</param>
    public AtlasS3Export(IS3Objects objects, string prefix, string cluster)
    {
        _objects = objects;
        _root = (prefix.Length > 0 ? prefix + "/" : "") + cluster + "/";
    }

    public IS3Objects Objects => _objects;

    /// <summary>"s3://bucket/" is the caller's; this is the cluster's folder in it, for messages.</summary>
    public string Root => _root;

    public static string ServerDir(AtlasProcess p) => p.Type == ShardMongos ? "mongos" : "mongod";

    public static string AuditDir(AtlasProcess p) => p.Type == ShardMongos ? "mongos_audit" : "mongod_audit";

    /// <summary>A process's folder for one log type: its host is the internal name, its /processes id without the port.</summary>
    public string Folder(AtlasProcess p, string dir)
    {
        var colon = p.Id.LastIndexOf(':');
        var host = colon > 0 ? p.Id[..colon] : p.Id;
        if (host.Length == 0)
        {
            throw new SourceException($"Atlas gives process {p.Host} no internal host name, which names its folder in the S3 export");
        }
        return _root + host + "/" + dir + "/";
    }

    /// <summary>The time in an object's name, in Unix microseconds.</summary>
    public static bool TryNameMicros(string key, out long micros)
    {
        micros = 0;
        var name = key.AsSpan(key.LastIndexOf('/') + 1);
        var dash = name.IndexOf('-');
        if (dash < 0 || !DateTime.TryParseExact(name[..dash], "yyyyMMdd'T'HHmmss.fff'Z'", CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var t))
        {
            return false;
        }
        micros = (t - DateTime.UnixEpoch).Ticks / 10;
        return true;
    }

    /// <summary>An object name's time stamp, which keys sort by: every key at or after it is named at or after the time.</summary>
    public static string Stamp(long micros) =>
        DateTime.UnixEpoch.AddTicks(micros * 10).ToString("yyyyMMdd'T'HHmmss.fff'Z'", CultureInfo.InvariantCulture);

    /// <summary>The folder's objects named from <paramref name="fromMicros"/> to <paramref name="toMicros"/>, in name order.</summary>
    public async Task<List<string>> NamedBetweenAsync(string folder, long fromMicros, long toMicros, CancellationToken ct)
    {
        var found = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var prefix in Prefixes(folder, fromMicros, toMicros))
        {
            await foreach (var key in _objects.KeysAsync(prefix, prefix + Stamp(fromMicros), ct))
            {
                if (!TryNameMicros(key, out var t))
                {
                    continue;
                }
                if (t > toMicros)
                {
                    break;
                }
                if (t >= fromMicros)
                {
                    found[key] = t;
                }
            }
        }
        return InNameOrder(found);
    }

    /// <summary>
    /// After a stall: the folder's objects named after <paramref name="afterMicros"/> whose first record is still before
    /// the window's end plus the reader's grace, in name order. Each prefix is read until its first object that is not.
    /// </summary>
    /// <remarks>
    /// An object's records are out of order by up to 38 s, so a first record past the end plus the 60-second grace means
    /// none of its records is the window's. Usually the first object past the margin already shows it: one read of its
    /// first bytes per folder and window. An empty object is passed over; one whose first record has no time is read.
    /// </remarks>
    public async Task<List<string>> HoldingWindowAfterAsync(string folder, long afterMicros, long windowEndMicros, bool audit, CancellationToken ct)
    {
        var found = new Dictionary<string, long>(StringComparer.Ordinal);
        var until = windowEndMicros + MinuteAggregator.S3GraceMicros;
        foreach (var prefix in Prefixes(folder, afterMicros, afterMicros + DayMicros))
        {
            var taken = 0;
            await foreach (var key in _objects.KeysAsync(prefix, prefix + Stamp(afterMicros), ct))
            {
                if (!TryNameMicros(key, out var t) || t <= afterMicros)
                {
                    continue;
                }
                var (empty, first) = await FirstRecordAsync(key, audit, ct);
                if (empty)
                {
                    continue;
                }
                if (first is { } f && f >= until)
                {
                    break;
                }
                found[key] = t;
                if (++taken >= MaxStalledObjects)
                {
                    break;
                }
            }
        }
        return InNameOrder(found);
    }

    /// <summary>Whether the folder holds an object named from <paramref name="micros"/> to <paramref name="nowMicros"/> and beyond.</summary>
    public async Task<bool> AnyNamedSinceAsync(string folder, long micros, long nowMicros, CancellationToken ct)
    {
        foreach (var prefix in Prefixes(folder, micros, nowMicros + DayMicros))
        {
            await foreach (var key in _objects.KeysAsync(prefix, prefix + Stamp(micros), ct))
            {
                if (TryNameMicros(key, out var t) && t >= micros)
                {
                    return true;
                }
            }
        }
        return false;
    }

    /// <summary>The folder's newest object named at or after <paramref name="micros"/>, or null.</summary>
    public async Task<string?> NewestSinceAsync(string folder, long micros, long nowMicros, CancellationToken ct)
    {
        var found = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var prefix in Prefixes(folder, micros, nowMicros + DayMicros))
        {
            await foreach (var key in _objects.KeysAsync(prefix, prefix + Stamp(micros), ct))
            {
                if (TryNameMicros(key, out var t) && t >= micros)
                {
                    found[key] = t;
                }
            }
        }
        var ordered = InNameOrder(found);
        return ordered.Count == 0 ? null : ordered[^1];
    }

    /// <summary>Whether the folder holds any object at all.</summary>
    public async Task<bool> AnyAsync(string folder, CancellationToken ct)
    {
        await foreach (var _ in _objects.KeysAsync(folder, null, ct))
        {
            return true;
        }
        return false;
    }

    /// <summary>Where the export's live objects start in one folder, and whether a live object was found at all.</summary>
    public readonly record struct LiveStart(long Micros, bool Found);

    /// <summary>
    /// Where the export's live objects start in this folder; null when it holds none. The first object named less
    /// than <see cref="BackfillAfterMicros"/> after its first line. Not found (every object read is still the first
    /// backfill): the newest one read, which the caller must not keep.
    /// </summary>
    /// <remarks>
    /// The backfill objects come first in name order and the live ones after, so the walk reads the first bytes of
    /// the keys at 0, 1, 3, 7, 15... as it lists them, stops listing at the first live one, then halves back between it
    /// and the last backfill one: a few dozen small reads, and a listing only as long as the backfill. A folder's keys
    /// are in name order in either layout: date folders sort by date.
    /// </remarks>
    public async Task<LiveStart?> LiveStartAsync(string folder, bool audit, CancellationToken ct)
    {
        var keys = new List<string>();
        int low = -1, high = -1, next = 0;
        await foreach (var key in _objects.KeysAsync(folder, null, ct))
        {
            if (!TryNameMicros(key, out _))
            {
                continue;
            }
            keys.Add(key);
            if (keys.Count - 1 == next)
            {
                if (await IsLiveAsync(key, audit, ct))
                {
                    high = next;
                    break;
                }
                low = next;
                next = next * 2 + 1;
            }
            if (keys.Count >= LiveStartMaxKeys)
            {
                break;
            }
        }
        if (keys.Count == 0)
        {
            return null;
        }
        if (high < 0)
        {
            // No live key at a checked place before the listing ended: check the last one.
            if (low == keys.Count - 1 || !await IsLiveAsync(keys[^1], audit, ct))
            {
                return new LiveStart(NameMicros(keys[^1]), Found: false);
            }
            high = keys.Count - 1;
        }
        // keys[low] is backfill (low = -1: none checked) and keys[high] live; the first live key is in between.
        while (high - low > 1)
        {
            var mid = low + (high - low) / 2;
            if (await IsLiveAsync(keys[mid], audit, ct))
            {
                high = mid;
            }
            else
            {
                low = mid;
            }
        }
        return new LiveStart(NameMicros(keys[high]), Found: true);
    }

    /// <summary>
    /// An object's first record: whether the object is empty, and the record's own time (null when the first bytes hold
    /// no record with a time). The first 64 KiB, then 1 MiB when the first record is longer.
    /// </summary>
    public async Task<(bool Empty, long? Micros)> FirstRecordAsync(string key, bool audit, CancellationToken ct)
    {
        foreach (var bytes in new[] { HeadBytes, LongHeadBytes })
        {
            var head = await _objects.HeadAsync(key, bytes, ct);
            if (head.Length == 0)
            {
                return (true, null);
            }
            var newline = Array.IndexOf(head, (byte)'\n');
            if (newline < 0 && head.Length == bytes)
            {
                continue;
            }
            var record = newline < 0 ? head.AsSpan() : head.AsSpan(0, newline);
            var inner = new S3Envelope().Inner(record);
            return (false, LineScan.TryTimestampAt(inner, LineScan.TopLevelValue(inner, audit ? "ts"u8 : "t"u8), out var micros) ? micros : null);
        }
        return (false, null);
    }

    private async Task<bool> IsLiveAsync(string key, bool audit, CancellationToken ct)
    {
        // A first record without a time is read as live: at worst a backfill object's old lines are read and dropped.
        var (_, first) = await FirstRecordAsync(key, audit, ct);
        return first is not { } t || NameMicros(key) - t < BackfillAfterMicros;
    }

    private static long NameMicros(string key) => TryNameMicros(key, out var t) ? t : 0;

    private const long DayMicros = 86_400_000_000L;

    /// <summary>
    /// Where a time range is listed: the folder itself (the plain layout), and each date folder from the day before
    /// <paramref name="fromMicros"/> to the day of <paramref name="toMicros"/> (the daily layout). A listing of the
    /// folder itself with StartAfter a name's stamp skips the date folders of the same year ('-' sorts before a digit),
    /// and the caller merges what more than one listing finds.
    /// </summary>
    private static IEnumerable<string> Prefixes(string folder, long fromMicros, long toMicros)
    {
        yield return folder;
        var day = DateTime.UnixEpoch.AddTicks(fromMicros * 10).Date.AddDays(-1);
        var last = DateTime.UnixEpoch.AddTicks(toMicros * 10).Date;
        for (; day <= last; day = day.AddDays(1))
        {
            yield return folder + day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "/";
        }
    }

    /// <summary>Keys by their name's time, then the key: parts of one batch are named alike.</summary>
    private static List<string> InNameOrder(Dictionary<string, long> found) =>
        found.OrderBy(k => k.Value).ThenBy(k => k.Key, StringComparer.Ordinal).Select(k => k.Key).ToList();
}
