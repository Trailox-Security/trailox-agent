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

    /// <summary>The "folders" one level under <paramref name="prefix"/> (delimiter '/'), from the first page.</summary>
    Task<IReadOnlyList<string>> FoldersAsync(string prefix, CancellationToken ct);

    /// <summary>The object, streamed; the caller disposes it.</summary>
    Task<Stream> OpenAsync(string key, CancellationToken ct);

    /// <summary>The object's first <paramref name="bytes"/> bytes (fewer when it is shorter).</summary>
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

    public async Task<IReadOnlyList<string>> FoldersAsync(string prefix, CancellationToken ct)
    {
        var page = await Call(() => _client.Value.ListObjectsV2Async(
            new ListObjectsV2Request { BucketName = _bucket, Prefix = prefix, Delimiter = "/" }, ct), $"list s3://{_bucket}/{prefix}");
        return page.CommonPrefixes ?? new List<string>();
    }

    public async Task<Stream> OpenAsync(string key, CancellationToken ct)
    {
        var response = await Call(() => _client.Value.GetObjectAsync(new GetObjectRequest { BucketName = _bucket, Key = key }, ct), $"read s3://{_bucket}/{key}");
        return response.ResponseStream;
    }

    public async Task<byte[]> HeadAsync(string key, int bytes, CancellationToken ct)
    {
        using var response = await Call(() => _client.Value.GetObjectAsync(
            new GetObjectRequest { BucketName = _bucket, Key = key, ByteRange = new ByteRange(0, bytes - 1) }, ct), $"read s3://{_bucket}/{key}");
        using var buffer = new MemoryStream();
        await response.ResponseStream.CopyToAsync(buffer, ct);
        return buffer.ToArray();
    }

    /// <summary>One S3 call, its failure worded as what to fix.</summary>
    private async Task<T> Call<T>(Func<Task<T>> call, string what)
    {
        try
        {
            return await call();
        }
        catch (AmazonS3Exception ex) when (ex.ErrorCode is "AccessDenied" or "AllAccessDisabled" || ex.StatusCode == System.Net.HttpStatusCode.Forbidden)
        {
            throw new SourceException($"{what}: access denied. The agent's AWS identity needs s3:ListBucket on the bucket for the "
                + "cluster's prefix and s3:GetObject on its objects (and kms:Decrypt when the bucket uses SSE-KMS)");
        }
        catch (AmazonS3Exception ex) when (ex.ErrorCode == "NoSuchBucket")
        {
            throw new SourceException($"{what}: the bucket does not exist (options.s3_bucket)");
        }
        catch (AmazonS3Exception ex) when (ex.ErrorCode is "PermanentRedirect" or "AuthorizationHeaderMalformed" or "IllegalLocationConstraintException"
            || ex.StatusCode == System.Net.HttpStatusCode.MovedPermanently)
        {
            throw new SourceException($"{what}: the bucket is not in {_region} (options.s3_region is the bucket's region)");
        }
        catch (AmazonServiceException ex)
        {
            // S3's other answers, and the credential chain's own services (STS for IRSA) refusing the agent's identity.
            throw new SourceException($"{what} failed: {ex.ErrorCode} {ex.Message}".TrimEnd());
        }
        catch (AmazonClientException ex)
        {
            // Raised before any request: no credential source in the chain answered.
            throw new SourceException($"{what}: no AWS credentials ({ex.Message}). Give the agent an AWS identity that can read "
                + "the bucket: IRSA or EKS Pod Identity on Kubernetes, an instance profile, or AWS_* variables");
        }
    }
}

/// <summary>
/// The Atlas S3 log export of one cluster in the customer's bucket: its key layout, and which objects hold a window
/// (PLAN_S3.md §4.1; Phase 0 facts §18).
/// </summary>
/// <remarks>
/// <para>Keys: <c>{prefix}/{cluster}/{host}/{dir}/[{yyyy-MM-dd}/]{yyyyMMddTHHmmss.fffZ}-{dir}[.partNNNNN-of-NNNNN].log</c>.
/// The host is the process's internal name (its /processes id without the port), and dir is mongod, mongod_audit,
/// mongos or mongos_audit. The date folder is Atlas's "Daily folders" path choice.</para>
/// <para>🔴 AN OBJECT IS NAMED AFTER ITS NEWEST LINE, AND AT MOST 173.6 s AFTER ITS OLDEST (§18.4, §20.2). So a window's
///    lines are all in the objects named from its start to its end plus <see cref="SelectAfterEndMicros"/>, and an
///    object named before the start holds none of them.</para>
/// <para>🔴 EXCEPT THE EXPORT'S FIRST BACKFILL: each node's current log file at the moment the export was enabled, in
///    objects named then, holding hours of older lines (§18.3). Name-based selection never reads it for the hours it
///    holds (S1: a new S3 source starts at the export's live start, <see cref="LiveStartAsync"/>).</para>
/// </remarks>
public sealed class AtlasS3Export
{
    /// <summary>How long after a window's end its last object can be named: the worst measured was 173.6 s (§20.2).</summary>
    public const long SelectAfterEndMicros = 5 * 60_000_000L;

    /// <summary>An object named this long after its first line is the export's first backfill; live ones: at most 174 s.</summary>
    internal const long BackfillAfterMicros = 10 * 60_000_000L;

    /// <summary>The bytes read from an object to find its first record.</summary>
    internal const int HeadBytes = 64 * 1024;

    /// <summary>The most keys the live-start search walks before it settles for the last one it checked.</summary>
    internal const int LiveStartMaxKeys = 100_000;

    private const string ShardMongos = "SHARD_MONGOS";

    private readonly IS3Objects _objects;
    private readonly string _root;
    /// <summary>Each folder's layout once known: true for date folders. A folder seen empty is asked again.</summary>
    private readonly Dictionary<string, bool> _daily = new(StringComparer.Ordinal);

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
        var keys = new List<string>();
        foreach (var prefix in await DayPrefixesAsync(folder, fromMicros, toMicros, ct))
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
                    keys.Add(key);
                }
            }
        }
        return keys;
    }

    /// <summary>Whether the folder holds an object named at or after <paramref name="micros"/>.</summary>
    public async Task<bool> AnyNamedSinceAsync(string folder, long micros, CancellationToken ct)
    {
        foreach (var prefix in await DayPrefixesAsync(folder, micros, long.MaxValue, ct))
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
    public async Task<string?> NewestSinceAsync(string folder, long micros, CancellationToken ct)
    {
        string? newest = null;
        foreach (var prefix in await DayPrefixesAsync(folder, micros, long.MaxValue, ct))
        {
            await foreach (var key in _objects.KeysAsync(prefix, prefix + Stamp(micros), ct))
            {
                if (TryNameMicros(key, out var t) && t >= micros)
                {
                    newest = key;
                }
            }
        }
        return newest;
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

    /// <summary>
    /// S1: where the export's live objects start in this folder, in Unix microseconds; null when it holds none.
    /// The first object named less than <see cref="BackfillAfterMicros"/> after its first line.
    /// </summary>
    /// <remarks>
    /// The backfill objects come first in name order and the live ones after, so the walk reads the first bytes of
    /// the keys at 0, 1, 3, 7, 15... as it lists them, stops listing at the first live one, then halves back between it
    /// and the last backfill one: a few dozen small reads, and a listing only as long as the backfill.
    /// </remarks>
    public async Task<long?> LiveStartAsync(string folder, bool audit, CancellationToken ct)
    {
        var keys = new List<string>();
        int low = -1, high = -1, next = 0;
        await foreach (var key in AllKeysAsync(folder, ct))
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
            // No live key at a checked place before the listing ended: check the last one, or settle for it at the cap.
            if (low == keys.Count - 1 || !await IsLiveAsync(keys[^1], audit, ct))
            {
                return NameMicros(keys[^1]);
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
        return NameMicros(keys[high]);
    }

    /// <summary>The time of an object's first record, from its first bytes; null when they hold no record with a time.</summary>
    public async Task<long?> FirstRecordMicrosAsync(string key, bool audit, CancellationToken ct)
    {
        var head = await _objects.HeadAsync(key, HeadBytes, ct);
        var newline = Array.IndexOf(head, (byte)'\n');
        var record = newline < 0 ? head.AsSpan() : head.AsSpan(0, newline);
        var inner = new S3Envelope().Inner(record);
        return LineScan.TryTimestampAt(inner, LineScan.TopLevelValue(inner, audit ? "ts"u8 : "t"u8), out var micros) ? micros : null;
    }

    private async Task<bool> IsLiveAsync(string key, bool audit, CancellationToken ct)
    {
        // A first record without a time is read as live: at worst a backfill object's old lines are read and dropped.
        var first = await FirstRecordMicrosAsync(key, audit, ct);
        return first is not { } t || NameMicros(key) - t < BackfillAfterMicros;
    }

    private static long NameMicros(string key) => TryNameMicros(key, out var t) ? t : 0;

    /// <summary>Every key of the folder in name order, through its date folders when it has them.</summary>
    private async IAsyncEnumerable<string> AllKeysAsync(string folder, [EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var prefix in await DayPrefixesAsync(folder, long.MinValue, long.MaxValue, ct))
        {
            await foreach (var key in _objects.KeysAsync(prefix, null, ct))
            {
                yield return key;
            }
        }
    }

    /// <summary>
    /// Where to list for a time range: the folder itself, or with Atlas's "Daily folders" path its date folders that the
    /// range touches, oldest first.
    /// </summary>
    private async Task<IReadOnlyList<string>> DayPrefixesAsync(string folder, long fromMicros, long toMicros, CancellationToken ct)
    {
        if (_daily.TryGetValue(folder, out var known) && !known)
        {
            return new[] { folder };
        }
        // The date folders are listed every time: a new one appears each day.
        var days = (await _objects.FoldersAsync(folder, ct))
            .Where(p => p.Length == folder.Length + 11 && DateTime.TryParseExact(p.AsSpan(folder.Length, 10), "yyyy-MM-dd",
                CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
            .Order(StringComparer.Ordinal)
            .ToList();
        if (days.Count == 0)
        {
            // The plain layout, once the folder has an object to show it; until then nothing is cached.
            if (await AnyAsync(folder, ct))
            {
                _daily[folder] = false;
            }
            return new[] { folder };
        }
        _daily[folder] = true;
        var first = fromMicros == long.MinValue ? "" : Day(fromMicros);
        var last = toMicros == long.MaxValue ? "9999-12-31" : Day(toMicros);
        return days.Where(p => string.CompareOrdinal(p.Substring(folder.Length, 10), first) >= 0
            && string.CompareOrdinal(p.Substring(folder.Length, 10), last) <= 0).ToList();
    }

    private static string Day(long micros) =>
        DateTime.UnixEpoch.AddTicks(micros * 10).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
}
