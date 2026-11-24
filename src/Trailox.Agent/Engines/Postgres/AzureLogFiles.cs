using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Azure.Core;
using Azure.Identity;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>One server log file of an Azure Database for PostgreSQL flexible server, as its list gives it.</summary>
/// <param name="Url">Where the file is read: a link that Azure signs for a limited time.</param>
public sealed record AzureLogFile(string Name, long LastModifiedMicros, long SizeBytes, string Url)
{
    /// <summary>The hour in the name (<c>postgresql_2026_01_15_10_00_00.log</c>); null for a name with none.</summary>
    public long? StartMicros => PgLogFileNames.StartMicros(Name);
}

/// <summary>The two reads the agent makes for <c>log_source: azure_files</c>. A seam, so tests run without Azure.</summary>
public interface IAzureLogFiles
{
    /// <summary>The server's log files.</summary>
    Task<IReadOnlyList<AzureLogFile>> ListAsync(CancellationToken ct);

    /// <summary>Up to <paramref name="count"/> bytes of the file from <paramref name="offset"/>: fewer at its end, none past it.</summary>
    Task<byte[]> ReadAsync(AzureLogFile file, long offset, int count, CancellationToken ct);
}

/// <summary>A file's link no longer reads (it expired, or the file is gone): the files are listed again.</summary>
public sealed class AzureLogFileLinkException : SourceException
{
    public AzureLogFileLinkException(string message) : base(message)
    {
    }
}

/// <summary>
/// The Azure Resource Manager calls of the agent in one subscription, shared by every source it reads there: a list of
/// log files is one read of the subscription's.
/// </summary>
/// <remarks>
/// Azure meters a subscription's reads per identity: a burst, then a steady rate, shared with whatever else the identity
/// reads. One list per window is far below it; the budget keeps several sources, or a long catch-up, from taking it.
/// </remarks>
public sealed class AzureCallBudget
{
    public const double DefaultCallsPerSecond = 2;

    private static readonly ConcurrentDictionary<string, AzureCallBudget> BySubscription = new(StringComparer.OrdinalIgnoreCase);

    private readonly RdsCallBudget _slots;

    public AzureCallBudget(double callsPerSecond) => _slots = new RdsCallBudget(callsPerSecond);

    public static AzureCallBudget For(string subscription) => BySubscription.GetOrAdd(subscription, _ => new AzureCallBudget(DefaultCallsPerSecond));

    /// <summary>Waits for the next free slot.</summary>
    public Task WaitAsync(CancellationToken ct)
    {
        var wait = _slots.Reserve(DateTime.UtcNow);
        return wait > TimeSpan.Zero ? Task.Delay(wait, ct) : Task.CompletedTask;
    }
}

/// <summary>
/// The server's log files through Azure Resource Manager (<c>Microsoft.DBforPostgreSQL/flexibleServers/logFiles</c>),
/// as the agent's own Azure identity, and each file read in ranges from the link the list gives.
/// </summary>
/// <remarks>
/// The identity: the agent's managed identity first (workload identity on AKS, then the VM's or the container's own),
/// then a service principal from <c>AZURE_TENANT_ID</c>, <c>AZURE_CLIENT_ID</c> and <c>AZURE_CLIENT_SECRET</c> (or
/// <c>AZURE_CLIENT_CERTIFICATE_PATH</c>). It needs <c>Microsoft.DBforPostgreSQL/flexibleServers/logFiles/read</c> on
/// the server, and nothing else: the links are signed, so reading a file takes no role.
/// </remarks>
public sealed class ArmAzureLogFiles : IAzureLogFiles
{
    public const string ApiVersion = "2024-08-01";

    private static readonly TokenRequestContext Arm = new(new[] { "https://management.azure.com/.default" });

    private readonly HttpClient _http;
    private readonly TokenCredential _credential;
    private readonly AzureCallBudget _budget;
    private readonly string _server;
    private readonly string _list;
    private AccessToken _token;

    public ArmAzureLogFiles(HttpClient http, string subscription, string resourceGroup, string server, TokenCredential? credential = null)
    {
        _http = http;
        _credential = credential ?? Credential();
        _budget = AzureCallBudget.For(subscription);
        _server = server;
        _list = $"https://management.azure.com/subscriptions/{Uri.EscapeDataString(subscription)}/resourceGroups/{Uri.EscapeDataString(resourceGroup)}"
            + $"/providers/Microsoft.DBforPostgreSQL/flexibleServers/{Uri.EscapeDataString(server)}/logFiles?api-version={ApiVersion}";
    }

    /// <summary>The managed identity first, then a service principal from the environment; no developer sign-in.</summary>
    internal static TokenCredential Credential() => new ChainedTokenCredential(
        new DefaultAzureCredential(new DefaultAzureCredentialOptions
        {
            // Only workload identity and the managed identity: DefaultAzureCredential probes the managed identity
            // endpoint briefly, so a host with none goes on to the service principal at once.
            ExcludeEnvironmentCredential = true,
            ExcludeAzureCliCredential = true,
            ExcludeAzureDeveloperCliCredential = true,
            ExcludeAzurePowerShellCredential = true,
            ExcludeVisualStudioCredential = true,
            ExcludeVisualStudioCodeCredential = true,
            ExcludeInteractiveBrowserCredential = true,
            ExcludeBrokerCredential = true,
        }),
        new EnvironmentCredential());

    public async Task<IReadOnlyList<AzureLogFile>> ListAsync(CancellationToken ct)
    {
        var files = new List<AzureLogFile>();
        string? next = _list;
        while (next != null)
        {
            await _budget.WaitAsync(ct);
            using var request = new HttpRequestMessage(HttpMethod.Get, next);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await TokenAsync(ct));
            using var response = await SendAsync(request, ct);
            var body = await response.Content.ReadAsStringAsync(ct);
            if (!response.IsSuccessStatusCode)
            {
                throw Worded(response.StatusCode, body);
            }
            using var doc = JsonDocument.Parse(body);
            foreach (var f in doc.RootElement.TryGetProperty("value", out var value) ? value.EnumerateArray() : default)
            {
                if (Parse(f) is { } file)
                {
                    files.Add(file);
                }
            }
            next = doc.RootElement.TryGetProperty("nextLink", out var link) && link.ValueKind == JsonValueKind.String ? link.GetString() : null;
            // A next page is Resource Manager's own, never a host the answer could name.
            if (next != null && !next.StartsWith("https://management.azure.com/", StringComparison.OrdinalIgnoreCase))
            {
                throw new SourceException($"list the log files of {_server}: the next page is not on management.azure.com");
            }
        }
        return files;
    }

    /// <summary>One entry of the list: name, last write, size and link; null for one without a link.</summary>
    internal static AzureLogFile? Parse(JsonElement f)
    {
        var props = f.TryGetProperty("properties", out var p) ? p : default;
        string? Str(JsonElement o, string key) => o.ValueKind == JsonValueKind.Object && o.TryGetProperty(key, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;
        var name = Str(f, "name");
        var url = Str(props, "url");
        if (name == null || url == null || !Uri.TryCreate(url, UriKind.Absolute, out var uri) || uri.Scheme != Uri.UriSchemeHttps)
        {
            return null;
        }
        var modified = DateTimeOffset.TryParse(Str(props, "lastModifiedTime"), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var m)
            ? (m.UtcDateTime - DateTime.UnixEpoch).Ticks / 10
            : 0;
        var kb = props.ValueKind == JsonValueKind.Object && props.TryGetProperty("sizeInKb", out var size) && size.TryGetInt64(out var s) ? s : 0;
        return new AzureLogFile(name, modified, kb * AzureLogReader.SizeUnit, url);
    }

    public async Task<byte[]> ReadAsync(AzureLogFile file, long offset, int count, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, file.Url);
        request.Headers.Range = new RangeHeaderValue(offset, offset + count - 1);
        using var response = await SendAsync(request, ct);
        switch (response.StatusCode)
        {
            case HttpStatusCode.PartialContent:
                return await response.Content.ReadAsByteArrayAsync(ct);
            case HttpStatusCode.OK:
                // A server that ignored the range sent the whole file.
                var all = await response.Content.ReadAsByteArrayAsync(ct);
                return offset >= all.Length ? Array.Empty<byte>() : all.AsSpan((int)offset, (int)Math.Min(count, all.Length - offset)).ToArray();
            case HttpStatusCode.RequestedRangeNotSatisfiable:
                return Array.Empty<byte>();
            case HttpStatusCode.Forbidden or HttpStatusCode.NotFound:
                throw new AzureLogFileLinkException($"read {file.Name} of {_server}: its link answered {(int)response.StatusCode}");
            default:
                throw new SourceException($"read {file.Name} of {_server} failed: {(int)response.StatusCode} {response.ReasonPhrase}");
        }
    }

    private async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        try
        {
            return await _http.SendAsync(request, HttpCompletionOption.ResponseContentRead, ct);
        }
        catch (HttpRequestException ex)
        {
            throw new SourceException($"Azure {request.RequestUri?.Host}: {ex.Message}");
        }
    }

    private async Task<string> TokenAsync(CancellationToken ct)
    {
        if (_token.ExpiresOn > DateTimeOffset.UtcNow.AddMinutes(5))
        {
            return _token.Token;
        }
        try
        {
            _token = await _credential.GetTokenAsync(Arm, ct);
            return _token.Token;
        }
        catch (AuthenticationFailedException ex)
        {
            throw new SourceException($"list the log files of {_server}: no Azure identity ({Runner.EndpointSession.Trim(ex.Message)}). Give the agent "
                + "a managed identity, or a service principal in AZURE_TENANT_ID, AZURE_CLIENT_ID and AZURE_CLIENT_SECRET");
        }
    }

    /// <summary>A failed list as what to change.</summary>
    private SourceException Worded(HttpStatusCode status, string body)
    {
        var code = "";
        try
        {
            using var doc = JsonDocument.Parse(body);
            code = doc.RootElement.TryGetProperty("error", out var e) && e.TryGetProperty("code", out var c) ? c.GetString() ?? "" : "";
        }
        catch (JsonException)
        {
        }
        return status switch
        {
            HttpStatusCode.Forbidden or HttpStatusCode.Unauthorized => new SourceException(
                $"list the log files of {_server}: access denied ({code}). The agent's Azure identity needs "
                + "Microsoft.DBforPostgreSQL/flexibleServers/logFiles/read on the server"),
            HttpStatusCode.NotFound => new SourceException(
                $"list the log files of {_server}: no such server ({code}): check options.azure_subscription, azure_resource_group and azure_server"),
            _ => new SourceException($"list the log files of {_server} failed: {(int)status} {code}".TrimEnd()),
        };
    }
}

/// <summary>What one read of the Azure files did.</summary>
public sealed class AzureReadStats
{
    private readonly HashSet<string> _files = new(StringComparer.Ordinal);

    /// <summary>The files read, each once however often a look-back went back to it.</summary>
    public int Files => _files.Count;

    internal void Read(string file) => _files.Add(file);

    /// <summary>Everything downloaded, the look-backs' and the binary searches' reads included.</summary>
    public long Bytes { get; internal set; }
    public int Reads { get; internal set; }
}

/// <summary>
/// Reads the lines of an Azure server's log for a stretch of time, through <see cref="IAzureLogFiles"/>. Azure writes
/// one file an hour and keeps it 1 to 7 days, so a window reads its part of a file only: the offset where the window
/// begins is found by halving the file on the times of its lines, then the file is read from there in ranges.
/// </summary>
public sealed class AzureLogReader : IPgLogLines
{
    public const int ChunkBytes = 4 << 20;
    public const int ProbeBytes = 64 << 10;

    /// <summary>
    /// What one of the list's <c>sizeInKb</c> is: a thousand bytes, rounded down (a file of 103,233 bytes is listed as
    /// 103), so the size listed is a lower bound.
    /// </summary>
    public const int SizeUnit = 1000;

    private readonly IAzureLogFiles _api;
    private readonly TimeSpan _interval;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly int _probeBytes;
    private IReadOnlyList<AzureLogFile>? _files;
    private DateTime _nextRead = DateTime.MinValue;

    /// <param name="readsPerSecond">How fast the files are read, at most.</param>
    public AzureLogReader(IAzureLogFiles api, double readsPerSecond, Func<TimeSpan, CancellationToken, Task>? delay = null,
        int probeBytes = ProbeBytes)
    {
        _api = api;
        _interval = TimeSpan.FromSeconds(1 / Math.Max(0.1, readsPerSecond));
        _delay = delay ?? Task.Delay;
        _probeBytes = probeBytes;
    }

    public AzureReadStats Stats { get; } = new();

    /// <summary>The files, oldest first; listed once per reader, and again when a link no longer reads.</summary>
    public async Task<IReadOnlyList<AzureLogFile>> FilesAsync(CancellationToken ct, bool again = false)
    {
        if (_files == null || again)
        {
            _files = (await _api.ListAsync(ct))
                .Where(f => f.Name.EndsWith(".log", StringComparison.Ordinal))
                .OrderBy(f => f.StartMicros ?? long.MinValue).ThenBy(f => f.Name, StringComparer.Ordinal).ToList();
        }
        return _files;
    }

    /// <summary>
    /// Whether the log already holds what was written up to <paramref name="micros"/>: the newest file's last line is past
    /// it. Azure writes the files about every 20 minutes, each time the current hour's and the hour before's, so a
    /// newer file alone does not say the hour before is whole: its last lines can come with the next write.
    /// </summary>
    public async Task<bool> WrittenPastAsync(long micros, Func<string, long?> timeOf, CancellationToken ct)
    {
        var files = await FilesAsync(ct);
        if (files.LastOrDefault() is not { } newest)
        {
            return false;
        }
        var offset = Math.Max(0, newest.SizeBytes - _probeBytes);
        var tail = await ReadAsync(newest, offset, _probeBytes + 2 * SizeUnit, ct);
        return LastTime(tail, timeOf, wholeFirstLine: offset == 0) is { } last && last >= micros;
    }

    public async IAsyncEnumerable<PgFileLine> LinesAsync(long fromMicros, long toMicros, Func<string, long?> timeOf,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var margin = PgWindowReader.StopMarginMicros;
        var files = await FilesAsync(ct);
        foreach (var file in files.Where(f => (f.StartMicros ?? long.MinValue) <= toMicros + margin && f.LastModifiedMicros >= fromMicros - margin))
        {
            Stats.Read(file.Name);
            var offset = (file.StartMicros ?? long.MinValue) < fromMicros ? await OffsetOfAsync(file, fromMicros - margin, timeOf, ct) : 0;
            await foreach (var line in LinesFromAsync(file, offset, ct))
            {
                yield return new PgFileLine(file.Name, line);
                if (timeOf(line) is { } at && at > toMicros + margin)
                {
                    yield break;
                }
            }
        }
    }

    /// <summary>A byte offset at or before the file's first line of <paramref name="targetMicros"/>, as <see cref="PgLogDirectory.OffsetOf"/>.</summary>
    private async Task<long> OffsetOfAsync(AzureLogFile file, long targetMicros, Func<string, long?> timeOf, CancellationToken ct)
    {
        // The list gives the size in whole thousands of bytes, rounded down, and the file grows while it is read.
        long lo = 0, hi = file.SizeBytes + SizeUnit;
        while (hi - lo > _probeBytes)
        {
            var mid = lo + (hi - lo) / 2;
            var probe = await ReadAsync(file, mid, _probeBytes, ct);
            if (PgLogDirectory.FirstTime(probe, timeOf) is { } at && at < targetMicros)
            {
                lo = mid;
            }
            else
            {
                hi = mid;
            }
        }
        return lo;
    }

    /// <summary>The file's lines from <paramref name="offset"/>, in ranges; past 0, the first (partial) line is left out.</summary>
    private async IAsyncEnumerable<string> LinesFromAsync(AzureLogFile file, long offset, [EnumeratorCancellation] CancellationToken ct)
    {
        var pending = Array.Empty<byte>();
        var skip = offset > 0;
        while (true)
        {
            var chunk = await ReadAsync(file, offset, ChunkBytes, ct);
            offset += chunk.Length;
            var data = pending.Length == 0 ? chunk : Concat(pending, chunk);
            var start = 0;
            for (var nl = Array.IndexOf(data, (byte)'\n', start); nl >= 0; nl = Array.IndexOf(data, (byte)'\n', start))
            {
                var line = Encoding.UTF8.GetString(data, start, nl - start);
                start = nl + 1;
                if (skip)
                {
                    skip = false;
                    continue;
                }
                yield return line;
            }
            pending = data.AsSpan(start).ToArray();
            if (chunk.Length < ChunkBytes)
            {
                // The end of the file: Azure writes whole lines, so what is left is the last one.
                if (pending.Length > 0 && !skip)
                {
                    yield return Encoding.UTF8.GetString(pending);
                }
                yield break;
            }
        }
    }

    private async Task<byte[]> ReadAsync(AzureLogFile file, long offset, int count, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        if (_nextRead > now)
        {
            await _delay(_nextRead - now, ct);
        }
        _nextRead = (_nextRead > now ? _nextRead : now) + _interval;
        Stats.Reads++;
        // The newest link for the file: a list made since its caller had it may have given a new one.
        file = _files?.FirstOrDefault(f => f.Name == file.Name) ?? file;
        byte[] bytes;
        try
        {
            bytes = await _api.ReadAsync(file, offset, count, ct);
        }
        catch (AzureLogFileLinkException)
        {
            // The link expired: a new list gives a new one. A file that is no longer listed has none to read.
            if ((await FilesAsync(ct, again: true)).FirstOrDefault(f => f.Name == file.Name) is not { } fresh)
            {
                return Array.Empty<byte>();
            }
            bytes = await _api.ReadAsync(fresh, offset, count, ct);
        }
        Stats.Bytes += bytes.Length;
        Runner.TaskHeartbeat.Beat();
        return bytes;
    }

    /// <param name="wholeFirstLine">The bytes start at the file's start, so the first line is whole, not the tail of one.</param>
    private static long? LastTime(byte[] bytes, Func<string, long?> timeOf, bool wholeFirstLine)
    {
        var lines = Encoding.UTF8.GetString(bytes).Split('\n');
        for (var i = lines.Length - 1; i >= (wholeFirstLine ? 0 : 1); i--)
        {
            if (timeOf(lines[i]) is { } at)
            {
                return at;
            }
        }
        return null;
    }

    private static byte[] Concat(byte[] a, byte[] b)
    {
        var all = new byte[a.Length + b.Length];
        a.CopyTo(all, 0);
        b.CopyTo(all, a.Length);
        return all;
    }
}
