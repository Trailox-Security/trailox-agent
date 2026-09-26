using System.Net.Http.Headers;
using System.Text.Json;

namespace Trailox.Agent.Engines.Databricks;

/// <summary>
/// The workspace's service principal directory, read once a day so that Trailox can show an
/// automated identity by its name: Databricks records a service principal in its system tables
/// by application id alone. Read-only (<c>GET</c> on the workspace's SCIM API) and needs no grant:
/// any workspace principal may list it.
/// </summary>
/// <remarks>
/// Only three attributes are asked for and shipped: the application id, the display name and
/// whether the principal is active, each row led by the listing's UTC date as every catalog
/// stream's is. Users are not listed; the system tables already name a user by email.
/// </remarks>
public sealed class DbxDirectory
{
    public const string ListPath = "/api/2.0/preview/scim/v2/ServicePrincipals";
    public const int PageSize = 100;

    /// <summary>100,000 principals. A directory that never ends must not hold the task forever.</summary>
    internal const int MaxPages = 1000;

    /// <summary>
    /// The stream's columns, in order. Every value is sent as a string, as the statement API sends
    /// them; snapshot_date is the listing's UTC date, as <c>current_date()</c> is in the catalog SELECTs.
    /// </summary>
    public static readonly IReadOnlyList<string> Columns = new[] { "snapshot_date", "application_id", "display_name", "active" };

    private readonly HttpClient _api;
    private readonly DbxAuth _auth;
    private readonly string _host;

    public DbxDirectory(HttpClient api, DbxAuth auth, string host)
    {
        _api = api;
        _auth = auth;
        _host = host;
    }

    /// <summary>One page of the directory. SCIM counts from 1.</summary>
    public static string PageQuery(int startIndex) =>
        $"{ListPath}?attributes=applicationId,displayName,active&startIndex={startIndex}&count={PageSize}";

    /// <summary>One page: its rows, the directory's stated total, and how many entries it held.</summary>
    /// <param name="ResourceCount">Every entry on the page, usable or not: paging advances and stops on it.</param>
    public sealed record Page(List<JsonElement> Rows, int? TotalResults, int ResourceCount);

    /// <summary>
    /// The rows of one page, as JSON arrays of <see cref="Columns"/>. An entry without an
    /// application id is dropped; a missing name is sent as empty; only an explicit false (or
    /// "false") is inactive.
    /// </summary>
    /// <remarks>
    /// SCIM leaves out <c>Resources</c> entirely on an empty list, so its absence is an empty page.
    /// </remarks>
    public static Page ParsePage(JsonElement root, string snapshotDate)
    {
        var rows = new List<JsonElement>();
        var resourceCount = 0;
        if (root.ValueKind == JsonValueKind.Object
            && root.TryGetProperty("Resources", out var resources)
            && resources.ValueKind == JsonValueKind.Array)
        {
            foreach (var resource in resources.EnumerateArray())
            {
                resourceCount++;
                if (resource.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }
                var id = StringOf(resource, "applicationId");
                if (id.Length == 0)
                {
                    continue;
                }
                rows.Add(JsonSerializer.SerializeToElement(new[] { snapshotDate, id, StringOf(resource, "displayName"), IsActive(resource) ? "true" : "false" }));
            }
        }

        int? total = root.ValueKind == JsonValueKind.Object
                     && root.TryGetProperty("totalResults", out var t)
                     && t.ValueKind == JsonValueKind.Number
                     && t.TryGetInt32(out var n)
            ? n
            : null;
        return new Page(rows, total, resourceCount);
    }

    /// <summary>
    /// Every service principal in the workspace, read whole before anything is uploaded, so a
    /// refusal surfaces as a source error rather than a partial upload.
    /// </summary>
    /// <remarks>
    /// Four stops, because the stated total alone is not trusted: the stated total reached; an
    /// empty page; a page none of whose principals is new (a directory that ignored startIndex
    /// would otherwise return its first page forever); and, with no total, a short page.
    /// <see cref="MaxPages"/> bounds whatever those miss.
    /// </remarks>
    public async Task<IReadOnlyList<JsonElement>> ListAsync(CancellationToken ct) =>
        await ListAsync((path, token) => GetAsync(path, token), DateTime.UtcNow, ct);

    /// <param name="nowUtc">When the listing starts: every row carries this one date, even across midnight.</param>
    internal static async Task<IReadOnlyList<JsonElement>> ListAsync(
        Func<string, CancellationToken, Task<JsonElement>> get, DateTime nowUtc, CancellationToken ct)
    {
        var snapshotDate = nowUtc.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var listed = new List<JsonElement>();
        var start = 1;

        for (var page = 0; page < MaxPages; page++)
        {
            var parsed = ParsePage(await get(PageQuery(start), ct), snapshotDate);
            var added = 0;
            foreach (var row in parsed.Rows)
            {
                if (seen.Add(row[1].GetString() ?? ""))
                {
                    listed.Add(row);
                    added++;
                }
            }

            var done = parsed.ResourceCount == 0
                       || (parsed.Rows.Count > 0 && added == 0)
                       || (parsed.TotalResults is int stated && start - 1 + parsed.ResourceCount >= stated)
                       || (parsed.TotalResults is null && parsed.ResourceCount < PageSize);
            if (done)
            {
                break;
            }
            start += parsed.ResourceCount;
        }

        return listed;
    }

    /// <remarks>
    /// Every way this can fail ends as a <see cref="SourceException"/>, which the task runner
    /// reports as the task's failure. A timeout or an answer that is not JSON would otherwise
    /// escape the runner, leave the task unanswered, and have it handed back at every check-in.
    /// </remarks>
    private async Task<JsonElement> GetAsync(string pathAndQuery, CancellationToken ct)
    {
        string payload;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"https://{_host}{pathAndQuery}");
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await _auth.TokenAsync(ct));
            using var response = await _api.SendAsync(request, ct);
            payload = await response.Content.ReadAsStringAsync(ct);
            if (!response.IsSuccessStatusCode)
            {
                throw new DbxException((int)response.StatusCode,
                    "the workspace would not list its service principals: " + Trim(payload));
            }
        }
        catch (TaskCanceledException ex) when (!ct.IsCancellationRequested)
        {
            throw new SourceException("the workspace did not answer the service principal listing in time (" + ex.Message + ")");
        }

        try
        {
            using var doc = JsonDocument.Parse(payload);
            return doc.RootElement.Clone();
        }
        catch (JsonException)
        {
            throw new SourceException("the workspace answered the service principal listing with something that is not JSON: " + Trim(payload));
        }
    }

    private static string Trim(string payload) => payload.Length > 600 ? payload[..600] : payload;

    private static string StringOf(JsonElement resource, string name) =>
        resource.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() ?? "" : "";

    /// <summary>SCIM sends a boolean; a string "false" reads the same way, and absent means active.</summary>
    private static bool IsActive(JsonElement resource)
    {
        if (!resource.TryGetProperty("active", out var flag))
        {
            return true;
        }
        return flag.ValueKind switch
        {
            JsonValueKind.False => false,
            JsonValueKind.String => !string.Equals(flag.GetString(), "false", StringComparison.OrdinalIgnoreCase),
            _ => true,
        };
    }
}
