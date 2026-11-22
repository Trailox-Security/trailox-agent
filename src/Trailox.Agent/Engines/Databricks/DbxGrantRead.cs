using System.Globalization;
using System.Text.Json;

namespace Trailox.Agent.Engines.Databricks;

/// <summary>
/// The grant read (stream <c>catalog_directory</c>, agent 1.20.0): the answers that tell who reaches each table through
/// a group, sent as the workspace gave them.
/// </summary>
/// <remarks>
/// <para>In this order, as the same principal, read-only (SELECT statements and GET requests):</para>
/// <list type="number">
/// <item>The visibility statement (<see cref="DbxRawSelects.GrantVisibility"/>): for each catalog, whether this principal
/// may read every grant on it, and every group that holds a privilege or owns a table.</item>
/// <item>The workspace's users, service principals and groups, from its SCIM API, page by page.</item>
/// <item>Each group that holds a grant and was not listed, by name.</item>
/// <item>By id: each group that came without members and whose members reach a grant (until the workspace refuses one),
/// and each member the listings did not return.</item>
/// <item>For groups that still show no members: the candidates statement (<see cref="DbxRawSelects.GrantCandidates"/>),
/// then Unity Catalog's effective permissions on the fewest tables that cover them, asked for each of those groups and
/// each active identity. An identity whose answer names a group as a privilege's principal is in that group.</item>
/// </list>
/// <para>Every answer is kept with its request and HTTP status, a refusal included, and sent once the read is done. A
/// failed statement or listing ends the read; what was read before it is still sent, and Trailox treats the read as
/// incomplete. The bounds keep a large directory from holding the task: <see cref="MaxLookups"/> reads one by one,
/// <see cref="MaxTables"/> tables, <see cref="MaxCalls"/> effective-permission requests, and
/// <see cref="EffectiveTimeLimit"/> for those requests.</para>
/// </remarks>
public sealed class DbxGrantRead
{
    /// <summary>
    /// The stream's columns, in order. snapshot_date is the read's UTC date; read_id names one read, so the answers of a
    /// task that was run twice are never taken for one read.
    /// </summary>
    public static readonly IReadOnlyList<string> Columns = new[] { "snapshot_date", "read_id", "seq", "kind", "request", "status", "body" };

    public const string KindSql = "sql";
    public const string KindGet = "get";
    public const string VisibilityRequest = "visibility";
    public const string CandidatesRequest = "candidates";

    public const string UsersPath = "/api/2.0/preview/scim/v2/Users";
    public const string ServicePrincipalsPath = "/api/2.0/preview/scim/v2/ServicePrincipals";
    public const string GroupsPath = "/api/2.0/preview/scim/v2/Groups";
    public const string EffectivePermissionsPath = "/api/2.1/unity-catalog/effective-permissions/table/";

    public const string UserAttributes = "id,userName,active,groups";
    public const string ServicePrincipalAttributes = "id,applicationId,active,groups";
    public const string GroupAttributes = "id,displayName,members,groups";

    public const int PageSize = 100;
    internal const int MaxPages = 1000;

    /// <summary>Groups and identities read one by one: by name, and by id.</summary>
    internal const int MaxLookups = 400;

    internal const int MaxTables = 10;
    internal const int MaxCalls = 1000;
    internal const int MaxAnswerPages = 5;
    internal static readonly TimeSpan EffectiveTimeLimit = TimeSpan.FromMinutes(5);

    public const string AccountUsers = "account users";
    public const string WorkspaceUsers = "users";
    public const string WorkspaceAdmins = "admins";
    public const string WorkspaceUsersPrefix = "_workspace_users_workspace_";
    public const string WorkspaceAdminsPrefix = "_workspace_admins_workspace_";

    /// <summary>One HTTP answer: its status (0 when there was none) and its body, or why there was none.</summary>
    public sealed record Answer(int Status, string Body);

    private readonly Func<string, CancellationToken, Task<Answer>> _get;
    private readonly Func<string, CancellationToken, Task<IReadOnlyList<JsonElement>>> _sql;
    private readonly string _visibilitySql;
    private readonly string _candidatesSql;

    private readonly List<(string Kind, string Request, int Status, string Body)> _entries = new();
    private readonly Dictionary<string, (string Name, bool Active)> _identities = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _groupNames = new(StringComparer.Ordinal);   // id -> name, not account users
    private readonly Queue<(string Id, JsonElement Group)> _pending = new();
    private readonly Dictionary<string, string> _memberless = new(StringComparer.Ordinal);   // id -> name
    private readonly Dictionary<string, string> _shownNone = new(StringComparer.Ordinal);    // id -> name, still memberless
    private readonly HashSet<string> _fetched = new(StringComparer.Ordinal);
    private readonly HashSet<(string Member, string Group)> _nested = new();                   // group in group, by name
    private readonly HashSet<(string Member, bool MemberIsGroup, string GroupId, string Display)> _upward = new();
    private bool _byIdRefused;
    private int _lookups;
    private string _readId = "";

    /// <param name="get">One GET on the workspace (path and query); never throws but for cancellation.</param>
    /// <param name="sql">One statement's rows, each a JSON array; throws when the statement fails.</param>
    public DbxGrantRead(Func<string, CancellationToken, Task<Answer>> get,
        Func<string, CancellationToken, Task<IReadOnlyList<JsonElement>>> sql, string visibilitySql, string candidatesSql)
    {
        _get = get;
        _sql = sql;
        _visibilitySql = visibilitySql;
        _candidatesSql = candidatesSql;
    }

    /// <summary>The whole read, as the stream's rows (<see cref="Columns"/>).</summary>
    /// <remarks>The visibility statement failing fails the task: without it nothing else can be read.</remarks>
    /// <param name="readId">This read's id; a new one when null (tests pass one).</param>
    public async Task<IReadOnlyList<JsonElement>> ReadAsync(DateTime nowUtc, CancellationToken ct, string? readId = null)
    {
        _readId = readId ?? Guid.NewGuid().ToString("D");
        var visibility = await _sql(_visibilitySql, ct);
        Record(KindSql, VisibilityRequest, 200, JsonSerializer.Serialize(visibility));
        var granted = visibility.Select(r => Cell(r, 3)).Where(n => n.Length > 0).Distinct(StringComparer.Ordinal).ToList();

        if (await ListAsync(UsersPath, UserAttributes, r => AddIdentity(StringOf(r, "id"), r, "userName"), ct)
            && await ListAsync(ServicePrincipalsPath, ServicePrincipalAttributes, r => AddIdentity(StringOf(r, "id"), r, "applicationId"), ct)
            && await ListAsync(GroupsPath, GroupAttributes, AddListedGroup, ct))
        {
            await LookUpGrantedAsync(granted, ct);
            await FollowMembersAsync(granted, ct);
            var hidden = Hidden(granted);
            if (hidden.Count > 0)
            {
                await ReadEffectiveAsync(hidden, ct);
            }
        }
        return Rows(nowUtc);
    }

    // ------------------------------------------------------------------------------------------------ paths ----

    /// <summary>One page of a SCIM listing. SCIM counts from 1.</summary>
    public static string PageQuery(string path, string attributes, int startIndex) =>
        $"{path}?attributes={attributes}&startIndex={startIndex}&count={PageSize}";

    /// <summary>One group by display name. A double quote in the name is dropped from the filter.</summary>
    public static string ByNameQuery(string name)
    {
        var quote = (char)34;
        var filter = Uri.EscapeDataString("displayName eq " + quote + name.Replace(quote.ToString(), string.Empty) + quote);
        return $"{GroupsPath}?filter={filter}&attributes={GroupAttributes}";
    }

    public static string GroupById(string id) => $"{GroupsPath}/{Uri.EscapeDataString(id)}?attributes={GroupAttributes}";

    public static string IdentityById(string id, bool isUser) =>
        isUser
            ? $"{UsersPath}/{Uri.EscapeDataString(id)}?attributes=id,userName,active,groups"
            : $"{ServicePrincipalsPath}/{Uri.EscapeDataString(id)}?attributes=id,applicationId,active,groups";

    /// <summary>One page of one principal's effective permissions on one table (catalog.schema.table).</summary>
    public static string EffectivePermissions(string table, string principal, string? pageToken) =>
        $"{EffectivePermissionsPath}{Uri.EscapeDataString(table)}?max_results=0&principal={Uri.EscapeDataString(principal)}"
        + (string.IsNullOrEmpty(pageToken) ? string.Empty : $"&page_token={Uri.EscapeDataString(pageToken)}");

    // ----------------------------------------------------------------------------------------------- listing ----

    /// <summary>
    /// Every resource of one listing, page by page: it stops at an empty page, a page with nothing new, or the stated
    /// total. False when a page could not be read (or the listing never ended): the read stops there.
    /// </summary>
    private async Task<bool> ListAsync(string path, string attributes, Action<JsonElement> add, CancellationToken ct)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var start = 1;
        for (var page = 0; page < MaxPages; page++)
        {
            if (await GetJsonAsync(PageQuery(path, attributes, start), ct) is not JsonElement root)
            {
                return false;
            }
            var count = 0;
            var added = 0;
            if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("Resources", out var resources)
                && resources.ValueKind == JsonValueKind.Array)
            {
                foreach (var resource in resources.EnumerateArray())
                {
                    count++;
                    if (resource.ValueKind != JsonValueKind.Object)
                    {
                        continue;
                    }
                    var id = StringOf(resource, "id");
                    if (id.Length > 0 && seen.Add(id))
                    {
                        add(resource.Clone());
                        added++;
                    }
                }
            }
            int? total = root.ValueKind == JsonValueKind.Object && root.TryGetProperty("totalResults", out var t)
                         && t.ValueKind == JsonValueKind.Number && t.TryGetInt32(out var n)
                ? n
                : null;
            if (count == 0 || added == 0 || (total is int stated && start - 1 + count >= stated))
            {
                return true;
            }
            start += count;
        }
        return false;
    }

    private void AddIdentity(string id, JsonElement identity, string nameField)
    {
        var name = StringOf(identity, nameField);
        if (id.Length == 0 || name.Length == 0)
        {
            return;
        }
        _identities[id] = (name, IsActive(identity));
        AddUpward(name, false, identity);
    }

    private void AddListedGroup(JsonElement group)
    {
        var name = StringOf(group, "displayName");
        var id = StringOf(group, "id");
        if (name == AccountUsers)
        {
            AddUpward(name, true, group);
            return;
        }
        _groupNames[id] = name;
        _pending.Enqueue((id, group));
    }

    // ------------------------------------------------------------------------------------------------- walk ----

    /// <summary>Each granted group the listing did not return, by its display name.</summary>
    private async Task LookUpGrantedAsync(IReadOnlyList<string> granted, CancellationToken ct)
    {
        foreach (var wanted in granted.Where(NeedsLookup))
        {
            if (_groupNames.ContainsValue(wanted) || !Spend())
            {
                continue;
            }
            if (await GetJsonAsync(ByNameQuery(wanted), ct) is not JsonElement page
                || page.ValueKind != JsonValueKind.Object || !page.TryGetProperty("Resources", out var resources)
                || resources.ValueKind != JsonValueKind.Array)
            {
                continue;
            }
            foreach (var g in resources.EnumerateArray())
            {
                if (g.ValueKind == JsonValueKind.Object && StringOf(g, "displayName") == wanted
                    && !_groupNames.ContainsKey(StringOf(g, "id")))
                {
                    _groupNames[StringOf(g, "id")] = wanted;
                    _pending.Enqueue((StringOf(g, "id"), g.Clone()));
                }
            }
        }
    }

    /// <summary>
    /// Reads the members of every pending group, then by id each group that came without members and reaches a grant,
    /// until nothing new is found.
    /// </summary>
    private async Task FollowMembersAsync(IReadOnlyList<string> granted, CancellationToken ct)
    {
        do
        {
            while (_pending.Count > 0)
            {
                var (id, group) = _pending.Dequeue();
                var name = StringOf(group, "displayName");
                if (name.Length == 0)
                {
                    continue;
                }
                AddUpward(name, true, group);
                // SCIM leaves an empty list out, so no members and an empty list read the same way.
                if (!group.TryGetProperty("members", out var members) || members.ValueKind != JsonValueKind.Array
                    || members.GetArrayLength() == 0)
                {
                    _memberless[id] = name;
                    continue;
                }
                foreach (var m in members.EnumerateArray())
                {
                    if (m.ValueKind == JsonValueKind.Object)
                    {
                        await FollowMemberAsync(m, name, ct);
                    }
                }
            }
        }
        while (await FetchMemberlessAsync(granted, ct));
    }

    /// <summary>One member: known already, or read by id (a group the listing did not return, or an identity).</summary>
    private async Task FollowMemberAsync(JsonElement member, string groupName, CancellationToken ct)
    {
        var id = StringOf(member, "value");
        var kind = MemberKind(StringOf(member, "$ref"));
        var maybeGroup = kind is null or "Groups";
        if (kind != "Groups" && _identities.ContainsKey(id))
        {
            return;
        }
        if (maybeGroup && _groupNames.TryGetValue(id, out var known))
        {
            _nested.Add((known, groupName));
            return;
        }
        if (maybeGroup && (await FetchGroupAsync(id, ct)).Name is string fetched)
        {
            _nested.Add((fetched, groupName));
            return;
        }
        if (kind is "Users" or "ServicePrincipals" && id.Length > 0 && Spend())
        {
            if (await GetJsonAsync(IdentityById(id, kind == "Users"), ct) is JsonElement identity
                && identity.ValueKind == JsonValueKind.Object)
            {
                AddIdentity(id, identity, kind == "Users" ? "userName" : "applicationId");
            }
        }
    }

    /// <summary>
    /// Reads by id each group that came without members and whose members reach a grant; after the workspace refuses
    /// one (403), no other group is read this way. True when a group was queued or a new nesting was learned.
    /// </summary>
    private async Task<bool> FetchMemberlessAsync(IReadOnlyList<string> granted, CancellationToken ct)
    {
        var reaching = Reaching(granted);
        var queued = false;
        foreach (var (id, name) in _memberless.Where(g => reaching.Contains(g.Value)).ToArray())
        {
            _memberless.Remove(id);
            if (!_fetched.Contains(id) && !_byIdRefused)
            {
                var (fetched, status) = await FetchGroupAsync(id, ct);
                if (fetched is not null)
                {
                    queued = true;
                    continue;
                }
                _byIdRefused |= status == 403;
            }
            _shownNone[id] = name;
        }
        return queued || LearnUpward();
    }

    /// <summary>Nestings named by the `groups` attribute, for the groups still without members. True when one is new.</summary>
    private bool LearnUpward()
    {
        var added = false;
        foreach (var (id, name) in _shownNone)
        {
            foreach (var u in _upward.Where(u => u.MemberIsGroup && u.Member != name
                         && ((id.Length > 0 && u.GroupId == id) || (u.GroupId.Length == 0 && u.Display == name))))
            {
                added |= _nested.Add((u.Member, name));
            }
        }
        return added;
    }

    /// <summary>A group read by id and queued; Name null when it cannot be read, with the status the workspace gave.</summary>
    private async Task<(string? Name, int Status)> FetchGroupAsync(string id, CancellationToken ct)
    {
        if (id.Length == 0 || !Spend())
        {
            return (null, 0);
        }
        var answer = await GetAsync(GroupById(id), ct);
        if (answer.Status != 200 || Parse(answer.Body) is not JsonElement g || g.ValueKind != JsonValueKind.Object)
        {
            return (null, answer.Status);
        }
        var name = StringOf(g, "displayName");
        if (name.Length == 0)
        {
            return (null, answer.Status);
        }
        _groupNames[id] = name;
        _fetched.Add(id);
        _pending.Enqueue((id, g));
        return (name, answer.Status);
    }

    /// <summary>
    /// The groups whose members reach a grant: each granted group (`admins` for a granted workspace admins name), and
    /// every group nested in one. The everyone groups are not read: every user is in them.
    /// </summary>
    private HashSet<string> Reaching(IReadOnlyList<string> granted)
    {
        var reaching = new HashSet<string>(granted.Where(NeedsLookup), StringComparer.Ordinal);
        if (granted.Any(g => g.StartsWith(WorkspaceAdminsPrefix, StringComparison.Ordinal)))
        {
            reaching.Add(WorkspaceAdmins);
        }
        bool grew;
        do
        {
            grew = false;
            foreach (var (member, group) in _nested.Where(n => reaching.Contains(n.Group)).ToArray())
            {
                grew |= reaching.Add(member);
            }
        }
        while (grew);
        return reaching;
    }

    /// <summary>The groups that reach a grant and showed no members, the special ones left out (not read this way).</summary>
    private IReadOnlyList<string> Hidden(IReadOnlyList<string> granted)
    {
        var reaching = Reaching(granted);
        return _shownNone.Values.Where(reaching.Contains).Where(g => !IsSpecial(g))
            .Distinct(StringComparer.Ordinal).OrderBy(g => g, StringComparer.Ordinal).ToArray();
    }

    // ----------------------------------------------------------------------------------- effective permissions ----

    /// <summary>
    /// The candidates statement, then every hidden group and every active identity on each chosen table. Stops at the
    /// first refusal or failure, at the time limit, or before starting when it would take more than the call bound.
    /// </summary>
    private async Task ReadEffectiveAsync(IReadOnlyList<string> hidden, CancellationToken ct)
    {
        IReadOnlyList<JsonElement> rows;
        try
        {
            rows = await _sql(_candidatesSql, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            Record(KindSql, CandidatesRequest, 0, Trim(ex.Message));
            return;
        }
        Record(KindSql, CandidatesRequest, 200, JsonSerializer.Serialize(rows));
        var tables = Cover(hidden, rows.Select(r => (Group: Cell(r, 0), Table: Cell(r, 1))));
        var identities = _identities.Values.Where(i => i.Active).Select(i => i.Name)
            .Distinct(StringComparer.Ordinal).OrderBy(n => n, StringComparer.Ordinal);
        var principals = hidden.Concat(identities).Distinct(StringComparer.Ordinal).ToArray();
        if (tables.Count == 0 || tables.Count * principals.Length > MaxCalls)
        {
            return;
        }

        using var limit = CancellationTokenSource.CreateLinkedTokenSource(ct);
        limit.CancelAfter(EffectiveTimeLimit);
        try
        {
            foreach (var table in tables)
            {
                foreach (var principal in principals)
                {
                    string? token = null;
                    for (var page = 0; page < MaxAnswerPages; page++)
                    {
                        var answer = await GetAsync(EffectivePermissions(table, principal, token), limit.Token);
                        if (answer.Status != 200)
                        {
                            return;
                        }
                        token = Parse(answer.Body) is JsonElement root && root.ValueKind == JsonValueKind.Object
                                && root.TryGetProperty("next_page_token", out var t) && t.ValueKind == JsonValueKind.String
                            ? t.GetString()
                            : null;
                        if (string.IsNullOrEmpty(token))
                        {
                            break;
                        }
                    }
                }
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // The time limit: what was asked so far is sent, and Trailox finds the rest missing.
        }
    }

    /// <summary>
    /// The fewest tables that give every hidden group one on which it holds SELECT or MODIFY: greedy, the table that
    /// covers the most groups still uncovered, then by name; at most <see cref="MaxTables"/>.
    /// </summary>
    internal static IReadOnlyList<string> Cover(IReadOnlyList<string> hidden, IEnumerable<(string Group, string Table)> candidates)
    {
        var reach = candidates
            .Where(c => c.Table.Length > 0)
            .GroupBy(c => c.Table, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.Select(c => c.Group).ToHashSet(StringComparer.Ordinal), StringComparer.Ordinal);
        var left = new HashSet<string>(hidden, StringComparer.Ordinal);
        var chosen = new List<string>();
        while (left.Count > 0 && chosen.Count < MaxTables)
        {
            var best = reach
                .Select(t => (Table: t.Key, Gain: t.Value.Count(left.Contains)))
                .Where(t => t.Gain > 0)
                .OrderByDescending(t => t.Gain)
                .ThenBy(t => t.Table, StringComparer.Ordinal)
                .FirstOrDefault();
            if (best.Table is null)
            {
                break;
            }
            chosen.Add(best.Table);
            left.ExceptWith(reach[best.Table]);
        }
        return chosen;
    }

    // ---------------------------------------------------------------------------------------------- helpers ----

    /// <summary>One GET, recorded. Never throws but for cancellation.</summary>
    private async Task<Answer> GetAsync(string path, CancellationToken ct)
    {
        var answer = await _get(path, ct);
        Record(KindGet, path, answer.Status, answer.Body);
        return answer;
    }

    /// <summary>One GET, recorded; its JSON body when it answered 200 with JSON, otherwise null.</summary>
    private async Task<JsonElement?> GetJsonAsync(string path, CancellationToken ct)
    {
        var answer = await GetAsync(path, ct);
        return answer.Status == 200 ? Parse(answer.Body) : null;
    }

    private void Record(string kind, string request, int status, string body) => _entries.Add((kind, request, status, body));

    private IReadOnlyList<JsonElement> Rows(DateTime nowUtc)
    {
        var day = nowUtc.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        return _entries
            .Select((e, i) => JsonSerializer.SerializeToElement(new object[] { day, _readId, i + 1, e.Kind, e.Request, e.Status, e.Body }))
            .ToArray();
    }

    private bool Spend()
    {
        if (_lookups >= MaxLookups)
        {
            return false;
        }
        _lookups++;
        return true;
    }

    private void AddUpward(string member, bool memberIsGroup, JsonElement resource)
    {
        if (member.Length == 0 || !resource.TryGetProperty("groups", out var groups) || groups.ValueKind != JsonValueKind.Array)
        {
            return;
        }
        foreach (var g in groups.EnumerateArray())
        {
            if (g.ValueKind == JsonValueKind.Object)
            {
                _upward.Add((member, memberIsGroup, StringOf(g, "value"), StringOf(g, "display")));
            }
        }
    }

    /// <summary>A granted name that is not a group every member is in, nor one of the workspace's own grant names.</summary>
    internal static bool NeedsLookup(string name) =>
        name != AccountUsers && name != WorkspaceUsers
        && !name.StartsWith(WorkspaceUsersPrefix, StringComparison.Ordinal)
        && !name.StartsWith(WorkspaceAdminsPrefix, StringComparison.Ordinal);

    /// <summary>Groups whose grants go by another name (`admins`), or that every identity is in.</summary>
    internal static bool IsSpecial(string group) =>
        group is AccountUsers or WorkspaceUsers or WorkspaceAdmins
        || group.StartsWith(WorkspaceUsersPrefix, StringComparison.Ordinal)
        || group.StartsWith(WorkspaceAdminsPrefix, StringComparison.Ordinal);

    /// <summary>"Users", "Groups" or "ServicePrincipals" from a SCIM $ref such as "Users/123"; null when absent.</summary>
    internal static string? MemberKind(string reference)
    {
        if (string.IsNullOrEmpty(reference))
        {
            return null;
        }
        var slash = reference.TrimEnd('/').LastIndexOf('/');
        var head = slash > 0 ? reference[..slash] : reference;
        var last = head.Split('/').Last();
        return last is "Users" or "Groups" or "ServicePrincipals" ? last : null;
    }

    private static JsonElement? Parse(string body)
    {
        try
        {
            using var doc = JsonDocument.Parse(body);
            return doc.RootElement.Clone();
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string Cell(JsonElement row, int index) =>
        row.ValueKind == JsonValueKind.Array && row.GetArrayLength() > index && row[index].ValueKind == JsonValueKind.String
            ? row[index].GetString() ?? ""
            : "";

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

    internal static string Trim(string text) => text.Length > 600 ? text[..600] : text;
}
