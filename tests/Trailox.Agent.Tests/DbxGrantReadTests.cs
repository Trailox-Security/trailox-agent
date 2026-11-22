using System.Text.Json;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.Databricks;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests;

/// <summary>
/// The grant read (agent 1.20): which requests it makes, in which order, and what it sends. The requests, the two
/// statements and the upload are part of the protocol, so they are pinned exactly, path and query included.
/// </summary>
public class DbxGrantReadTests
{
    private const string Day = "2026-03-14";
    private static readonly DateTime Now = new(2026, 3, 14, 12, 0, 0, DateTimeKind.Utc);

    // A workspace of the shape the read exists for: two groups that hold grants and show no members to this
    // principal, one nested in the other, read by id refused, and the members visible only through effective permissions.
    private const string Self = "00000000-0000-0000-0000-0000000000c0";
    private const string Analyst = "00000000-0000-0000-0000-0000000000a1";
    private const string Assistant = "00000000-0000-0000-0000-0000000000a2";
    private const string Loader = "00000000-0000-0000-0000-0000000000a3";
    private const string InnerId = "100";
    private const string OuterId = "200";
    private const string Table = "main.raw.events";

    private const string Visibility = "visibility statement";
    private const string Candidates = "candidates statement";

    private sealed class FakeWorkspace
    {
        public Dictionary<string, DbxGrantRead.Answer> Answers { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, IReadOnlyList<JsonElement>> Statements { get; } = new(StringComparer.Ordinal);
        public List<string> Asked { get; } = new();
        public List<string> Unexpected { get; } = new();

        public Task<DbxGrantRead.Answer> GetAsync(string path, CancellationToken ct)
        {
            Asked.Add(path);
            if (Answers.TryGetValue(path, out var answer))
            {
                return Task.FromResult(answer);
            }
            Unexpected.Add(path);
            return Task.FromResult(new DbxGrantRead.Answer(404, """{"error_code":"NOT_FOUND"}"""));
        }

        public Task<IReadOnlyList<JsonElement>> SqlAsync(string sql, CancellationToken ct)
        {
            Asked.Add("sql:" + sql);
            return Statements.TryGetValue(sql, out var rows)
                ? Task.FromResult(rows)
                : throw new SourceException("the statement failed: " + sql);
        }

        public DbxGrantRead Read() => new(GetAsync, SqlAsync, Visibility, Candidates);
    }

    private static JsonElement Row(params string?[] cells) => JsonSerializer.SerializeToElement(cells);

    private static DbxGrantRead.Answer Ok(string body) => new(200, body);

    private static string Page(int total, params string[] resources) =>
        $$"""{"totalResults":{{total}},"startIndex":1,"itemsPerPage":{{resources.Length}},"Resources":[{{string.Join(",", resources)}}]}""";

    private static string Identity(string id, string nameField, string name, bool active = true) =>
        $$"""{"id":"{{id}}","{{nameField}}":"{{name}}","active":{{(active ? "true" : "false")}}}""";

    private static string Group(string id, string name, string? members = null) =>
        members is null ? $$"""{"id":"{{id}}","displayName":"{{name}}"}""" : $$"""{"id":"{{id}}","displayName":"{{name}}","members":[{{members}}]}""";

    private static string Assignment((string Principal, string Privilege) held) =>
        "{\"principal\":\"" + held.Principal + "\",\"privileges\":[{\"privilege\":\"" + held.Privilege + "\"}]}";

    private static string Assignments(params (string Principal, string Privilege)[] held) =>
        "{\"privilege_assignments\":[" + string.Join(",", held.Select(Assignment)) + "]}";

    /// <summary>The hidden-groups workspace, answered fully.</summary>
    private static FakeWorkspace HiddenGroups()
    {
        var w = new FakeWorkspace();
        w.Statements[Visibility] = new[]
        {
            Row(Day, "main", "1", null),
            Row(Day, null, "1", null),
            Row(Day, null, "1", "account users"),
            Row(Day, null, "1", "data-leads"),
            Row(Day, null, "1", "data-team"),
        };
        w.Statements[Candidates] = new[]
        {
            Row("account users", "main.sales.accounts"),
            Row("data-leads", "main.finance.invoices"),
            Row("data-leads", Table),
            Row("data-team", Table),
        };
        w.Answers[DbxGrantRead.PageQuery(DbxGrantRead.UsersPath, DbxGrantRead.UserAttributes, 1)] = Ok(Page(3,
            Identity("u1", "userName", "owner@example.test"),
            Identity("u2", "userName", "plain@example.test"),
            Identity("u3", "userName", "gone@example.test", active: false)));
        w.Answers[DbxGrantRead.PageQuery(DbxGrantRead.ServicePrincipalsPath, DbxGrantRead.ServicePrincipalAttributes, 1)] = Ok(Page(4,
            Identity("s0", "applicationId", Self),
            Identity("s1", "applicationId", Analyst),
            Identity("s2", "applicationId", Assistant),
            Identity("s3", "applicationId", Loader)));
        w.Answers[DbxGrantRead.PageQuery(DbxGrantRead.GroupsPath, DbxGrantRead.GroupAttributes, 1)] = Ok(Page(5,
            Group("1", "account users"),
            Group("2", "admins"),
            Group("3", "users"),
            Group(InnerId, "data-leads"),
            Group(OuterId, "data-team")));
        w.Answers[DbxGrantRead.GroupById(InnerId)] = new(403, """{"error_code":"PERMISSION_DENIED"}""");

        var inner = Assignments(("data-leads", "MODIFY"), ("data-team", "SELECT"));
        var outer = Assignments(("data-team", "SELECT"));
        var none = Assignments(("account users", "SELECT"));
        w.Answers[DbxGrantRead.EffectivePermissions(Table, "data-leads", null)] = Ok(inner);
        w.Answers[DbxGrantRead.EffectivePermissions(Table, "data-team", null)] = Ok(outer);
        // The analyst's answer comes in two pages.
        w.Answers[DbxGrantRead.EffectivePermissions(Table, Analyst, null)] =
            Ok("""{"privilege_assignments":[{"principal":"data-leads","privileges":[{"privilege":"MODIFY"}]}],"next_page_token":"t/2"}""");
        w.Answers[DbxGrantRead.EffectivePermissions(Table, Analyst, "t/2")] = Ok(outer);
        w.Answers[DbxGrantRead.EffectivePermissions(Table, Assistant, null)] = Ok(inner);
        w.Answers[DbxGrantRead.EffectivePermissions(Table, Loader, null)] = Ok(outer);
        foreach (var other in new[] { Self, "owner@example.test", "plain@example.test" })
        {
            w.Answers[DbxGrantRead.EffectivePermissions(Table, other, null)] = Ok(none);
        }
        return w;
    }

    private static List<(string Kind, string Request, int Status)> Entries(IReadOnlyList<JsonElement> rows) =>
        rows.Select(r => (r[3].GetString()!, r[4].GetString()!, r[5].GetInt32())).ToList();

    private const string ReadId = "00000000-0000-4000-8000-000000000001";

    /// <summary>
    /// The two statements for a source with no excluded catalog, as the protocol has them. Compared with runs of
    /// whitespace collapsed.
    /// </summary>
    private const string PinnedVisibility = """
        WITH cats AS (
            SELECT DISTINCT table_catalog AS catalog
            FROM system.information_schema.tables
            WHERE table_catalog NOT IN ('system', 'samples', '__databricks_internal') AND table_schema <> 'information_schema'
        ),
        mine AS (
            SELECT DISTINCT catalog_name AS catalog
            FROM system.information_schema.catalog_privileges
            WHERE replace(upper(privilege_type), ' ', '_') IN ('READ_METADATA', 'MANAGE')
              AND (grantee = current_user() OR is_account_group_member(grantee) OR is_member(grantee))
            UNION
            SELECT catalog_name
            FROM system.information_schema.catalogs
            WHERE catalog_owner = current_user() OR is_account_group_member(catalog_owner) OR is_member(catalog_owner)
        ),
        metastore AS (
            SELECT count(*) AS n
            FROM system.information_schema.metastore_privileges
            WHERE replace(upper(privilege_type), ' ', '_') IN ('READ_METADATA', 'MANAGE')
              AND (grantee = current_user() OR is_account_group_member(grantee) OR is_member(grantee))
        )
        SELECT date_format(current_date(), 'yyyy-MM-dd') AS `day`,
               c.catalog AS `catalog`,
               CASE WHEN m.catalog IS NOT NULL OR (SELECT n FROM metastore) > 0 THEN 1 ELSE 0 END AS `visible`,
               CAST(NULL AS STRING) AS `granted_group`
        FROM cats c
        LEFT JOIN mine m ON m.catalog = c.catalog
        UNION ALL
        SELECT date_format(current_date(), 'yyyy-MM-dd'), CAST(NULL AS STRING), 1, CAST(NULL AS STRING)
        UNION ALL
        SELECT DISTINCT date_format(current_date(), 'yyyy-MM-dd'), CAST(NULL AS STRING), 1, name
        FROM (
            SELECT grantee AS name FROM system.information_schema.table_privileges
            WHERE table_catalog NOT IN ('system', 'samples', '__databricks_internal') AND table_schema <> 'information_schema' AND upper(COALESCE(inherited_from, 'NONE')) = 'NONE' AND replace(upper(privilege_type), ' ', '_') IN ('SELECT', 'MODIFY', 'ALL_PRIVILEGES')
            UNION ALL
            SELECT grantee FROM system.information_schema.schema_privileges
            WHERE catalog_name NOT IN ('system', 'samples', '__databricks_internal')
              AND schema_name <> 'information_schema' AND upper(COALESCE(inherited_from, 'NONE')) = 'NONE' AND replace(upper(privilege_type), ' ', '_') IN ('SELECT', 'MODIFY', 'ALL_PRIVILEGES')
            UNION ALL
            SELECT grantee FROM system.information_schema.catalog_privileges
            WHERE catalog_name NOT IN ('system', 'samples', '__databricks_internal') AND upper(COALESCE(inherited_from, 'NONE')) = 'NONE' AND replace(upper(privilege_type), ' ', '_') IN ('SELECT', 'MODIFY', 'ALL_PRIVILEGES')
            UNION ALL
            SELECT table_owner FROM system.information_schema.tables
            WHERE table_catalog NOT IN ('system', 'samples', '__databricks_internal') AND table_schema <> 'information_schema'
        ) p
        WHERE COALESCE(name, '') <> '' AND name NOT LIKE '%@%'
          AND NOT (length(name) = 36 AND name RLIKE '^[0-9a-fA-F-]+$')
        ORDER BY `catalog`
        """;

    private const string PinnedCandidates = """
        SELECT grantee AS `group`, full_name AS `table`
        FROM (
            SELECT grantee, full_name, row_number() OVER (PARTITION BY grantee ORDER BY full_name) AS rn
            FROM (
                SELECT DISTINCT grantee, concat_ws('.', table_catalog, table_schema, table_name) AS full_name
                FROM system.information_schema.table_privileges
                WHERE table_catalog NOT IN ('system', 'samples', '__databricks_internal') AND table_schema <> 'information_schema'
                  AND replace(upper(privilege_type), ' ', '_') IN ('SELECT', 'MODIFY')
                  AND COALESCE(grantee, '') <> '' AND grantee NOT LIKE '%@%'
                  AND NOT (length(grantee) = 36 AND grantee RLIKE '^[0-9a-fA-F-]+$')
            ) d
        ) r
        WHERE rn <= 20
        ORDER BY `group`, `table`
        """;

    private static string Collapse(string sql) => System.Text.RegularExpressions.Regex.Replace(sql, @"\s+", " ").Trim();

    private static string RepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !File.Exists(Path.Combine(dir.FullName, "trailox-agent.sln")))
        {
            dir = dir.Parent;
        }
        Assert.NotNull(dir);
        return dir!.FullName;
    }

    /// <summary>The read as the agent uploads it: one JSON object per line, keyed by the stream's columns.</summary>
    private static async Task<string> UploadedAsync(IReadOnlyList<JsonElement> rows)
    {
        static async IAsyncEnumerable<JsonElement> Each(IReadOnlyList<JsonElement> rows)
        {
            foreach (var row in rows)
            {
                yield return row;
            }
            await Task.CompletedTask;
        }
        using var stream = new NdjsonRowStream(DbxGrantRead.Columns, Each(rows), CancellationToken.None);
        using var reader = new StreamReader(stream);
        return await reader.ReadToEndAsync();
    }

    [Fact]
    public async Task PathsArePinned()
    {
        Assert.Equal("/api/2.0/preview/scim/v2/Users?attributes=id,userName,active,groups&startIndex=101&count=100",
            DbxGrantRead.PageQuery(DbxGrantRead.UsersPath, DbxGrantRead.UserAttributes, 101));
        Assert.Equal("/api/2.0/preview/scim/v2/ServicePrincipals?attributes=id,applicationId,active,groups&startIndex=1&count=100",
            DbxGrantRead.PageQuery(DbxGrantRead.ServicePrincipalsPath, DbxGrantRead.ServicePrincipalAttributes, 1));
        Assert.Equal("/api/2.0/preview/scim/v2/Groups?attributes=id,displayName,members,groups&startIndex=1&count=100",
            DbxGrantRead.PageQuery(DbxGrantRead.GroupsPath, DbxGrantRead.GroupAttributes, 1));
        Assert.Equal("/api/2.0/preview/scim/v2/Groups?filter=displayName%20eq%20%22data%20team%22&attributes=id,displayName,members,groups",
            DbxGrantRead.ByNameQuery("data \"team\""));
        Assert.Equal("/api/2.0/preview/scim/v2/Groups/a%2Fb?attributes=id,displayName,members,groups", DbxGrantRead.GroupById("a/b"));
        Assert.Equal("/api/2.0/preview/scim/v2/Users/7?attributes=id,userName,active,groups", DbxGrantRead.IdentityById("7", true));
        Assert.Equal("/api/2.0/preview/scim/v2/ServicePrincipals/7?attributes=id,applicationId,active,groups",
            DbxGrantRead.IdentityById("7", false));
        Assert.Equal("/api/2.1/unity-catalog/effective-permissions/table/main.raw.events?max_results=0&principal=data-leads",
            DbxGrantRead.EffectivePermissions(Table, "data-leads", null));
        Assert.Equal("/api/2.1/unity-catalog/effective-permissions/table/main.raw.events?max_results=0&principal=a%40b.test&page_token=t%2F2",
            DbxGrantRead.EffectivePermissions(Table, "a@b.test", "t/2"));
        await Task.CompletedTask;
    }

    [Fact]
    public async Task HiddenGroups_AsksTheDirectoryThenEffectivePermissionsInOrder()
    {
        var w = HiddenGroups();
        var rows = await w.Read().ReadAsync(Now, CancellationToken.None);

        Assert.Empty(w.Unexpected);
        var asked = Entries(rows).Select(e => e.Request).ToList();
        var expected = new List<string>
        {
            DbxGrantRead.VisibilityRequest,
            DbxGrantRead.PageQuery(DbxGrantRead.UsersPath, DbxGrantRead.UserAttributes, 1),
            DbxGrantRead.PageQuery(DbxGrantRead.ServicePrincipalsPath, DbxGrantRead.ServicePrincipalAttributes, 1),
            DbxGrantRead.PageQuery(DbxGrantRead.GroupsPath, DbxGrantRead.GroupAttributes, 1),
            // The first group that reaches a grant is read by id; it is refused, so the other is not asked.
            DbxGrantRead.GroupById(InnerId),
            DbxGrantRead.CandidatesRequest,
            // One table covers both groups: the groups first, then every active identity, in ordinal order.
            DbxGrantRead.EffectivePermissions(Table, "data-leads", null),
            DbxGrantRead.EffectivePermissions(Table, "data-team", null),
            DbxGrantRead.EffectivePermissions(Table, Analyst, null),
            DbxGrantRead.EffectivePermissions(Table, Analyst, "t/2"),
            DbxGrantRead.EffectivePermissions(Table, Assistant, null),
            DbxGrantRead.EffectivePermissions(Table, Loader, null),
            DbxGrantRead.EffectivePermissions(Table, Self, null),
            DbxGrantRead.EffectivePermissions(Table, "owner@example.test", null),
            DbxGrantRead.EffectivePermissions(Table, "plain@example.test", null),
        };
        Assert.Equal(expected, asked);
        Assert.DoesNotContain(asked, a => a.Contains("gone%40example.test", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Rows_CarryTheDayASequenceTheKindTheStatusAndTheBody()
    {
        var w = HiddenGroups();
        var rows = await w.Read().ReadAsync(Now, CancellationToken.None);

        Assert.Equal(new[] { "snapshot_date", "read_id", "seq", "kind", "request", "status", "body" }, DbxGrantRead.Columns);
        Assert.All(rows, r => Assert.Equal(Day, r[0].GetString()));
        var readId = rows[0][1].GetString();
        Assert.True(Guid.TryParse(readId, out _));
        Assert.All(rows, r => Assert.Equal(readId, r[1].GetString()));
        Assert.Equal(Enumerable.Range(1, rows.Count), rows.Select(r => r[2].GetInt32()));
        var visibility = rows[0];
        Assert.Equal(("sql", "visibility", 200), (visibility[3].GetString(), visibility[4].GetString(), visibility[5].GetInt32()));
        var shipped = JsonDocument.Parse(visibility[6].GetString()!).RootElement;
        Assert.Equal(5, shipped.GetArrayLength());
        Assert.Equal("data-team", shipped[4][3].GetString());
        var refused = rows.Single(r => r[4].GetString() == DbxGrantRead.GroupById(InnerId));
        Assert.Equal(("get", 403), (refused[3].GetString(), refused[5].GetInt32()));
        Assert.Contains("PERMISSION_DENIED", refused[6].GetString());

        // Each read has its own id.
        var again = await HiddenGroups().Read().ReadAsync(Now, CancellationToken.None);
        Assert.NotEqual(readId, again[0][1].GetString());
    }

    [Fact]
    public async Task ByIdAllowed_FollowsTheMembersAndAsksNoEffectivePermissions()
    {
        var w = HiddenGroups();
        w.Answers[DbxGrantRead.GroupById(InnerId)] = Ok(Group(InnerId, "data-leads",
            $$"""{"value":"s1","$ref":"ServicePrincipals/s1"},{"value":"s2","$ref":"ServicePrincipals/s2"}"""));
        w.Answers[DbxGrantRead.GroupById(OuterId)] = Ok(Group(OuterId, "data-team",
            $$"""{"value":"s3","$ref":"ServicePrincipals/s3"},{"value":"{{InnerId}}","$ref":"Groups/{{InnerId}}"}"""));

        var rows = await w.Read().ReadAsync(Now, CancellationToken.None);

        var asked = Entries(rows).Select(e => e.Request).ToList();
        Assert.Contains(DbxGrantRead.GroupById(OuterId), asked);
        Assert.DoesNotContain(DbxGrantRead.CandidatesRequest, asked);
        Assert.DoesNotContain(asked, a => a.StartsWith(DbxGrantRead.EffectivePermissionsPath, StringComparison.Ordinal));
    }

    [Fact]
    public async Task UnknownMembers_AreReadByIdOfTheirKind()
    {
        var w = HiddenGroups();
        w.Answers[DbxGrantRead.PageQuery(DbxGrantRead.GroupsPath, DbxGrantRead.GroupAttributes, 1)] = Ok(Page(2,
            Group("1", "account users"),
            Group(OuterId, "data-team",
                """{"value":"900","$ref":"Groups/900"},{"value":"u9","$ref":"Users/u9"},{"value":"s9","$ref":"ServicePrincipals/s9"},{"value":"u1","$ref":"Users/u1"}""")));
        w.Answers[DbxGrantRead.GroupById("900")] = Ok(Group("900", "nested", """{"value":"s1","$ref":"ServicePrincipals/s1"}"""));
        w.Answers[DbxGrantRead.IdentityById("u9", true)] = Ok(Identity("u9", "userName", "late@example.test"));
        w.Answers[DbxGrantRead.IdentityById("s9", false)] = Ok(Identity("s9", "applicationId", "00000000-0000-0000-0000-0000000000a9"));
        w.Answers[DbxGrantRead.ByNameQuery("data-leads")] = Ok(Page(0));

        var rows = await w.Read().ReadAsync(Now, CancellationToken.None);

        var asked = Entries(rows).Select(e => e.Request).ToList();
        Assert.Contains(DbxGrantRead.ByNameQuery("data-leads"), asked);       // granted, not listed
        Assert.Contains(DbxGrantRead.GroupById("900"), asked);              // a member group the listing did not return
        Assert.Contains(DbxGrantRead.IdentityById("u9", true), asked);      // a user the listing did not return
        Assert.Contains(DbxGrantRead.IdentityById("s9", false), asked);     // a service principal likewise
        Assert.DoesNotContain(DbxGrantRead.IdentityById("u1", true), asked); // listed: not read again
        Assert.DoesNotContain(DbxGrantRead.ByNameQuery("data-team"), asked); // listed: not looked up
    }

    [Fact]
    public async Task AListingRefused_EndsTheRead()
    {
        var w = HiddenGroups();
        w.Answers[DbxGrantRead.PageQuery(DbxGrantRead.ServicePrincipalsPath, DbxGrantRead.ServicePrincipalAttributes, 1)] =
            new(403, """{"error_code":"PERMISSION_DENIED"}""");

        var rows = await w.Read().ReadAsync(Now, CancellationToken.None);

        var entries = Entries(rows);
        Assert.Equal(3, entries.Count);
        Assert.Equal(403, entries[^1].Status);
        Assert.DoesNotContain(entries, e => e.Request.StartsWith(DbxGrantRead.GroupsPath, StringComparison.Ordinal));
    }

    [Fact]
    public async Task Listing_PagesByWhatEachPageHeld()
    {
        var w = HiddenGroups();
        var first = Enumerable.Range(0, 100).Select(i => Identity($"x{i}", "userName", $"user{i}@example.test")).ToArray();
        w.Answers[DbxGrantRead.PageQuery(DbxGrantRead.UsersPath, DbxGrantRead.UserAttributes, 1)] = Ok(Page(101, first));
        w.Answers[DbxGrantRead.PageQuery(DbxGrantRead.UsersPath, DbxGrantRead.UserAttributes, 101)] =
            Ok(Page(101, Identity("x100", "userName", "user100@example.test")));

        var rows = await w.Read().ReadAsync(Now, CancellationToken.None);

        var asked = Entries(rows).Select(e => e.Request).ToList();
        Assert.Contains(DbxGrantRead.PageQuery(DbxGrantRead.UsersPath, DbxGrantRead.UserAttributes, 101), asked);
        Assert.DoesNotContain(DbxGrantRead.PageQuery(DbxGrantRead.UsersPath, DbxGrantRead.UserAttributes, 102), asked);
    }

    [Fact]
    public async Task EffectivePermissions_StopAtTheFirstRefusal()
    {
        var w = HiddenGroups();
        w.Answers[DbxGrantRead.EffectivePermissions(Table, "data-team", null)] = new(403, """{"error_code":"PERMISSION_DENIED"}""");

        var rows = await w.Read().ReadAsync(Now, CancellationToken.None);

        var effective = Entries(rows).Where(e => e.Request.StartsWith(DbxGrantRead.EffectivePermissionsPath, StringComparison.Ordinal)).ToList();
        Assert.Equal(2, effective.Count);
        Assert.Equal(403, effective[^1].Status);
    }

    [Fact]
    public async Task EffectivePermissions_NotStartedBeyondTheCallBound()
    {
        var w = HiddenGroups();
        var many = Enumerable.Range(0, 1000).Select(i => Identity($"y{i}", "userName", $"many{i}@example.test")).ToArray();
        w.Answers[DbxGrantRead.PageQuery(DbxGrantRead.UsersPath, DbxGrantRead.UserAttributes, 1)] = Ok(Page(1000, many[..100]));
        for (var start = 101; start <= 901; start += 100)
        {
            w.Answers[DbxGrantRead.PageQuery(DbxGrantRead.UsersPath, DbxGrantRead.UserAttributes, start)] =
                Ok(Page(1000, many[(start - 1)..(start + 99)]));
        }

        var rows = await w.Read().ReadAsync(Now, CancellationToken.None);

        var entries = Entries(rows);
        Assert.Contains(entries, e => e.Request == DbxGrantRead.CandidatesRequest);
        Assert.DoesNotContain(entries, e => e.Request.StartsWith(DbxGrantRead.EffectivePermissionsPath, StringComparison.Ordinal));
    }

    [Fact]
    public async Task TheCandidatesStatementFailing_IsRecordedAndEndsTheRead()
    {
        var w = HiddenGroups();
        w.Statements.Remove(Candidates);

        var rows = await w.Read().ReadAsync(Now, CancellationToken.None);

        var last = Entries(rows)[^1];
        Assert.Equal(("sql", DbxGrantRead.CandidatesRequest, 0), last);
    }

    [Fact]
    public async Task TheVisibilityStatementFailing_FailsTheTask()
    {
        var w = HiddenGroups();
        w.Statements.Remove(Visibility);

        await Assert.ThrowsAsync<SourceException>(() => w.Read().ReadAsync(Now, CancellationToken.None));
        Assert.DoesNotContain(w.Asked, a => a.StartsWith("/", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Lookups_StopAtTheBound()
    {
        var w = HiddenGroups();
        w.Statements[Visibility] = Enumerable.Range(0, 500).Select(i => Row(Day, null, "1", $"granted-{i:D3}"))
            .Prepend(Row(Day, "main", "1", null)).ToArray();
        for (var i = 0; i < 500; i++)
        {
            w.Answers[DbxGrantRead.ByNameQuery($"granted-{i:D3}")] = Ok(Page(0));
        }

        var rows = await w.Read().ReadAsync(Now, CancellationToken.None);

        var byName = Entries(rows).Count(e => e.Request.Contains("filter=", StringComparison.Ordinal));
        Assert.Equal(400, byName);
    }

    [Fact]
    public async Task SpecialGroups_AreNotAskedThroughEffectivePermissions()
    {
        // A granted workspace admins name makes `admins` reach a grant; it shows no members and is refused by id like
        // the others, but its members are not read this way, so it is never asked about.
        var w = HiddenGroups();
        w.Statements[Visibility] = w.Statements[Visibility].Append(Row(Day, null, "1", "_workspace_admins_workspace_123")).ToArray();
        w.Statements[Candidates] = w.Statements[Candidates].Append(Row("admins", Table)).ToArray();

        var rows = await w.Read().ReadAsync(Now, CancellationToken.None);

        var asked = Entries(rows).Select(e => e.Request).ToList();
        Assert.Contains(DbxGrantRead.EffectivePermissions(Table, "data-leads", null), asked);
        Assert.DoesNotContain(DbxGrantRead.EffectivePermissions(Table, "admins", null), asked);
    }

    [Fact]
    public void Cover_PicksTheFewestTablesThenByName()
    {
        var candidates = new[] { ("a", "t2"), ("b", "t2"), ("a", "t1"), ("c", "t3"), ("b", "t1") };
        Assert.Equal(new[] { "t1", "t3" }, DbxGrantRead.Cover(new[] { "a", "b", "c" }, candidates));
        Assert.Empty(DbxGrantRead.Cover(new[] { "z" }, candidates));
    }

    [Fact]
    public void Statements_ArePinnedApartFromTheExcludedCatalogs()
    {
        var none = new EndpointConfig();
        Assert.Equal(Collapse(PinnedVisibility), Collapse(DbxRawSelects.GrantVisibility(none)));
        Assert.Equal(Collapse(PinnedCandidates), Collapse(DbxRawSelects.GrantCandidates(none)));
        // information_schema is no customer table: its views' owner is a system name, not a group.
        Assert.Equal(3, DbxRawSelects.GrantVisibility(none).Split("table_schema <> 'information_schema'").Length - 1);
    }

    /// <summary>The read's upload, byte for byte: part of the protocol, so a change shows here first.</summary>
    [Fact]
    public async Task TheHiddenGroupsRead_UploadIsPinned()
    {
        var rows = await HiddenGroups().Read().ReadAsync(Now, CancellationToken.None, ReadId);
        var fixture = File.ReadAllText(Path.Combine(RepoRoot(), "tests", "Trailox.Agent.Tests", "Fixtures", "dbx-grant-read-hidden-groups.ndjson"));
        Assert.Equal(fixture.Replace("\r\n", "\n"), await UploadedAsync(rows));
    }

    [Fact]
    public void Statements_AskAboutThisPrincipalAndKeepTheExcludedCatalogsOut()
    {
        var endpoint = new EndpointConfig { ExcludedDatabases = new List<string> { "scratch" } };
        var visibility = DbxRawSelects.GrantVisibility(endpoint);
        Assert.Contains("is_account_group_member(grantee) OR is_member(grantee)", visibility);
        Assert.Contains("'scratch'", visibility);
        // Direct grants only, in each of the three privilege views the granted groups come from.
        Assert.Equal(3, visibility.Split("upper(COALESCE(inherited_from, 'NONE')) = 'NONE'").Length - 1);
        Assert.Contains("metastore_privileges", visibility);
        var candidates = DbxRawSelects.GrantCandidates(endpoint);
        Assert.Contains("rn <= 20", candidates);
        Assert.Contains("'scratch'", candidates);
        var other = DbxRawSelects.CatalogOtherGrants(endpoint);
        foreach (var kind in new[] { "'catalog_privilege'", "'catalog_owner'", "'schema_owner'", "'table_owner'" })
        {
            Assert.Contains(kind, other);
        }
        Assert.Equal(other, DbxRawSelects.ForTask(endpoint, new AgentTask { Stream = ProtocolInfo.Streams.CatalogOtherGrants }));
    }
}
