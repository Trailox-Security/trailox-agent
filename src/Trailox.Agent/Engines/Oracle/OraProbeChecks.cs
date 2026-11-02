using System.Globalization;
using System.Text.RegularExpressions;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Engines.Oracle;

/// <summary>One partition of the trail: its end comes from the dictionary's <c>HIGH_VALUE</c>.</summary>
public sealed record OraPartition(string Name, DateTime? EndUtc, string Tablespace);

/// <summary>A tablespace's files and free space. What it can still take counts what its files may grow to.</summary>
public sealed record OraSpace(long FileBytes, long MaxBytes, long FreeBytes)
{
    public long UsedBytes => FileBytes - FreeBytes;

    public long RoomBytes => MaxBytes - FileBytes + FreeBytes;
}

/// <summary>What the probe read from the server, as plain values. A null means it could not be read.</summary>
public sealed class OraFacts
{
    public DateTime NowUtc { get; set; }
    public string Kind { get; set; } = "";
    public string Host { get; set; } = "";
    public string Product { get; set; } = "";
    public string Version { get; set; } = "";
    public int Major { get; set; }
    public string OwnUser { get; set; } = "";
    public string Program { get; set; } = "";
    public string Container { get; set; } = "";
    public string? AuditSessionId { get; set; }
    public bool RdsAdmin { get; set; }
    public string? UnifiedOption { get; set; }
    public Dictionary<string, string?> Parameters { get; } = new(StringComparer.OrdinalIgnoreCase);
    public bool PollReadable { get; set; }

    /// <summary>The objects the agent has no grant on, by the name a grant takes.</summary>
    public SortedSet<string> Unreadable { get; } = new(StringComparer.Ordinal);

    /// <summary>The objects that did not answer in time: nothing is known about them, a grant included.</summary>
    public SortedSet<string> Slow { get; } = new(StringComparer.Ordinal);

    public bool? Partitioned { get; set; }
    public long? TrailRows { get; set; }
    public long? TrailBlocks { get; set; }
    public string? Interval { get; set; }
    public string? DefaultTablespace { get; set; }
    public string? ConfiguredTablespace { get; set; }
    public IReadOnlyList<OraPartition> Partitions { get; set; } = Array.Empty<OraPartition>();
    public OraSpace? Space { get; set; }

    /// <summary>What the trail grew by in the last 24 hours, projected to a day.</summary>
    public long? GrowthBytesPerDay { get; set; }
    public bool? Spilled { get; set; }
    public bool SourceColumn { get; set; }
    public List<OraclePolicy> Policies { get; set; } = new();
    public bool PoliciesRead { get; set; }
    public List<OraclePolicyObject> PolicyObjects { get; set; } = new();
    public bool PolicyObjectsRead { get; set; }
    public Dictionary<string, string?>? Limits { get; set; }
    public DateTime? PasswordExpiry { get; set; }
    public int? OldAuditSettings { get; set; }
    public bool? PurgeJob { get; set; }
}

/// <summary>
/// What the probe tells the customer to fix: each check is ok, warn or fail, with what to change. All of it decides on
/// plain values (<see cref="OraFacts"/>). 🔴 A FAILED CHECK REFUSES THE SOURCE: <see cref="OraSource"/> then runs no poll,
/// which only a source that could never return a record must meet. The size of a day fails nothing: a busy trail is read
/// less often.
/// </summary>
public static class OraProbeChecks
{
    /// <summary>The days the trail's tablespace must hold: the longest catch-up and one more.</summary>
    public const int RoomDays = 8;

    private const int ExpiryWarnDays = 14;

    public const string LevelLogsOnly = "logs_only", LevelPartial = "partial", LevelSelective = "selective", LevelFull = "full";

    private static readonly string[] BasePolicies = { "TRAILOX_LOGON", "TRAILOX_DDL", "TRAILOX_ANYPRIV", "TRAILOX_DATAPUMP" };

    /// <summary>The policies known to write a failed sign-in: the two an installation brings, by release, and ours.</summary>
    private static readonly string[] FailedSignInPolicies = { "ORA_LOGON_FAILURES", "ORA_LOGIN_LOGOUT", "TRAILOX_LOGON" };

    private static readonly Regex RdsHost = new(@"\.rds\.amazonaws\.com(\.cn)?\z", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex Editions = new("Enterprise Edition|Standard Edition 2|Standard Edition|Free|Express Edition|Personal Edition", RegexOptions.Compiled);

    /// <summary>
    /// Every check, in the order the source page shows them. The first ones can refuse the source, and the rest say what
    /// would make it better.
    /// </summary>
    public static IEnumerable<OracleCheck> All(OraFacts f)
    {
        yield return Container(f.Container);
        yield return Platform(f.Kind, f.Host, f.RdsAdmin);
        yield return PollView(f.PollReadable, f.OwnUser);
        yield return Trail(f.Parameters.GetValueOrDefault("audit_trail"), f.AuditSessionId, Platform(f.Host, f.RdsAdmin));
        yield return Views(f.Unreadable, f.Slow, f.OwnUser);
        yield return Partition(f);
        yield return Room(f);
        yield return Spillover(f.Spilled);
        yield return FailedSignIns(f.PoliciesRead, f.Policies, f.Major);
        yield return DefaultPolicies(f.PoliciesRead, f.Policies);
        yield return Level(f.PoliciesRead, f.Policies, f.PolicyObjects.Count);
        yield return Profile(f.Limits);
        yield return ResourceLimit(f.Parameters.GetValueOrDefault("resource_limit"));
        yield return SourceColumn(f.SourceColumn);
        yield return OldSettings(f.UnifiedOption, f.OldAuditSettings);
        yield return PurgeJob(f.PurgeJob);
        yield return PasswordExpiry(f.PasswordExpiry, f.NowUtc);
    }

    /// <summary>The source's <c>caps.oracle</c>: the facts as the check-in reports them, with the checks.</summary>
    public static OracleCaps Caps(OraFacts f, IReadOnlyList<OracleCheck> checks)
    {
        var today = Today(f);
        return new OracleCaps
        {
            Platform = Platform(f.Host, f.RdsAdmin),
            Product = f.Product,
            Edition = Editions.Match(f.Product).Value,
            Container = f.Container,
            OwnUser = f.OwnUser,
            OwnProgram = f.Program,
            ProbedAtMicros = (f.NowUtc - DateTime.UnixEpoch).Ticks / 10,
            UnifiedMode = f.UnifiedOption switch { "TRUE" => "pure", "FALSE" => "mixed", _ => "" },
            AuditTrail = f.Parameters.GetValueOrDefault("audit_trail") ?? "",
            TrailPartitioned = f.Partitioned,
            TrailInterval = f.Interval ?? "",
            TrailRows = f.TrailRows,
            TrailBlocks = f.TrailBlocks,
            TodayPartition = today?.Name ?? "",
            TodayPartitionEndMicros = today?.EndUtc is { } end ? (end - DateTime.UnixEpoch).Ticks / 10 : 0,
            TodayTablespace = TrailTablespace(f) ?? "",
            ConfiguredTablespace = f.ConfiguredTablespace ?? "",
            RoomBytes = f.Space?.RoomBytes,
            GrowthBytesPerDay = f.GrowthBytesPerDay,
            Spilled = f.Spilled,
            SourceColumn = f.SourceColumn,
            ObservedLevel = f.PoliciesRead ? ObservedLevel(f.Policies) : "",
            PoliciesRead = f.PoliciesRead,
            Policies = f.Policies.Where(p => p.Name.StartsWith("TRAILOX_", StringComparison.Ordinal)).ToList(),
            PolicyObjectsRead = f.PolicyObjectsRead,
            PolicyObjects = f.PolicyObjects,
            Checks = checks.ToList(),
        };
    }

    /// <summary>
    /// <c>rds</c> or <c>self_managed</c>, by what the server answers as well as by its host name: an RDS instance behind a
    /// name of the customer's still has RDS's own user.
    /// </summary>
    internal static string Platform(string host, bool rdsAdmin) => rdsAdmin || RdsHost.IsMatch(host) ? "rds" : "self_managed";

    public static OracleCheck Platform(string kind, string host, bool rdsAdmin)
    {
        var platform = Platform(host, rdsAdmin);
        return (kind == "cloud") == (platform == "rds")
            ? Ok("platform", platform == "rds" ? "Amazon RDS for Oracle" : "a self-managed server")
            : Warn("platform", $"kind is {kind} in agent.yaml, but the server looks like {(platform == "rds" ? "Amazon RDS for Oracle" : "a self-managed server")}: "
                + "set kind to cloud for RDS and onprem otherwise");
    }

    public static OracleCheck Container(string container) => container == "CDB$ROOT"
        ? Fail("container", "the agent signed in to the CDB root, which holds no application's records: set options.service_name to the PDB's service")
        : Ok("container", $"the agent reads {container}");

    public static OracleCheck PollView(bool readable, string user) => readable
        ? Ok("poll_view", "the agent reads the audit trail")
        : Fail("poll_view", $"the agent cannot read the audit trail: GRANT SELECT ON AUDSYS.UNIFIED_AUDIT_TRAIL TO {user}");

    /// <summary>Whether the trail writes at all. With no audit session ID, no policy on this server will ever write a record.</summary>
    public static OracleCheck Trail(string? auditTrail, string? sessionId, string platform)
    {
        if (!string.IsNullOrEmpty(sessionId) && sessionId != "0")
        {
            return Ok("audit_trail", "this server writes audit records");
        }
        return Fail("audit_trail", platform == "rds"
            ? $"this instance writes no audit record (AUDIT_TRAIL is {auditTrail ?? "unknown"}, and the agent's session has no audit session ID): "
                + "give it a parameter group of your own with audit_trail set to DB (a default group cannot be changed or copied, "
                + "so create one in the same family; copy a group of your own), attach it and reboot once"
            : $"this server writes no audit record (AUDIT_TRAIL is {auditTrail ?? "unknown"}, and the agent's session has no audit session ID): "
                + "set audit_trail to DB (one restart of the database) or link the server with unified auditing");
    }

    /// <param name="unreadable">The views the server refused: a grant fixes those.</param>
    /// <param name="slow">The views that did not answer in time: no grant fixes those, and a later probe may read them.</param>
    /// <param name="user">The agent's user.</param>
    public static OracleCheck Views(IReadOnlyCollection<string> unreadable, IReadOnlyCollection<string> slow, string user)
    {
        var problems = new List<string>();
        if (unreadable.Count > 0)
        {
            problems.Add($"the agent cannot read {string.Join(", ", unreadable)}, so the checks that use them say nothing: "
                + $"grant SELECT on each to {user} (on RDS, through rdsadmin.rdsadmin_util.grant_sys_object)");
        }
        if (slow.Count > 0)
        {
            problems.Add($"{string.Join(", ", slow)} did not answer in time, so the checks that use them say nothing until a later probe");
        }
        return problems.Count == 0 ? Ok("views", "the agent reads every view it uses") : Warn("views", string.Join("; ", problems));
    }

    /// <summary>The partition that holds today's records; null when the day's first record has not opened it yet.</summary>
    public static OraPartition? Today(OraFacts f) =>
        f.Partitions.Where(p => p.EndUtc > f.NowUtc).OrderBy(p => p.EndUtc).FirstOrDefault();

    /// <summary>
    /// Where today's records go: today's partition, or, until the day's first record opens it, where a new partition goes,
    /// which is the table's own default unless that says to use the trail's setting.
    /// </summary>
    public static string? TrailTablespace(OraFacts f) => Today(f)?.Tablespace
        ?? (f.DefaultTablespace is { Length: > 0 } and not "DEFAULT" ? f.DefaultTablespace : f.ConfiguredTablespace);

    /// <summary>
    /// A poll reads all of today's partition. A monthly one means the month to date, and a location set for the trail
    /// applies only from the next partition, so until this one ends its records stay where they are.
    /// </summary>
    public static OracleCheck Partition(OraFacts f)
    {
        if (f.Partitioned == false)
        {
            return Warn("partition", "the trail is not partitioned, so each poll reads all of it");
        }
        if (f.Interval == null)
        {
            return Warn("partition", "the trail's partitioning cannot be read");
        }
        var problems = new List<string>();
        if (!f.Interval.Contains("DAY", StringComparison.OrdinalIgnoreCase))
        {
            problems.Add($"the trail's interval is {f.Interval}: run DBMS_AUDIT_MGMT.ALTER_PARTITION_INTERVAL(1, 'DAY')");
        }
        if (Today(f) is { } today)
        {
            var end = today.EndUtc!.Value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            if (today.EndUtc - f.NowUtc > TimeSpan.FromDays(1))
            {
                problems.Add($"today's partition {today.Name} is not daily: until it ends on {end}, each poll reads all of it");
            }
            if (f.ConfiguredTablespace is { Length: > 0 } configured && today.Tablespace != configured)
            {
                problems.Add($"today's partition is in {today.Tablespace}, not in {configured}, until it ends on {end}");
            }
        }
        return problems.Count == 0 ? Ok("partition", "the trail has daily partitions") : Warn("partition", string.Join("; ", problems));
    }

    public static OracleCheck Room(OraFacts f)
    {
        var tablespace = TrailTablespace(f);
        if (tablespace == null || f.Space is not { } space)
        {
            return Warn("room", "the trail's tablespace and its room cannot be read");
        }
        var problems = new List<string>();
        if (tablespace == "SYSAUX")
        {
            problems.Add("the trail is in SYSAUX: when SYSAUX fills, every record goes to spillover files and every audited statement slows sharply. "
                + "Give it a tablespace of its own with DBMS_AUDIT_MGMT.SET_AUDIT_TRAIL_LOCATION");
        }
        if (f.GrowthBytesPerDay is { } growth and > 0 && space.RoomBytes < RoomDays * growth)
        {
            problems.Add($"{tablespace} has room for {Gb(space.RoomBytes)} and the trail grows about {Gb(growth)} a day: it needs room for {RoomDays} days");
        }
        return problems.Count == 0 ? Ok("room", $"{tablespace} has room for {Gb(space.RoomBytes)}") : Warn("room", string.Join("; ", problems));
    }

    public static OracleCheck Spillover(bool? spilled) => spilled switch
    {
        null => Warn("spillover", "the spillover files cannot be looked at"),
        true => Warn("spillover", "a record sits in a spillover file, so every poll reads those files, slowly. If the tablespace is full, every audited "
            + "statement is slowing too: make room, then run DBMS_AUDIT_MGMT.LOAD_UNIFIED_AUDIT_FILES. A read-only standby always spills"),
        false => Ok("spillover", "no record in a spillover file"),
    };

    /// <summary>
    /// Whether a failed sign-in reaches the trail. No record says so, because a quiet trail looks the same as a deaf one: only
    /// the enabled policies can tell. An upgraded database has neither of the two an installation enables, and from 23ai the
    /// failures' has another name.
    /// </summary>
    public static OracleCheck FailedSignIns(bool read, IReadOnlyList<OraclePolicy> policies, int major)
    {
        if (!read)
        {
            return Warn("failed_signins", "the enabled policies cannot be read, so it cannot be told whether failed sign-ins are audited");
        }
        var enabled = policies.Where(p => FailedSignInPolicies.Contains(p.Name, StringComparer.Ordinal) && p.Failure == "YES").ToList();
        var fix = $"run AUDIT POLICY {(major >= 23 ? "ORA_LOGIN_LOGOUT" : "ORA_LOGON_FAILURES")} WHENEVER NOT SUCCESSFUL";
        if (enabled.Count == 0)
        {
            return Warn("failed_signins", $"no policy the agent knows audits failed sign-ins, so they may never reach the trail: {fix}");
        }
        return enabled.FirstOrDefault(CoversEveryone) is { } all
            ? Ok("failed_signins", $"failed sign-ins are audited ({all.Name})")
            : Warn("failed_signins", $"failed sign-ins are audited only for {string.Join(", ", enabled.Select(p => p.EntityName))}: {fix}");
    }

    /// <summary>Enabled for all users, or for all but those it names.</summary>
    private static bool CoversEveryone(OraclePolicy policy) =>
        policy.EntityName == "ALL USERS" || policy.EnabledOption.StartsWith("EXCEPT", StringComparison.OrdinalIgnoreCase);

    /// <summary>The policy for security changes that an installation enables by itself: an upgraded database does not have it.</summary>
    public static OracleCheck DefaultPolicies(bool read, IReadOnlyList<OraclePolicy> policies)
    {
        if (!read)
        {
            return Warn("default_policies", "the enabled policies cannot be read");
        }
        return policies.Any(p => p.Name == "ORA_SECURECONFIG")
            ? Ok("default_policies", "security changes are audited")
            : Warn("default_policies", "run AUDIT POLICY ORA_SECURECONFIG: until then those events are not in the trail");
    }

    /// <summary>Which audit level the enabled TRAILOX_* policies make up.</summary>
    public static string ObservedLevel(IEnumerable<OraclePolicy> policies)
    {
        var enabled = policies.Select(p => p.Name).ToHashSet(StringComparer.Ordinal);
        var parts = BasePolicies.Count(enabled.Contains);
        if (enabled.Contains("TRAILOX_FULL"))
        {
            return parts == BasePolicies.Length ? LevelFull : LevelPartial;
        }
        if (parts == BasePolicies.Length)
        {
            return LevelSelective;
        }
        return parts > 0 || enabled.Contains("TRAILOX_TABLES") || enabled.Contains("TRAILOX_PERSONS") ? LevelPartial : LevelLogsOnly;
    }

    /// <param name="read">The enabled policies could be read. Without that an empty list says nothing: no level can be told.</param>
    public static OracleCheck Level(bool read, IReadOnlyList<OraclePolicy> policies, int objects = 0)
    {
        if (!read)
        {
            return Warn("level", "the enabled policies cannot be read, so the audit level cannot be told");
        }
        var level = ObservedLevel(policies);
        if (objects >= OraSql.MaxPolicyObjects)
        {
            return Warn("level", $"the TRAILOX_ policies name more than {OraSql.MaxPolicyObjects} objects, and only that many are reported");
        }
        var ours = policies.Where(p => p.Name.StartsWith("TRAILOX_", StringComparison.Ordinal))
            .Select(p => $"{p.Name} {p.EnabledOption} {p.EntityName}").ToList();
        var message = ours.Count == 0 ? "no TRAILOX_ policy is enabled: the trail holds what the default policies write" : "enabled: " + string.Join(", ", ours);
        return level == LevelPartial
            ? Warn("level", $"only part of a level is enabled, so some events are not audited ({message})")
            : Ok("level", message);
    }

    /// <summary>The server's own bounds on the agent's sessions, so that a read can never run away.</summary>
    public static OracleCheck Profile(IReadOnlyDictionary<string, string?>? limits)
    {
        if (limits == null)
        {
            return Warn("profile", "the agent's limits cannot be read");
        }
        var problems = new List<string>();
        if (limits.GetValueOrDefault("SESSIONS_PER_USER") != "2")
        {
            problems.Add($"SESSIONS_PER_USER is {limits.GetValueOrDefault("SESSIONS_PER_USER") ?? "not set"}, not 2");
        }
        foreach (var limit in new[] { "CPU_PER_CALL", "LOGICAL_READS_PER_CALL", "IDLE_TIME" })
        {
            if (!long.TryParse(limits.GetValueOrDefault(limit), NumberStyles.None, CultureInfo.InvariantCulture, out _))
            {
                problems.Add($"{limit} has no limit");
            }
        }
        return problems.Count == 0
            ? Ok("profile", "the agent's profile is in force")
            : Warn("profile", $"the agent's user does not have its profile: {string.Join(", ", problems)}. Run the setup script's profile step");
    }

    public static OracleCheck ResourceLimit(string? value) => string.Equals(value, "TRUE", StringComparison.OrdinalIgnoreCase)
        ? Ok("resource_limit", "the server enforces the agent's limits")
        : Warn("resource_limit", $"RESOURCE_LIMIT is {value ?? "unknown"}, so the server enforces none of the profile's limits on the agent's sessions: set it to TRUE");

    public static OracleCheck SourceColumn(bool present) => Ok("source_column", present
        ? "the trail says which records come from spillover files"
        : "this release's trail does not say which records come from spillover files (it does from 19.21)");

    public static OracleCheck OldSettings(string? unifiedOption, int? overlapping)
    {
        if (overlapping == null)
        {
            return Warn("old_audit_settings", "the old audit settings cannot be read");
        }
        return unifiedOption == "FALSE" && overlapping > 0
            ? Warn("old_audit_settings", $"{overlapping} old AUDIT settings are in force beside the policies, in mixed mode, so some actions may be written twice and to SYS.AUD$ too: "
                + "list them in DBA_STMT_AUDIT_OPTS and DBA_PRIV_AUDIT_OPTS")
            : Ok("old_audit_settings", "no old audit setting overlaps the policies");
    }

    /// <summary>Whether a job purges the trail. It is reported, never a warning: whether to purge is the customer's call.</summary>
    public static OracleCheck PurgeJob(bool? present) => present switch
    {
        null => Warn("purge_job", "the trail's purge jobs cannot be read"),
        false => Ok("purge_job", "no job purges the unified trail"),
        true => Ok("purge_job", "a job purges the unified trail"),
    };

    public static OracleCheck PasswordExpiry(DateTime? expiry, DateTime now) =>
        expiry is { } at && at - now < TimeSpan.FromDays(ExpiryWarnDays)
            ? Warn("password_expiry", $"the agent's password expires on {at.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}: "
                + "set a new one, give it to the agent and restart the agent")
            : Ok("password_expiry", expiry == null ? "the agent's password does not expire" : "the agent's password is not near its expiry");

    private static string Gb(long bytes) => (bytes / 1e9).ToString("0.#", CultureInfo.InvariantCulture) + " GB";

    private static OracleCheck Ok(string name, string message) => new() { Name = name, Status = "ok", Message = message };

    private static OracleCheck Warn(string name, string message) => new() { Name = name, Status = "warn", Message = message };

    private static OracleCheck Fail(string name, string message) => new() { Name = name, Status = "fail", Message = message };
}
