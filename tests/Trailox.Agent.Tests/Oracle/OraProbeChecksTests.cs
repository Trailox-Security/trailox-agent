using Trailox.Agent.Engines.Oracle;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Tests.Oracle;

/// <summary>Each of the probe's checks, on plain values: what passes, what warns with what to change, and what refuses the source.</summary>
public class OraProbeChecksTests
{
    private static readonly DateTime Now = new(2026, 10, 22, 6, 0, 0, DateTimeKind.Utc);

    private static OraFacts Facts() => new()
    {
        NowUtc = Now,
        Kind = "cloud",
        Host = "ledger.abc123def456.eu-west-1.rds.amazonaws.com",
        Product = "Oracle Database 19c Standard Edition 2 Release 19.0.0.0.0 - Production",
        Version = "19.32.0.0.0",
        Major = 19,
        OwnUser = "TRAILOX_MONITOR",
        Program = "trailox-agent.dll",
        Container = "LEDGER",
        AuditSessionId = "2828371651",
        RdsAdmin = true,
        UnifiedOption = "FALSE",
        PollReadable = true,
        Partitioned = true,
        Interval = "NUMTODSINTERVAL(1, 'DAY')",
        ConfiguredTablespace = "TRAILOX_AUDIT",
        Partitions = new[]
        {
            new OraPartition("SYS_P181", new DateTime(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc), "SYSAUX"),
            new OraPartition("SYS_P241", new DateTime(2026, 10, 23, 0, 0, 0, DateTimeKind.Utc), "TRAILOX_AUDIT"),
        },
        Space = new OraSpace(10_000_000_000, 100_000_000_000, 1_000_000_000),
        Spilled = false,
        PoliciesRead = true,
        Policies = new List<OraclePolicy> { Failures(), new() { Name = "ORA_SECURECONFIG" } },
        Limits = new Dictionary<string, string?> { ["SESSIONS_PER_USER"] = "2", ["CPU_PER_CALL"] = "6000", ["IDLE_TIME"] = "10", ["LOGICAL_READS_PER_CALL"] = "300000" },
        OldAuditSettings = 0,
        PurgeJob = true,
        Parameters = { ["audit_trail"] = "DB", ["resource_limit"] = "TRUE" },
    };

    private static OraclePolicy Failures(string name = "ORA_LOGON_FAILURES", string failure = "YES", string option = "BY USER", string entity = "ALL USERS") =>
        new() { Name = name, Failure = failure, EnabledOption = option, EntityName = entity };

    private static OracleCheck Check(OraFacts f, string name) => Assert.Single(OraProbeChecks.All(f), c => c.Name == name);

    [Fact]
    public void A_source_set_up_as_printed_passes_every_check()
    {
        Assert.All(OraProbeChecks.All(Facts()), c => Assert.Equal("ok", c.Status));
    }

    [Theory]
    [InlineData("CDB$ROOT", "fail")]
    [InlineData("LEDGER", "ok")]
    public void The_cdb_root_is_refused(string container, string status)
    {
        var check = OraProbeChecks.Container(container);
        Assert.Equal(status, check.Status);
        if (status == "fail")
        {
            Assert.Contains("service_name", check.Message);
        }
    }

    [Theory]
    [InlineData("0")]
    [InlineData("")]
    [InlineData(null)]
    public void A_session_with_no_audit_session_id_fails_and_on_rds_names_the_parameter_group(string? sessionId)
    {
        var check = OraProbeChecks.Trail("NONE", sessionId, "rds");
        Assert.Equal("fail", check.Status);
        Assert.Contains("parameter group", check.Message);
        Assert.Contains("NONE", check.Message);
        Assert.Equal("fail", OraProbeChecks.Trail("NONE", sessionId, "self_managed").Status);
        Assert.DoesNotContain("parameter group", OraProbeChecks.Trail("NONE", sessionId, "self_managed").Message);
    }

    [Fact]
    public void The_poll_view_is_a_failure_and_any_other_view_a_warning_that_names_the_grant()
    {
        var f = Facts();
        f.PollReadable = false;
        Assert.Equal("fail", Check(f, "poll_view").Status);
        Assert.Contains("GRANT SELECT ON AUDSYS.UNIFIED_AUDIT_TRAIL TO TRAILOX_MONITOR", Check(f, "poll_view").Message);

        f = Facts();
        f.Unreadable.Add("DBA_PART_TABLES");
        Assert.Equal("warn", Check(f, "views").Status);
        Assert.Contains("DBA_PART_TABLES", Check(f, "views").Message);
    }

    /// <summary>No grant helps a view that was too slow, so the message must not send the customer to look for one.</summary>
    [Fact]
    public void A_view_that_did_not_answer_warns_with_no_grant_to_give()
    {
        var f = Facts();
        f.Slow.Add("DBA_FREE_SPACE");
        var check = Check(f, "views");
        Assert.Equal("warn", check.Status);
        Assert.Contains("DBA_FREE_SPACE did not answer in time", check.Message);
        Assert.DoesNotContain("grant", check.Message, StringComparison.OrdinalIgnoreCase);

        f.Unreadable.Add("DBA_PART_TABLES");
        Assert.Contains("grant SELECT", Check(f, "views").Message);
        Assert.Contains("DBA_FREE_SPACE did not answer in time", Check(f, "views").Message);
    }

    /// <summary>Size is no reason to refuse: a busy trail is read less often. Only a source that could never return a record fails.</summary>
    [Fact]
    public void The_size_of_a_day_fails_no_check()
    {
        var f = Facts();
        f.GrowthBytesPerDay = 1_000_000_000_000;
        var checks = OraProbeChecks.All(f).ToList();
        Assert.DoesNotContain(checks, c => c.Status == "fail");
        Assert.DoesNotContain(checks, c => c.Name == "day_limit");
    }

    [Fact]
    public void An_unpartitioned_trail_says_so()
    {
        var f = Facts();
        f.Partitioned = false;
        f.Interval = null;
        Assert.Contains("not partitioned", Check(f, "partition").Message);
    }

    [Fact]
    public void Little_room_warns()
    {
        var f = Facts();
        f.Space = new OraSpace(10_000_000_000, 10_000_000_000, 1_000_000_000);
        f.GrowthBytesPerDay = 1_000_000_000;
        Assert.Equal("warn", Check(f, "room").Status);
        Assert.Contains("8 days", Check(f, "room").Message);
        Assert.DoesNotContain("purge", Check(f, "room").Message, StringComparison.OrdinalIgnoreCase);
        f.GrowthBytesPerDay = 100_000_000;
        Assert.Equal("ok", Check(f, "room").Status);
    }

    [Fact]
    public void The_room_counts_what_the_files_may_grow_to()
    {
        Assert.Equal(91_000_000_000, new OraSpace(10_000_000_000, 100_000_000_000, 1_000_000_000).RoomBytes);
    }

    [Fact]
    public void A_trail_in_sysaux_warns()
    {
        var f = Facts();
        f.ConfiguredTablespace = "SYSAUX";
        f.Partitions = new[] { new OraPartition("SYS_P241", Now.AddHours(18), "SYSAUX") };
        Assert.Equal("warn", Check(f, "room").Status);
        Assert.Contains("SYSAUX", Check(f, "room").Message);
    }

    [Fact]
    public void A_monthly_partition_warns_with_its_end_and_the_interval_call()
    {
        var f = Facts();
        f.Interval = "NUMTOYMINTERVAL(1, 'MONTH')";
        f.Partitions = new[] { new OraPartition("SYS_P241", new DateTime(2026, 11, 1, 0, 0, 0, DateTimeKind.Utc), "TRAILOX_AUDIT") };
        var check = Check(f, "partition");
        Assert.Equal("warn", check.Status);
        Assert.Contains("ALTER_PARTITION_INTERVAL", check.Message);
        Assert.Contains("2026-11-01", check.Message);
    }

    [Fact]
    public void A_partition_outside_the_trails_tablespace_warns_with_its_end()
    {
        var f = Facts();
        f.Partitions = new[] { new OraPartition("SYS_P241", Now.AddHours(18), "SYSAUX") };
        Assert.Equal("warn", Check(f, "partition").Status);
        Assert.Contains("2026-10-23", Check(f, "partition").Message);
    }

    [Fact]
    public void A_day_with_no_record_yet_has_no_partition_and_nothing_to_warn_of()
    {
        var f = Facts();
        f.Partitions = new[] { new OraPartition("SYS_P181", Now.AddDays(-20), "SYSAUX") };
        Assert.Null(OraProbeChecks.Today(f));
        Assert.Equal("ok", Check(f, "partition").Status);
        Assert.Equal("TRAILOX_AUDIT", OraProbeChecks.TrailTablespace(f));
    }

    [Fact]
    public void The_tables_own_default_for_new_partitions_goes_first_unless_it_says_default()
    {
        var f = Facts();
        f.Partitions = Array.Empty<OraPartition>();
        f.DefaultTablespace = "OTHER";
        Assert.Equal("OTHER", OraProbeChecks.TrailTablespace(f));
        f.DefaultTablespace = "DEFAULT";
        Assert.Equal("TRAILOX_AUDIT", OraProbeChecks.TrailTablespace(f));
    }

    [Fact]
    public void A_spill_file_warns()
    {
        Assert.Equal("warn", OraProbeChecks.Spillover(true).Status);
        Assert.Equal("warn", OraProbeChecks.Spillover(null).Status);
        Assert.Equal("ok", OraProbeChecks.Spillover(false).Status);
    }

    [Fact]
    public void The_policy_for_security_changes_missing_warns_with_its_statement()
    {
        var check = OraProbeChecks.DefaultPolicies(true, Array.Empty<OraclePolicy>());
        Assert.Equal("warn", check.Status);
        Assert.Contains("AUDIT POLICY ORA_SECURECONFIG", check.Message);
        Assert.Equal("warn", OraProbeChecks.DefaultPolicies(false, Array.Empty<OraclePolicy>()).Status);
    }

    [Theory]
    [InlineData("ORA_LOGON_FAILURES", "BY USER", "ALL USERS")]
    [InlineData("ORA_LOGIN_LOGOUT", "BY USER", "ALL USERS")]
    [InlineData("TRAILOX_LOGON", "EXCEPT USER", "TRAILOX_MONITOR")]
    public void Failed_sign_ins_are_audited_when_a_policy_covers_everyone_for_failures(string name, string option, string entity)
    {
        var check = OraProbeChecks.FailedSignIns(true, new[] { Failures(name, option: option, entity: entity) }, 19);
        Assert.Equal("ok", check.Status);
        Assert.Contains(name, check.Message);
    }

    /// <summary>The constant "ok" this check replaces said yes to all three.</summary>
    [Theory]
    [InlineData("no policy", "OTHER_POLICY", "YES", "BY USER", "ALL USERS")]
    [InlineData("no policy", "ORA_LOGON_FAILURES", "NO", "BY USER", "ALL USERS")]
    [InlineData("only for APP_ADMIN", "ORA_LOGON_FAILURES", "YES", "BY USER", "APP_ADMIN")]
    public void Failed_sign_ins_that_may_not_reach_the_trail_warn_with_the_statement(string says, string name, string failure, string option, string entity)
    {
        var check = OraProbeChecks.FailedSignIns(true, new[] { Failures(name, failure, option, entity) }, 19);
        Assert.Equal("warn", check.Status);
        Assert.Contains(says, check.Message);
        Assert.Contains("AUDIT POLICY ORA_LOGON_FAILURES WHENEVER NOT SUCCESSFUL", check.Message);
    }

    [Fact]
    public void From_23ai_the_failures_policy_has_another_name()
    {
        Assert.Equal("ok", OraProbeChecks.FailedSignIns(true, new[] { Failures("ORA_LOGIN_LOGOUT") }, 23).Status);
        Assert.Contains("AUDIT POLICY ORA_LOGIN_LOGOUT WHENEVER NOT SUCCESSFUL", OraProbeChecks.FailedSignIns(true, Array.Empty<OraclePolicy>(), 23).Message);
    }

    /// <summary>An empty list after a refused read is not "nothing audits them", and no "ok" may come from it either.</summary>
    [Fact]
    public void Policies_that_cannot_be_read_say_nothing_about_failed_sign_ins()
    {
        var check = OraProbeChecks.FailedSignIns(false, Array.Empty<OraclePolicy>(), 19);
        Assert.Equal("warn", check.Status);
        Assert.Contains("cannot be read", check.Message);
    }

    [Theory]
    [InlineData("logs_only")]
    [InlineData("selective", "TRAILOX_LOGON", "TRAILOX_DDL", "TRAILOX_ANYPRIV", "TRAILOX_DATAPUMP")]
    [InlineData("selective", "TRAILOX_LOGON", "TRAILOX_DDL", "TRAILOX_ANYPRIV", "TRAILOX_DATAPUMP", "TRAILOX_TABLES")]
    [InlineData("full", "TRAILOX_LOGON", "TRAILOX_DDL", "TRAILOX_ANYPRIV", "TRAILOX_DATAPUMP", "TRAILOX_FULL")]
    [InlineData("partial", "TRAILOX_LOGON")]
    [InlineData("partial", "TRAILOX_TABLES")]
    [InlineData("partial", "TRAILOX_FULL")]
    public void The_observed_level_follows_the_policies_enabled(string level, params string[] names)
    {
        var policies = names.Select(n => new OraclePolicy { Name = n }).ToList();
        Assert.Equal(level, OraProbeChecks.ObservedLevel(policies));
        Assert.Equal(level == "partial" ? "warn" : "ok", OraProbeChecks.Level(true, policies).Status);
    }

    [Fact]
    public void A_profile_not_in_force_warns_and_names_the_limit()
    {
        var f = Facts();
        f.Limits!["SESSIONS_PER_USER"] = "UNLIMITED";
        f.Limits["LOGICAL_READS_PER_CALL"] = "UNLIMITED";
        var check = Check(f, "profile");
        Assert.Equal("warn", check.Status);
        Assert.Contains("SESSIONS_PER_USER", check.Message);
        Assert.Contains("LOGICAL_READS_PER_CALL", check.Message);
    }

    [Fact]
    public void Resource_limit_off_warns()
    {
        Assert.Equal("warn", OraProbeChecks.ResourceLimit("FALSE").Status);
        Assert.Equal("ok", OraProbeChecks.ResourceLimit("TRUE").Status);
    }

    [Fact]
    public void Old_settings_warn_only_in_mixed_mode()
    {
        Assert.Equal("warn", OraProbeChecks.OldSettings("FALSE", 3).Status);
        Assert.Equal("ok", OraProbeChecks.OldSettings("TRUE", 3).Status);
        Assert.Equal("ok", OraProbeChecks.OldSettings("FALSE", 0).Status);
        Assert.Equal("warn", OraProbeChecks.OldSettings("FALSE", null).Status);
    }

    /// <summary>Whether to purge is the customer's call, so no job is reported and never pressed on them.</summary>
    [Fact]
    public void A_purge_job_is_reported_and_its_absence_is_no_warning()
    {
        Assert.Equal("ok", OraProbeChecks.PurgeJob(false).Status);
        Assert.Contains("no job", OraProbeChecks.PurgeJob(false).Message);
        Assert.DoesNotContain("setup", OraProbeChecks.PurgeJob(false).Message);
        Assert.Equal("ok", OraProbeChecks.PurgeJob(true).Status);
        Assert.Equal("warn", OraProbeChecks.PurgeJob(null).Status);
    }

    [Fact]
    public void A_password_near_its_expiry_warns()
    {
        Assert.Equal("warn", OraProbeChecks.PasswordExpiry(Now.AddDays(13), Now).Status);
        Assert.Equal("ok", OraProbeChecks.PasswordExpiry(Now.AddDays(15), Now).Status);
        Assert.Equal("ok", OraProbeChecks.PasswordExpiry(null, Now).Status);
    }

    [Theory]
    [InlineData("cloud", "ledger.abc.eu-west-1.rds.amazonaws.com", false, "rds", "ok")]
    [InlineData("cloud", "ledger.abc.cn-north-1.rds.amazonaws.com.cn", false, "rds", "ok")]
    [InlineData("cloud", "db.example.test", true, "rds", "ok")]
    [InlineData("onprem", "192.0.2.10", false, "self_managed", "ok")]
    [InlineData("onprem", "ledger.abc.eu-west-1.rds.amazonaws.com", false, "rds", "warn")]
    [InlineData("onprem", "db.example.test", true, "rds", "warn")]
    [InlineData("cloud", "192.0.2.10", false, "self_managed", "warn")]
    public void The_platform_comes_from_the_host_and_the_server_and_warns_when_kind_disagrees(string kind, string host, bool rdsAdmin, string platform, string status)
    {
        Assert.Equal(platform, OraProbeChecks.Platform(host, rdsAdmin));
        Assert.Equal(status, OraProbeChecks.Platform(kind, host, rdsAdmin).Status);
    }

    /// <summary>The messages reach the customer's source page, as they are.</summary>
    [Fact]
    public void Every_message_is_plain_ascii_and_every_name_a_plain_key()
    {
        var f = Facts();
        f.Container = "CDB$ROOT";
        f.AuditSessionId = "0";
        f.Spilled = true;
        f.Limits = null;
        f.GrowthBytesPerDay = long.MaxValue / 2;
        foreach (var check in OraProbeChecks.All(f))
        {
            Assert.Matches("^[a-z][a-z0-9_]{0,59}$", check.Name);
            Assert.True(check.Message.All(char.IsAscii), check.Message);
            Assert.DoesNotContain("error", check.Message, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void Policy_objects_at_the_cap_warn_that_some_are_not_reported()
    {
        Assert.Equal("warn", OraProbeChecks.Level(true, Array.Empty<OraclePolicy>(), OraSql.MaxPolicyObjects).Status);
    }

    /// <summary>An empty list after a refused read is not "no policy is enabled": that would read as a level switched off.</summary>
    [Fact]
    public void Policies_that_cannot_be_read_tell_no_level()
    {
        var f = Facts();
        f.PoliciesRead = false;
        f.Policies = new List<OraclePolicy>();

        Assert.Equal("warn", Check(f, "level").Status);
        Assert.Contains("cannot be read", Check(f, "level").Message);
        var caps = OraProbeChecks.Caps(f, OraProbeChecks.All(f).ToList());
        Assert.False(caps.PoliciesRead);
        Assert.Equal("", caps.ObservedLevel);
    }

    [Fact]
    public void The_caps_carry_what_the_server_said_and_only_our_policies()
    {
        var f = Facts();
        f.Policies.Add(new OraclePolicy { Name = "TRAILOX_LOGON", EnabledOption = "EXCEPT", EntityName = "TRAILOX_MONITOR" });
        f.GrowthBytesPerDay = 1_300_000_000;
        var caps = OraProbeChecks.Caps(f, OraProbeChecks.All(f).ToList());
        Assert.Equal(("rds", "Standard Edition 2", "LEDGER", "mixed"), (caps.Platform, caps.Edition, caps.Container, caps.UnifiedMode));
        Assert.Equal(("TRAILOX_MONITOR", "trailox-agent.dll"), (caps.OwnUser, caps.OwnProgram));
        Assert.Equal((Now - DateTime.UnixEpoch).Ticks / 10, caps.ProbedAtMicros);
        Assert.Equal(("SYS_P241", "TRAILOX_AUDIT"), (caps.TodayPartition, caps.TodayTablespace));
        Assert.Equal(1_300_000_000, caps.GrowthBytesPerDay);
        Assert.Equal("TRAILOX_LOGON", Assert.Single(caps.Policies).Name);
    }
}

public class OraGrowthTests
{
    private static readonly DateTime Start = new(2026, 10, 22, 6, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void Two_probes_an_hour_apart_give_what_was_seen_and_its_projection()
    {
        var growth = new OraGrowth();
        growth.Add(Start, "T", 1_000);
        Assert.Null(growth.BytesPerDay);
        growth.Add(Start.AddMinutes(30), "T", 1_500);
        Assert.Null(growth.GrownBytes);
        growth.Add(Start.AddHours(1), "T", 2_000);
        Assert.Equal(1_000, growth.GrownBytes);
        Assert.Equal(24_000, growth.BytesPerDay);
    }

    /// <summary>A purge frees a day's partition every day, and the growth around it still happened.</summary>
    [Fact]
    public void A_purge_counts_as_no_growth_and_the_growth_around_it_stays()
    {
        var growth = new OraGrowth();
        growth.Add(Start, "T", 5_000);
        growth.Add(Start.AddHours(1), "T", 6_000);
        growth.Add(Start.AddHours(2), "T", 1_000);
        growth.Add(Start.AddHours(3), "T", 2_000);
        Assert.Equal(2_000, growth.GrownBytes);
        Assert.Equal(16_000, growth.BytesPerDay);
    }

    [Fact]
    public void The_hour_after_a_purge_is_not_taken_for_a_whole_day()
    {
        var growth = new OraGrowth();
        for (var hour = 0; hour < 16; hour++)
        {
            growth.Add(Start.AddHours(hour), "T", 1_000_000_000);
        }
        growth.Add(Start.AddHours(16), "T", 1_090_000_000);
        Assert.Equal(90_000_000, growth.GrownBytes);
        Assert.Equal(135_000_000, growth.BytesPerDay);
    }

    [Fact]
    public void A_moved_trail_starts_the_measure_again()
    {
        var growth = new OraGrowth();
        growth.Add(Start, "T", 5_000);
        growth.Add(Start.AddHours(1), "OTHER", 6_000);
        Assert.Null(growth.BytesPerDay);
    }

    [Fact]
    public void The_measure_covers_the_last_day_so_a_quiet_night_counts()
    {
        var growth = new OraGrowth();
        for (var hour = 0; hour <= 48; hour++)
        {
            growth.Add(Start.AddHours(hour), "T", hour <= 12 ? hour * 1_000 : 12_000);
        }
        Assert.Equal(0, growth.GrownBytes);
    }
}
