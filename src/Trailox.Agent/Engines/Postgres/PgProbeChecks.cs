using Trailox.Agent.Protocol;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// What the probe tells the customer to fix: each check is ok, warn or fail, with what to change. A check never fails the
/// probe; a failed one shows on the source page.
/// </summary>
public static class PgProbeChecks
{
    private static readonly HashSet<string> UtcZones = new(StringComparer.OrdinalIgnoreCase)
    {
        "UTC", "Etc/UTC", "GMT", "Etc/GMT", "UCT", "Etc/UCT", "Zulu", "Etc/Zulu", "Universal", "Etc/Universal",
    };

    public static PostgresCheck Role(bool superuser, int connectionLimit, bool readAllStats, bool readAllSettings)
    {
        var problems = new List<string>();
        if (superuser)
        {
            problems.Add("it is a superuser: give the agent a role with only CONNECT, pg_read_all_stats and pg_read_all_settings");
        }
        if (connectionLimit is < 1 or > 2)
        {
            problems.Add("its connection limit is not 2 (ALTER ROLE ... CONNECTION LIMIT 2)");
        }
        if (!readAllStats)
        {
            problems.Add("it is not a member of pg_read_all_stats: other users' statements show without their id");
        }
        if (!readAllSettings)
        {
            problems.Add("it is not a member of pg_read_all_settings: some settings cannot be read");
        }
        return problems.Count == 0
            ? Ok("role", "the agent's role reads statistics and settings, and nothing else")
            : Warn("role", "the agent's role: " + string.Join("; ", problems));
    }

    public static IEnumerable<PostgresCheck> Settings(IReadOnlyDictionary<string, string> s, string logSource)
    {
        var missing = new List<string>();
        if (!On(s.GetValueOrDefault("log_connections", "")))
        {
            missing.Add("log_connections");
        }
        if (!On(s.GetValueOrDefault("log_disconnections", "")))
        {
            missing.Add("log_disconnections");
        }
        if (s.GetValueOrDefault("log_statement", "") is not ("ddl" or "mod" or "all"))
        {
            missing.Add("log_statement = 'ddl'");
        }
        yield return missing.Count == 0
            ? Ok("logging", "sign-ins, failed sign-ins and DDL are logged")
            : Warn("logging", "set " + string.Join(", ", missing) + ": until then those events are not in the log");

        var language = s.GetValueOrDefault("lc_messages", "");
        yield return English(language)
            ? Ok("lc_messages", "the log is written in English")
            : Warn("lc_messages", $"the log is written in {language}: the agent reads English only. Set lc_messages = 'C'");

        var zone = s.GetValueOrDefault("log_timezone", "");
        yield return UtcZones.Contains(zone)
            ? Ok("log_timezone", "the log's times are UTC")
            : Fail("log_timezone", $"the log's times are in {zone}: set log_timezone = 'UTC'");

        if (logSource == PostgresEngine.LogSourceFile && !On(s.GetValueOrDefault("logging_collector", "")))
        {
            yield return Fail("logging_collector", "logging_collector is off, so the server writes no log files: turn it on (one restart)");
        }
    }

    /// <param name="extversion">The installed extension's version in <paramref name="database"/>; null when it is not created.</param>
    public static PostgresCheck StatStatements(string? extversion, IReadOnlyDictionary<string, string> s, string database)
    {
        if (PgCatalog.ExtensionVersion(extversion) is { } version)
        {
            return version >= new Version(1, 8)
                ? Ok("pg_stat_statements", "read volume per user is collected")
                : Warn("pg_stat_statements", $"pg_stat_statements {extversion} is older than this server's: read volume per user is collected, "
                    + $"but run ALTER EXTENSION pg_stat_statements UPDATE in database {database} to read it as the server counts it");
        }
        var loaded = s.GetValueOrDefault("shared_preload_libraries", "")
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Any(l => l.Trim('"') == "pg_stat_statements");
        return Warn("pg_stat_statements", loaded
            ? $"pg_stat_statements is loaded but not created in database {database}: CREATE EXTENSION pg_stat_statements"
            : "pg_stat_statements is not loaded: add it to shared_preload_libraries (one restart), then CREATE EXTENSION pg_stat_statements. "
                + "Until then there is no read volume per user");
    }

    /// <summary>Whether every replica listed could be signed in to; one that cannot is skipped by every task.</summary>
    public static PostgresCheck ReplicasAnswer(int listed, IReadOnlyList<string> failures) => failures.Count == 0
        ? Ok("instances", $"the agent signs in to all {listed} replicas")
        : Warn("instances", $"{failures.Count} of {listed} replicas cannot be signed in to, so their counters, settings and statement "
            + "texts are skipped: " + string.Join("; ", failures));

    public static PostgresCheck Replicas(long streaming, int listed) => streaming > listed
        ? Warn("replicas", $"{streaming} replicas stream from the primary and {listed} are listed in options.instances: the others' sign-ins are not read")
        : Ok("replicas", listed == 0 ? "no replica" : $"{listed} replicas are read");

    /// <summary>How many minutes one file covers: the smallest gap between the names of the newest files; 0 when unknown.</summary>
    public static int FileMinutes(IReadOnlyList<long> startsMicros)
    {
        var newest = startsMicros.OrderBy(t => t).TakeLast(7).ToList();
        var gaps = newest.Zip(newest.Skip(1), (a, b) => b - a).Where(g => g > 0).ToList();
        return gaps.Count == 0 ? 0 : (int)Math.Round(gaps.Min() / 60_000_000.0);
    }

    public static PostgresCheck LogFiles(string instance, int files, int minutes) => files == 0
        ? Fail("log_files", $"{instance}: no log file found")
        : minutes is 0 or <= 10
            ? Ok("log_files", $"{instance}: {files} log files")
            : minutes <= 60
                ? Warn("log_files", $"{instance}: each log file covers {minutes} minutes, so a window reads up to {(minutes / 10 + 1) / 2.0:0.#} times "
                    + "what the server wrote. Set log_rotation_age = 10 and a file name with minutes")
                : Fail("log_files", $"{instance}: each log file covers {minutes} minutes, so the agent would read each line many times over. "
                    + "Set log_rotation_age = 10 and a file name with minutes");

    private static bool On(string value) =>
        value is "on" or "true" or "1" || value.Contains("authorization", StringComparison.Ordinal) || value == "all";

    private static bool English(string language) =>
        language.Length == 0 || language is "C" or "POSIX" || language.StartsWith("C.", StringComparison.Ordinal)
        || language.StartsWith("en", StringComparison.OrdinalIgnoreCase);

    private static PostgresCheck Ok(string name, string message) => new() { Name = name, Status = "ok", Message = message };

    private static PostgresCheck Warn(string name, string message) => new() { Name = name, Status = "warn", Message = message };

    private static PostgresCheck Fail(string name, string message) => new() { Name = name, Status = "fail", Message = message };
}
