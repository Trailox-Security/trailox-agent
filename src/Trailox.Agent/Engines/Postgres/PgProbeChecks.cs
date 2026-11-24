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

    /// <param name="platform">The probe's platform (<see cref="PgPlatform"/>): Azure runs with no logging collector, and lets none be turned on.</param>
    public static IEnumerable<PostgresCheck> Settings(IReadOnlyDictionary<string, string> s, string logSource, string platform = "")
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

        if (logSource == PostgresEngine.LogSourceFile && platform != PgPlatform.Azure && !On(s.GetValueOrDefault("logging_collector", "")))
        {
            yield return Fail("logging_collector", "logging_collector is off, so the server writes no log files: turn it on (one restart)");
        }

        // A pgaudit line carries no CONTEXT, and a function's RAISE LOG 'AUDIT: ...' does; terse writes no CONTEXT at all.
        if (Loads(s, "pgaudit"))
        {
            yield return string.Equals(s.GetValueOrDefault("log_error_verbosity", ""), "terse", StringComparison.OrdinalIgnoreCase)
                ? Warn("log_error_verbosity", "log_error_verbosity is terse, so a line written by a function's RAISE LOG cannot be told "
                    + "from pgaudit's own: set log_error_verbosity = 'default'")
                : Ok("log_error_verbosity", "pgaudit's lines can be told from lines that functions write");
        }
    }

    private static bool Loads(IReadOnlyDictionary<string, string> s, string library) =>
        s.GetValueOrDefault("shared_preload_libraries", "")
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Any(l => l.Trim('"') == library);

    /// <param name="extversion">The installed extension's version in <paramref name="database"/>; null when it is not created.</param>
    /// <remarks>
    /// ⚠ CREATED IS NOT COUNTING. With <c>pg_stat_statements.track = none</c> (Azure's default) the extension is there and
    ///   records nothing, so the counters read as an idle server. A setting the role cannot read is not taken for none.
    /// </remarks>
    public static PostgresCheck StatStatements(string? extversion, IReadOnlyDictionary<string, string> s, string database)
    {
        var tracksNothing = string.Equals(s.GetValueOrDefault("pg_stat_statements.track", ""), "none", StringComparison.OrdinalIgnoreCase);
        if (PgCatalog.ExtensionVersion(extversion) is { } version)
        {
            if (tracksNothing)
            {
                return Warn("pg_stat_statements", "pg_stat_statements.track is none, so pg_stat_statements records no statement and there "
                    + "is no read volume per user: set pg_stat_statements.track = top");
            }
            return version >= new Version(1, 8)
                ? Ok("pg_stat_statements", "read volume per user is collected")
                : Warn("pg_stat_statements", $"pg_stat_statements {extversion} is older than this server's: read volume per user is collected, "
                    + $"but run ALTER EXTENSION pg_stat_statements UPDATE in database {database} to read it as the server counts it");
        }
        var track = tracksNothing ? "; and set pg_stat_statements.track = top, which is none" : "";
        return Warn("pg_stat_statements", Loads(s, "pg_stat_statements")
            ? $"pg_stat_statements is loaded but not created in database {database}: CREATE EXTENSION pg_stat_statements" + track
            : "pg_stat_statements is not loaded: add it to shared_preload_libraries (one restart), then CREATE EXTENSION pg_stat_statements. "
                + "Until then there is no read volume per user");
    }

    /// <summary>
    /// <c>log_line_prefix</c>, as the server has it, and which of <c>%u</c> (user), <c>%c</c> (session) and <c>%p</c>
    /// (process) it lacks. It shapes stderr lines only: jsonlog and csvlog records carry every field, whatever the prefix.
    /// </summary>
    /// <param name="format">The format the agent reads: stderr, csvlog or jsonlog; "" when no file was found.</param>
    public static PostgresCheck LinePrefix(string prefix, string format)
    {
        var escapes = PgLinePrefix.Escapes(prefix);
        var lacks = new[] { 'u', 'c', 'p' }.Where(e => !escapes.Contains(e)).Select(e => "%" + e).ToList();
        var shown = $"log_line_prefix is '{(prefix.Length > MaxPrefixShown ? prefix[..MaxPrefixShown] + "..." : prefix)}'"
            + (lacks.Count == 0 ? "" : $" (it has no {string.Join(", ", lacks)})");
        if (format is "csvlog" or "jsonlog")
        {
            return Ok("log_line_prefix", $"{shown}: the agent reads {format}, whose records name their user, database and session whatever the prefix");
        }
        return StderrLogParser.Reads(prefix)
            ? Ok("log_line_prefix", $"{shown}: each line names its user, database and process")
            : Warn("log_line_prefix", $"{shown}: the agent reads stderr lines with the prefix '{StderrLogParser.RdsPrefix}' only, so lines "
                + "with this one count as unread");
    }

    private const int MaxPrefixShown = 80;

    /// <summary>log_files when no file was found in the log directory.</summary>
    public static PostgresCheck NoLogFiles(string directory, string platform) => platform == PgPlatform.Azure
        ? Fail("log_files", "Azure Database for PostgreSQL flexible server has no log directory to mount, so the agent reads no server "
            + "log: sign-ins, failed sign-ins and DDL are not collected")
        : Fail("log_files", $"no log file in {directory}: check logging_collector, log_directory and the mount");

    /// <summary>Whether every replica listed could be signed in to; one that cannot is skipped by every task.</summary>
    public static PostgresCheck ReplicasAnswer(int listed, IReadOnlyList<string> failures) => failures.Count == 0
        ? Ok("instances", listed == 1 ? "the agent signs in to the replica" : $"the agent signs in to all {listed} replicas")
        : Warn("instances", (listed == 1 ? "the replica" : $"{failures.Count} of {listed} replicas")
            + $" cannot be signed in to, so {(failures.Count == 1 ? "its" : "their")} counters, settings and statement texts are skipped: "
            + string.Join("; ", failures));

    public static PostgresCheck Replicas(long streaming, int listed) => streaming > listed
        ? Warn("replicas", $"{ReplicaCount(streaming)} {(streaming == 1 ? "streams" : "stream")} from the primary and {listed} "
            + $"{(listed == 1 ? "is" : "are")} listed in options.instances: the sign-ins of {ReplicaCount(streaming - listed)} are not read")
        : Ok("replicas", listed == 0 ? "no replica" : $"{ReplicaCount(listed)} {(listed == 1 ? "is" : "are")} read");

    private static string ReplicaCount(long n) => n == 1 ? "1 replica" : $"{n} replicas";

    /// <summary>How many minutes one file covers: the smallest gap between the names of the newest files; 0 when unknown.</summary>
    public static int FileMinutes(IReadOnlyList<long> startsMicros)
    {
        var newest = startsMicros.OrderBy(t => t).TakeLast(7).ToList();
        var gaps = newest.Zip(newest.Skip(1), (a, b) => b - a).Where(g => g > 0).ToList();
        return gaps.Count == 0 ? 0 : (int)Math.Round(gaps.Min() / 60_000_000.0);
    }

    /// <remarks>
    /// Files under 20 minutes are ok: a window then reads less than one and a half times what the server wrote, and an
    /// irregular gap between two names (a reboot's file) is no setting to fix. Before, such a file warned "up to 1 times".
    /// </remarks>
    public static PostgresCheck LogFiles(string instance, int files, int minutes) => files == 0
        ? Fail("log_files", $"{instance}: no log file found")
        : minutes is 0 or < 20
            ? Ok("log_files", $"{instance}: {files} log files")
            : minutes <= 60
                ? Warn("log_files", $"{instance}: each log file covers {minutes} minutes, so a window reads up to {(minutes / 10 + 1) / 2.0:0.#} times "
                    + "what the server wrote. Set log_rotation_age = 10 and a file name with minutes")
                : Fail("log_files", $"{instance}: each log file covers {minutes} minutes, so the agent would read each line many times over. "
                    + "Set log_rotation_age = 10 and a file name with minutes");

    /// <summary>
    /// A log directory's files, and its <c>log_rotation_age</c>: a window is said to be missing only when no file started for
    /// a whole rotation. With no rotation by time (0, or a setting the role cannot read) a deleted log file
    /// cannot be told from a quiet server; with 20 minutes or more, files deleted over less than that are not told. Both
    /// warn, so neither is silent. Files of their own that rotate by
    /// size can look fine by their names alone.
    /// </summary>
    public static PostgresCheck LogFiles(string instance, int files, int minutes, int rotationMinutes)
    {
        var check = LogFiles(instance, files, minutes);
        return check.Status != "ok" ? check
            : rotationMinutes == 0
                ? Warn("log_files", $"{instance}: {files} log files, but log_rotation_age is 0 or cannot be read, so a log file deleted "
                    + "from the directory cannot be told from a quiet server. Set log_rotation_age = 10")
            : rotationMinutes >= 20
                ? Warn("log_files", $"{instance}: {files} log files, but log_rotation_age is {rotationMinutes} minutes, so log files deleted "
                    + $"from the directory are told only when no file started for {rotationMinutes} minutes. Set log_rotation_age = 10")
            : check;
    }

    private static bool On(string value) =>
        value is "on" or "true" or "1" || value.Contains("authorization", StringComparison.Ordinal) || value == "all";

    /// <summary>Whether <c>lc_messages</c> writes English (C, POSIX, en_*; unset is C).</summary>
    internal static bool English(string language) =>
        language.Length == 0 || language is "C" or "POSIX" || language.StartsWith("C.", StringComparison.Ordinal)
        || language.StartsWith("en", StringComparison.OrdinalIgnoreCase);

    private static PostgresCheck Ok(string name, string message) => new() { Name = name, Status = "ok", Message = message };

    private static PostgresCheck Warn(string name, string message) => new() { Name = name, Status = "warn", Message = message };

    private static PostgresCheck Fail(string name, string message) => new() { Name = name, Status = "fail", Message = message };
}
