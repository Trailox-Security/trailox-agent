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

    /// <remarks>The logging collector is checked once the log directory is listed: <see cref="LoggingCollector"/>.</remarks>
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
        var line = PgLineFormat.Of(prefix);
        if (!line.HasTime)
        {
            return Fail("log_line_prefix", $"{shown}: it has no %t, %m or %n, so no stderr line has a time and the agent reads none of them");
        }
        if (line.ClientEscape == 'a' && !StderrLogParser.Reads(prefix))
        {
            return Warn("log_line_prefix", $"{shown}: it writes the application name, which any client chooses, so a client can make "
                + "its own lines read as another user's or as a sign-in. The agent counts a line that holds a level in its text as "
                + "forged, and does not read it; csvlog and jsonlog quote the name");
        }
        var forgeable = line.ClientText && !StderrLogParser.Reads(prefix)
            ? ". A refused client's user or database name is written into the prefix as it is: a line forged in one counts as unread"
            : "";
        if (line.HasUser)
        {
            return Ok("log_line_prefix", $"{shown}: each line names its user{forgeable}");
        }
        if (line.HasSession)
        {
            return Ok("log_line_prefix", $"{shown}: the agent finds each line's user, database and client by its session (%c), "
                + $"on the session's sign-in line{forgeable}");
        }
        if (line.HasPid)
        {
            return Ok("log_line_prefix", $"{shown}: the agent finds each line's user, database and client by its process (%p), "
                + $"on the process's newest sign-in line, up to {PgSessionMap.PidLookBackMicros / 3_600_000_000} hours before{forgeable}");
        }
        return Warn("log_line_prefix", $"{shown}: a line names no user, session or process, so the agent cannot tell whose a statement "
            + "is: those lines count as unread. Sign-ins, failed sign-ins and disconnections name their user and are read");
    }

    /// <summary>
    /// A self-managed server with the logging collector off writes its log to stderr, which another program can save to
    /// files (a service manager, a package's start script): the agent reads those files where they are. Only with no
    /// file found does it ask for the logging collector. Null when there is nothing to check.
    /// </summary>
    public static PostgresCheck? LoggingCollector(IReadOnlyDictionary<string, string> s, string logSource, string platform, bool filesFound)
    {
        if (logSource != PostgresEngine.LogSourceFile || platform == PgPlatform.Azure || On(s.GetValueOrDefault("logging_collector", "")))
        {
            return null;
        }
        return filesFound
            ? Ok("logging_collector", "logging_collector is off: the agent reads the log files it found, as they are written")
            : Fail("logging_collector", "logging_collector is off, so the server writes no log files: turn it on (one restart), or "
                + "mount the directory where its stderr is saved");
    }

    private const int MaxPrefixShown = 80;

    /// <summary>log_files when no file was found in the log directory.</summary>
    public static PostgresCheck NoLogFiles(string directory, string platform) => platform == PgPlatform.Azure
        ? Fail("log_files", "Azure Database for PostgreSQL flexible server has no log directory to mount: read its server log with "
            + "log_source: azure_files")
        : Fail("log_files", $"no log file in {directory}: check logging_collector, log_directory and the mount");

    /// <summary>log_files of Azure's server log download: one file an hour, kept for logfiles.retention_days.</summary>
    public static PostgresCheck AzureLogFiles(string server, int files, long oldestMicros) => files == 0
        ? Fail("log_files", $"{server}: no server log file to download: set the server parameter logfiles.download_enable = on")
        : Ok("log_files", $"{server}: {files} log files, the oldest from "
            + DateTime.UnixEpoch.AddTicks(oldestMicros * 10).ToString("yyyy-MM-dd HH:mm", System.Globalization.CultureInfo.InvariantCulture)
            + " UTC. Azure keeps them for logfiles.retention_days (at most 7): a longer outage loses the gap");

    /// <summary>How old the newest row may be before the probe says nothing sends the server's log any more.</summary>
    internal const long LogAnalyticsSilentMicros = 3_600_000_000;

    /// <summary>
    /// The server's rows in Log Analytics over the last day. None, or none for an hour, fails: on Azure the platform's own
    /// sign-ins write rows every minute, so silence means nothing sends the server's <c>PostgreSQLLogs</c> there.
    /// </summary>
    /// <param name="otherTables">
    /// Other tables that hold the server's rows. With none in the target's table, that is a table given in
    /// <c>options.azure_log_table</c> while the rows go to the other; with some, a second diagnostic setting.
    /// </param>
    /// <param name="workspaces">The workspaces that hold the server's rows in the target's table.</param>
    public static PostgresCheck AzureLogAnalytics(string server, AzureLogTarget target, long rowsLastDay, long oldestMicros, long newestMicros,
        long nowMicros, IReadOnlyList<string> otherTables, IReadOnlyList<string> workspaces)
    {
        var table = target.Table;
        string Utc(long micros) =>
            DateTime.UnixEpoch.AddTicks(micros * 10).ToString("yyyy-MM-dd HH:mm", System.Globalization.CultureInfo.InvariantCulture) + " UTC";
        if (rowsLastDay == 0)
        {
            return Fail("log_analytics", otherTables.Count > 0
                ? $"{server}: no row of its server log came to {table} in the last day, but {string.Join(", ", otherTables)} has them: "
                    + "set options.azure_log_table to it, or leave it out"
                : $"{server}: no row of its server log came to Log Analytics in the last day in {table}. Send its PostgreSQLLogs "
                    + "category there with a diagnostic setting (destination table: Resource specific), or read it with log_source: azure_files");
        }
        if (nowMicros - newestMicros > LogAnalyticsSilentMicros)
        {
            return Fail("log_analytics", $"{server}: the newest row of its server log in {table} is from {Utc(newestMicros)}: nothing has "
                + "sent it there since. Check the server is running and its diagnostic setting still sends PostgreSQLLogs");
        }
        var notes = new List<string>();
        if (workspaces.Count > 1)
        {
            notes.Add($"its rows reach {workspaces.Count} workspaces ({string.Join(", ", workspaces)}), each with the same rows: the agent "
                + $"reads {target.Workspace}'s only");
        }
        if (otherTables.Count > 0)
        {
            notes.Add($"{string.Join(", ", otherTables)} also holds them: the agent reads {table} only");
        }
        var message = $"{server}: {rowsLastDay} rows of its server log came to {table} in the last day, the newest from "
            + $"{Utc(newestMicros)}, the oldest kept from {Utc(oldestMicros)}. Log Analytics leaves out a record longer than about 64 KB";
        return notes.Count > 0 ? Warn("log_analytics", message + "; " + string.Join("; ", notes)) : Ok("log_analytics", message);
    }

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
