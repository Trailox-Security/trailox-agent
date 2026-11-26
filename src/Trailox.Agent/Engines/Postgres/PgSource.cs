using Npgsql;
using Trailox.Agent.Config;
using Trailox.Agent.Protocol;
using Streams = Trailox.Agent.Protocol.ProtocolInfo.Streams;

namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// One PostgreSQL source: its servers (the primary and, on RDS, the replicas listed in agent.yaml), the server log of
/// each, read through the RDS API, Azure's server log download or from a mounted directory, and the SQL reads of
/// <see cref="PgCatalog"/>.
/// </summary>
public sealed class PgSource : IEngineSource
{
    /// <summary>A window ends at least this long before now: RDS shows a new line about 5 seconds after it is written.</summary>
    internal static readonly TimeSpan NotBefore = TimeSpan.FromSeconds(30);

    /// <summary>Windows that ended longer ago than this are caught up at half the call rate.</summary>
    internal static readonly TimeSpan CatchUp = TimeSpan.FromHours(1);

    internal const double LiveCallsPerSecond = 4;
    internal const double CatchUpCallsPerSecond = 2;

    private readonly EndpointConfig _endpoint;
    private readonly PgCatalog _catalog;
    private readonly IRdsLogs? _rds;

    /// <summary>The server's log files on Azure (<c>log_source: azure_files</c>); null for any other source.</summary>
    private readonly IAzureLogFiles? _azure;

    /// <summary>The server's rows in Log Analytics (<c>log_source: azure_log_analytics</c>); null for any other source.</summary>
    private readonly IAzureLogQuery? _logAnalytics;

    /// <summary>The Log Analytics table given in <c>options.azure_log_table</c>; null to read the one the rows are in.</summary>
    private readonly string? _givenTable;

    /// <summary>
    /// Where the server's rows in Log Analytics are read (<see cref="LogTargetOf"/>), from the newest probe, or the first
    /// window when no probe found it: null until then. No window reads a table it has not seen the rows in.
    /// </summary>
    private AzureLogTarget? _logTarget;

    /// <summary>The Region's RDS API calls, shared with this agent's other sources there; null without the RDS API.</summary>
    private readonly RdsCallBudget? _budget;
    private readonly Func<DateTime> _utcNow;
    private readonly IReadOnlyList<PgInstance> _instances;
    private readonly string? _logDirectory;

    /// <summary>The platform, once the server gave a definite answer: it does not change while the agent runs.</summary>
    private string? _platform;

    /// <summary>
    /// Whether the server writes its log in English (<c>lc_messages</c>), from the newest probe; English until a probe says
    /// otherwise. The window reader counts what it cannot recognise by it.
    /// </summary>
    private bool _english = true;

    /// <summary>
    /// <c>log_rotation_age</c> in minutes, from the newest probe; 0 until one reads it. The window reader tells a window
    /// whose file is gone by it.
    /// </summary>
    private int _rotationMinutes;

    /// <summary>
    /// The server says its logging collector is off: its stderr goes to files another program writes and rotates, so
    /// <c>log_rotation_age</c> says nothing of them. A window of such files is never said to be missing.
    /// </summary>
    private bool _collectorOff;

    /// <summary>
    /// The server's <c>log_line_prefix</c>, from the newest probe; null until one reads it (the stderr parser then reads
    /// RDS's). The parser of a log file follows it.
    /// </summary>
    private PgLineFormat? _prefix;

    /// <summary>
    /// The sessions of the server's log, for a prefix that names no user (<see cref="PgSessionMap"/>): kept from window to
    /// window, and started again when the prefix changes.
    /// </summary>
    private PgSessionMap? _sessions;

    /// <summary>Azure's default <c>log_line_prefix</c>, read until a probe reads the server's own.</summary>
    public const string AzureDefaultPrefix = "%t-%c-";

    /// <param name="entraCredential">The identity for <c>options.azure_sign_in: entra</c>; by default the agent's Azure identity.</param>
    public PgSource(EndpointConfig endpoint, IRdsLogs? rds = null, Func<DateTime>? utcNow = null, IAzureLogFiles? azure = null,
        HttpClient? http = null, IAzureLogQuery? logAnalytics = null, Azure.Core.TokenCredential? entraCredential = null)
    {
        _endpoint = endpoint;
        _catalog = new PgCatalog(endpoint, entraCredential);
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        var source = endpoint.Options.GetValueOrDefault("log_source");
        if (source == PostgresEngine.LogSourceRdsApi)
        {
            _instances = PostgresEngine.Instances(endpoint, out _);
            _rds = rds ?? new AwsRdsLogs(endpoint.Options["rds_region"]);
            _budget = RdsCallBudget.For(endpoint.Options["rds_region"]);
        }
        else if (source == PostgresEngine.LogSourceAzureFiles)
        {
            var server = endpoint.Options["azure_server"];
            _instances = new[] { new PgInstance(server, endpoint.Host, "primary") };
            _azure = azure ?? new ArmAzureLogFiles(http ?? new HttpClient(), endpoint.Options["azure_subscription"],
                endpoint.Options["azure_resource_group"], server);
        }
        else if (source == PostgresEngine.LogSourceAzureLogAnalytics)
        {
            var server = endpoint.Options["azure_server"];
            _instances = new[] { new PgInstance(server, endpoint.Host, "primary") };
            _logAnalytics = logAnalytics ?? new ArmLogAnalyticsQuery(http ?? new HttpClient(), endpoint.Options["azure_subscription"],
                endpoint.Options["azure_resource_group"], server, endpoint.Options.GetValueOrDefault("azure_workspace_id"));
            _givenTable = endpoint.Options.GetValueOrDefault("azure_log_table");
        }
        else
        {
            _instances = new[] { new PgInstance(endpoint.Host, endpoint.Host, "primary") };
            _logDirectory = endpoint.Options.GetValueOrDefault("log_directory", "");
        }
    }

    public async Task<EndpointCaps> ProbeAsync(CancellationToken ct)
    {
        var checks = new List<PostgresCheck>();
        var caps = new PostgresCaps
        {
            LogSource = _rds != null ? PostgresEngine.LogSourceRdsApi
                : _azure != null ? PostgresEngine.LogSourceAzureFiles
                : _logAnalytics != null ? PostgresEngine.LogSourceAzureLogAnalytics
                : PostgresEngine.LogSourceFile,
        };
        string version;
        Dictionary<string, string> settings;
        await using (var conn = await _catalog.OpenAsync(_endpoint.Host, _catalog.Database, ct))
        {
            (caps.ServerVersionNum, version) = await ScalarPairAsync(conn, PgSql.ServerVersion, ct);
            if (caps.ServerVersionNum < 130000)
            {
                throw new SourceException($"PostgreSQL {version} is not supported: the agent reads PostgreSQL 13 and later");
            }
            if (_catalog.Entra is { } entra)
            {
                checks.Add(PgProbeChecks.EntraSignIn(_endpoint.Host, _endpoint.Username, entra.Identity));
            }
            checks.Add(await RoleCheckAsync(conn, ct));
            settings = await SettingsAsync(conn, ct);
            // Asked at every probe that may call an Aurora function: one can be planted after the platform is known.
            var mayCallAurora = _rds != null && _platform != PgPlatform.Rds
                && MayCallAurora(await ScalarAsync(conn, PgSql.AuroraFunctionListed, ct));
            caps.Platform = _platform ?? await PlatformAsync(conn, mayCallAurora, ct);
            _catalog.Platform = caps.Platform;
            checks.AddRange(PgProbeChecks.Settings(settings, caps.LogSource, caps.Platform));
            _english = PgProbeChecks.English(settings.GetValueOrDefault("lc_messages", ""));
            _rotationMinutes = int.TryParse(settings.GetValueOrDefault("log_rotation_age", ""), System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture, out var minutes) ? minutes : 0;
            // log_rotation_age is the logging collector's: with it off, whatever saves the server's stderr rotates the files.
            _collectorOff = settings.GetValueOrDefault("logging_collector", "") == "off";
            var extension = await ScalarAsync(conn, PgSql.StatStatementsVersion, ct) as string;
            caps.StatStatements = extension != null;
            checks.Add(PgProbeChecks.StatStatements(extension, settings, _catalog.Database));
            var aurora = caps.Platform == PgPlatform.Aurora;
            if (_rds != null)
            {
                // A call that fails, or is not made, reads as 0: the check then counts the replicas in options.instances.
                var replicas = aurora && !mayCallAurora ? 0L
                    : Convert.ToInt64(await ScalarAsync(conn, aurora ? PgSql.AuroraReplicas : PgSql.StreamingReplicas, ct, pinned: aurora) ?? 0L);
                checks.Add(PgProbeChecks.Replicas(replicas, _instances.Count - 1));
            }
        }
        if (_instances.Count > 1)
        {
            checks.Add(await ReplicasAnswerAsync(ct));
        }
        var (instances, logChecks, format) = _rds != null ? await RdsLogsAsync(ct)
            : _azure != null ? await AzureLogsAsync(ct)
            : _logAnalytics != null ? await LogAnalyticsAsync(ct)
            : DirectoryLogs(caps.Platform);
        caps.Instances = instances;
        caps.LogFormat = format;
        checks.AddRange(logChecks);
        if (PgProbeChecks.LoggingCollector(settings, caps.LogSource, caps.Platform, format.Length > 0) is { } loggingCollector)
        {
            checks.Add(loggingCollector);
        }
        if (settings.TryGetValue("log_line_prefix", out var prefix))
        {
            checks.Add(PgProbeChecks.LinePrefix(prefix, format));
            Prefix(prefix);
        }
        caps.Checks = checks;
        var oldest = instances.Where(i => i.OldestLogMicros > 0).Select(i => i.OldestLogMicros).DefaultIfEmpty(0).Min();
        return new EndpointCaps
        {
            Version = version.Length > 64 ? version[..64] : version,
            Columns = new List<string>(),
            HasSessionLog = true,
            TextReadable = true,
            EarliestEventMicros = oldest,
            EarliestSessionMicros = oldest,
            Postgres = caps,
        };
    }

    public async Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct)
    {
        switch (task.Stream)
        {
            case Streams.PgLog:
                if (_utcNow() - FromMicros(task.EndMicros) < NotBefore)
                {
                    throw new SourceException("the server has not written the whole window yet") { NotYetPublished = true };
                }
                return await StreamedAsync((rows, token) => LogAsync(task, rows, token), ct);
            case Streams.PgStatSnapshot:
                return await StreamedAsync((rows, token) => _catalog.SnapshotAsync(_instances, rows, token), ct);
            case Streams.CatalogSettings:
                return await StreamedAsync((rows, token) => _catalog.SettingsAsync(_instances, rows, token), ct);
            case Streams.CatalogStatementText:
                return await StreamedAsync((rows, token) => _catalog.StatementTextAsync(_instances, rows, token), ct);
            case Streams.CatalogUsers:
                return await StreamedAsync((rows, token) => _catalog.UsersAsync(CatalogInstances, rows, token), ct);
            case Streams.CatalogTables:
            case Streams.CatalogColumns:
            case Streams.CatalogGrants:
                return await StreamedAsync((rows, token) => _catalog.PerDatabaseAsync(CatalogInstances, task.Stream, rows, token), ct);
            default:
                return null;
        }
    }

    /// <summary>
    /// Where the catalog is read: the first replica when there is one (a physical replica's catalog is the primary's), and
    /// the primary while it cannot be reached.
    /// </summary>
    private IReadOnlyList<PgInstance> CatalogInstances => _instances.Count > 1 ? new[] { _instances[1], _instances[0] } : new[] { _instances[0] };

    private async Task LogAsync(AgentTask task, RowWriter rows, CancellationToken ct)
    {
        var callsPerSecond = _utcNow() - FromMicros(task.EndMicros) > CatchUp ? CatchUpCallsPerSecond : LiveCallsPerSecond;
        foreach (var instance in _instances)
        {
            if (_rds != null)
            {
                // RDS writes its own prefix, which names the user on every line.
                var window = new PgWindowReader(instance.Id, task.StartMicros, task.EndMicros, _endpoint.StoreRawQueryText, rows.Sink, rows,
                    _english, _rotationMinutes);
                await window.ReadAsync(new RdsLogReader(_rds, instance.Id, callsPerSecond, budget: _budget), ct);
                window.WriteCounts();
            }
            else if (_azure != null)
            {
                await AzureWindowAsync(task, instance, rows, callsPerSecond, ct);
            }
            else if (_logAnalytics != null)
            {
                await LogAnalyticsWindowAsync(task, instance, rows, ct);
            }
            else
            {
                var directory = new PgLogDirectory(_logDirectory!);
                var window = new PgWindowReader(instance.Id, task.StartMicros, task.EndMicros, _endpoint.StoreRawQueryText, rows.Sink, rows,
                    _english, _collectorOff ? 0 : _rotationMinutes, _prefix, _sessions, directory);
                await window.ReadAsync(directory, ct);
                window.WriteCounts();
            }
            await rows.FlushAsync(ct);
        }
    }

    /// <summary>
    /// A window older than this is read as Azure's files hold it: Azure writes them about every 20 minutes, the hour
    /// before's too, up to about 25 minutes after that hour ended.
    /// </summary>
    internal static readonly TimeSpan AzureSettled = TimeSpan.FromHours(2);

    /// <summary>
    /// One window of Azure's server log download. Azure writes the files about every 20 minutes, so a recent window
    /// waits until a line past its end is there (or <see cref="AzureSettled"/> has gone by: an idle server writes nothing
    /// to wait for).
    /// </summary>
    private async Task AzureWindowAsync(AgentTask task, PgInstance instance, RowWriter rows, double callsPerSecond, CancellationToken ct)
    {
        if (_prefix == null)
        {
            Prefix(AzureDefaultPrefix);
        }
        var reader = new AzureLogReader(_azure!, callsPerSecond);
        var recent = _utcNow() - FromMicros(task.EndMicros) < AzureSettled;
        if (recent && !await reader.WrittenPastAsync(task.EndMicros + PgWindowReader.StopMarginMicros, _prefix!.TimeOf, ct))
        {
            throw new SourceException("Azure has not written the whole window to the server's log files yet") { NotYetPublished = true };
        }
        var window = new PgWindowReader(instance.Id, task.StartMicros, task.EndMicros, _endpoint.StoreRawQueryText, rows.Sink, rows,
            _english, _rotationMinutes, _prefix, _sessions, reader, azureFile: true);
        await window.ReadAsync(reader, ct);
        window.WriteCounts();
    }

    /// <summary>
    /// A window older than this needs only some row of the server after it: every row of the window has arrived by then.
    /// </summary>
    internal static readonly TimeSpan LogAnalyticsSettled = TimeSpan.FromHours(1);

    /// <summary>
    /// How far past a window a row must have reached Log Analytics before the window is read. Rows reach it in batches,
    /// 120 to 347 s after they are written, and a batch can come after a newer one: at worst 176 s behind the newest row
    /// already there (6,480 rows over 5.5 hours, measured).
    /// </summary>
    internal const long LogAnalyticsArrivalMicros = 300_000_000;

    /// <summary>
    /// One window of the server's rows in Log Analytics. A recent window waits until a row stamped
    /// <see cref="LogAnalyticsArrivalMicros"/> past its end is there; an older one needs a row after its end.
    /// </summary>
    /// <remarks>
    /// 🔴 No row after a window is never read as an empty window: on Azure the platform's own sign-ins write rows every
    ///    minute, so none means the server is stopped or nothing sends its log there any more (a diagnostic setting
    ///    removed or changed). The window fails and says so, while the download still holds those days.
    /// </remarks>
    private async Task LogAnalyticsWindowAsync(AgentTask task, PgInstance instance, RowWriter rows, CancellationToken ct)
    {
        if (_prefix == null)
        {
            Prefix(AzureDefaultPrefix);
        }
        var target = _logTarget ?? (await LogTargetAsync(ct)).Target;
        var reader = new AzureLogAnalyticsReader(_logAnalytics!, target);
        var recent = _utcNow() - FromMicros(task.EndMicros) < LogAnalyticsSettled;
        if (!await reader.WrittenPastAsync(task.EndMicros + (recent ? LogAnalyticsArrivalMicros : 0), ct))
        {
            throw recent
                ? new SourceException("Log Analytics has not received the whole window yet") { NotYetPublished = true }
                : new SourceException($"Log Analytics has no row of {instance.Id} in {target.Table} after the window: the server is stopped, "
                    + "or nothing sends its PostgreSQLLogs there any more (check its diagnostic setting). The server log download keeps "
                    + "the last logfiles.retention_days");
        }
        var window = new PgWindowReader(instance.Id, task.StartMicros, task.EndMicros, _endpoint.StoreRawQueryText, rows.Sink, rows,
            _english, _rotationMinutes, _prefix, _sessions, reader, azureFile: true);
        await window.ReadAsync(reader, ct);
        window.WriteCounts();
    }

    /// <summary>
    /// The server's rows in Log Analytics: which table and workspace hold them (<see cref="LogTargetOf"/>), how many came
    /// in the last day, the newest and the oldest. A workspace keeps them for its retention.
    /// </summary>
    private async Task<(List<PostgresInstance>, List<PostgresCheck>, string)> LogAnalyticsAsync(CancellationToken ct)
    {
        var report = new PostgresInstance { Id = _instances[0].Id, Role = "primary" };
        var checks = new List<PostgresCheck>();
        try
        {
            var (target, tables) = await LogTargetAsync(ct);
            var inTable = tables.Where(r => r.Table == target.Table && r.Rows > 0).ToList();
            var read = inTable.Where(r => target.Workspace == null || r.Workspace == target.Workspace).ToList();
            var rows = read.Sum(r => r.Rows);
            report.OldestLogMicros = rows > 0 ? await _logAnalytics!.OldestMicrosAsync(target, ct) ?? 0 : 0;
            checks.Add(PgProbeChecks.AzureLogAnalytics(report.Id, target, rows, report.OldestLogMicros,
                read.Select(r => r.NewestMicros ?? 0).DefaultIfEmpty(0).Max(), (_utcNow() - DateTime.UnixEpoch).Ticks / 10,
                tables.Where(r => r.Table != target.Table && r.Rows > 0).Select(r => r.Table).Distinct().ToList(),
                inTable.Select(r => r.Workspace).Distinct().ToList()));
        }
        catch (SourceException ex)
        {
            checks.Add(new PostgresCheck { Name = "log_analytics", Status = "fail", Message = Runner.EndpointSession.Trim(ex.Message) });
        }
        return (new List<PostgresInstance> { report }, checks, "stderr");
    }

    /// <summary>Finds where the server's rows are (<see cref="LogTargetOf"/>) and keeps it for the windows.</summary>
    private async Task<(AzureLogTarget Target, IReadOnlyList<AzureLogTableRows> Tables)> LogTargetAsync(CancellationToken ct)
    {
        var tables = await _logAnalytics!.TablesAsync(ct);
        var target = LogTargetOf(tables, _givenTable);
        _logTarget = target;
        return (target, tables);
    }

    /// <summary>
    /// Where the server's rows are read: the table given, else the one that holds them (the resource-specific one first,
    /// and it when neither does). 🔴 When they reach more than one workspace (two diagnostic settings), the server's own
    /// query answers every row once per workspace, as Microsoft documents a resource's query (not measured: only one
    /// workspace was there to send to): only the workspace with the most of them is read.
    /// </summary>
    internal static AzureLogTarget LogTargetOf(IReadOnlyList<AzureLogTableRows> tables, string? given = null)
    {
        var table = given ?? AzureLogTables.All.FirstOrDefault(t => tables.Any(r => r.Table == t && r.Rows > 0)) ?? AzureLogTables.ResourceSpecific;
        var workspaces = tables.Where(r => r.Table == table && r.Rows > 0 && r.Workspace.Length > 0)
            .OrderByDescending(r => r.Rows).ThenBy(r => r.Workspace, StringComparer.Ordinal).ToList();
        return new AzureLogTarget(table, workspaces.Count > 1 ? workspaces[0].Workspace : null);
    }

    /// <summary>The server's log files on Azure: how many, and the oldest; Azure keeps them for <c>logfiles.retention_days</c>.</summary>
    private async Task<(List<PostgresInstance>, List<PostgresCheck>, string)> AzureLogsAsync(CancellationToken ct)
    {
        var report = new PostgresInstance { Id = _instances[0].Id, Role = "primary" };
        var checks = new List<PostgresCheck>();
        try
        {
            var files = await new AzureLogReader(_azure!, LiveCallsPerSecond).FilesAsync(ct);
            var starts = files.Select(f => f.StartMicros).OfType<long>().OrderBy(t => t).ToList();
            report.OldestLogMicros = starts.FirstOrDefault();
            report.LogFileMinutes = PgProbeChecks.FileMinutes(starts);
            checks.Add(PgProbeChecks.AzureLogFiles(report.Id, files.Count, report.OldestLogMicros));
        }
        catch (SourceException ex)
        {
            checks.Add(new PostgresCheck { Name = "log_files", Status = "fail", Message = Runner.EndpointSession.Trim(ex.Message) });
        }
        return (new List<PostgresInstance> { report }, checks, "stderr");
    }

    /// <summary>The prefix the newest probe read: a new one gets a parser, and sessions, of its own.</summary>
    internal void Prefix(string prefix)
    {
        if (_prefix?.Prefix == prefix)
        {
            return;
        }
        _prefix = PgLineFormat.Of(prefix);
        _sessions = _prefix.HasUser ? null : new PgSessionMap(_prefix);
    }

    private async Task<(List<PostgresInstance>, List<PostgresCheck>, string)> RdsLogsAsync(CancellationToken ct)
    {
        var instances = new List<PostgresInstance>();
        var checks = new List<PostgresCheck>();
        foreach (var instance in _instances)
        {
            var report = new PostgresInstance { Id = instance.Id, Role = instance.Role };
            instances.Add(report);
            try
            {
                var files = await _rds!.ListAsync(instance.Id, 0, "postgresql.log.", ct);
                var starts = files.Where(f => PgLogFileNames.FormatOf(f.Name) == PgLogFormat.Stderr)
                    .Select(f => PgLogFileNames.StartMicros(f.Name)).OfType<long>().OrderBy(t => t).ToList();
                report.OldestLogMicros = starts.FirstOrDefault();
                report.LogFileMinutes = PgProbeChecks.FileMinutes(starts);
                checks.Add(PgProbeChecks.LogFiles(instance.Id, starts.Count, report.LogFileMinutes));
            }
            catch (SourceException ex)
            {
                checks.Add(new PostgresCheck { Name = "log_files", Status = "fail", Message = Runner.EndpointSession.Trim(ex.Message) });
            }
        }
        return (instances, checks, "stderr");
    }

    private (List<PostgresInstance>, List<PostgresCheck>, string) DirectoryLogs(string platform)
    {
        var report = new PostgresInstance { Id = _instances[0].Id, Role = "primary" };
        var checks = new List<PostgresCheck>();
        var format = "";
        try
        {
            var directory = new PgLogDirectory(_logDirectory!);
            var files = directory.List();
            var newest = PgLogFileNames.ForWindow(files, long.MinValue, long.MaxValue - PgWindowReader.StopMarginMicros, 0)
                .OrderBy(f => f.LastWrittenMicros).LastOrDefault();
            if (newest == null)
            {
                checks.Add(PgProbeChecks.NoLogFiles(_logDirectory!, platform));
            }
            else
            {
                format = PgWindowReader.FormatName(newest.Format);
                using (var stream = directory.Open(newest))
                {
                    _ = stream.ReadByte();
                }
                var starts = files.Where(f => f.Format == newest.Format).Select(f => f.StartMicros).OfType<long>().OrderBy(t => t).ToList();
                report.OldestLogMicros = starts.FirstOrDefault();
                report.LogFileMinutes = PgProbeChecks.FileMinutes(starts);
                checks.Add(_collectorOff
                    ? PgProbeChecks.LogFiles(report.Id, starts.Count, report.LogFileMinutes)
                    : PgProbeChecks.LogFiles(report.Id, starts.Count, report.LogFileMinutes, _rotationMinutes));
            }
        }
        catch (SourceException ex)
        {
            checks.Add(new PostgresCheck { Name = "log_files", Status = "fail", Message = Runner.EndpointSession.Trim(ex.Message) });
        }
        return (new List<PostgresInstance> { report }, checks, format);
    }

    /// <summary>
    /// Signs in to each replica once: one that cannot be reached is skipped by every task (its counters, settings and texts),
    /// and this check is where that shows. A rejected password is not caught: it fails the probe, as on the primary.
    /// </summary>
    private async Task<PostgresCheck> ReplicasAnswerAsync(CancellationToken ct)
    {
        var failed = new List<string>();
        foreach (var replica in _instances.Skip(1))
        {
            try
            {
                await using var conn = await _catalog.OpenAsync(replica.Host, _catalog.Database, ct);
            }
            catch (SourceException ex) when (!ex.AuthenticationRejected)
            {
                failed.Add(Runner.EndpointSession.Trim(ex.Message));
            }
        }
        return PgProbeChecks.ReplicasAnswer(_instances.Count - 1, failed);
    }

    private static async Task<PostgresCheck> RoleCheckAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(PgSql.OwnRole, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return new PostgresCheck { Name = "role", Status = "warn", Message = "the agent's role was not found in pg_roles" };
        }
        return PgProbeChecks.Role(reader.GetBoolean(0), reader.GetInt32(1), reader.GetBoolean(2), reader.GetBoolean(3));
    }

    private static async Task<Dictionary<string, string>> SettingsAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        var settings = new Dictionary<string, string>(StringComparer.Ordinal);
        await using var cmd = new NpgsqlCommand(PgSql.Settings, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            settings[reader.GetString(0)] = reader.IsDBNull(1) ? "" : reader.GetString(1);
        }
        return settings;
    }

    /// <summary>
    /// <c>aurora</c>, <c>rds</c>, <c>azure</c> or <c>self_managed</c>. An RDS API source asks <see cref="PgSql.AuroraVersion"/>
    /// once: on plain RDS the call fails, and each failure is a line in the customer's server log. An answer that is not
    /// definite (any failure but "function does not exist") is not kept, so the next probe asks again. Neither is a
    /// planted function's: without <paramref name="mayCall"/> the call is not made, and the answer is not known. Any
    /// other source is Azure when the server defines Azure's own setting (<see cref="PgSql.AzureMarker"/>).
    /// </summary>
    private async Task<string> PlatformAsync(NpgsqlConnection conn, bool mayCall, CancellationToken ct)
    {
        if (_rds == null)
        {
            var marker = await ScalarAsync(conn, PgSql.AzureMarker, ct);
            var own = PlatformOf(false, null, marker is true);
            if (marker is bool)
            {
                _platform = own;
            }
            return own;
        }
        string? answer = null;
        if (mayCall)
        {
            try
            {
                answer = AuroraAnswer(await AuroraCallAsync(conn, PgSql.AuroraVersion, ct), null);
            }
            catch (PostgresException ex)
            {
                answer = AuroraAnswer(null, ex.SqlState);
            }
        }
        var platform = PlatformOf(true, answer);
        if (answer != null)
        {
            _platform = platform;
        }
        return platform;
    }

    /// <summary>
    /// What <c>aurora_version()</c> said: its version when it answered; <c>""</c> for "function does not exist" (not
    /// Aurora); null for any other failure (not known).
    /// </summary>
    internal static string? AuroraAnswer(object? value, string? sqlState) => sqlState switch
    {
        null => value as string ?? "",
        PostgresErrorCodes.UndefinedFunction => "",
        _ => null,
    };

    /// <summary>
    /// What <see cref="PgSql.AuroraFunctionListed"/> said, as leave to call an Aurora function: only a definite "none is
    /// listed". A failed check (null) calls nothing either.
    /// </summary>
    internal static bool MayCallAurora(object? listed) => listed is false;

    /// <summary>
    /// The platform caps report: an RDS API source is Aurora when it answered a version, else RDS; any other source is
    /// Azure where the server defines Azure's own setting, else self-managed.
    /// </summary>
    internal static string PlatformOf(bool rdsApi, string? auroraVersion, bool azure = false) =>
        !rdsApi ? (azure ? PgPlatform.Azure : PgPlatform.SelfManaged)
        : string.IsNullOrEmpty(auroraVersion) ? PgPlatform.Rds : PgPlatform.Aurora;

    /// <param name="pinned">An Aurora call: through <see cref="AuroraCallAsync"/>.</param>
    private static async Task<object?> ScalarAsync(NpgsqlConnection conn, string sql, CancellationToken ct, bool pinned = false)
    {
        try
        {
            if (pinned)
            {
                return await AuroraCallAsync(conn, sql, ct);
            }
            await using var cmd = new NpgsqlCommand(sql, conn);
            var value = await cmd.ExecuteScalarAsync(ct);
            return value is DBNull ? null : value;
        }
        catch (PostgresException)
        {
            return null;
        }
    }

    /// <summary>
    /// An Aurora call in a transaction of its own, behind <see cref="PgSql.PinSearchPath"/>: no function a database user
    /// created, before the listing check or after it, can answer it. A failure rolls the transaction back and throws.
    /// </summary>
    internal static async Task<object?> AuroraCallAsync(NpgsqlConnection conn, string sql, CancellationToken ct)
    {
        await using var tx = await conn.BeginTransactionAsync(ct);
        await using (var pin = new NpgsqlCommand(PgSql.PinSearchPath, conn, tx))
        {
            await pin.ExecuteNonQueryAsync(ct);
        }
        await using var cmd = new NpgsqlCommand(sql, conn, tx);
        var value = await cmd.ExecuteScalarAsync(ct);
        await tx.CommitAsync(ct);
        return value is DBNull ? null : value;
    }

    private static async Task<(int, string)> ScalarPairAsync(NpgsqlConnection conn, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return (reader.GetInt32(0), reader.GetString(1));
    }

    /// <summary>The rows of one task, through <see cref="StreamedRows"/>.</summary>
    internal static Task<RowStream> StreamedAsync(Func<RowWriter, CancellationToken, Task> produce, CancellationToken ct) =>
        StreamedRows.OpenAsync(produce, ct);

    private static DateTime FromMicros(long micros) => DateTime.UnixEpoch.AddTicks(micros * 10);
}
