using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.MongoDb;
using Trailox.Agent.Engines.Oracle;
using Trailox.Agent.Protocol;
using Trailox.Agent.Runner;
using Streams = Trailox.Agent.Protocol.ProtocolInfo.Streams;

namespace Trailox.Agent.Tests.Oracle;

/// <summary>A task end to end, from the rows behind the driver seam to the rows the upload reads; and the probe on the same seam.</summary>
public class OraSourceTests
{
    private static readonly DateTime Now = new(2026, 10, 22, 7, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Start = Now.AddMinutes(-30);

    private static long Micros(DateTime at) => (at - DateTime.UnixEpoch).Ticks / 10;

    private static EndpointConfig Endpoint(bool text = true) => new()
    {
        Alias = "ledger",
        Engine = "oracle",
        Kind = "cloud",
        Host = "ledger.abc123def456.eu-west-1.rds.amazonaws.com",
        Port = 1521,
        Tls = false,
        Username = "TRAILOX_MONITOR",
        Password = "not-a-real-password",
        StoreRawQueryText = text,
        Options = new Dictionary<string, string> { ["service_name"] = "LEDGER" },
    };

    private static AgentTask Task(string stream) =>
        new() { ChunkId = "c1", EndpointId = 1, Stream = stream, StartMicros = Micros(Start), EndMicros = Micros(Now) };

    private static object?[] Trail(DateTime at, string user, string action, long returnCode = 0, string? text = null, string? obj = "ACCOUNTS", string? source = "DB",
        long? length = null)
    {
        var row = new object?[OraSql.ColumnSource + 1];
        row[OraSql.ColumnTime] = at;
        row[OraSql.ColumnInstance] = 1L;
        row[OraSql.ColumnUser] = user;
        row[OraSql.ColumnAction] = action;
        row[OraSql.ColumnReturnCode] = returnCode;
        row[OraSql.ColumnSchema] = "APP";
        row[OraSql.ColumnObject] = obj;
        row[OraSql.ColumnProgram] = "JDBC Thin Client";
        row[OraSql.ColumnOsUser] = "svc";
        row[OraSql.ColumnUserHost] = "app-1";
        row[OraSql.ColumnTerminal] = "unknown";
        row[OraSql.ColumnAuthentication] = "(TYPE=(DATABASE));(CLIENT ADDRESS=((ADDRESS=(PROTOCOL=tcp)(HOST=192.0.2.10)(PORT=34770))));";
        row[OraSql.ColumnPolicies] = "TRAILOX_FULL";
        row[OraSql.ColumnAuditType] = "Standard";
        row[OraSql.ColumnSessionId] = 2828371651L;
        row[OraSql.ColumnEntryId] = 1L;
        row[OraSql.ColumnStatementId] = 1L;
        row[OraSql.ColumnTextLength] = length ?? (long)(text?.Length ?? 0);
        row[OraSql.ColumnText] = text;
        row[OraSql.ColumnSource] = source;
        return row;
    }

    private static async Task<List<JsonElement>> RowsAsync(RowStream stream)
    {
        await using (stream)
        {
            using var text = new StreamReader(stream.Body);
            var body = await text.ReadToEndAsync();
            return body.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(l => JsonDocument.Parse(l).RootElement.Clone()).ToList();
        }
    }

    private static FakeOra Poll(bool source, params object?[][] rows)
    {
        var fake = new FakeOra();
        fake.Answer(OraSql.SourceColumn, source ? new[] { new object?[] { 1L } } : Array.Empty<object?[]>());
        fake.Answer(OraSql.Poll(text: true, source: source), rows);
        return fake;
    }

    private static async Task<List<JsonElement>> PollAsync(FakeOra fake, EndpointConfig? endpoint = null)
    {
        var source = new OraSource(endpoint ?? Endpoint(), fake, () => Now);
        return await RowsAsync((await source.OpenAsync(Task(Streams.OraAudit), CancellationToken.None))!);
    }

    private static JsonElement Summary(List<JsonElement> rows) => Assert.Single(rows, r => r.GetProperty("action_name").GetString() == OraPoll.SummaryAction);

    [Fact]
    public async Task A_window_ships_each_record_with_its_columns_and_one_count_row_at_the_windows_start()
    {
        var rows = await PollAsync(Poll(true, Trail(Start.AddMinutes(1), "BOB", "LOGON", 1017, obj: null)));

        Assert.Equal(2, rows.Count);
        var record = rows[0];
        Assert.Equal("2026-10-22T06:31:00.000000Z", record.GetProperty("event_time").GetString());
        Assert.Equal(("BOB", "LOGON", 1017), (record.GetProperty("dbusername").GetString(), record.GetProperty("action_name").GetString(), record.GetProperty("return_code").GetInt32()));
        Assert.Equal("", record.GetProperty("object_name").GetString());
        Assert.Equal(2828371651L, record.GetProperty("sessionid").GetInt64());
        Assert.Contains("HOST=192.0.2.10", record.GetProperty("authentication_type").GetString());
        Assert.Equal(1, record.GetProperty("event_count").GetInt32());

        var count = Summary(rows);
        Assert.Equal("2026-10-22T06:30:00.000000Z", count.GetProperty("event_time").GetString());
        Assert.Equal(1, count.GetProperty("event_count").GetInt32());
        using var json = JsonDocument.Parse(count.GetProperty("summary").GetString()!);
        Assert.Equal(1, json.RootElement.GetProperty("records").GetInt32());
        Assert.Equal(1, json.RootElement.GetProperty("by_action").GetProperty("LOGON").GetInt32());
    }

    [Fact]
    public async Task An_empty_window_still_ships_its_count_row()
    {
        var rows = await PollAsync(Poll(true));
        Assert.Equal(0, JsonDocument.Parse(Assert.Single(rows).GetProperty("summary").GetString()!).RootElement.GetProperty("records").GetInt32());
    }

    [Fact]
    public async Task The_poll_binds_its_window_and_our_user_and_sets_the_session_first()
    {
        var fake = Poll(true);
        await PollAsync(fake);

        Assert.Equal(OraSql.SessionSettings, fake.Statements.Take(OraSql.SessionSettings.Count));
        var binds = fake.Binds.Last().ToDictionary(b => b.Name, b => b.Value);
        Assert.Equal(Start, binds["lo"]);
        Assert.Equal(Now, binds["hi"]);
        Assert.Equal("TRAILOX_MONITOR", binds["me"]);
    }

    [Fact]
    public async Task Text_is_redacted_and_the_trails_trailing_nul_goes()
    {
        var rows = await PollAsync(Poll(true, Trail(Start.AddMinutes(1), "ADMIN", "ALTER USER", text: "ALTER USER a IDENTIFIED BY S3cret\0")));

        Assert.Equal("ALTER USER a IDENTIFIED BY <redacted>", rows[0].GetProperty("sql_text").GetString());
    }

    /// <summary>Every text ships as its shape, grouped or not: no pattern can find a value in stored code or dynamic SQL.</summary>
    [Fact]
    public async Task A_statement_that_is_not_grouped_ships_no_value_or_comment_either()
    {
        var rows = await PollAsync(Poll(true, Trail(Start.AddMinutes(1), "ADMIN", "CREATE PROCEDURE",
            text: "CREATE PROCEDURE p AS /* key S3cret */ BEGIN EXECUTE IMMEDIATE 'ALTER USER a IDENT' || 'IFIED BY S3cret'; x := 42; END;")));

        Assert.Equal("CREATE PROCEDURE p AS BEGIN EXECUTE IMMEDIATE ? || ?; x := ?; END;", rows[0].GetProperty("sql_text").GetString());
    }

    [Fact]
    public async Task A_record_with_no_return_code_does_not_read_as_a_success()
    {
        var rows = await PollAsync(Poll(true, Trail(Start.AddMinutes(1), "BOB", "LOGON")));
        Assert.Equal(0, rows[0].GetProperty("return_code").GetInt32());

        var record = Trail(Start.AddMinutes(1), "BOB", "LOGON");
        record[OraSql.ColumnReturnCode] = null;
        rows = await PollAsync(Poll(true, record));
        Assert.Equal(-1, rows[0].GetProperty("return_code").GetInt32());
    }

    [Fact]
    public async Task A_text_in_pieces_is_joined_and_a_longer_one_is_marked()
    {
        var record = Trail(Start.AddMinutes(1), "ADMIN", "CREATE TABLE", text: new string('a', 1000), length: 4100);
        record[OraSql.ColumnText + 1] = new string('b', 1000);
        record[OraSql.ColumnText + 2] = new string('c', 1000);
        record[OraSql.ColumnText + 3] = new string('d', 1000);
        var rows = await PollAsync(Poll(true, record));

        Assert.Equal(OraSql.MaxTextChars, rows[0].GetProperty("sql_text").GetString()!.Length);
        Assert.Equal(1, rows[0].GetProperty("truncated").GetInt32());
        Assert.Equal(4100, rows[0].GetProperty("sql_text_length").GetInt32());
    }

    /// <summary>Grants, DDL and sign-ins stay one row each; reads and writes of one shape become one row with the count.</summary>
    [Fact]
    public async Task Statements_group_by_minute_and_shape_and_changes_never_do()
    {
        var rows = await PollAsync(Poll(true,
            Trail(Start.AddMinutes(1), "APP", "SELECT", text: "SELECT * FROM accounts WHERE id = 1"),
            Trail(Start.AddMinutes(1).AddSeconds(5), "APP", "SELECT", text: "SELECT * FROM accounts WHERE id = 2"),
            Trail(Start.AddMinutes(1).AddSeconds(6), "APP", "GRANT", text: "GRANT SELECT ON a TO b"),
            Trail(Start.AddMinutes(1).AddSeconds(7), "APP", "GRANT", text: "GRANT SELECT ON a TO b")));

        var select = Assert.Single(rows, r => r.GetProperty("action_name").GetString() == "SELECT");
        Assert.Equal(2, select.GetProperty("event_count").GetInt32());
        Assert.Equal("SELECT * FROM accounts WHERE id = ?", select.GetProperty("sql_text").GetString());
        Assert.Equal(16, select.GetProperty("shape_hash").GetString()!.Length);
        Assert.Equal(2, rows.Count(r => r.GetProperty("action_name").GetString() == "GRANT"));
        using var json = JsonDocument.Parse(Summary(rows).GetProperty("summary").GetString()!);
        Assert.Equal((4, 2, 1), (json.RootElement.GetProperty("records").GetInt32(), json.RootElement.GetProperty("shipped").GetInt32(), json.RootElement.GetProperty("audit_rows").GetInt32()));
    }

    [Fact]
    public async Task No_value_of_a_grouped_statement_leaves_and_a_password_in_it_is_gone_first()
    {
        var rows = await PollAsync(Poll(true, Trail(Start.AddMinutes(1), "APP", "EXECUTE", text: "BEGIN pay('4111', 12.50, password => 'S3cret'); END;")));

        Assert.Equal("BEGIN pay(?, password => ?); END;", rows[0].GetProperty("sql_text").GetString());
    }

    [Fact]
    public async Task With_no_statement_text_none_is_selected_or_shipped_and_statements_still_group()
    {
        var fake = new FakeOra();
        fake.Answer(OraSql.Poll(text: false, source: false),
            Trail(Start.AddMinutes(1), "APP", "SELECT", source: null),
            Trail(Start.AddMinutes(1).AddSeconds(1), "APP", "SELECT", source: null));
        var rows = await PollAsync(fake, Endpoint(text: false));

        var select = Assert.Single(rows, r => r.GetProperty("action_name").GetString() == "SELECT");
        Assert.Equal(("", 2), (select.GetProperty("sql_text").GetString(), select.GetProperty("event_count").GetInt32()));
    }

    [Fact]
    public async Task Records_from_spillover_files_are_counted_only_where_the_view_says_so()
    {
        var rows = await PollAsync(Poll(true, Trail(Start.AddMinutes(1), "BOB", "LOGON", source: "FILE"), Trail(Start.AddMinutes(1), "AL", "LOGON")));
        Assert.Equal(1, JsonDocument.Parse(Summary(rows).GetProperty("summary").GetString()!).RootElement.GetProperty("file_records").GetInt32());

        rows = await PollAsync(Poll(false, Trail(Start.AddMinutes(1), "BOB", "LOGON")));
        Assert.Equal(JsonValueKind.Null, JsonDocument.Parse(Summary(rows).GetProperty("summary").GetString()!).RootElement.GetProperty("file_records").ValueKind);
    }

    [Fact]
    public async Task A_poll_after_a_probe_does_not_ask_about_the_source_column_again()
    {
        var fake = Probed();
        fake.Answer(OraSql.Poll(text: true, source: true));
        var source = new OraSource(Endpoint(), fake, () => Now);
        await source.ProbeAsync(CancellationToken.None);
        var asked = fake.Statements.Count(s => s == OraSql.SourceColumn);

        await RowsAsync((await source.OpenAsync(Task(Streams.OraAudit), CancellationToken.None))!);

        Assert.Equal(asked, fake.Statements.Count(s => s == OraSql.SourceColumn));
        Assert.Contains(OraSql.Poll(text: true, source: true), fake.Statements);
    }

    /// <summary>A window can hold a bulk load's records: the fetch stops while the upload is behind.</summary>
    [Fact]
    public async Task Rows_are_streamed_so_an_upload_that_is_behind_stops_the_fetch()
    {
        const int total = 400_000;
        FakeRows? produced = null;
        var fake = new FakeOra();
        fake.Answer(OraSql.SourceColumn);
        fake.Answer(OraSql.Poll(text: true, source: false), _ => produced = new FakeRows(Endless(total)));

        var stream = (await new OraSource(Endpoint(), fake, () => Now).OpenAsync(Task(Streams.OraAudit), CancellationToken.None))!;
        await using (stream)
        {
            var read = -1;
            while (read != produced!.Read)
            {
                read = produced.Read;
                await System.Threading.Tasks.Task.Delay(50);
            }
            Assert.InRange(produced.Read, 1, total - 1);

            using var text = new StreamReader(stream.Body);
            Assert.Equal(total + 1, (await text.ReadToEndAsync()).Count(c => c == '\n'));
        }
    }

    private static IEnumerable<object?[]> Endless(int rows)
    {
        for (var i = 0; i < rows; i++)
        {
            yield return Trail(Start.AddSeconds(i % 1000), "ADMIN", "CREATE TABLE", text: "CREATE TABLE t" + i);
        }
    }

    [Fact]
    public async Task The_heartbeat_beats_every_few_thousand_rows()
    {
        var beats = 0;
        await using var rows = new FakeRows(Endless(12_000));
        var buffer = new System.Buffers.ArrayBufferWriter<byte>();

        await new OraPoll(Micros(Start), true, false, new RowWriter(new RowSink(buffer)), () => beats++).ReadAsync(rows, CancellationToken.None);

        Assert.Equal(2, beats);
    }

    [Fact]
    public async Task The_liveness_file_is_touched_when_a_poll_starts()
    {
        var path = Path.Combine(Path.GetTempPath(), $"trailox-ora-heartbeat-{Guid.NewGuid():N}");
        try
        {
            TaskHeartbeat.Attach(new HealthFile(path));
            await PollAsync(Poll(false));
            Assert.True(File.Exists(path));
        }
        finally
        {
            TaskHeartbeat.Attach(null);
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData(1013, true)]
    [InlineData(2393, false)]
    [InlineData(2395, false)]
    public async Task A_read_stopped_by_a_bound_says_so_so_the_next_window_is_shorter(int number, bool timedOut)
    {
        var fake = new FakeOra();
        fake.Answer(OraSql.SourceColumn);
        fake.Fails(OraSql.Poll(text: true, source: false), number, "stopped", timedOut);
        var source = new OraSource(Endpoint(), fake, () => Now);

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Task(Streams.OraAudit), CancellationToken.None));

        Assert.True(ex.TooLargeAtSource);
        Assert.False(ex.AuthenticationRejected);
    }

    /// <summary>A driver's command timeout may not reach a fetch, so the limit is held above it, on every call.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_fetch_that_hangs_is_stopped_at_the_limit_and_says_so_even_with_a_driver_that_ignores_its_token(bool deaf)
    {
        var fake = new FakeOra();
        fake.Answer(OraSql.SourceColumn);
        fake.Answer(OraSql.Poll(text: true, source: false), _ => new FakeRows(Array.Empty<object?[]>()) { HangsAtEnd = true, DeafToToken = deaf });
        var source = new OraSource(Endpoint(), fake, () => Now, TimeSpan.FromMilliseconds(100));

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Task(Streams.OraAudit), CancellationToken.None));

        Assert.True(ex.TooLargeAtSource);
    }

    /// <summary>The task's limit reaches the audit read, and no other statement changes with it.</summary>
    [Fact]
    public async Task A_tasks_read_limit_holds_the_audit_read_and_nothing_else()
    {
        var fake = new FakeOra();
        fake.Answer(OraSql.SourceColumn);
        fake.Answer(OraSql.Poll(text: true, source: false));
        var task = Task(Streams.OraAudit);
        task.ReadLimitSeconds = 300;

        await RowsAsync((await new OraSource(Endpoint(), fake, () => Now).OpenAsync(task, CancellationToken.None))!);

        Assert.Equal(new TimeSpan?[] { null, TimeSpan.FromMinutes(5) }, fake.Limits);
    }

    /// <summary>An older gateway sends no limit and changes nothing, and no value can take away the limit's bound.</summary>
    [Theory]
    [InlineData(null, null)]
    [InlineData(0, null)]
    [InlineData(-1, null)]
    [InlineData(1800, 1800)]
    [InlineData(int.MaxValue, OraSql.MaxReadLimitSeconds)]
    public void The_read_limit_comes_from_the_task_within_reason(int? seconds, int? limit) =>
        Assert.Equal(limit is { } set ? TimeSpan.FromSeconds(set) : null, OraSql.ReadLimit(seconds));

    /// <summary>The bound above the seam and the driver's own timeout must move together, or the shorter one still stops the read.</summary>
    [Theory]
    [InlineData(null, OraSql.CommandTimeoutSeconds)]
    [InlineData(1800.0, 1800)]
    [InlineData(0.2, 1)]
    public void The_drivers_timeout_follows_the_limit(double? limit, int seconds) =>
        Assert.Equal(seconds, OdpDriver.TimeoutSeconds(limit is { } set ? TimeSpan.FromSeconds(set) : null));

    [Fact]
    public async Task A_read_limit_longer_than_the_sessions_lets_a_long_fetch_finish()
    {
        FakeOra Slow()
        {
            var fake = new FakeOra();
            fake.Answer(OraSql.SourceColumn);
            fake.Answer(OraSql.Poll(text: true, source: false), _ => new FakeRows(Array.Empty<object?[]>()) { EndsAfter = TimeSpan.FromMilliseconds(300) });
            return fake;
        }
        var callLimit = TimeSpan.FromMilliseconds(100);
        var task = Task(Streams.OraAudit);
        task.ReadLimitSeconds = 30;

        var rows = await RowsAsync((await new OraSource(Endpoint(), Slow(), () => Now, callLimit).OpenAsync(task, CancellationToken.None))!);

        Assert.Single(rows);
        var stopped = await Assert.ThrowsAsync<SourceException>(() => new OraSource(Endpoint(), Slow(), () => Now, callLimit).OpenAsync(Task(Streams.OraAudit), CancellationToken.None));
        Assert.True(stopped.TooLargeAtSource);
    }

    /// <summary>The time reaches the task whether the read finished or a limit stopped it before its first row.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_poll_counts_what_it_waited_on_the_database(bool stopped)
    {
        var fake = new FakeOra();
        fake.Answer(OraSql.SourceColumn);
        fake.Answer(OraSql.Poll(text: true, source: false), _ => new FakeRows(Array.Empty<object?[]>()) { EndsAfter = TimeSpan.FromMilliseconds(200) });
        var source = new OraSource(Endpoint(), fake, () => Now, TimeSpan.FromMilliseconds(stopped ? 100 : 2000));
        var task = Task(Streams.OraAudit);

        if (stopped)
        {
            await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(task, CancellationToken.None));
        }
        else
        {
            await RowsAsync((await source.OpenAsync(task, CancellationToken.None))!);
        }

        Assert.True(task.ReadTime.Millis >= (stopped ? 90 : 190), $"{task.ReadTime.Millis} ms");
    }

    /// <summary>The limit is on each call: a long window of quick fetches is no reason to stop.</summary>
    [Fact]
    public async Task A_long_read_of_quick_fetches_runs_past_the_limit()
    {
        var fake = new FakeOra();
        fake.Answer(OraSql.SourceColumn);
        fake.Answer(OraSql.Poll(text: true, source: false), _ => new FakeRows(Slowly(6, TimeSpan.FromMilliseconds(40))));
        var source = new OraSource(Endpoint(), fake, () => Now, TimeSpan.FromMilliseconds(150));

        var rows = await RowsAsync((await source.OpenAsync(Task(Streams.OraAudit), CancellationToken.None))!);

        Assert.Equal(7, rows.Count);
    }

    private static IEnumerable<object?[]> Slowly(int rows, TimeSpan each)
    {
        for (var i = 0; i < rows; i++)
        {
            Thread.Sleep(each);
            yield return Trail(Start.AddSeconds(i), "ADMIN", "CREATE TABLE", text: "CREATE TABLE t" + i);
        }
    }

    [Fact]
    public async Task Any_other_failure_of_the_read_is_no_bound()
    {
        var fake = new FakeOra();
        fake.Answer(OraSql.SourceColumn);
        fake.Fails(OraSql.Poll(text: true, source: false), 942, "table or view does not exist");
        var source = new OraSource(Endpoint(), fake, () => Now);

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Task(Streams.OraAudit), CancellationToken.None));

        Assert.False(ex.TooLargeAtSource);
        Assert.Contains("ORA-00942", ex.Message);
    }

    /// <summary>A later fetch can hit a per-call limit too, with rows already on their way: the failure then comes through the upload's stream.</summary>
    [Fact]
    public async Task A_bound_hit_after_the_first_rows_still_carries_its_flag()
    {
        var fake = new FakeOra();
        fake.Answer(OraSql.SourceColumn);
        fake.Answer(OraSql.Poll(text: true, source: false), _ =>
            new FakeRows(Endless(20_000)) { FailsAtEnd = new OraDriverException(2395, "ORA-02395: exceeded call limit on IO usage") });

        var stream = (await new OraSource(Endpoint(), fake, () => Now).OpenAsync(Task(Streams.OraAudit), CancellationToken.None))!;
        await using (stream)
        {
            var ex = await Assert.ThrowsAsync<SourceException>(() => stream.Body.CopyToAsync(Stream.Null));
            Assert.True(ex.TooLargeAtSource);
        }
    }

    [Theory]
    [InlineData(1017, "fix the password")]
    [InlineData(28001, "set a new password")]
    [InlineData(1045, "grant CREATE SESSION")]
    public async Task A_refused_sign_in_stops_every_later_sign_in_until_a_restart(int number, string fix)
    {
        var fake = new FakeOra { OpenFails = new OraDriverException(number, $"ORA-{number:D5}: refused") };
        var source = new OraSource(Endpoint(), fake, () => Now);
        var session = new EndpointSession(Endpoint(), source, TimeSpan.FromMinutes(60));

        await session.ProbeAsync(NullLogger.Instance, CancellationToken.None);

        Assert.False(session.NeedsProbe);
        Assert.Contains(fix, session.ProbeError);
        Assert.Contains("then restart the agent", session.ProbeError);

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Task(Streams.OraAudit), CancellationToken.None));
        Assert.True(ex.AuthenticationRejected);
        Assert.Equal(1, fake.Opens);
    }

    /// <summary>
    /// Anyone who can reach the listener can lock a known user with wrong passwords, and the lock ends without the agent. A stop until
    /// a restart would leave the trail unread after that; a retry a minute would write 1,440 failed sign-ins a day.
    /// </summary>
    [Fact]
    public async Task A_locked_account_writes_no_failed_sign_in_for_an_hour_then_one()
    {
        var at = Now;
        var locked = new OraDriverException(28000, "ORA-28000: The account is locked.");
        var fake = new FakeOra { OpenFails = locked };
        var source = new OraSource(Endpoint(), fake, () => at);
        var session = new EndpointSession(Endpoint(), source, TimeSpan.FromMinutes(60), () => at);

        await session.ProbeAsync(NullLogger.Instance, CancellationToken.None);

        Assert.Equal(1, fake.Opens);
        Assert.Contains("unlock the account", session.ProbeError);
        Assert.Contains("no restart is needed", session.ProbeError);
        Assert.DoesNotContain("restart the agent", session.ProbeError);

        at = Now.Add(OraErrors.LockedRetry).AddSeconds(-1);
        Assert.False(session.NeedsProbe);
        var ex = await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Task(Streams.OraAudit), CancellationToken.None));
        Assert.True(ex.AuthenticationRejected);
        Assert.Equal(1, fake.Opens);

        at = Now.Add(OraErrors.LockedRetry).AddSeconds(1);
        Assert.True(session.NeedsProbe);
        await session.ProbeAsync(NullLogger.Instance, CancellationToken.None);
        Assert.Equal(2, fake.Opens);
        Assert.False(session.NeedsProbe);
    }

    [Fact]
    public async Task An_unlocked_account_goes_on_with_no_restart()
    {
        var at = Now;
        var fake = Probed();
        fake.OpenFails = new OraDriverException(28000, "ORA-28000: The account is locked.");
        fake.Answer(OraSql.Poll(text: true, source: true));
        var source = new OraSource(Endpoint(), fake, () => at);
        var session = new EndpointSession(Endpoint(), source, TimeSpan.FromMinutes(60), () => at);
        await session.ProbeAsync(NullLogger.Instance, CancellationToken.None);

        fake.OpenFails = null;
        at = Now.Add(OraErrors.LockedRetry).AddSeconds(1);
        await session.ProbeAsync(NullLogger.Instance, CancellationToken.None);

        Assert.Null(session.ProbeError);
        Assert.False(session.NeedsProbe);
        await RowsAsync((await source.OpenAsync(Task(Streams.OraAudit), CancellationToken.None))!);
    }

    /// <summary>An hour after a lock the password may turn out wrong too: that one still stops, or the lock would come back.</summary>
    [Fact]
    public async Task A_wrong_password_found_after_a_lock_still_stops()
    {
        var at = Now;
        var fake = new FakeOra { OpenFails = new OraDriverException(28000, "ORA-28000: The account is locked.") };
        var session = new EndpointSession(Endpoint(), new OraSource(Endpoint(), fake, () => at), TimeSpan.FromMinutes(60), () => at);
        await session.ProbeAsync(NullLogger.Instance, CancellationToken.None);

        fake.OpenFails = new OraDriverException(1017, "ORA-01017: invalid username/password; logon denied");
        at = Now.Add(OraErrors.LockedRetry).AddSeconds(1);
        await session.ProbeAsync(NullLogger.Instance, CancellationToken.None);
        at = at.AddDays(30);

        Assert.False(session.NeedsProbe);
        Assert.Contains("restart the agent", session.ProbeError);
    }

    [Theory]
    [InlineData(12541)]
    [InlineData(12514)]
    [InlineData(0)]
    public async Task A_network_error_keeps_the_retry(int number)
    {
        var fake = new FakeOra { OpenFails = new OraDriverException(number, "the listener did not answer") };
        var session = new EndpointSession(Endpoint(), new OraSource(Endpoint(), fake, () => Now), TimeSpan.FromMinutes(60));

        await session.ProbeAsync(NullLogger.Instance, CancellationToken.None);
        await session.ProbeAsync(NullLogger.Instance, CancellationToken.None);

        Assert.True(session.NeedsProbe);
        Assert.Equal(2, fake.Opens);
    }

    /// <summary>A server that answers the probe as a source set up by the printed script does.</summary>
    private static FakeOra Probed(string container = "LEDGER", string? auditSessionId = "2828371651")
    {
        var fake = new FakeOra();
        fake.Answer(OraSql.Product, new object?[] { "Oracle Database 19c Standard Edition 2 Release 19.0.0.0.0 - Production", "19.32.0.0.0" });
        fake.Answer(OraSql.Session, new object?[] { "TRAILOX_MONITOR", container, "trailox-agent.dll", auditSessionId });
        fake.Answer(OraSql.RdsAdmin, new object?[] { 1L });
        fake.Answer(OraSql.Options, new object?[] { "Unified Auditing", "FALSE" }, new object?[] { "Partitioning", "FALSE" });
        fake.Answer(OraSql.Parameters, new object?[] { "audit_trail", "DB" }, new object?[] { "resource_limit", "TRUE" });
        fake.Answer(OraSql.SourceColumn, new object?[] { 1L });
        fake.Answer(OraSql.TrailStatistics, new object?[] { 1000L, 50L, "YES", Now });
        fake.Answer(OraSql.TrailPartitioning, new object?[] { "NUMTODSINTERVAL(1, 'DAY')", "TRAILOX_AUDIT" });
        fake.Answer(OraSql.TrailPartitions,
            new object?[] { "SYS_P181", "TIMESTAMP' 2026-10-01 00:00:00'", "SYSAUX" },
            new object?[] { "SYS_P241", "TIMESTAMP' 2026-10-23 00:00:00'", "TRAILOX_AUDIT" });
        fake.Answer(OraSql.TrailConfiguredTablespace, new object?[] { "TRAILOX_AUDIT" });
        fake.Answer(OraSql.TablespaceFiles, new object?[] { 10_000_000_000L, 100_000_000_000L });
        fake.Answer(OraSql.TablespaceFree, new object?[] { 1_000_000_000L });
        fake.Answer(OraSql.EnabledPolicies,
            new object?[] { "ORA_LOGON_FAILURES", "BY USER", "ALL USERS", "USER", "NO", "YES" },
            new object?[] { "ORA_SECURECONFIG", "BY USER", "ALL USERS", "USER", "YES", "YES" },
            new object?[] { "TRAILOX_LOGON", "EXCEPT USER", "TRAILOX_MONITOR", "USER", "YES", "YES" });
        fake.Answer(OraSql.PolicyObjects, new object?[] { "TRAILOX_TABLES", "APP", "ACCOUNTS" });
        fake.Answer(OraSql.OwnResourceLimits,
            new object?[] { "SESSIONS_PER_USER", "2" }, new object?[] { "CPU_PER_CALL", "6000" }, new object?[] { "IDLE_TIME", "10" }, new object?[] { "LOGICAL_READS_PER_CALL", "300000" });
        fake.Answer(OraSql.OwnAccount, new object?[] { null, "OPEN" });
        fake.Answer(OraSql.CleanupJobs, new object?[] { "TRAILOX_PURGE", "ENABLED", "UNIFIED AUDIT TRAIL" });
        return fake;
    }

    [Fact]
    public async Task The_probe_reports_what_the_server_says_and_opens_one_session()
    {
        var fake = Probed();
        var caps = await new OraSource(Endpoint(), fake, () => Now).ProbeAsync(CancellationToken.None);

        Assert.Equal("19.32.0.0.0", caps.Version);
        Assert.True(caps.HasSessionLog);
        Assert.Equal(0, caps.EarliestEventMicros);
        var oracle = caps.Oracle!;
        Assert.Equal(("rds", "LEDGER", "TRAILOX_MONITOR", "trailox-agent.dll"), (oracle.Platform, oracle.Container, oracle.OwnUser, oracle.OwnProgram));
        Assert.Equal(("SYS_P241", "TRAILOX_AUDIT", "mixed"), (oracle.TodayPartition, oracle.TodayTablespace, oracle.UnifiedMode));
        Assert.Equal(Micros(new DateTime(2026, 10, 23, 0, 0, 0, DateTimeKind.Utc)), oracle.TodayPartitionEndMicros);
        Assert.True(oracle.SourceColumn);
        Assert.Equal("partial", oracle.ObservedLevel);
        Assert.True(oracle.PoliciesRead);
        Assert.True(oracle.PolicyObjectsRead);
        Assert.Equal(("TRAILOX_TABLES", "APP", "ACCOUNTS"), (oracle.PolicyObjects[0].Policy, oracle.PolicyObjects[0].Schema, oracle.PolicyObjects[0].Name));
        Assert.Equal(1, fake.Opens);
        Assert.Equal(1, fake.MostSessions);
        Assert.Equal(0, fake.OpenSessions);
    }

    [Fact]
    public async Task The_probe_on_a_server_older_than_19c_says_so()
    {
        var fake = Probed();
        fake.Answer(OraSql.Product, new object?[] { "Oracle Database 18c Enterprise Edition", "18.3.0.0.0" });

        var ex = await Assert.ThrowsAsync<SourceException>(() => new OraSource(Endpoint(), fake, () => Now).ProbeAsync(CancellationToken.None));

        Assert.Contains("19c", ex.Message);
        Assert.Null(ex.RefusedCaps);
    }

    /// <summary>Only a lost session ends the probe: an optional read that fails for any other reason must not stop a healthy source.</summary>
    [Theory]
    [InlineData(1013)]
    [InlineData(2395)]
    [InlineData(1039)]
    public async Task An_optional_read_that_fails_for_any_reason_is_a_warning(int number)
    {
        var fake = Probed();
        fake.Fails(OraSql.TablespaceFree, number);

        var caps = await new OraSource(Endpoint(), fake, () => Now).ProbeAsync(CancellationToken.None);

        Assert.Contains("DBA_FREE_SPACE", Assert.Single(caps.Oracle!.Checks, c => c.Name == "views").Message);
        Assert.Equal("warn", Assert.Single(caps.Oracle.Checks, c => c.Name == "room").Status);
    }

    private static FakeRows Hangs(bool deaf = false) => new(Array.Empty<object?[]>()) { HangsAtEnd = true, DeafToToken = deaf };

    private static OraSource Bounded(FakeOra fake) => new(Endpoint(), fake, () => Now, TimeSpan.FromMilliseconds(100));

    /// <summary>A slow optional view must not cost the source its polls, whether the driver stops at the limit or not.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task An_optional_read_that_hangs_fails_nothing_and_is_named_as_slow(bool deaf)
    {
        var fake = Probed();
        fake.Answer(OraSql.TablespaceFree, _ => Hangs(deaf));

        var caps = await Bounded(fake).ProbeAsync(CancellationToken.None);

        var views = Assert.Single(caps.Oracle!.Checks, c => c.Name == "views");
        Assert.Contains("DBA_FREE_SPACE", views.Message);
        Assert.Contains("did not answer in time", views.Message);
        Assert.DoesNotContain(caps.Oracle.Checks, c => c.Status == "fail");
    }

    /// <summary>The profile's limits stop a read as a time limit does: no grant would help, so the message must not ask for one.</summary>
    [Theory]
    [InlineData(2393)]
    [InlineData(2395)]
    public async Task An_optional_read_the_profile_stopped_is_slow_and_no_missing_grant(int number)
    {
        var fake = Probed();
        fake.Fails(OraSql.TablespaceFree, number);

        var caps = await new OraSource(Endpoint(), fake, () => Now).ProbeAsync(CancellationToken.None);

        var views = Assert.Single(caps.Oracle!.Checks, c => c.Name == "views").Message;
        Assert.Contains("DBA_FREE_SPACE did not answer in time", views);
        Assert.DoesNotContain("grant", views, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>The reason must reach the report even when nothing on that connection can be closed.</summary>
    [Fact]
    public async Task A_read_stopped_on_a_connection_that_will_not_close_still_says_why()
    {
        var fake = new FakeOra { CloseHangs = true };
        fake.Answer(OraSql.SourceColumn);
        fake.Answer(OraSql.Poll(text: true, source: false), _ => new FakeRows(Array.Empty<object?[]>()) { HangsAtEnd = true, DeafToToken = true, DisposeHangs = true });
        var source = new OraSource(Endpoint(), fake, () => Now, TimeSpan.FromMilliseconds(100));

        var open = source.OpenAsync(Task(Streams.OraAudit), CancellationToken.None);
        var stopped = await Assert.ThrowsAsync<SourceException>(() => open.WaitAsync(TimeSpan.FromSeconds(20)));

        Assert.True(stopped.TooLargeAtSource);
    }

    /// <summary>A stopped call that ended leaves the session as it was, so the probe reads the rest.</summary>
    [Fact]
    public async Task The_reads_after_a_stopped_read_go_on()
    {
        var fake = Probed();
        fake.Answer(OraSql.TablespaceFree, _ => Hangs());

        var caps = await Bounded(fake).ProbeAsync(CancellationToken.None);

        Assert.True(caps.Oracle!.PoliciesRead);
        Assert.Equal("ok", Assert.Single(caps.Oracle.Checks, c => c.Name == "profile").Status);
    }

    /// <summary>A stopped call that never ended must not make each later read wait out its limit on the same connection.</summary>
    [Fact]
    public async Task The_reads_after_a_call_that_will_not_stop_fail_at_once_and_are_named_too()
    {
        var fake = Probed();
        fake.Answer(OraSql.TablespaceFree, _ => Hangs(deaf: true));
        fake.Answer(OraSql.SpillAny, _ => Hangs());

        var caps = await Bounded(fake).ProbeAsync(CancellationToken.None);

        var views = Assert.Single(caps.Oracle!.Checks, c => c.Name == "views").Message;
        Assert.Contains("V_$UNIFIED_AUDIT_TRAIL", views);
        Assert.Contains("AUDIT_UNIFIED_ENABLED_POLICIES", views);
        Assert.DoesNotContain(OraSql.SpillAny, fake.Statements);
    }

    /// <summary>Only a refusal says the grant is missing. A read that took too long must not refuse a source that has it.</summary>
    [Fact]
    public async Task A_poll_view_read_that_hangs_refuses_nothing()
    {
        var fake = Probed();
        fake.Answer(OraSql.PollViewReadable, _ => Hangs());

        var caps = await Bounded(fake).ProbeAsync(CancellationToken.None);

        Assert.Equal("ok", Assert.Single(caps.Oracle!.Checks, c => c.Name == "poll_view").Status);
        Assert.Contains("UNIFIED_AUDIT_TRAIL did not answer in time", Assert.Single(caps.Oracle.Checks, c => c.Name == "views").Message);
    }

    /// <summary>The answer holds for the life of the process, so a read that told nothing must not settle it.</summary>
    [Fact]
    public async Task A_source_column_read_that_hangs_is_asked_again_by_the_next_poll()
    {
        var fake = Probed();
        fake.Answer(OraSql.SourceColumn, _ => Hangs());
        var source = Bounded(fake);
        await source.ProbeAsync(CancellationToken.None);

        fake.Answer(OraSql.SourceColumn, new object?[] { 1L });
        fake.Answer(OraSql.Poll(text: true, source: true));
        await RowsAsync((await source.OpenAsync(Task(Streams.OraAudit), CancellationToken.None))!);

        Assert.Contains(OraSql.Poll(text: true, source: true), fake.Statements);
    }

    [Fact]
    public async Task A_session_lost_in_the_probe_fails_it()
    {
        var fake = Probed();
        fake.Fails(OraSql.TablespaceFree, 3113);

        await Assert.ThrowsAsync<SourceException>(() => new OraSource(Endpoint(), fake, () => Now).ProbeAsync(CancellationToken.None));
    }

    [Theory]
    [InlineData("ENABLED", "a job purges")]
    [InlineData("DISABLED", "no job purges")]
    public async Task A_disabled_purge_job_is_no_purge_job(string status, string message)
    {
        var fake = Probed();
        fake.Answer(OraSql.CleanupJobs, new object?[] { "TRAILOX_PURGE", status, "UNIFIED AUDIT TRAIL" });

        var caps = await new OraSource(Endpoint(), fake, () => Now).ProbeAsync(CancellationToken.None);

        var check = Assert.Single(caps.Oracle!.Checks, c => c.Name == "purge_job");
        Assert.Equal("ok", check.Status);
        Assert.Contains(message, check.Message);
    }

    [Fact]
    public async Task The_probe_says_whether_failed_sign_ins_reach_the_trail()
    {
        var fake = Probed();
        var caps = await new OraSource(Endpoint(), fake, () => Now).ProbeAsync(CancellationToken.None);
        Assert.Equal("ok", Assert.Single(caps.Oracle!.Checks, c => c.Name == "failed_signins").Status);

        fake.Answer(OraSql.EnabledPolicies, new object?[] { "ORA_SECURECONFIG", "BY USER", "ALL USERS", "USER", "YES", "YES" });
        caps = await new OraSource(Endpoint(), fake, () => Now).ProbeAsync(CancellationToken.None);
        Assert.Equal("warn", Assert.Single(caps.Oracle!.Checks, c => c.Name == "failed_signins").Status);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task Each_policy_view_says_on_its_own_whether_it_was_read(bool policiesReadable, bool objectsReadable)
    {
        var fake = Probed();
        if (!policiesReadable)
        {
            fake.Fails(OraSql.EnabledPolicies, 942);
        }
        if (!objectsReadable)
        {
            fake.Fails(OraSql.PolicyObjects, 942);
        }

        var caps = await new OraSource(Endpoint(), fake, () => Now).ProbeAsync(CancellationToken.None);

        Assert.Equal(policiesReadable, caps.Oracle!.PoliciesRead);
        Assert.Equal(objectsReadable, caps.Oracle.PolicyObjectsRead);
    }

    /// <summary>What the check-in sends: both flags under their camelCase names, on the wire and not only in the object.</summary>
    [Fact]
    public async Task The_flags_reach_the_check_in_by_name()
    {
        var caps = await new OraSource(Endpoint(), Probed(), () => Now).ProbeAsync(CancellationToken.None);

        using var json = JsonDocument.Parse(JsonSerializer.Serialize(caps, ProtocolInfo.Json));
        var oracle = json.RootElement.GetProperty("oracle");
        Assert.True(oracle.GetProperty("policiesRead").GetBoolean());
        Assert.True(oracle.GetProperty("policyObjectsRead").GetBoolean());
    }

    [Fact]
    public async Task A_view_with_no_grant_is_a_warning_and_the_probe_goes_on()
    {
        var fake = Probed();
        fake.Fails(OraSql.TrailPartitioning, 942);
        fake.Fails(OraSql.SpillAny, 1031);

        var caps = await new OraSource(Endpoint(), fake, () => Now).ProbeAsync(CancellationToken.None);

        var views = Assert.Single(caps.Oracle!.Checks, c => c.Name == "views");
        Assert.Equal("warn", views.Status);
        Assert.Contains("DBA_PART_TABLES", views.Message);
        Assert.Contains("V_$UNIFIED_AUDIT_TRAIL", views.Message);
        Assert.Null(caps.Oracle.Spilled);
    }

    [Theory]
    [InlineData("CDB$ROOT", "2828371651", "container")]
    [InlineData("LEDGER", "0", "audit_trail")]
    [InlineData("LEDGER", null, "audit_trail")]
    public async Task A_failed_check_refuses_the_source_with_what_the_probe_found(string container, string? sessionId, string check)
    {
        var fake = Probed(container, sessionId);
        var source = new OraSource(Endpoint(), fake, () => Now);

        var ex = await Assert.ThrowsAsync<SourceException>(() => source.ProbeAsync(CancellationToken.None));

        Assert.NotNull(ex.RefusedCaps);
        Assert.Equal("fail", Assert.Single(ex.RefusedCaps!.Oracle!.Checks, c => c.Name == check).Status);
    }

    [Fact]
    public async Task A_poll_view_with_no_grant_refuses_the_source()
    {
        var fake = Probed();
        fake.Fails(OraSql.PollViewReadable, 942);
        var ex = await Assert.ThrowsAsync<SourceException>(() => new OraSource(Endpoint(), fake, () => Now).ProbeAsync(CancellationToken.None));

        Assert.Contains("GRANT SELECT ON AUDSYS.UNIFIED_AUDIT_TRAIL", ex.Message);
    }

    [Fact]
    public async Task A_refused_source_reports_its_caps_with_the_reason_and_is_probed_again_in_minutes_not_at_every_check_in()
    {
        var at = Now;
        var fake = Probed("CDB$ROOT");
        var session = new EndpointSession(Endpoint(), new OraSource(Endpoint(), fake, () => at), TimeSpan.FromMinutes(60), () => at);

        await session.ProbeAsync(NullLogger.Instance, CancellationToken.None);

        Assert.False(session.NeedsProbe);
        at = Now.Add(EndpointSession.RefusedProbeInterval).AddSeconds(-1);
        Assert.False(session.NeedsProbe);
        at = Now.Add(EndpointSession.RefusedProbeInterval).AddSeconds(1);
        Assert.True(session.NeedsProbe);
        var report = session.Report();
        Assert.Contains("service_name", report.ProbeError);
        Assert.Equal("fail", Assert.Single(report.Caps.Oracle!.Checks, c => c.Name == "container").Status);
    }

    [Fact]
    public async Task A_refused_source_runs_no_poll_and_a_fix_lets_it_through()
    {
        var fake = Probed("CDB$ROOT");
        var source = new OraSource(Endpoint(), fake, () => Now);
        await Assert.ThrowsAsync<SourceException>(() => source.ProbeAsync(CancellationToken.None));
        var opens = fake.Opens;

        await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Task(Streams.OraAudit), CancellationToken.None));
        await Assert.ThrowsAsync<SourceException>(() => source.OpenAsync(Task(Streams.CatalogUsers), CancellationToken.None));
        Assert.Equal(opens, fake.Opens);

        fake.Answer(OraSql.Session, new object?[] { "TRAILOX_MONITOR", "LEDGER", "trailox-agent.dll", "2828371651" });
        await source.ProbeAsync(CancellationToken.None);
        fake.Answer(OraSql.Poll(text: true, source: true));
        await RowsAsync((await source.OpenAsync(Task(Streams.OraAudit), CancellationToken.None))!);
    }

    /// <summary>A busy trail is read less often. Its size refuses nothing, and the polls go on.</summary>
    [Fact]
    public async Task A_day_of_any_size_refuses_nothing()
    {
        var at = Now;
        var fake = Probed();
        var source = new OraSource(Endpoint(), fake, () => at);
        await source.ProbeAsync(CancellationToken.None);

        at = Now.AddHours(6);
        fake.Answer(OraSql.TablespaceFiles, new object?[] { 90_000_000_000L, 100_000_000_000L });
        var caps = await source.ProbeAsync(CancellationToken.None);

        Assert.Equal(320_000_000_000, caps.Oracle!.GrowthBytesPerDay);
        Assert.DoesNotContain(caps.Oracle.Checks, c => c.Status == "fail");
        fake.Answer(OraSql.Poll(text: true, source: true));
        await RowsAsync((await source.OpenAsync(Task(Streams.OraAudit), CancellationToken.None))!);
    }

    [Fact]
    public async Task A_stream_this_engine_does_not_make_is_null()
    {
        Assert.Null(await new OraSource(Endpoint(), new FakeOra(), () => Now).OpenAsync(Task(Streams.PgLog), CancellationToken.None));
    }

    [Fact]
    public async Task Users_ship_with_their_snapshot_date()
    {
        var fake = new FakeOra();
        fake.Answer(OraSql.Users, _ => new FakeRows(new[] { new object?[] { "BOB", "OPEN", null, null, Now, "DEFAULT", "PASSWORD", "N" } },
            "username", "account_status", "lock_date", "expiry_date", "created", "profile", "authentication_type", "oracle_maintained"));

        var rows = await RowsAsync((await new OraSource(Endpoint(), fake, () => Now).OpenAsync(Task(Streams.CatalogUsers), CancellationToken.None))!);

        var row = Assert.Single(rows);
        Assert.Equal(("BOB", "2026-10-22"), (row.GetProperty("username").GetString(), row.GetProperty("snapshot_date").GetString()));
        Assert.Equal("2026-10-22T07:00:00.000000Z", row.GetProperty("created").GetString());
        Assert.Equal(JsonValueKind.Null, row.GetProperty("lock_date").ValueKind);
    }

    [Fact]
    public async Task Grants_read_each_kind_on_one_session()
    {
        var fake = new FakeOra();
        foreach (var sql in OraSql.Grants)
        {
            fake.Answer(sql, _ => new FakeRows(new[] { new object?[] { "x", "BOB", "P", null, null, "NO" } }, "kind", "grantee", "privilege", "owner", "table_name", "grantable"));
        }

        var rows = await RowsAsync((await new OraSource(Endpoint(), fake, () => Now).OpenAsync(Task(Streams.CatalogGrants), CancellationToken.None))!);

        Assert.Equal(3, rows.Count);
        Assert.Equal(1, fake.Opens);
    }

    [Fact]
    public async Task A_stream_cut_at_its_limit_ends_with_a_row_that_says_so()
    {
        var fake = new FakeOra();
        fake.Answer(OraSql.Tables, _ => new FakeRows(Enumerable.Range(0, OraSql.MaxTables).Select(i => new object?[] { "APP", "T" + i, 1L, 1L, "NO", null }),
            "owner", "table_name", "num_rows", "blocks", "partitioned", "last_analyzed"));

        var rows = await RowsAsync((await new OraSource(Endpoint(), fake, () => Now).OpenAsync(Task(Streams.CatalogTables), CancellationToken.None))!);

        Assert.Equal(OraSql.MaxTables + 1, rows.Count);
        Assert.Equal(OraCatalog.TruncatedKind, rows[^1].GetProperty("partitioned").GetString());
        Assert.Equal(rows[0].EnumerateObject().Select(p => p.Name), rows[^1].EnumerateObject().Select(p => p.Name));
    }

    [Fact]
    public async Task Columns_are_read_a_page_of_tables_at_a_time_each_on_its_own_connection()
    {
        const int tables = OraSql.ColumnPageTables + 1;
        var fake = new FakeOra();
        fake.Answer(OraSql.TableNames, Enumerable.Range(0, tables).Select(i => new object?[] { "APP", $"T{i:D5}" }).ToArray());
        fake.Answer(OraSql.Columns, _ => new FakeRows(new[] { new object?[] { "APP", "T", "C", "NUMBER", 1L } }, "owner", "table_name", "column_name", "data_type", "column_id"));

        var rows = await RowsAsync((await new OraSource(Endpoint(), fake, () => Now).OpenAsync(Task(Streams.CatalogColumns), CancellationToken.None))!);

        Assert.Equal(2, rows.Count);
        Assert.Equal(3, fake.Opens);
        Assert.Equal(1, fake.MostSessions);
        var second = fake.Binds.Last().ToDictionary(b => b.Name, b => b.Value);
        Assert.Equal(("T01000", "T01000"), (second["t1"], second["t2"]));
    }

    [Fact]
    public async Task A_setting_with_no_grant_is_a_row_that_says_so_and_the_others_are_read()
    {
        var fake = new FakeOra();
        fake.Answer(OraSql.Settings[0].Sql, new object?[] { "Unified Auditing", "FALSE", null });
        fake.Fails(OraSql.Settings[1].Sql, 1031);

        var rows = await RowsAsync((await new OraSource(Endpoint(), fake, () => Now).OpenAsync(Task(Streams.CatalogSettings), CancellationToken.None))!);

        Assert.Equal(2, rows.Count);
        Assert.Equal(("option.Unified Auditing", "FALSE"), (rows[0].GetProperty("name").GetString(), rows[0].GetProperty("setting").GetString()));
        Assert.Equal(("unreadable.V_$PARAMETER", "ORA-01031"), (rows[1].GetProperty("name").GetString(), rows[1].GetProperty("setting").GetString()));
    }

    [Theory]
    [InlineData("TIMESTAMP' 2026-10-29 00:00:00'", 2026, 10, 29)]
    public void A_partitions_end_is_read_from_its_high_value(string text, int year, int month, int day)
    {
        Assert.Equal(new DateTime(year, month, day, 0, 0, 0, DateTimeKind.Utc), OraProbe.HighValue(text));
    }

    [Fact]
    public void A_high_value_with_no_date_has_no_end()
    {
        Assert.Null(OraProbe.HighValue("MAXVALUE"));
        Assert.Null(OraProbe.HighValue(null));
    }

    [Fact]
    public void Each_plain_value_writes_as_its_own_json_type()
    {
        var buffer = new System.Buffers.ArrayBufferWriter<byte>();
        var sink = new RowSink(buffer);
        sink.Writer.WriteStartObject();
        OraCatalog.WriteValue(sink.Writer, "far", new DateTime(9999, 12, 31, 0, 0, 0, DateTimeKind.Utc));
        OraCatalog.WriteValue(sink.Writer, "real", 1.5);
        OraCatalog.WriteValue(sink.Writer, "whole", 7L);
        OraCatalog.WriteValue(sink.Writer, "digits", "123456789012345678901234567890");
        OraCatalog.WriteValue(sink.Writer, "none", null);
        sink.Writer.WriteEndObject();
        sink.Writer.Flush();

        using var row = JsonDocument.Parse(buffer.WrittenMemory);
        Assert.Equal(JsonValueKind.Null, row.RootElement.GetProperty("far").ValueKind);
        Assert.Equal(1.5, row.RootElement.GetProperty("real").GetDouble());
        Assert.Equal(7, row.RootElement.GetProperty("whole").GetInt64());
        Assert.Equal("123456789012345678901234567890", row.RootElement.GetProperty("digits").GetString());
        Assert.Equal(JsonValueKind.Null, row.RootElement.GetProperty("none").ValueKind);
    }
}
