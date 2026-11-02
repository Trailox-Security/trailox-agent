using System.Reflection;
using System.Text.RegularExpressions;
using Trailox.Agent.Config;
using Trailox.Agent.Engines;
using Trailox.Agent.Engines.Oracle;

namespace Trailox.Agent.Tests.Oracle;

/// <summary>The agent's statements, pinned. A wrong one here costs the customer's database, not ours.</summary>
public class OraSqlTests
{
    private static readonly string Poll = OraSql.Poll(text: true, source: true);

    /// <summary>Every statement the engine holds, read from the class, so a new one is held to these rules too.</summary>
    private static IEnumerable<string> Every()
    {
        foreach (var field in typeof(OraSql).GetFields(BindingFlags.Public | BindingFlags.Static))
        {
            switch (field.GetValue(null))
            {
                case string s:
                    yield return s;
                    break;
                case IReadOnlyList<string> list:
                    foreach (var s in list)
                    {
                        yield return s;
                    }
                    break;
                case IReadOnlyList<(string, string, string Sql)> settings:
                    foreach (var s in settings)
                    {
                        yield return s.Sql;
                    }
                    break;
            }
        }
        yield return Poll;
        yield return OraSql.Poll(text: false, source: false);
    }

    /// <summary>A read of its own: the probe cannot go on without the session's row, so a server that refuses the role must not take that row with it.</summary>
    [Fact]
    public void The_role_is_read_on_its_own()
    {
        Assert.Equal("SELECT SYS_CONTEXT('USERENV', 'DATABASE_ROLE') FROM SYS.DUAL", OraSql.DatabaseRole);
        Assert.DoesNotContain("DATABASE_ROLE", OraSql.Session);
    }

    [Fact]
    public void The_poll_cuts_its_window_on_the_utc_column_only()
    {
        Assert.Contains("WHERE event_timestamp_utc >= CAST(:lo AS TIMESTAMP) AND event_timestamp_utc < CAST(:hi AS TIMESTAMP)", Poll);
        Assert.DoesNotMatch(@"(?i)\bevent_timestamp\b", Poll);
    }

    [Fact]
    public void The_poll_keeps_a_null_and_drops_only_our_own_successful_reads()
    {
        Assert.Contains("NOT (NVL(dbusername, ' ') = :me AND dbproxy_username IS NULL AND NVL(return_code, -1) = 0", Poll);
        Assert.Contains("NVL(action_name, ' ') = 'SELECT')", Poll);
        Assert.DoesNotContain("dbusername <>", Poll);
    }

    /// <summary>A sign-in by the monitoring user is recorded: of its successful records, only its reads are left out.</summary>
    [Theory]
    [InlineData("SELECT", true)]
    [InlineData("LOGON", false)]
    [InlineData("LOGOFF", false)]
    [InlineData("LOGOFF BY CLEANUP", false)]
    public void Of_our_own_records_the_poll_leaves_out_only_reads(string action, bool leftOut)
    {
        var filter = Poll[Poll.IndexOf("AND NOT (", StringComparison.Ordinal)..];

        Assert.Equal(leftOut, filter.Contains($"'{action}'", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("ORDER BY")]
    [InlineData("SQL_BINDS")]
    [InlineData("EXPLAIN PLAN")]
    [InlineData("EVENT_TIMESTAMP ")]
    public void The_poll_never_names(string text)
    {
        Assert.DoesNotContain(text, Poll, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void No_lob_enters_the_result()
    {
        var bare = Regex.Replace(Poll, @"SYS\.DBMS_LOB\.(SUBSTR|GETLENGTH)\(sql_text[^)]*\)", "");
        Assert.DoesNotContain("sql_text,", bare);
        Assert.DoesNotMatch(@"(?m)\bsql_text\s*$", bare);
        Assert.Contains("SYS.DBMS_LOB.GETLENGTH(sql_text) AS sql_text_length", Poll);
    }

    /// <summary>One DBMS_LOB.SUBSTR of 4,000 characters fails on a text of multi-byte characters, and with it the whole window, for ever.</summary>
    [Fact]
    public void Each_piece_of_the_text_fits_a_varchar2_at_four_bytes_a_character()
    {
        var pieces = Regex.Matches(Poll, @"DBMS_LOB\.SUBSTR\(sql_text, (\d+), (\d+)\)").Select(m => (Chars: int.Parse(m.Groups[1].Value), From: int.Parse(m.Groups[2].Value))).ToList();
        Assert.Equal(OraSql.TextPieces, pieces.Count);
        Assert.All(pieces, p => Assert.True(p.Chars * 4 <= 4000));
        Assert.Equal(OraSql.MaxTextChars, pieces.Sum(p => p.Chars));
        for (var i = 1; i < pieces.Count; i++)
        {
            Assert.Equal(pieces[i - 1].From + pieces[i - 1].Chars, pieces[i].From);
        }
    }

    [Fact]
    public void With_no_statement_text_the_text_is_not_selected_at_all()
    {
        var poll = OraSql.Poll(text: false, source: true);
        Assert.DoesNotContain("sql_text,", poll);
        Assert.DoesNotContain("DBMS_LOB", poll);
    }

    [Fact]
    public void The_source_column_is_named_only_where_the_probe_found_it()
    {
        Assert.Matches(@"\bsource\b\s+FROM", Poll);
        Assert.DoesNotMatch(@"(?m)^\s*source\s*$", OraSql.Poll(text: true, source: false));
    }

    [Fact]
    public void The_column_positions_match_the_statement()
    {
        var select = Poll[(Poll.IndexOf("*/", StringComparison.Ordinal) + 2)..Poll.IndexOf("FROM AUDSYS", StringComparison.Ordinal)];
        var columns = Regex.Replace(select, @"\([^()]*\)", "").Split(',').Select(c => c.Trim().Split(' ')[^1].ToLowerInvariant()).ToList();
        var positions = new (int Position, string Column)[]
        {
            (OraSql.ColumnTime, "event_timestamp_utc"), (OraSql.ColumnInstance, "instance_id"), (OraSql.ColumnUser, "dbusername"), (OraSql.ColumnProxy, "dbproxy_username"),
            (OraSql.ColumnAction, "action_name"), (OraSql.ColumnReturnCode, "return_code"), (OraSql.ColumnSchema, "object_schema"), (OraSql.ColumnObject, "object_name"),
            (OraSql.ColumnProgram, "client_program_name"), (OraSql.ColumnOsUser, "os_username"), (OraSql.ColumnUserHost, "userhost"), (OraSql.ColumnTerminal, "terminal"),
            (OraSql.ColumnAuthentication, "authentication_type"), (OraSql.ColumnPolicies, "unified_audit_policies"), (OraSql.ColumnPrivilege, "system_privilege_used"),
            (OraSql.ColumnAuditType, "audit_type"), (OraSql.ColumnSessionId, "sessionid"), (OraSql.ColumnEntryId, "entry_id"), (OraSql.ColumnStatementId, "statement_id"),
            (OraSql.ColumnDataPump, "dp_text_parameters1"), (OraSql.ColumnTextLength, "sql_text_length"), (OraSql.ColumnText, "sql_text_1"),
            (OraSql.ColumnText + 3, "sql_text_4"), (OraSql.ColumnSource, "source"),
        };
        Assert.All(positions, p => Assert.Equal(p.Column, columns[p.Position]));
        Assert.Equal(OraSql.ColumnSource + 1, columns.Count);
    }

    [Theory]
    [InlineData("DBA_SEGMENTS")]
    [InlineData("ACTIVE_SESSION_HISTORY")]
    [InlineData("DBA_HIST_")]
    [InlineData("SQL_MONITOR")]
    [InlineData("EXPLAIN PLAN")]
    [InlineData("SELECT_CATALOG_ROLE")]
    public void No_statement_names(string text)
    {
        Assert.All(Every(), sql => Assert.DoesNotContain(text, sql, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void The_trail_is_never_counted()
    {
        Assert.All(Every().Where(s => s.Contains("UNIFIED_AUDIT_TRAIL", StringComparison.OrdinalIgnoreCase) || s.Contains("AUD$UNIFIED", StringComparison.OrdinalIgnoreCase)),
            sql => Assert.DoesNotContain("COUNT(", sql, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void The_spill_files_are_read_to_their_first_row()
    {
        Assert.Contains("ROWNUM <= 1", OraSql.SpillAny);
    }

    [Fact]
    public void Every_dictionary_object_is_named_with_its_owner()
    {
        var names = new Regex(@"\b(?:FROM|JOIN)\s+([A-Za-z_$#.]+)", RegexOptions.IgnoreCase);
        var objects = Every().SelectMany(sql => names.Matches(sql).Select(m => m.Groups[1].Value)).Distinct().ToList();
        Assert.NotEmpty(objects);
        Assert.All(objects, name => Assert.Matches(@"^(SYS|AUDSYS)\.[A-Z_$]+$", name));
    }

    [Fact]
    public void The_only_function_of_a_package_the_agent_calls_is_named_with_its_owner()
    {
        var calls = Every().SelectMany(sql => Regex.Matches(sql, @"\b[A-Za-z_.]*DBMS_[A-Z_]+\.[A-Z_]+").Select(m => m.Value)).Distinct();
        Assert.All(calls, call => Assert.StartsWith("SYS.DBMS_LOB.", call));
    }

    [Fact]
    public void Every_statement_only_reads_or_sets_the_session()
    {
        var statements = Every().Where(s => Regex.IsMatch(s, @"^\s*[A-Z]{3,}\s") && !s.Contains("TRAILOX_", StringComparison.Ordinal) || s.Contains("SELECT", StringComparison.Ordinal)).ToList();
        Assert.NotEmpty(statements);
        Assert.All(statements, sql => Assert.Matches(@"^\s*(SELECT|ALTER SESSION) ", sql));
    }

    [Fact]
    public void A_session_runs_in_utc_with_no_parallel_query()
    {
        Assert.Contains(OraSql.SessionSettings, s => s.StartsWith("ALTER SESSION SET TIME_ZONE = 'UTC'", StringComparison.Ordinal));
        Assert.Contains("ALTER SESSION DISABLE PARALLEL QUERY", OraSql.SessionSettings);
    }

    [Fact]
    public void Columns_are_read_in_pages_of_tables_with_no_row_cut()
    {
        Assert.Contains("(owner > :o1 OR (owner = :o1 AND table_name >= :t1))", OraSql.Columns);
        Assert.Contains("(owner < :o2 OR (owner = :o2 AND table_name <= :t2))", OraSql.Columns);
        Assert.DoesNotContain("ROWNUM", OraSql.Columns);
        Assert.Equal(new[] { (0, 999), (1000, 1999), (2000, 2000) }, OraCatalog.Pages(2001));
        Assert.Empty(OraCatalog.Pages(0));
    }

    [Fact]
    public void Tables_are_cut_at_their_limit_and_only_customers_owners_are_read()
    {
        Assert.Contains("oracle_maintained = 'N'", OraSql.Tables);
        Assert.Contains("oracle_maintained = 'N'", OraSql.Columns);
        Assert.Contains("ROWNUM <= :cap", OraSql.Tables);
    }
}

/// <summary>Only one file may name the driver: another driver then replaces that file and nothing else.</summary>
public class OraDriverSeamTests
{
    private const string DriverFile = "OdpDriver.cs";

    private static string SourceDirectory()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
        {
            if (File.Exists(Path.Combine(dir.FullName, "trailox-agent.sln")))
            {
                return Path.Combine(dir.FullName, "src", "Trailox.Agent");
            }
        }
        throw new InvalidOperationException("the repository is not above the test's directory");
    }

    private static IEnumerable<string> Sources() => Directory.EnumerateFiles(SourceDirectory(), "*.cs", SearchOption.AllDirectories)
        .Where(f => !f.Split(Path.DirectorySeparatorChar).Any(p => p is "bin" or "obj"));

    [Fact]
    public void Only_the_driver_file_references_the_drivers_namespace()
    {
        var naming = Sources().Where(f => File.ReadAllText(f).Contains("Oracle.ManagedDataAccess", StringComparison.Ordinal)).Select(Path.GetFileName).ToList();
        Assert.Equal(new[] { DriverFile }, naming);
    }

    [Fact]
    public void No_other_file_names_a_type_of_the_driver()
    {
        var driverTypes = new Regex(@"\b(OracleConnection|OracleCommand|OracleDataReader|OracleException|OracleParameter|OracleDbType|OracleDecimal|OracleTimeStamp|OracleDate|OracleString|OracleConfiguration)\b");
        var naming = Sources().Where(f => Path.GetFileName(f) != DriverFile && driverTypes.IsMatch(File.ReadAllText(f))).Select(Path.GetFileName).ToList();
        Assert.Empty(naming);
    }

    /// <summary>The seam itself carries plain types only, so the other side of it can be anything.</summary>
    [Fact]
    public void The_seam_carries_plain_types_only()
    {
        var types = new[] { typeof(IOraDriver), typeof(IOraSession), typeof(IOraRows) };
        var members = types.SelectMany(t => t.GetMethods()).SelectMany(m => m.GetParameters().Select(p => p.ParameterType).Append(m.ReturnType));
        Assert.All(members, t => Assert.DoesNotContain("ManagedDataAccess", t.FullName ?? t.Name));
    }

    [Fact]
    public void Nothing_but_the_oracle_engine_uses_the_driver_file()
    {
        var naming = Sources().Where(f => Path.GetFileName(f) != DriverFile && File.ReadAllText(f).Contains("OdpDriver", StringComparison.Ordinal)).Select(Path.GetFileName).ToList();
        Assert.All(naming, name => Assert.StartsWith("Ora", name));
    }
}

public class OracleEngineTests
{
    private static EndpointConfig Valid() => new()
    {
        Alias = "ledger",
        Engine = "oracle",
        Kind = "cloud",
        Host = "ledger.abc123def456.eu-west-1.rds.amazonaws.com",
        Port = 1521,
        Tls = false,
        Username = "TRAILOX_MONITOR",
        Options = new Dictionary<string, string> { ["service_name"] = "LEDGER" },
    };

    private static readonly OracleEngine Engine = new();

    [Fact]
    public void A_block_as_printed_is_valid_and_the_engine_is_registered()
    {
        Assert.Empty(Engine.Validate(Valid()));
        Assert.Same(EngineRegistry.Require("oracle").GetType(), typeof(OracleEngine));
    }

    [Fact]
    public void An_unknown_option_is_refused()
    {
        var e = Valid();
        e.Options["password"] = "x";
        Assert.Contains(Engine.Validate(e), p => p.StartsWith("options.password:", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("")]
    [InlineData("LEDGER)(HOST=192.0.2.66")]
    [InlineData("a b")]
    [InlineData("a=b")]
    public void A_service_name_that_could_change_the_descriptor_is_refused(string name)
    {
        var e = Valid();
        e.Options["service_name"] = name;
        Assert.Contains(Engine.Validate(e), p => p.StartsWith("options.service_name:", StringComparison.Ordinal));
    }

    [Fact]
    public void A_line_break_after_a_service_name_never_reaches_the_descriptor()
    {
        var e = Valid();
        e.Options["service_name"] = "LEDGER\n";
        Assert.Empty(Engine.Validate(e));
        Assert.Equal("LEDGER", e.Options["service_name"]);
    }

    [Fact]
    public void The_service_name_is_required()
    {
        var e = Valid();
        e.Options.Clear();
        Assert.Contains(Engine.Validate(e), p => p.StartsWith("options.service_name:", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("LEDGER.example.test")]
    [InlineData("ledger_svc")]
    [InlineData("pdb1-svc")]
    public void Oracle_service_name_characters_pass(string name)
    {
        var e = Valid();
        e.Options["service_name"] = name;
        Assert.Empty(Engine.Validate(e));
    }

    [Fact]
    public void An_unquoted_username_is_stored_as_the_server_writes_it()
    {
        var e = Valid();
        e.Username = " trailox_monitor ";
        Assert.Empty(Engine.Validate(e));
        Assert.Equal("TRAILOX_MONITOR", e.Username);
    }

    [Theory]
    [InlineData("\"Mixed\"")]
    [InlineData("a b")]
    [InlineData("")]
    public void A_username_that_is_not_a_plain_name_is_refused(string name)
    {
        var e = Valid();
        e.Username = name;
        Assert.Contains(Engine.Validate(e), p => p.StartsWith("username:", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(false, 1521)]
    [InlineData(true, 2484)]
    public void The_default_port_follows_tls(bool tls, int port)
    {
        var e = Valid();
        e.Port = 0;
        e.Tls = tls;
        Assert.Empty(Engine.Validate(e));
        Assert.Equal(port, e.Port);
    }

    [Theory]
    [InlineData("db.example.test)(PORT=1")]
    [InlineData("a b")]
    public void A_host_that_could_change_the_descriptor_is_refused(string host)
    {
        var e = Valid();
        e.Host = host;
        Assert.Contains(Engine.Validate(e), p => p.StartsWith("host:", StringComparison.Ordinal));
    }

    [Fact]
    public void The_descriptor_holds_the_address_and_with_tls_checks_the_certificate()
    {
        Assert.Equal("(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=192.0.2.10)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=LEDGER)))",
            new OraTarget("192.0.2.10", 1521, "LEDGER", false, "U", "p").Descriptor);
        Assert.Equal("(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=192.0.2.10)(PORT=2484))(CONNECT_DATA=(SERVICE_NAME=LEDGER))(SECURITY=(SSL_SERVER_DN_MATCH=TRUE)))",
            new OraTarget("192.0.2.10", 2484, "LEDGER", true, "U", "p").Descriptor);
    }

    [Fact]
    public void A_target_never_prints_its_password()
    {
        Assert.DoesNotContain("S3cret", new OraTarget("192.0.2.10", 1521, "LEDGER", false, "U", "S3cret").ToString());
    }

    [Fact]
    public void The_block_in_agent_yaml_loads_with_its_password_reference()
    {
        const string yaml = """
            version: 1
            gateway: https://agent.trailox.io
            endpoints:
              - alias: ledger
                engine: oracle
                kind: cloud
                host: ledger.abc123def456.eu-west-1.rds.amazonaws.com
                port: 1521
                tls: false
                username: trailox_monitor
                password_env: TRAILOX_ORA_PASSWORD
                options:
                  service_name: LEDGER
            """;
        var (config, _) = ConfigLoader.Parse(yaml, new Secrets());
        Assert.Equal(("TRAILOX_MONITOR", "from-env"), (config.Endpoints[0].Username, config.Endpoints[0].Password));
    }

    private sealed class Secrets : ISecretReader
    {
        public string? FromEnvironment(string variable) => variable == "TRAILOX_ORA_PASSWORD" ? "from-env" : null;

        public string FromFile(string path) => throw new FileNotFoundException(path);
    }
}
