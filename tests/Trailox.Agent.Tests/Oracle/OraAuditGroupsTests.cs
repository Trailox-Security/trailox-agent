using Trailox.Agent.Engines.Oracle;

namespace Trailox.Agent.Tests.Oracle;

/// <summary>The shape keeps a statement and loses its values, on Oracle's own quoting.</summary>
public class OraStatementShapeTests
{
    [Theory]
    [InlineData("SELECT * FROM t WHERE a = 'x'", "SELECT * FROM t WHERE a = ?")]
    [InlineData("SELECT * FROM t WHERE a = 'it''s'", "SELECT * FROM t WHERE a = ?")]
    [InlineData("SELECT * FROM t WHERE a = N'x'", "SELECT * FROM t WHERE a = ?")]
    [InlineData("SELECT * FROM t WHERE a = n'x'", "SELECT * FROM t WHERE a = ?")]
    [InlineData("SELECT * FROM t WHERE a = q'[it's]' AND b = 1", "SELECT * FROM t WHERE a = ? AND b = ?")]
    [InlineData("SELECT * FROM t WHERE a = Q'{it's}'", "SELECT * FROM t WHERE a = ?")]
    [InlineData("SELECT * FROM t WHERE a = q'(it's)'", "SELECT * FROM t WHERE a = ?")]
    [InlineData("SELECT * FROM t WHERE a = q'<it's>'", "SELECT * FROM t WHERE a = ?")]
    [InlineData("SELECT * FROM t WHERE a = q'!it's!'", "SELECT * FROM t WHERE a = ?")]
    [InlineData("SELECT * FROM t WHERE a = q'Xit'sX'", "SELECT * FROM t WHERE a = ?")]
    [InlineData("SELECT * FROM t WHERE a = nq'[it's]'", "SELECT * FROM t WHERE a = ?")]
    [InlineData("SELECT * FROM t WHERE a = NQ'[a]b]'", "SELECT * FROM t WHERE a = ?")]
    public void Every_quoting_of_a_string_becomes_one_mark(string sql, string expected)
    {
        Assert.Equal(expected, OraStatementShape.Of(sql));
    }

    [Theory]
    [InlineData("SELECT 1, 2.5, .5, 1e5, 1.5E-3, 7f, 7d FROM dual", "SELECT ? FROM dual")]
    [InlineData("INSERT INTO t VALUES (1, 'a', 2)", "INSERT INTO t VALUES (?)")]
    [InlineData("SELECT * FROM t WHERE id IN (1, 2, 3)", "SELECT * FROM t WHERE id IN (?)")]
    [InlineData("SELECT * FROM t WHERE id IN (1,2,3)", "SELECT * FROM t WHERE id IN (?)")]
    [InlineData("INSERT INTO t VALUES (1), (2), (3)", "INSERT INTO t VALUES (?)")]
    [InlineData("SELECT 0x1F FROM dual", "SELECT ? FROM dual")]
    public void Numbers_and_lists_become_one_mark(string sql, string expected)
    {
        Assert.Equal(expected, OraStatementShape.Of(sql));
    }

    [Theory]
    [InlineData("SELECT a FROM t WHERE id = :1 AND b = :name AND c = :B1", "SELECT a FROM t WHERE id = :1 AND b = :name AND c = :B1")]
    [InlineData("SELECT \"Mixed Name\" FROM \"T\"", "SELECT \"Mixed Name\" FROM \"T\"")]
    [InlineData("UPDATE t1 SET col2 = :a", "UPDATE t1 SET col2 = :a")]
    [InlineData("BEGIN app.pkg$run#1(:a); END;", "BEGIN app.pkg$run#1(:a); END;")]
    public void Names_and_placeholders_stay(string sql, string expected)
    {
        Assert.Equal(expected, OraStatementShape.Of(sql));
    }

    [Theory]
    [InlineData("SELECT /*+ FULL(t) */ a  FROM\n t -- note\n WHERE b = 1", "SELECT a FROM t WHERE b = ?")]
    [InlineData("/* it's */ SELECT 'x' FROM dual", "SELECT ? FROM dual")]
    [InlineData("-- it's\nSELECT 1 FROM dual", "SELECT ? FROM dual")]
    public void Comments_go_and_white_space_collapses(string sql, string expected)
    {
        Assert.Equal(expected, OraStatementShape.Of(sql));
    }

    [Theory]
    [InlineData("SELECT * FROM t WHERE a = 'never closed AND b = 2", "SELECT * FROM t WHERE a = ?")]
    [InlineData("SELECT * FROM t WHERE a = q'[never closed' AND b = 'x'", "SELECT * FROM t WHERE a = ?")]
    [InlineData("SELECT * FROM t WHERE a = q'", "SELECT * FROM t WHERE a = ?")]
    [InlineData("SELECT * FROM t WHERE a = nq'!never", "SELECT * FROM t WHERE a = ?")]
    [InlineData("SELECT \"never closed FROM t WHERE a = 'value'", "SELECT ?")]
    public void It_fails_closed_on_a_quote_that_never_ends(string sql, string expected)
    {
        Assert.Equal(expected, OraStatementShape.Of(sql));
    }

    [Theory]
    [InlineData("SELECT * FROM t WHERE a = 'O'Brien' AND b = 'x'", "SELECT * FROM t WHERE a = ?")]
    [InlineData("SELECT 1q'[it's secret]' FROM t", "SELECT ?")]
    [InlineData("SELECT a\"b\" FROM t WHERE c = 1", "SELECT a?")]
    public void It_fails_closed_on_a_word_that_touches_a_quote(string sql, string expected)
    {
        Assert.Equal(expected, OraStatementShape.Of(sql));
    }

    [Theory]
    [InlineData("SELECT * FROM t WHERE ssn = \"123-45-6789\"", "SELECT * FROM t WHERE ssn = ?")]
    [InlineData("SELECT * FROM t WHERE a <> \"x\" AND \"B\" = 1", "SELECT * FROM t WHERE a <> ? AND \"B\" = ?")]
    [InlineData("SELECT :\"x\" FROM dual", "SELECT :? FROM dual")]
    public void Double_quotes_compared_with_something_hold_a_value(string sql, string expected)
    {
        Assert.Equal(expected, OraStatementShape.Of(sql));
    }

    [Fact]
    public void Ddl_keeps_its_structure_and_loses_its_values()
    {
        Assert.Equal("GRANT SELECT ON app.t TO bob", OraStatementShape.Of("GRANT SELECT ON app.t TO bob"));
        Assert.Equal("CREATE TABLE t (a NUMBER(?), b VARCHAR2(?) DEFAULT ?)", OraStatementShape.Of("CREATE TABLE t (a NUMBER(10, 2), b VARCHAR2(30) DEFAULT 'x')"));
        Assert.Equal("BEGIN UTL_FILE.PUT(f, ?); END;", OraStatementShape.Of("BEGIN UTL_FILE.PUT(f, 'row'); END;"));
    }

    [Fact]
    public void A_word_that_ends_in_q_is_no_quote()
    {
        Assert.Equal("SELECT seq FROM faq WHERE a = ?", OraStatementShape.Of("SELECT seq FROM faq WHERE a = 'x'"));
    }

    /// <summary>The text from a cut at any point keeps no value, whichever quote the cut falls in.</summary>
    [Fact]
    public void A_cut_at_any_point_keeps_no_value()
    {
        const string sql = "SELECT * FROM t WHERE a = 'Zq1' AND b = q'[Zq2's]' AND c = nq'{Zq3}' AND d = 'Zq''4'";
        for (var cut = 0; cut <= sql.Length; cut++)
        {
            Assert.DoesNotContain("Zq", OraStatementShape.Of(sql[..cut]));
        }
    }
}

public class OraAuditGroupsTests
{
    private const long Minute = 60_000_000;
    private static readonly long T0 = (new DateTime(2026, 10, 22, 6, 0, 0, DateTimeKind.Utc) - DateTime.UnixEpoch).Ticks / 10;

    private static OraRecord Record(long micros, string user = "APP", string action = "SELECT", string table = "ACCOUNTS", string program = "app@host",
        long returnCode = 0, string address = "192.0.2.10", int port = 40000) => new()
        {
            Micros = micros,
            Instance = 1,
            User = user,
            Action = action,
            Schema = "APP",
            Object = table,
            Program = program,
            OsUser = "svc",
            UserHost = "app-1",
            ReturnCode = returnCode,
            Authentication = $"(TYPE=(DATABASE));(CLIENT ADDRESS=((ADDRESS=(PROTOCOL=tcp)(HOST={address})(PORT={port}))));",
            Policies = "TRAILOX_FULL",
            SessionId = 7,
            EntryId = 1,
            StatementId = 1,
        };

    private static (OraAuditGroups Groups, List<(long Micros, long Count, string User, string Object, string Shape, string Program, string Proxy, string Hash)> Rows) Groups()
    {
        var rows = new List<(long, long, string, string, string, string, string, string)>();
        return (new OraAuditGroups(g => rows.Add((g.MinuteMicros, g.Count, g.Labels[OraAuditGroups.User], g.Labels[OraAuditGroups.Object],
            g.Labels[OraAuditGroups.Shape], g.Labels[OraAuditGroups.Program], g.Labels[OraAuditGroups.Proxy], g.Labels[OraAuditGroups.Hash]))), rows);
    }

    [Theory]
    [InlineData("SELECT", true)]
    [InlineData("INSERT", true)]
    [InlineData("UPDATE", true)]
    [InlineData("DELETE", true)]
    [InlineData("MERGE", true)]
    [InlineData("UPSERT", true)]
    [InlineData("EXECUTE", true)]
    [InlineData("PL/SQL EXECUTE", true)]
    [InlineData("LOGON", false)]
    [InlineData("LOGOFF", false)]
    [InlineData("LOGOFF BY CLEANUP", false)]
    [InlineData("CREATE TABLE", false)]
    [InlineData("GRANT", false)]
    [InlineData("REVOKE", false)]
    [InlineData("ALTER USER", false)]
    [InlineData("CREATE ROLE", false)]
    [InlineData("AUDIT", false)]
    [InlineData("NOAUDIT", false)]
    [InlineData("CREATE AUDIT POLICY", false)]
    public void Reads_writes_and_calls_group_and_nothing_else_does(string action, bool grouped)
    {
        Assert.Equal(grouped, OraAuditGroups.IsGrouped(action));
    }

    [Fact]
    public void Records_with_the_same_key_in_a_minute_are_one_row_with_their_count()
    {
        var (groups, rows) = Groups();
        groups.Add(Record(T0 + 1), "SELECT ?");
        groups.Add(Record(T0 + 2, port: 40001), "SELECT ?");
        groups.Add(Record(T0 + Minute - 1), "SELECT ?");
        groups.Finish();
        var row = Assert.Single(rows);
        Assert.Equal(3, row.Count);
    }

    [Theory]
    [InlineData("user")]
    [InlineData("action")]
    [InlineData("object")]
    [InlineData("program")]
    [InlineData("result")]
    [InlineData("address")]
    [InlineData("shape")]
    public void A_record_that_differs_in_any_part_of_the_key_is_a_row_of_its_own(string part)
    {
        var (groups, rows) = Groups();
        groups.Add(Record(T0), "SELECT ?");
        groups.Add(part switch
        {
            "user" => Record(T0, user: "OTHER"),
            "action" => Record(T0, action: "UPDATE"),
            "object" => Record(T0, table: "OTHER"),
            "program" => Record(T0, program: "other"),
            "result" => Record(T0, returnCode: 942),
            "address" => Record(T0, address: "192.0.2.11"),
            _ => Record(T0),
        }, part == "shape" ? "SELECT ? FROM other" : "SELECT ?");
        groups.Finish();
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public void A_minute_is_a_key_of_its_own()
    {
        var (groups, rows) = Groups();
        groups.Add(Record(T0), "SELECT ?");
        groups.Add(Record(T0 + Minute), "SELECT ?");
        groups.Finish();
        Assert.Equal(new[] { T0, T0 + Minute }, rows.Select(r => r.Micros).Order());
    }

    /// <summary>The poll has no ORDER BY: records of a minute come mixed with the next one's.</summary>
    [Fact]
    public void Records_out_of_order_inside_the_grace_still_join_their_minute()
    {
        var (groups, rows) = Groups();
        groups.Add(Record(T0 + (2 * Minute) - 1), "SELECT ?");
        groups.Add(Record(T0 + 1), "SELECT ?");
        groups.Add(Record(T0 + (2 * Minute) - 2), "SELECT ?");
        groups.Add(Record(T0 + 2), "SELECT ?");
        groups.Finish();
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal(2, r.Count));
        Assert.Equal(0, groups.LateRecords);
    }

    [Fact]
    public void A_record_later_than_the_grace_opens_a_second_row_and_the_counts_still_add_up()
    {
        var (groups, rows) = Groups();
        groups.Add(Record(T0 + 1), "SELECT ?");
        groups.Add(Record(T0 + (10 * Minute)), "SELECT ?");
        groups.Add(Record(T0 + 2), "SELECT ?");
        groups.Finish();
        Assert.Equal(3, rows.Count);
        Assert.Equal(3, rows.Sum(r => r.Count));
        Assert.Equal(1, groups.LateRecords);
    }

    /// <summary>No two rows share a hash, even two rows of one minute and key.</summary>
    [Fact]
    public void Every_row_has_a_hash_of_its_own_even_a_second_row_of_one_minute_and_key()
    {
        var (groups, rows) = Groups();
        groups.Add(Record(T0 + 1), "SELECT ?");
        groups.Add(Record(T0 + 2, address: "192.0.2.11"), "SELECT ?");
        groups.Add(Record(T0 + (10 * Minute)), "SELECT ?");
        groups.Add(Record(T0 + 3), "SELECT ?");
        groups.Finish();
        Assert.Equal(4, rows.Count);
        Assert.Equal(4, rows.Select(r => r.Hash).Distinct().Count());
        Assert.All(rows, r => Assert.Equal(16, r.Hash.Length));
    }

    [Fact]
    public void A_row_past_the_caps_has_a_hash_too()
    {
        var (groups, rows) = Groups();
        for (var i = 0; i < OraAuditGroups.UserShapeCap + 1; i++)
        {
            groups.Add(Record(T0 + i), $"SELECT {i}");
        }
        groups.Finish();
        Assert.All(rows, r => Assert.NotEmpty(r.Hash));
    }

    /// <summary>One record read first from the end of the window holds the newest time still; the groups behind it must ship anyway.</summary>
    [Fact]
    public void What_it_holds_does_not_grow_with_the_window_whatever_the_order()
    {
        var (groups, rows) = Groups();
        groups.Add(Record(T0 + (59 * Minute)), "SELECT last");
        for (var i = 0; i < 2 * OraAuditGroups.MaxOpenGroups; i++)
        {
            groups.Add(Record(T0 + (i % 58 * Minute), user: $"U{i}"), "SELECT ?");
        }
        Assert.NotEmpty(rows);
        groups.Finish();
        Assert.Equal((2 * OraAuditGroups.MaxOpenGroups) + 1, rows.Sum(r => r.Count));
    }

    [Fact]
    public void Past_the_cap_a_user_keeps_user_action_and_object_without_the_shape()
    {
        var (groups, rows) = Groups();
        for (var i = 0; i < OraAuditGroups.UserShapeCap + 3; i++)
        {
            groups.Add(Record(T0 + i), $"SELECT {i}");
        }
        groups.Finish();
        Assert.Equal(OraAuditGroups.UserShapeCap + 1, rows.Count);
        var many = Assert.Single(rows, r => r.Shape == OraAuditGroups.ManyShapes);
        Assert.Equal((3L, "APP", "ACCOUNTS", ""), (many.Count, many.User, many.Object, many.Program));
        Assert.Equal(3, groups.ShapelessRecords);
    }

    /// <summary>Past the cap the proxy stays: without it the act would read as the account's own.</summary>
    [Fact]
    public void Past_the_cap_a_user_keeps_its_proxy()
    {
        var (groups, rows) = Groups();
        for (var i = 0; i < OraAuditGroups.UserShapeCap + 1; i++)
        {
            groups.Add(Record(T0 + i), $"SELECT {i}");
        }
        var proxied = Record(T0 + 1000, table: "PAYROLL");
        proxied.Proxy = "MALLORY";
        groups.Add(proxied, "SELECT card FROM payroll");
        groups.Finish();
        var row = Assert.Single(rows, r => r.Object == "PAYROLL");
        Assert.Equal(("APP", "MALLORY"), (row.User, row.Proxy));
    }

    /// <summary>A record that worked can only name something that exists, so it cannot be hidden behind invented names.</summary>
    [Fact]
    public void A_read_that_worked_is_not_hidden_behind_a_users_failed_reads()
    {
        var (groups, rows) = Groups();
        for (var i = 0; i <= OraAuditGroups.UserShapeCap + OraAuditGroups.UserFailedObjectCap; i++)
        {
            groups.Add(Record(T0 + i, table: $"NOPE{i}", returnCode: 942), $"SELECT * FROM nope{i}");
        }
        groups.Add(Record(T0 + 5000, table: "PAYROLL"), "SELECT card FROM payroll");
        groups.Finish();
        Assert.Contains(rows, r => r.Object == "PAYROLL");
    }

    [Fact]
    public void One_user_past_the_cap_leaves_another_users_shapes_alone()
    {
        var (groups, rows) = Groups();
        for (var i = 0; i < OraAuditGroups.UserShapeCap + 3; i++)
        {
            groups.Add(Record(T0 + i, user: "NOISY"), $"SELECT {i}");
        }
        groups.Add(Record(T0 + 1000, user: "QUIET"), "SELECT quiet");
        groups.Finish();
        Assert.Contains(rows, r => r.User == "QUIET" && r.Shape == "SELECT quiet");
    }

    /// <summary>
    /// Failed reads of invented names can each carry an object of their own. Without a second cap one user would fill the
    /// minute, and everyone else's records would land in a row that names nobody.
    /// </summary>
    [Fact]
    public void One_user_cannot_fill_the_minute_with_invented_objects()
    {
        var (groups, rows) = Groups();
        for (var i = 0; i < OraAuditGroups.GroupCap + 2000; i++)
        {
            groups.Add(Record(T0 + i, user: "NOISY", table: $"NO_SUCH_{i}", returnCode: 942), $"SELECT * FROM no_such_{i}");
        }
        groups.Add(Record(T0 + 20_000, user: "QUIET"), "SELECT quiet");
        groups.Finish();
        Assert.Contains(rows, r => r.User == "QUIET" && r.Shape == "SELECT quiet");
        var many = Assert.Single(rows, r => r.Object == OraAuditGroups.ManyObjects);
        Assert.Equal("NOISY", many.User);
        Assert.Equal(OraAuditGroups.GroupCap + 2000, rows.Where(r => r.User == "NOISY").Sum(r => r.Count));
    }

    /// <summary>A program can raise any return code, so a code cannot be what keeps one user's rows apart.</summary>
    [Fact]
    public void Invented_return_codes_cannot_fill_the_minute_either()
    {
        var (groups, rows) = Groups();
        for (var i = 0; i < OraAuditGroups.GroupCap + 2000; i++)
        {
            groups.Add(Record(T0 + i, user: "NOISY", action: "EXECUTE", returnCode: 20000 + i), "BEGIN x; END;");
        }
        groups.Finish();
        Assert.True(rows.Count(r => r.User == "NOISY") <= OraAuditGroups.UserShapeCap + OraAuditGroups.UserFailedObjectCap + 1);
    }

    /// <summary>Many accounts together can fill a minute, but the records past it stay under their own names.</summary>
    [Fact]
    public void Past_the_group_cap_everyone_is_still_named()
    {
        var (groups, rows) = Groups();
        for (var u = 0; u < 30; u++)
        {
            for (var i = 0; i < OraAuditGroups.UserShapeCap; i++)
            {
                groups.Add(Record(T0 + i, user: $"U{u}"), $"SELECT {i}");
            }
        }
        groups.Add(Record(T0 + 20_000, user: "ALICE", table: "PAYROLL"), "SELECT card FROM payroll");
        groups.Finish();
        Assert.Equal(30 * OraAuditGroups.UserShapeCap + 1, rows.Sum(r => r.Count));
        Assert.Contains(rows, r => r.User == "ALICE" && r.Object == OraAuditGroups.ManyObjects);
    }

    [Theory]
    [InlineData("(TYPE=(DATABASE));(CLIENT ADDRESS=((ADDRESS=(PROTOCOL=tcp)(HOST=192.0.2.10)(PORT=34770))));(CLIENT ADDRESS=());", "192.0.2.10")]
    [InlineData("(TYPE=(DATABASE));", "")]
    [InlineData("", "")]
    public void The_key_reads_the_client_address_without_its_port(string authentication, string host)
    {
        Assert.Equal(host, OraAuditGroups.ClientHost(authentication));
    }
}
