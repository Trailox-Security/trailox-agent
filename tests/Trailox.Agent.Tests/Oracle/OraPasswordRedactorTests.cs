using Trailox.Agent.Engines.Oracle;

namespace Trailox.Agent.Tests.Oracle;

/// <summary>
/// No password leaves the customer's network, whatever Oracle masked itself. A statement that failed to parse is audited
/// with its text too, so a password that is no valid word has to go as well.
/// </summary>
public class OraPasswordRedactorTests
{
    private const string M = OraPasswordRedactor.Mask;

    [Theory]
    [InlineData("CREATE USER a IDENTIFIED BY S3cret", $"CREATE USER a IDENTIFIED BY {M}")]
    [InlineData("create user a identified by S3cret default tablespace users", $"create user a identified by {M} default tablespace users")]
    [InlineData("CREATE USER a IDENTIFIED BY \"p w;d\" ACCOUNT LOCK", $"CREATE USER a IDENTIFIED BY {M} ACCOUNT LOCK")]
    [InlineData("ALTER USER a IDENTIFIED BY *", $"ALTER USER a IDENTIFIED BY {M}")]
    [InlineData("ALTER USER a IDENTIFIED\n  BY\n  S3cret;", $"ALTER USER a IDENTIFIED\n  BY\n  {M}")]
    [InlineData("ALTER USER a IDENTIFIED BY new1 REPLACE old1", $"ALTER USER a IDENTIFIED BY {M} REPLACE {M}")]
    [InlineData("ALTER USER a IDENTIFIED BY \"new 1\" REPLACE \"old 1\" ACCOUNT UNLOCK", $"ALTER USER a IDENTIFIED BY {M} REPLACE {M} ACCOUNT UNLOCK")]
    [InlineData("ALTER USER a IDENTIFIED BY VALUES 'S:ABC;T:DEF'", $"ALTER USER a IDENTIFIED BY VALUES {M}")]
    [InlineData("ALTER USER a IDENTIFIED BY VALUES \"S:ABCDEF\"", $"ALTER USER a IDENTIFIED BY VALUES {M}")]
    [InlineData("ALTER USER a IDENTIFIED BY new REPLACE new2 REPLACE S3cret", $"ALTER USER a IDENTIFIED BY {M} REPLACE {M} REPLACE {M}")]
    [InlineData("ALTER USER a IDENTIFIED BY \"new\"REPLACE \"0ldSecret\"", $"ALTER USER a IDENTIFIED BY {M}REPLACE {M}")]
    [InlineData("ALTER USER a IDENTIFIED BY new/**/REPLACE 0ldSecret", $"ALTER USER a IDENTIFIED BY {M}/**/REPLACE {M}")]
    [InlineData("ALTER USER a IDENTIFIED BY VALUES", $"ALTER USER a IDENTIFIED BY {M}")]
    [InlineData("CREATE ROLE r IDENTIFIED BY S3cret", $"CREATE ROLE r IDENTIFIED BY {M}")]
    [InlineData("SET ROLE r IDENTIFIED BY S3cret", $"SET ROLE r IDENTIFIED BY {M}")]
    [InlineData("ADMINISTER KEY MANAGEMENT SET KEY IDENTIFIED BY \"k3y\" WITH BACKUP", $"ADMINISTER KEY MANAGEMENT SET KEY IDENTIFIED BY {M} WITH BACKUP")]
    [InlineData("CREATE PLUGGABLE DATABASE p ADMIN USER a IDENTIFIED BY S3cret FILE_NAME_CONVERT = NONE", $"CREATE PLUGGABLE DATABASE p ADMIN USER a IDENTIFIED BY {M} FILE_NAME_CONVERT = NONE")]
    public void A_password_after_identified_by_is_redacted_in_every_form(string sql, string expected)
    {
        Assert.Equal(expected, OraPasswordRedactor.Redact(sql));
    }

    [Theory]
    [InlineData("CREATE DATABASE LINK l CONNECT TO u IDENTIFIED BY p1 USING 'db'", $"CREATE DATABASE LINK l CONNECT TO u IDENTIFIED BY {M} USING 'db'")]
    [InlineData("CREATE SHARED PUBLIC DATABASE LINK l CONNECT TO u IDENTIFIED BY p1 AUTHENTICATED BY v IDENTIFIED BY p2 USING 'db'",
        $"CREATE SHARED PUBLIC DATABASE LINK l CONNECT TO u IDENTIFIED BY {M} AUTHENTICATED BY v IDENTIFIED BY {M} USING 'db'")]
    [InlineData("CREATE PUBLIC DATABASE LINK l CONNECT TO u IDENTIFIED BY VALUES ':1' USING 'db'", $"CREATE PUBLIC DATABASE LINK l CONNECT TO u IDENTIFIED BY VALUES {M} USING 'db'")]
    public void A_database_link_loses_every_password(string sql, string expected)
    {
        Assert.Equal(expected, OraPasswordRedactor.Redact(sql));
    }

    [Theory]
    [InlineData("CREATE USER a IDENTIFIED BY p@ss!w0rd DEFAULT TABLESPACE t", $"CREATE USER a IDENTIFIED BY {M} DEFAULT TABLESPACE t")]
    [InlineData("CREATE USER a IDENTIFIED BY pa'ss", $"CREATE USER a IDENTIFIED BY {M}")]
    [InlineData("CREATE USER a IDENTIFIED BY \"pw\"tail DEFAULT", $"CREATE USER a IDENTIFIED BY {M} DEFAULT")]
    [InlineData("ALTER USER a IDENTIFIED BY (\"my S3cret phrase\")", $"ALTER USER a IDENTIFIED BY {M}")]
    [InlineData("ALTER USER a IDENTIFIED BY =\"my S3cret phrase\"", $"ALTER USER a IDENTIFIED BY {M}")]
    [InlineData("ALTER USER a IDENTIFIED BY \"a\"\"b S3cret c\" DEFAULT TABLESPACE t", $"ALTER USER a IDENTIFIED BY {M} DEFAULT TABLESPACE t")]
    [InlineData("ALTER USER a IDENTIFIED BY abc\u00a0S3cret x", $"ALTER USER a IDENTIFIED BY {M} {M}")]
    [InlineData("ALTER USER a IDENTIFIED BY abc S3cret x", $"ALTER USER a IDENTIFIED BY {M} {M}")]
    [InlineData("ALTER USER a IDENTIFIED BY\" x\" S3cret", $"ALTER USER a IDENTIFIED BY{M} {M}")]
    public void A_password_that_is_no_valid_word_still_goes(string sql, string expected)
    {
        Assert.Equal(expected, OraPasswordRedactor.Redact(sql));
    }

    [Theory]
    [InlineData("CREATE USER a IDENTIFIED BY \"unterminated secret and more", $"CREATE USER a IDENTIFIED BY {M}")]
    [InlineData("CREATE USER a IDENTIFIED BY VALUES 'S:ABC and more", $"CREATE USER a IDENTIFIED BY VALUES {M}")]
    [InlineData("BEGIN x(password => 'never closed); END;", $"BEGIN x(password => {M}")]
    [InlineData("BEGIN DBMS_CREDENTIAL.CREATE_CREDENTIAL('c', 'u', 'never closed); END;", $"BEGIN DBMS_CREDENTIAL.CREATE_CREDENTIAL('{M}', '{M}', {M}")]
    [InlineData("BEGIN x(password => q'[never closed); END;", $"BEGIN x(password => {M}")]
    public void An_unterminated_quote_after_a_keyword_redacts_the_rest(string sql, string expected)
    {
        Assert.Equal(expected, OraPasswordRedactor.Redact(sql));
    }

    /// <summary>A quote that never ends means the lexer may be wrong about everything after it, so nothing after it ships.</summary>
    [Theory]
    [InlineData("ALTER USER \"bob IDENTIFIED BY S3cret", $"ALTER USER {M}")]
    [InlineData("SELECT ' FROM t; ALTER USER a IDENTIFIED/**/BY S3cret", $"SELECT {M}")]
    [InlineData("SELECT \" FROM t; BEGIN x(password => 'S3cret'); END;", $"SELECT {M}")]
    public void An_unterminated_quote_before_a_keyword_redacts_the_rest_too(string sql, string expected)
    {
        Assert.Equal(expected, OraPasswordRedactor.Redact(sql));
    }

    [Theory]
    [InlineData("ALTER USER a IDENTIFIED\u0000BY S3cret", $"ALTER USER a IDENTIFIED{M}")]
    [InlineData("ALTER USER a IDENTIFIED\u200bBY S3cret", $"ALTER USER a IDENTIFIED{M}")]
    [InlineData("ALTER USER a IDENTIFIED\u001fBY S3cret", $"ALTER USER a IDENTIFIED{M}")]
    public void Anything_but_a_clause_after_identified_redacts_the_rest(string sql, string expected)
    {
        Assert.Equal(expected, OraPasswordRedactor.Redact(sql));
    }

    [Theory]
    [InlineData("ADMINISTER KEY MANAGEMENT ALTER KEYSTORE PASSWORD IDENTIFIED BY old1 SET New1Secret WITH BACKUP",
        $"ADMINISTER KEY MANAGEMENT ALTER KEYSTORE PASSWORD IDENTIFIED BY {M} SET {M} WITH BACKUP")]
    [InlineData("ADMINISTER KEY MANAGEMENT ALTER KEYSTORE PASSWORD IDENTIFIED BY \"old 1\" SET \"New1Secret x\" WITH BACKUP",
        $"ADMINISTER KEY MANAGEMENT ALTER KEYSTORE PASSWORD IDENTIFIED BY {M} SET {M} WITH BACKUP")]
    [InlineData("ADMINISTER KEY MANAGEMENT EXPORT ENCRYPTION KEYS WITH SECRET \"Xp0rtS3cret\" TO '/p' IDENTIFIED BY x",
        $"ADMINISTER KEY MANAGEMENT EXPORT ENCRYPTION KEYS WITH SECRET {M} TO '/p' IDENTIFIED BY {M}")]
    [InlineData("ADMINISTER KEY MANAGEMENT EXPORT KEYS WITH SECRET Xp0rtS3cret TO '/p' IDENTIFIED BY x",
        $"ADMINISTER KEY MANAGEMENT EXPORT KEYS WITH SECRET {M} TO '/p' IDENTIFIED BY {M}")]
    [InlineData("ADMINISTER KEY MANAGEMENT ADD SECRET \"S3cret\" FOR CLIENT 'c' IDENTIFIED BY x",
        $"ADMINISTER KEY MANAGEMENT ADD SECRET {M} FOR CLIENT 'c' IDENTIFIED BY {M}")]
    [InlineData("ALTER PLUGGABLE DATABASE p UNPLUG INTO '/x.xml' ENCRYPT USING \"Tr4nsport\"", $"ALTER PLUGGABLE DATABASE p UNPLUG INTO '/x.xml' ENCRYPT USING {M}")]
    [InlineData("CREATE PLUGGABLE DATABASE p USING '/x.xml' DECRYPT USING \"Tr4nsport\"", $"CREATE PLUGGABLE DATABASE p USING '/x.xml' DECRYPT USING {M}")]
    public void A_secret_that_is_no_string_goes(string sql, string expected)
    {
        Assert.Equal(expected, OraPasswordRedactor.Redact(sql));
    }

    [Theory]
    [InlineData("BEGIN x(password => 'S3cret'); END;", $"BEGIN x(password => '{M}'); END;")]
    [InlineData("BEGIN x(PASSWORD=>'S3cret', username => 'bob'); END;", $"BEGIN x(PASSWORD=>'{M}', username => 'bob'); END;")]
    [InlineData("BEGIN x(p_password => q'[it's]'); END;", $"BEGIN x(p_password => '{M}'); END;")]
    [InlineData("BEGIN x(password => N'S3cret'); END;", $"BEGIN x(password => '{M}'); END;")]
    [InlineData("BEGIN x(password => nq'{S3cret}'); END;", $"BEGIN x(password => '{M}'); END;")]
    [InlineData("BEGIN x(password => 'a' || 'b'); END;", $"BEGIN x(password => '{M}' || '{M}'); END;")]
    [InlineData("DECLARE v_pwd VARCHAR2(4000) := 'S3cret'; BEGIN NULL; END;", $"DECLARE v_pwd VARCHAR2(4000) := '{M}'; BEGIN NULL; END;")]
    [InlineData("ADMINISTER KEY MANAGEMENT ADD SECRET 'k3y' FOR CLIENT 'c'", $"ADMINISTER KEY MANAGEMENT ADD SECRET {M} FOR CLIENT 'c'")]
    [InlineData("DECLARE v_pass VARCHAR2(30) := 'S3cret'; BEGIN NULL; END;", $"DECLARE v_pass VARCHAR2(30) := '{M}'; BEGIN NULL; END;")]
    [InlineData("DECLARE v_pw VARCHAR2(30) := 'S3cret'; BEGIN NULL; END;", $"DECLARE v_pw VARCHAR2(30) := '{M}'; BEGIN NULL; END;")]
    [InlineData("DECLARE v_password VARCHAR2(30) NOT NULL := 'S3cret'; BEGIN NULL; END;", $"DECLARE v_password VARCHAR2(30) NOT NULL := '{M}'; BEGIN NULL; END;")]
    [InlineData("BEGIN x(secret => \"S3cret\"); END;", $"BEGIN x(secret => '{M}'); END;")]
    public void A_string_after_a_name_that_says_secret_is_redacted(string sql, string expected)
    {
        Assert.Equal(expected, OraPasswordRedactor.Redact(sql));
    }

    [Theory]
    [InlineData("BEGIN DBMS_CREDENTIAL.CREATE_CREDENTIAL('cred', 'bob', 'S3cret'); END;", $"BEGIN DBMS_CREDENTIAL.CREATE_CREDENTIAL('{M}', '{M}', '{M}'); END;")]
    [InlineData("begin dbms_cloud.create_credential('cred', 'bob', 'S3cret'); end;", $"begin dbms_cloud.create_credential('{M}', '{M}', '{M}'); end;")]
    [InlineData("BEGIN SYS.DBMS_CREDENTIAL . UPDATE_CREDENTIAL('c', 'password', 'S3cret'); END;", $"BEGIN SYS.DBMS_CREDENTIAL . UPDATE_CREDENTIAL('{M}', '{M}', '{M}'); END;")]
    [InlineData("BEGIN \"DBMS_CREDENTIAL\".\"CREATE_CREDENTIAL\"('c', 'u', 'S3cret'); END;", $"BEGIN \"DBMS_CREDENTIAL\".\"CREATE_CREDENTIAL\"('{M}', '{M}', '{M}'); END;")]
    [InlineData("BEGIN UTL_HTTP.SET_AUTHENTICATION(r, 'bob', 'S3cret'); END;", $"BEGIN UTL_HTTP.SET_AUTHENTICATION(r, '{M}', '{M}'); END;")]
    [InlineData("BEGIN UTL_HTTP.SET_WALLET('file:/w', q'!S3cret!'); END;", $"BEGIN UTL_HTTP.SET_WALLET('{M}', '{M}'); END;")]
    [InlineData("BEGIN x := DBMS_LDAP.SIMPLE_BIND_S(l, 'cn=a', 'S3cret'); y('z'); END;", $"BEGIN x := DBMS_LDAP.SIMPLE_BIND_S(l, '{M}', '{M}'); y('z'); END;")]
    public void Every_string_in_a_call_with_a_secret_by_position_is_redacted(string sql, string expected)
    {
        Assert.Equal(expected, OraPasswordRedactor.Redact(sql));
    }

    [Fact]
    public void Every_listed_call_is_caught_as_written()
    {
        foreach (var call in OraPasswordRedactor.SecretCalls)
        {
            var redacted = OraPasswordRedactor.Redact($"BEGIN {call}('Secret-Value'); END;");
            Assert.DoesNotContain("Secret-Value", redacted);
        }
    }

    [Theory]
    [InlineData("BEGIN EXECUTE IMMEDIATE 'ALTER USER a IDENTIFIED BY \"S3cret\" ACCOUNT UNLOCK'; END;", $"BEGIN EXECUTE IMMEDIATE '{M}'; END;")]
    [InlineData("BEGIN EXECUTE IMMEDIATE 'ALTER USER a IDENTIFIED BY ' || 'S3cret'; END;", $"BEGIN EXECUTE IMMEDIATE '{M}' || '{M}'; END;")]
    [InlineData("BEGIN EXECUTE IMMEDIATE 'ALTER USER a IDENTIFIED BY ' || CHR(34) || 'S3cret' || CHR(34); END;", $"BEGIN EXECUTE IMMEDIATE '{M}' || CHR(34) || '{M}' || CHR(34); END;")]
    [InlineData("ALTER USER a\" IDENTIFIED BY S3cret\" DEFAULT TABLESPACE t", $"ALTER USER a'{M}' DEFAULT TABLESPACE t")]
    [InlineData("BEGIN EXECUTE IMMEDIATE q'[CREATE USER a identified  by x]'; END;", $"BEGIN EXECUTE IMMEDIATE '{M}'; END;")]
    [InlineData("BEGIN EXECUTE IMMEDIATE 'BEGIN DBMS_CLOUD.CREATE_CREDENTIAL(''c'',''u'',''p''); END;'; END;", $"BEGIN EXECUTE IMMEDIATE '{M}'; END;")]
    [InlineData("BEGIN s := 'host=h password=S3cret'; END;", $"BEGIN s := '{M}'; END;")]
    [InlineData("SELECT UTL_HTTP.REQUEST('https://bob:S3cret@192.0.2.1/x') FROM dual", $"SELECT UTL_HTTP.REQUEST('{M}') FROM dual")]
    [InlineData("SELECT f('https://bob:S3cret@192.0.2.1/x') FROM dual", $"SELECT f('{M}') FROM dual")]
    public void A_string_that_holds_a_password_itself_goes_whole(string sql, string expected)
    {
        Assert.Equal(expected, OraPasswordRedactor.Redact(sql));
    }

    [Theory]
    [InlineData("ALTER USER a IDENTIFIED /* new */ BY /* it is */ S3cret", $"ALTER USER a IDENTIFIED /* new */ BY /* it is */ {M}")]
    [InlineData("ALTER USER a IDENTIFIED -- new\nBY\n-- it is\nS3cret", $"ALTER USER a IDENTIFIED -- new\nBY\n-- it is\n{M}")]
    [InlineData("ALTER USER a IDENTIFIED BY S3cret -- done", $"ALTER USER a IDENTIFIED BY {M} -- done")]
    public void Comments_around_the_keywords_do_not_hide_a_password(string sql, string expected)
    {
        Assert.Equal(expected, OraPasswordRedactor.Redact(sql));
    }

    [Theory]
    [InlineData("SELECT \"IDENTIFIED\" BY_DAY FROM t")]
    [InlineData("SELECT \"IDENTIFIED\", by FROM t")]
    [InlineData("ALTER USER a IDENTIFIED EXTERNALLY")]
    [InlineData("ALTER USER a IDENTIFIED GLOBALLY AS 'CN=a'")]
    [InlineData("-- IDENTIFIED BY is only a comment here\nSELECT 1 FROM dual")]
    [InlineData("SELECT 'text' FROM passengers")]
    [InlineData("GRANT SELECT ON passwords TO app")]
    [InlineData("SELECT password FROM t WHERE name = 'bob'")]
    [InlineData("SELECT 1 FROM dual")]
    [InlineData("")]
    public void Text_with_no_password_comes_back_unchanged(string sql)
    {
        Assert.Equal(sql, OraPasswordRedactor.Redact(sql));
    }

    [Theory]
    [InlineData("SELECT q'[IDENTIFIED BY]' FROM t WHERE a = 'b'")]
    public void A_string_is_only_a_string_to_the_keywords(string sql)
    {
        // Masked, whole: a string that names the keywords is treated as if it held one.
        Assert.DoesNotContain("IDENTIFIED", OraPasswordRedactor.Redact(sql));
    }

    /// <summary>
    /// 🔴 What actually ships: the redactor, then the shape. Each of these got past the redactor alone, or would with one
    /// spelling more; none is left to a pattern, because no pattern can read dynamic SQL.
    /// </summary>
    [Theory]
    // Comments.
    [InlineData("-- ALTER USER app IDENTIFIED BY S3cret\nSELECT 1 FROM dual")]
    [InlineData("/* password: S3cret */ CREATE TABLE t (a INT)")]
    [InlineData("CREATE PROCEDURE p AS /* ALTER USER a IDENTIFIED BY S3cret */ BEGIN NULL; END;")]
    [InlineData("/* ALTER USER a IDENTIFIED BY S3cret")]
    [InlineData("ALTER USER a IDENTIFIED BY x /* note: was S3cret")]
    [InlineData("CREATE USER a IDENTIFIED BY x -- temp S3cret")]
    [InlineData("-- old\rALTER USER a IDENTIFIED BY S3cret")]
    // Values in statements that are not grouped.
    [InlineData("CREATE TABLE t AS SELECT * FROM c WHERE ssn = 'S3cret'")]
    [InlineData("EXPLAIN PLAN FOR SELECT * FROM c WHERE ssn = 'S3cret'")]
    [InlineData("CALL app.set_passwd('bob', 'S3cret')")]
    // Dynamic SQL, in stored code.
    [InlineData("CREATE PROCEDURE p AS BEGIN EXECUTE IMMEDIATE 'ALTER USER a IDENTIFIED/**/BY S3cret'; END;")]
    [InlineData("CREATE PROCEDURE p AS BEGIN EXECUTE IMMEDIATE 'ALTER USER a IDENT' || 'IFIED BY S3cret'; END;")]
    [InlineData("CREATE PROCEDURE p AS BEGIN EXECUTE IMMEDIATE 'ALTER USER a IDENTIFIED' || ' BY S3cret'; END;")]
    [InlineData("CREATE PROCEDURE p AS BEGIN EXECUTE IMMEDIATE 'ALTER USER a IDENTIFIED BY ' || LOWER('S3cret'); END;")]
    [InlineData("CREATE PROCEDURE p AS BEGIN EXECUTE IMMEDIATE CONCAT('ALTER USER a IDENTIFIED BY ', 'S3cret'); END;")]
    [InlineData("CREATE PROCEDURE p AS BEGIN EXECUTE IMMEDIATE 'ALTER USER a IDENTIFIED BY ' || v_x || 'S3cret'; END;")]
    [InlineData("CREATE PROCEDURE p AS BEGIN EXECUTE IMMEDIATE 'BEGIN DBMS_CLOUD . CREATE_CREDENTIAL(''c'',''u'',''S3cret''); END;'; END;")]
    [InlineData("CREATE PROCEDURE p AS BEGIN EXECUTE IMMEDIATE 'BEGIN DBMS_CREDENTIAL.CREATE_' || 'CREDENTIAL(''c'',''u'',''S3cret''); END;'; END;")]
    [InlineData("CREATE PROCEDURE p AS BEGIN EXECUTE IMMEDIATE REPLACE('ALTER USER a IDENT#FIED BY S3cret', '#', 'I'); END;")]
    [InlineData("CREATE PROCEDURE p AS BEGIN EXECUTE IMMEDIATE UNISTR('ALTER USER a \\0049DENTIFIED BY S3cret'); END;")]
    // Secrets by name or position that no list holds.
    [InlineData("CREATE PROCEDURE p AS BEGIN app.change_password('bob', 'S3cret'); END;")]
    [InlineData("CREATE PROCEDURE p AS BEGIN q := JSON_OBJECT('password' VALUE 'S3cret'); END;")]
    [InlineData("CREATE PROCEDURE p AS BEGIN x := 'sqlplus scott/S3cret@orcl'; END;")]
    [InlineData("CREATE PROCEDURE p AS v VARCHAR2(99) := '{\"password\":\"S3cret\"}'; BEGIN NULL; END;")]
    [InlineData("CREATE PROCEDURE p AS BEGIN UTL_HTTP.SET_HEADER(r, 'Authorization', 'Bearer S3cret'); END;")]
    [InlineData("CREATE PROCEDURE p AS BEGIN c := UTL_TCP.OPEN_CONNECTION('h', 443, NULL, NULL, 1024, 0, 'AL32UTF8', 0, 'file:/w', 'S3cret'); END;")]
    [InlineData("CREATE PROCEDURE p AS BEGIN DBMS_CREDENTIAL.CREATE_CREDENTIAL@lnk('c','u','S3cret'); END;")]
    [InlineData("CREATE PROCEDURE p AS BEGIN r := DBMS_CRYPTO.ENCRYPT(s, 4353, UTL_RAW.CAST_TO_RAW('S3cret')); END;")]
    // Quotes in the wrong place.
    [InlineData("BEGIN x('; y(password => 'S3cret'); END;")]
    [InlineData("SELECT f('https://bob:S3cret")]
    [InlineData("SELECT 1q'[it's S3cret]' AND x='y' FROM t")]
    [InlineData("SELECT * FROM t WHERE a = 'O'S3cret' AND b = '1'")]
    [InlineData("SELECT * FROM t WHERE a = \"S3cret\"")]
    [InlineData("SELECT :\"S3cret\" FROM dual")]
    public void Nothing_of_a_secret_ships_as_the_shape_of_the_redacted_text(string sql)
    {
        Assert.DoesNotContain("S3cret", OraStatementShape.Of(OraPasswordRedactor.Redact(sql)));
    }

    /// <summary>The text is cut at a fixed length, so anything can end anywhere.</summary>
    [Fact]
    public void A_cut_inside_a_password_leaves_none_of_it()
    {
        const string sql = "CREATE USER a IDENTIFIED BY \"Sup3rSecretValue\" ACCOUNT LOCK";
        for (var cut = sql.IndexOf('"') + 1; cut < sql.IndexOf(" ACCOUNT", StringComparison.Ordinal); cut++)
        {
            Assert.DoesNotContain("Sup3", OraPasswordRedactor.Redact(sql[..cut]));
        }
    }

    [Fact]
    public void No_input_makes_it_throw()
    {
        var random = new Random(4021);
        string[] parts =
        {
            "IDENTIFIED", "BY", "VALUES", "REPLACE", "password", "=>", ":=", "'", "''", "\"", "q'[", "]'", "q'", "nq'{", "}'", "N'", "/*", "*/", "--", "\n", " ",
            "(", ")", ",", ";", ".", "DBMS_CREDENTIAL", "CREATE_CREDENTIAL", "UTL_HTTP", "://", "@", "\0", "é", "\ud83d", "1", "||", "secret",
        };
        for (var i = 0; i < 20_000; i++)
        {
            var text = string.Concat(Enumerable.Range(0, random.Next(0, 24)).Select(_ => parts[random.Next(parts.Length)]));
            _ = OraPasswordRedactor.Redact(text);
            _ = OraStatementShape.Of(text);
        }
    }

    /// <summary>A password can sit after any of the fragments above: whatever surrounds it, it does not get out.</summary>
    [Fact]
    public void A_password_after_any_prefix_does_not_get_out()
    {
        var random = new Random(77);
        string[] before = { " ", "/* c */ ", "x ", "'a' ", "\"q\" ", "(", ", ", "-- c\n" };
        for (var i = 0; i < 5_000; i++)
        {
            var prefix = string.Concat(Enumerable.Range(0, random.Next(0, 4)).Select(_ => before[random.Next(before.Length)]));
            var redacted = OraPasswordRedactor.Redact($"{prefix}ALTER USER a IDENTIFIED BY Zq9Secret ACCOUNT LOCK");
            Assert.DoesNotContain("Zq9Secret", redacted);
        }
    }
}
