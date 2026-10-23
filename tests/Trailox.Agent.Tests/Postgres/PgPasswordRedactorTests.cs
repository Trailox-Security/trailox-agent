using Trailox.Agent.Engines.Postgres;

namespace Trailox.Agent.Tests.Postgres;

/// <summary>
/// No password leaves the customer's network: PostgreSQL writes <c>CREATE ROLE ... PASSWORD '...'</c> into its log as
/// typed, and <c>pg_stat_statements</c> keeps the literal too.
/// </summary>
public class PgPasswordRedactorTests
{
    [Theory]
    [InlineData("CREATE ROLE a LOGIN PASSWORD 'S3cret'", "CREATE ROLE a LOGIN PASSWORD '<redacted>'")]
    [InlineData("alter user a with encrypted password 'it''s'", "alter user a with encrypted password '<redacted>'")]
    [InlineData("ALTER ROLE a PASSWORD E'se\\'cret' VALID UNTIL '2027-01-01'", "ALTER ROLE a PASSWORD '<redacted>' VALID UNTIL '2027-01-01'")]
    [InlineData("ALTER ROLE a PASSWORD $$sec'ret$$", "ALTER ROLE a PASSWORD '<redacted>'")]
    [InlineData("ALTER ROLE a PASSWORD $p$sec$ret$p$ LOGIN", "ALTER ROLE a PASSWORD '<redacted>' LOGIN")]
    [InlineData("ALTER ROLE a PASSWORD U&'\\0073ecret'", "ALTER ROLE a PASSWORD '<redacted>'")]
    [InlineData("ALTER USER a PASSWORD 'SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk:c2VydmVy'", "ALTER USER a PASSWORD '<redacted>'")]
    [InlineData("ALTER USER a PASSWORD 'md5f3b1c2d4e5a6b7c8d9e0f1a2b3c4d5e6'", "ALTER USER a PASSWORD '<redacted>'")]
    [InlineData("ALTER ROLE a PASSWORD /* new */ 'x'", "ALTER ROLE a PASSWORD /* new */ '<redacted>'")]
    [InlineData("ALTER ROLE a PASSWORD\n-- line comment\n'x'", "ALTER ROLE a PASSWORD\n-- line comment\n'<redacted>'")]
    [InlineData("UPDATE app_users SET password = 'hash' WHERE id = 7", "UPDATE app_users SET password = '<redacted>' WHERE id = 7")]
    [InlineData("SELECT set_config('x', 'y', false), f(password => 'z')", "SELECT set_config('x', 'y', false), f(password => '<redacted>')")]
    public void A_string_after_password_is_redacted_in_every_form(string sql, string expected)
    {
        Assert.Equal(expected, PgPasswordRedactor.Redact(sql));
    }

    [Theory]
    [InlineData("CREATE USER MAPPING FOR u SERVER s OPTIONS (user 'remote', password 'p4ss')",
        "CREATE USER MAPPING FOR u SERVER s OPTIONS (user 'remote', password '<redacted>')")]
    [InlineData("ALTER SERVER s OPTIONS (ADD \"password\" 'p4ss')", "ALTER SERVER s OPTIONS (ADD \"password\" '<redacted>')")]
    [InlineData("CREATE SUBSCRIPTION s CONNECTION 'host=192.0.2.1 dbname=d password=p4ss' PUBLICATION p",
        "CREATE SUBSCRIPTION s CONNECTION 'host=192.0.2.1 dbname=d password=<redacted>' PUBLICATION p")]
    [InlineData("CREATE SUBSCRIPTION s CONNECTION 'host=h password=''p w'' dbname=d' PUBLICATION p",
        "CREATE SUBSCRIPTION s CONNECTION 'host=h password=<redacted> dbname=d' PUBLICATION p")]
    [InlineData("SELECT dblink_connect('c', 'postgresql://app:p4ss@192.0.2.1/d')", "SELECT dblink_connect('c', 'postgresql://app:<redacted>@192.0.2.1/d')")]
    [InlineData("SELECT dblink_connect('PASSWORD=p4ss host=h')", "SELECT dblink_connect('PASSWORD=<redacted> host=h')")]
    public void Connection_strings_and_options_lose_their_password(string sql, string expected)
    {
        Assert.Equal(expected, PgPasswordRedactor.Redact(sql));
    }

    [Theory]
    [InlineData("ALTER ROLE a PASSWORD NULL")]
    [InlineData("SELECT password FROM app_users WHERE name = 'bob'")]
    [InlineData("COMMENT ON COLUMN t.password_hash IS 'bcrypt'")]
    [InlineData("CREATE TABLE t (a text DEFAULT 'password')")]
    [InlineData("GRANT SELECT ON t TO r")]
    [InlineData("")]
    public void Text_with_no_password_is_unchanged(string sql)
    {
        Assert.Equal(sql, PgPasswordRedactor.Redact(sql));
    }

    [Theory]
    [InlineData("ALTER ROLE a PASSWORD 'abc", "ALTER ROLE a PASSWORD '<redacted>'")]
    [InlineData("ALTER ROLE a PASSWORD $$abc", "ALTER ROLE a PASSWORD '<redacted>'")]
    [InlineData("ALTER ROLE a PASSWORD E'ab\\", "ALTER ROLE a PASSWORD '<redacted>'")]
    public void A_cut_password_string_redacts_everything_after_the_keyword(string sql, string expected)
    {
        Assert.Equal(expected, PgPasswordRedactor.Redact(sql));
    }

    [Theory]
    // PostgreSQL joins two strings separated by a line break (also with a comment between them).
    [InlineData("ALTER ROLE a PASSWORD 'abc'\n'Zq9Tail'", "ALTER ROLE a PASSWORD '<redacted>'\n'<redacted>'")]
    [InlineData("ALTER ROLE a PASSWORD 'abc'\n-- c\n'Zq9Tail'", "ALTER ROLE a PASSWORD '<redacted>'\n-- c\n'<redacted>'")]
    [InlineData("ALTER ROLE a PASSWORD E'abc'\n  'Zq9Tail' LOGIN", "ALTER ROLE a PASSWORD '<redacted>'\n  '<redacted>' LOGIN")]
    // With standard_conforming_strings = off a backslash escapes the quote: the farther end is redacted to.
    [InlineData("ALTER ROLE a PASSWORD 'ab\\' Zq9Tail' LOGIN", "ALTER ROLE a PASSWORD '<redacted>' LOGIN")]
    [InlineData("ALTER ROLE a PASSWORD 'ab\\' Zq9Tail", "ALTER ROLE a PASSWORD '<redacted>'")]
    public void Strings_that_continue_a_password_and_backslash_escapes_are_redacted(string sql, string expected)
    {
        Assert.Equal(expected, PgPasswordRedactor.Redact(sql));
    }

    [Theory]
    [InlineData("CREATE SUBSCRIPTION s CONNECTION 'host=h password=ab''Zq9Tail dbname=d' PUBLICATION p",
        "CREATE SUBSCRIPTION s CONNECTION 'host=h password=<redacted> dbname=d' PUBLICATION p")]
    [InlineData("SELECT dblink_connect('password=abc\\ Zq9Tail host=h')", "SELECT dblink_connect('password=<redacted> host=h')")]
    [InlineData("SELECT dblink_connect(E'password=\\'Zq9 Tail\\' host=h')", "SELECT dblink_connect(E'password=<redacted> host=h')")]
    public void Connection_string_values_in_every_quoting_are_redacted(string sql, string expected)
    {
        Assert.Equal(expected, PgPasswordRedactor.Redact(sql));
    }

    [Theory]
    [InlineData("UPDATE u SET password = N'Zq9Tail'", "UPDATE u SET password = N'<redacted>'")]
    [InlineData("UPDATE u SET password = crypt('Zq9Tail', gen_salt('bf'))", "UPDATE u SET password = crypt('<redacted>', gen_salt('bf'))")]
    [InlineData("UPDATE u SET password = ('Zq9Tail')", "UPDATE u SET password = ('<redacted>')")]
    [InlineData("UPDATE u SET password = text 'Zq9Tail'", "UPDATE u SET password = text '<redacted>'")]
    [InlineData("DELETE FROM u WHERE password <> 'Zq9Tail'", "DELETE FROM u WHERE password <> '<redacted>'")]
    [InlineData("CREATE SERVER s FOREIGN DATA WRAPPER odbc_fdw OPTIONS (odbc_PWD 'Zq9Tail')", "CREATE SERVER s FOREIGN DATA WRAPPER odbc_fdw OPTIONS (odbc_PWD '<redacted>')")]
    [InlineData("SELECT aws_commons.create_aws_credentials('AKIAEXAMPLE', 'Zq9Tail', '')",
        "SELECT aws_commons.create_aws_credentials('<redacted>', '<redacted>', '<redacted>')")]
    public void Application_passwords_and_aws_credentials_are_redacted(string sql, string expected)
    {
        Assert.Equal(expected, PgPasswordRedactor.Redact(sql));
    }

    [Fact]
    public void Random_text_never_throws_and_never_keeps_a_password_that_follows_the_keyword()
    {
        var random = new Random(20261022);
        const string alphabet = "abcPASSWORDpassword '\"$\\-/*\n\t=>,();EUu&0123456789:@";
        string RandomText() =>
            new string(Enumerable.Range(0, random.Next(0, 40)).Select(_ => alphabet[random.Next(alphabet.Length)]).ToArray());
        for (var i = 0; i < 20_000; i++)
        {
            var tail = RandomText();
            var noise = RandomText();

            _ = PgPasswordRedactor.Redact(noise);
            // A password, then strings that continue it after a line break, then random text.
            var continued = string.Concat(Enumerable.Range(0, random.Next(0, 3)).Select(_ => "\n'Zq9ContinuedZq9'"));
            var redacted = PgPasswordRedactor.Redact("ALTER ROLE r PASSWORD 'Zq9TopSecretZq9'" + continued + tail);

            Assert.DoesNotContain("Zq9TopSecretZq9", redacted, StringComparison.Ordinal);
            Assert.DoesNotContain("Zq9ContinuedZq9", redacted, StringComparison.Ordinal);
        }
    }
}
