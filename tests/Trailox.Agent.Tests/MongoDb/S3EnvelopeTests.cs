using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Trailox.Agent.Engines.MongoDb;

namespace Trailox.Agent.Tests.MongoDb;

/// <summary>
/// The S3 export's envelope: its <c>log</c> member is the original line as a JSON string, and the
/// one-pass unwrap must hand out exactly what a JSON reader would.
/// </summary>
public class S3EnvelopeTests
{
    private static string Envelope(string log, JavaScriptEncoder? encoder = null)
    {
        var buffer = new MemoryStream();
        using (var w = new Utf8JsonWriter(buffer, new JsonWriterOptions { Encoder = encoder ?? JavaScriptEncoder.UnsafeRelaxedJsonEscaping }))
        {
            w.WriteStartObject();
            w.WriteString("service.name", "mongod");
            w.WriteString("mongodb.log.type", "mongod_audit");
            w.WriteString("log", log);
            w.WriteString("timestamp", "2026-10-19T13:30:31.545600Z");
            w.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.ToArray());
    }

    private static string Inner(string record) => Encoding.UTF8.GetString(new S3Envelope().Inner(Encoding.UTF8.GetBytes(record)));

    [Theory]
    [InlineData("""{"atype":"authCheck","ts":{"$date":"2026-10-19T12:04:32.000+00:00"}}""")]
    [InlineData("""{"msg":"say \"hi\"","path":"C:\\data\\db","nl":"a\nb\tc\r","ctl":"\u0001\u001f"}""")]
    [InlineData("""{"name":"Zoë 名前 🚀","emoji":"😀"}""")]
    [InlineData("")]
    public void The_inner_line_is_what_a_JSON_reader_reads(string log)
    {
        // Relaxed escaping (raw UTF-8), and the default one, which writes \uXXXX and surrogate pairs for anything non-ASCII.
        foreach (var encoder in new[] { JavaScriptEncoder.UnsafeRelaxedJsonEscaping, JavaScriptEncoder.Default })
        {
            Assert.Equal(log, Inner(Envelope(log, encoder)));
        }
    }

    [Fact]
    public void A_log_member_written_with_spacing_is_read_by_the_reader()
    {
        Assert.Equal("""{"a":1}""", Inner("""{ "service.name" : "mongod", "log" : "{\"a\":1}" }"""));
    }

    [Fact]
    public void A_key_that_only_looks_like_log_inside_another_value_is_not_taken()
    {
        // Inside a string value every quote is escaped, so an unescaped "log":" is only ever the member itself.
        var record = """{"note":"{\"log\":\"decoy\"}","log":"{\"real\":true}"}""";

        Assert.Equal("""{"real":true}""", Inner(record));
    }

    [Theory]
    [InlineData("""{"service.name":"mongod"}""")]
    [InlineData("""{"log":{"a":1}}""")]
    [InlineData("""not json""")]
    [InlineData("""{"log":"unterminated""")]
    [InlineData("""{"log":"bad \x escape"}""")]
    [InlineData("""{"log":"lone \ud800 surrogate"}""")]
    public void A_record_without_a_string_log_gives_an_empty_line(string record)
    {
        Assert.Equal("", Inner(record));
    }

    /// <summary>
    /// 🔴 A stackalloc inside the unescape loop took more stack at every escape: 100,000 of them overflowed a 1.5 MB
    ///    thread, which kills the process. 250,000 escapes on a 256 KiB thread now decode.
    /// </summary>
    [Fact]
    public void A_record_of_many_unicode_escapes_does_not_grow_the_stack()
    {
        var log = string.Concat(Enumerable.Repeat("<é😀", 250_000 / 4));
        string? inner = null;
        var thread = new Thread(() => inner = Inner(Envelope(log, JavaScriptEncoder.Default)), maxStackSize: 256 * 1024);

        thread.Start();
        thread.Join();

        Assert.Equal(log, inner);
    }

    [Fact]
    public void A_line_longer_than_the_buffer_grows_it()
    {
        var log = "{\"pad\":\"" + new string('x', 100_000) + "\\\"q\"}";

        Assert.Equal(log, Inner(Envelope(log)));
    }

    [Fact]
    public void The_buffer_is_reused_and_each_line_is_valid_until_the_next()
    {
        var envelope = new S3Envelope();
        var first = Encoding.UTF8.GetString(envelope.Inner(Encoding.UTF8.GetBytes(Envelope("{\"n\":\"one\\\"\"}"))));
        var second = Encoding.UTF8.GetString(envelope.Inner(Encoding.UTF8.GetBytes(Envelope("{\"n\":2}"))));

        Assert.Equal("{\"n\":\"one\\\"\"}", first);
        Assert.Equal("{\"n\":2}", second);
    }
}
