using System.Text;
using System.Text.Json;
using Trailox.Agent.Engines.MongoDb;

namespace Trailox.Agent.Tests.MongoDb;

/// <summary>
/// The shape is the PII boundary: no value of a customer's command may survive it. It must also be the same
/// for the same query whichever source read the line, because the Atlas S3 export rewrites every line.
/// </summary>
public class MongoShapeTests
{
    private static (string Shape, ulong Hash) Of(string json)
    {
        var shape = new MongoShape();
        var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));
        reader.Read();
        shape.Normalize(ref reader);
        return (shape.Text(), shape.Hash);
    }

    /// <summary>An insert as the audit log writes it (measured form), with literal values a customer would care about.</summary>
    private const string Insert = """
        { "insert" : "orders", "ordered" : true, "lsid" : { "id" : { "$binary" : "0XdqYzgYS3i1Z+UBRhn2Dg==", "$type" : "04" } },
          "txnNumber" : { "$numberLong" : "1" }, "$clusterTime" : { "clusterTime" : { "$timestamp" : { "t" : 1792409842, "i" : 1 } },
          "signature" : { "hash" : { "$binary" : "cESwFhmcqKRZ1+aW1Bqkh7uiSqE=", "$type" : "00" }, "keyId" : { "$numberLong" : "7000000000000000001" } } },
          "writeConcern" : { "w" : "majority" }, "$db" : "shop", "comment" : "lit-comment for alice@example.com",
          "documents" : [ { "_id" : { "$numberLong" : "1792409845531007" }, "customer" : 2773, "email" : "lit-alice@example.com",
          "status" : "new", "created" : { "$date" : "2026-10-19T11:37:28.062+00:00" }, "note" : "lit-free text with a card 4111 1111 1111 1111" } ] }
        """;

    /// <summary>
    /// 🔴 A shape allocates nothing once warm: every audit line is shaped, at 100,000 operations a second. Before 1.8.2
    ///    each sorted object made a new comparison delegate and a closure, 96% of the engine's allocation (252 bytes a
    ///    line, measured on the bench).
    /// </summary>
    [Fact]
    public void A_warm_shape_allocates_nothing()
    {
        var shape = new MongoShape();
        var json = Encoding.UTF8.GetBytes(Insert);
        void Once()
        {
            var reader = new Utf8JsonReader(json);
            reader.Read();
            shape.Normalize(ref reader);
        }
        for (var i = 0; i < 100; i++)
        {
            Once();
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            Once();
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(allocated < 1000, $"1,000 shapes allocated {allocated:N0} bytes");
    }

    [Fact]
    public void No_literal_value_survives_the_shape()
    {
        var (shape, _) = Of(Insert);

        Assert.DoesNotContain("lit-", shape, StringComparison.Ordinal);
        Assert.DoesNotContain("alice", shape, StringComparison.Ordinal);
        Assert.DoesNotContain("4111", shape, StringComparison.Ordinal);
        Assert.DoesNotContain("2773", shape, StringComparison.Ordinal);
        Assert.DoesNotContain("orders", shape, StringComparison.Ordinal);
        Assert.DoesNotContain("1792409845531007", shape, StringComparison.Ordinal);
        Assert.Equal(
            """{"documents":[{"_id":"?number","created":"?date","customer":"?number","email":"?string","note":"?string","status":"?string"}],"insert":"?string","ordered":"?bool"}""",
            shape);
    }

    [Fact]
    public void The_drivers_envelope_and_the_comment_are_left_out()
    {
        var (shape, _) = Of(Insert);

        foreach (var key in new[] { "lsid", "$clusterTime", "txnNumber", "writeConcern", "$db", "comment" })
        {
            Assert.DoesNotContain($"\"{key}\"", shape, StringComparison.Ordinal);
        }
    }

    /// <summary>The S3 export's form of the same command: keys in another order at every depth, arrays as objects keyed "1".."n", nulls gone.</summary>
    [Fact]
    public void The_S3_exports_rewritten_line_has_the_same_shape_as_the_APIs()
    {
        const string api = """{"find":"orders","filter":{"customer":4957,"status":{"$in":["new","paid","lit-x"]},"hookTime":null},"sort":{"created":-1},"limit":20,"lsid":{"id":{"$uuid":"7256e7ee-8a40-40ed-8c35-90fce3dddfb6"}},"$db":"shop"}""";
        const string s3 = """{"$db":"shop","limit":20.0,"sort":{"created":-1},"filter":{"status":{"$in":{"2":"paid","1":"new","3":"lit-y"}},"customer":4957.0},"find":"orders"}""";

        Assert.Equal(Of(api), Of(s3));
    }

    [Fact]
    public void A_wrapper_is_one_token_whatever_the_order_of_its_keys()
    {
        Assert.Equal(Of("""{"find":"c","filter":{"_id":{"$binary":"AAAA","$type":"04"}}}"""), Of("""{"find":"c","filter":{"_id":{"$type":"04","$binary":"AAAA"}}}"""));
        Assert.Equal("""{"filter":{"_id":"?binary"},"find":"?string"}""", Of("""{"find":"c","filter":{"_id":{"$type":"04","$binary":"AAAA"}}}""").Shape);
    }

    [Fact]
    public void A_list_of_values_is_one_shape_whatever_its_length()
    {
        Assert.Equal(Of("""{"count":"c","query":{"sku":{"$in":["a"]}}}"""), Of("""{"count":"c","query":{"sku":{"$in":["a","b","c","d","e","f"]}}}"""));
    }

    [Fact]
    public void An_insert_of_one_document_and_of_five_is_one_shape()
    {
        const string one = """{"insert":"c","documents":[{"a":1,"b":"x"}]}""";
        const string five = """{"insert":"c","documents":[{"a":1,"b":"x"},{"a":2,"b":"y"},{"b":"z","a":3},{"a":4,"b":"w"},{"a":5,"b":"v"}]}""";

        Assert.Equal(Of(one), Of(five));
    }

    [Fact]
    public void A_pipeline_keeps_its_stages_in_order()
    {
        var matchThenGroup = Of("""{"aggregate":"c","pipeline":[{"$match":{"a":1}},{"$group":{"_id":"$b"}}]}""");
        var groupThenMatch = Of("""{"aggregate":"c","pipeline":[{"$group":{"_id":"$b"}},{"$match":{"a":1}}]}""");

        Assert.NotEqual(matchThenGroup.Hash, groupThenMatch.Hash);
    }

    [Fact]
    public void Different_fields_are_different_shapes()
    {
        Assert.NotEqual(Of("""{"find":"c","filter":{"customer":1}}""").Hash, Of("""{"find":"c","filter":{"email":"x"}}""").Hash);
    }

    [Fact]
    public void An_empty_array_and_an_empty_object_are_one_token()
    {
        Assert.Equal(Of("""{"aggregate":"c","pipeline":[],"cursor":{}}"""), Of("""{"aggregate":"c","pipeline":{},"cursor":{}}"""));
    }

    [Fact]
    public void An_object_keyed_from_zero_is_an_object_not_an_array()
    {
        // The S3 export's arrays start at "1"; a document keyed "0".."n" stays a document.
        Assert.NotEqual(Of("""{"find":"c","filter":{"0":1,"1":2}}""").Shape, Of("""{"find":"c","filter":[1,2]}""").Shape);
    }

    [Fact]
    public void A_deep_document_stops_at_the_depth_limit_without_copying_values()
    {
        var deep = new StringBuilder("""{"find":"c","filter":""");
        for (var i = 0; i < 40; i++)
        {
            deep.Append("""{"a":""");
        }
        deep.Append("\"lit-deep\"");
        deep.Append('}', 40);
        deep.Append('}');

        var (shape, _) = Of(deep.ToString());

        Assert.Contains("?deep", shape, StringComparison.Ordinal);
        Assert.DoesNotContain("lit-", shape, StringComparison.Ordinal);
    }

    [Fact]
    public void The_hash_is_stable_across_runs()
    {
        // Pinned: a change to the rules or the hash must come with a new MongoShape.Version.
        Assert.Equal("""{"filter":{"customer":"?number"},"find":"?string","limit":"?number"}""", Of("""{"find":"orders","filter":{"customer":3931},"limit":20}""").Shape);
        Assert.Equal(1, MongoShape.Version);
    }
}
