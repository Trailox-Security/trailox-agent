using System.Buffers;
using System.Text;
using System.Text.Json;
using Trailox.Agent.Engines.MongoDb;

namespace Trailox.Agent.Tests.MongoDb;

/// <summary>
/// Atlas log lines (the measured forms of MongoDB 8.0, with every address, id and value replaced) through the
/// filter, the parser and the minute rows. Every line read lands in exactly one count, and every row carries
/// the columns the gateway's raw tables expect.
/// </summary>
public class MongoLogProcessorTests
{
    private const long Start = 1_792_400_400_000_000; // 2026-10-19T09:00:00Z
    private const long End = Start + 10 * 60_000_000;

    private static string AuthCheck(string time, string user = "app_orders", string ip = "10.1.0.5", string uuid = "zk70v4pUbutxtzTYyRHMog==", int customer = 4957) =>
        """{ "atype" : "authCheck", "ts" : { "$date" : "@0@" }, "uuid" : { "$binary" : "@1@", "$type" : "04" }, "local" : { "ip" : "10.0.0.10", "port" : 27017 }, "remote" : { "ip" : "@2@", "port" : 54523 }, "directRemote" : { "ip" : "@3@", "port" : 54523 }, "users" : [ { "user" : "@4@", "db" : "admin" } ], "roles" : [ { "role" : "readWrite", "db" : "shop" } ], "param" : { "command" : "find", "ns" : "shop.orders", "args" : { "find" : "orders", "filter" : { "customer" : @5@ }, "limit" : 20, "lsid" : { "id" : { "$binary" : "0XdqYzgYS3i1Z+UBRhn2Dg==", "$type" : "04" } }, "$db" : "shop" } }, "result" : 0 }""".Replace("@0@", (time).ToString()).Replace("@1@", (uuid).ToString()).Replace("@2@", (ip).ToString()).Replace("@3@", (ip).ToString()).Replace("@4@", (user).ToString()).Replace("@5@", (customer).ToString());

    /// <summary>The same operation as the Atlas S3 export writes it: keys reordered, arrays as objects keyed from "1".</summary>
    private static string AuthCheckS3(string time) =>
        """{"result":0,"param":{"ns":"shop.orders","args":{"$db":"shop","limit":20.0,"filter":{"customer":4957.0},"find":"orders"},"command":"find"},"users":{"1":{"db":"admin","user":"app_orders"}},"roles":{"1":{"db":"shop","role":"readWrite"}},"remote":{"port":54523,"ip":"10.1.0.5"},"local":{"port":27017,"ip":"10.0.0.10"},"uuid":{"$type":"04","$binary":"zk70v4pUbutxtzTYyRHMog=="},"ts":{"$date":"@0@"},"atype":"authCheck"}""".Replace("@0@", (time).ToString());

    private static string Slow(string time, int conn = 20627, int nreturned = 20, int duration = 266) =>
        """{"t":{"$date":"@0@"},"s":"I",  "c":"COMMAND",  "id":51803,   "ctx":"conn@1@","msg":"Slow query","attr":{"type":"command","isFromUserConnection":true,"ns":"shop.orders","collectionType":"normal","appName":"orders-api","queryShapeHash":"AB3EF63119968C071CA6294BE5F3756A764DCA007AF0F385A858F0BBF8E1B55E","command":{"find":"orders","filter":{"customer":3931},"limit":20,"$db":"shop"},"planSummary":"IXSCAN { customer: 1 }","keysExamined":20,"docsExamined":20,"nreturned":@2@,"reslen":6073,"cpuNanos":11744040,"remote":"10.1.0.5:49162","protocol":"op_msg","durationMillis":@3@}}""".Replace("@0@", (time).ToString()).Replace("@1@", (conn).ToString()).Replace("@2@", (nreturned).ToString()).Replace("@3@", (duration).ToString());

    private static string SignIn(string time, int conn = 20588) =>
        """{"t":{"$date":"@0@"},"s":"I",  "c":"ACCESS",   "id":5286306, "ctx":"conn@1@","msg":"Successfully authenticated","attr":{"client":"10.1.0.5:58097","isSpeculative":true,"isClusterMember":false,"mechanism":"SCRAM-SHA-256","user":"app_orders","db":"admin","result":0,"doc":{"application":{"name":"orders-api"},"driver":{"name":"nodejs","version":"6.10.0"},"os":{"type":"Linux"},"platform":"Node.js v22"},"extraInfo":{}}}""".Replace("@0@", (time).ToString()).Replace("@1@", (conn).ToString());

    private static string Failed(string time, int conn = 20531) =>
        """{"t":{"$date":"@0@"},"s":"I",  "c":"ACCESS",   "id":5286307, "ctx":"conn@1@","msg":"Failed to authenticate","attr":{"client":"198.51.100.7:61586","isSpeculative":true,"isClusterMember":false,"mechanism":"SCRAM-SHA-256","user":"admin7","db":"admin","error":"UserNotFound: Could not find user \"admin7\" for db \"admin\"","result":11,"doc":{"driver":{"name":"PyMongo|c","version":"4.10.1"},"application":{"name":"scanner"}},"extraInfo":{}}}""".Replace("@0@", (time).ToString()).Replace("@1@", (conn).ToString());

    private static string Accepted(string time, string uuid) =>
        """{"t":{"$date":"@0@"},"s":"I",  "c":"NETWORK",  "id":22943,   "ctx":"listener","msg":"Connection accepted","attr":{"remote":"10.1.0.5:52550","isLoadBalanced":false,"isProxyUnixSock":false,"uuid":{"uuid":{"$uuid":"@1@"}},"connectionId":20387,"local":"10.0.0.10:27017","connectionCount":56}}""".Replace("@0@", (time).ToString()).Replace("@1@", (uuid).ToString());

    private static string Starting(string time) =>
        """{"t":{"$date":"@0@"},"s":"I",  "c":"CONTROL",  "id":4615611, "ctx":"initandlisten","msg":"MongoDB starting","attr":{"pid":2161,"port":27017,"dbPath":"/srv/mongodb/example","architecture":"64-bit","host":"cluster0-shard-00-02.example.mongodb.net"}}""".Replace("@0@", (time).ToString());

    private static string Noise(string time) =>
        """{"t":{"$date":"@0@"},"s":"I",  "c":"NETWORK",  "id":6723804, "ctx":"conn20387","msg":"Ingress TLS handshake complete","attr":{"durationMillis":27}}""".Replace("@0@", (time).ToString());

    private static string StartupAudit(string time) =>
        """{ "atype" : "startup", "ts" : { "$date" : "@0@" }, "uuid" : { "$binary" : "3AgNncnwSlqDCdqthrg9Qw==", "$type" : "04" }, "local" : { "isSystemUser" : true }, "remote" : { "isSystemUser" : true }, "users" : [], "roles" : [], "param" : { "startupOptions" : { "auditLog" : { "destination" : "file", "filter" : "{\n  \"users.user\": {\n    \"$nin\": [\"mms-automation\"]\n  }\n}", "format" : "JSON" }, "setParameter" : { "auditAuthorizationSuccess" : "true" } }, "initialClusterServerParameters" : [] }, "result" : 0 }""".Replace("@0@", (time).ToString());

    private static (List<JsonElement> Rows, MongoLogProcessor Processor) Run(MongoLogStream stream, int groupCap, params string[] lines) =>
        Run(stream, groupCap, LineForm.Api, lines);

    private static (List<JsonElement> Rows, MongoLogProcessor Processor) Run(MongoLogStream stream, int groupCap, LineForm form, params string[] lines)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var processor = new MongoLogProcessor(stream, "cluster0-shard-00-02.example.mongodb.net", "REPLICA_PRIMARY", Start, End, new RowSink(buffer), groupCap, form);
        foreach (var line in lines)
        {
            processor.Process(Encoding.UTF8.GetBytes(line));
        }
        processor.Finish();
        var rows = Encoding.UTF8.GetString(buffer.WrittenSpan)
            .Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => JsonDocument.Parse(l).RootElement.Clone())
            .ToList();
        return (rows, processor);
    }

    private static (List<JsonElement> Rows, MongoLogProcessor Processor) Run(MongoLogStream stream, params string[] lines) =>
        Run(stream, MongoLogProcessor.DefaultGroupCap, lines);

    /// <summary>
    /// 🔴 Running out of memory fails the task: it is not one bad line. On agent 1.8.1 (2026-10-21) a window whose rows
    ///    outgrew the pod shipped exactly 128 MiB of them as complete, every later row counted malformed.
    /// </summary>
    [Fact]
    public void Running_out_of_memory_is_not_a_malformed_line()
    {
        var processor = new MongoLogProcessor(MongoLogStream.Connections, "h", "REPLICA_PRIMARY", Start, End, new RowSink(new Exhausted()));
        var line = Encoding.UTF8.GetBytes(Accepted("2026-10-19T09:00:01.000+00:00", "3b2a1e0e-5f8a-4a3e-9d61-7c2b8f4e1a90"));

        Assert.Throws<OutOfMemoryException>(() => processor.Process(line));
        Assert.Equal(0L, processor.Counts.Malformed);
    }

    /// <summary>A writer that cannot grow.</summary>
    private sealed class Exhausted : IBufferWriter<byte>
    {
        public void Advance(int count)
        {
        }

        public Memory<byte> GetMemory(int sizeHint = 0) => throw new OutOfMemoryException();

        public Span<byte> GetSpan(int sizeHint = 0) => throw new OutOfMemoryException();
    }

    [Fact]
    public void Operations_of_one_minute_and_key_are_one_row_with_their_count()
    {
        var (rows, p) = Run(MongoLogStream.Ops,
            AuthCheck("2026-10-19T09:00:01.000+00:00", customer: 1),
            AuthCheck("2026-10-19T09:00:30.000+00:00", customer: 2),
            AuthCheck("2026-10-19T09:00:59.999+00:00", customer: 3),
            AuthCheck("2026-10-19T09:01:00.000+00:00", customer: 4));

        Assert.Equal(2, rows.Count);
        var first = rows[0];
        Assert.Equal(3, first.GetProperty("event_count").GetInt64());
        Assert.Equal("app_orders", first.GetProperty("user").GetString());
        Assert.Equal("admin", first.GetProperty("auth_db").GetString());
        Assert.Equal("10.1.0.5", first.GetProperty("client_ip").GetString());
        Assert.Equal("", first.GetProperty("direct_ip").GetString());
        Assert.Equal("find", first.GetProperty("command").GetString());
        Assert.Equal("shop.orders", first.GetProperty("ns").GetString());
        Assert.Equal("authCheck", first.GetProperty("atype").GetString());
        Assert.Equal("cluster0-shard-00-02.example.mongodb.net", first.GetProperty("process").GetString());
        Assert.Equal("""{"filter":{"customer":"?number"},"find":"?string","limit":"?number"}""", first.GetProperty("shape").GetString());
        Assert.Equal(-1, first.GetProperty("audit_successes").GetInt32());
        Assert.StartsWith("2026-10-19T09:00:00", first.GetProperty("minute").GetString());
        Assert.Equal(4, p.Counts.Kept);
        Assert.Equal(p.Counts.LinesIn, p.Counts.Kept + p.Counts.Filtered + p.Counts.OutOfWindow + p.Counts.Malformed);
    }

    [Fact]
    public void The_window_is_start_inclusive_and_end_exclusive()
    {
        var (rows, p) = Run(MongoLogStream.Ops,
            AuthCheck("2026-10-19T08:59:59.999+00:00"),
            AuthCheck("2026-10-19T09:00:00.000+00:00"),
            AuthCheck("2026-10-19T09:09:59.999+00:00"),
            AuthCheck("2026-10-19T09:10:00.000+00:00"));

        Assert.Equal(2, p.Counts.Kept);
        Assert.Equal(2, p.Counts.OutOfWindow);
        Assert.Equal(2, rows.Sum(r => r.GetProperty("event_count").GetInt64()));
    }

    [Fact]
    public void An_S3_export_line_has_the_same_group_key_and_row_as_the_API_line()
    {
        var (api, _) = Run(MongoLogStream.Ops, MongoLogProcessor.DefaultGroupCap, LineForm.Api, AuthCheck("2026-10-19T09:00:01.000+00:00"));
        var (s3, p) = Run(MongoLogStream.Ops, MongoLogProcessor.DefaultGroupCap, LineForm.Rewritten, AuthCheckS3("2026-10-19T09:00:02.000+00:00"));

        Assert.Equal(1, p.Counts.Kept);
        var a = Assert.Single(api);
        var b = Assert.Single(s3);
        foreach (var column in new[] { "user", "auth_db", "client_ip", "connection_uuid", "command", "ns", "shape", "shape_hash", "minute" })
        {
            Assert.Equal(a.GetProperty(column).ToString(), b.GetProperty(column).ToString());
        }
    }

    [Fact]
    public void A_rewritten_line_is_read_by_its_top_level_keys_not_by_a_nested_key_of_the_same_name()
    {
        // "id" first appears inside attr (a session id) before the top-level id of the line.
        const string line = """{"attr":{"command":{"lsid":{"id":"x"}},"ns":"shop.orders","durationMillis":120},"msg":"Slow query","ctx":"conn9","id":51803,"t":{"$date":"2026-10-19T09:00:05.000+00:00"}}""";

        var (rows, p) = Run(MongoLogStream.SlowOps, MongoLogProcessor.DefaultGroupCap, LineForm.Rewritten, line);

        Assert.Equal(1, p.Counts.Kept);
        Assert.Equal(120, Assert.Single(rows).GetProperty("duration_ms_sum").GetInt64());
    }

    /// <summary>
    /// 🔴 A findAndModify's own "update" field is also a command name, and the S3 export can put it first: until 1.10.0
    ///    such a slow line shipped as update (5 of 183 slow rows in the bench, 2026-10-20). The command's key holds the
    ///    collection; "update" holds a document.
    /// </summary>
    [Theory]
    [InlineData("""{"update":{"$inc":{"n":1}},"query":{"_id":7},"findAndModify":"orders","new":true,"$db":"shop"}""", "findAndModify")]
    [InlineData("""{"explain":true,"pipeline":{"1":{"$match":{"a":1}}},"aggregate":"orders","$db":"shop"}""", "aggregate")]
    [InlineData("""{"aggregate":1,"pipeline":{"1":{"$currentOp":{}}},"$db":"admin"}""", "aggregate")]
    [InlineData("""{"verbosity":"queryPlanner","explain":{"find":"orders","filter":{"a":1}},"$db":"shop"}""", "explain")]
    [InlineData("""{"$db":"shop","updates":{"1":{"q":{"a":1},"u":{"$set":{"b":2}}}},"update":"orders"}""", "update")]
    public void A_slow_command_is_named_by_the_known_key_that_holds_its_collection(string command, string expected)
    {
        var line = """{"attr":{"command":@0@,"ns":"shop.orders","durationMillis":150},"msg":"Slow query","ctx":"conn9","id":51803,"t":{"$date":"2026-10-19T09:00:05.000+00:00"}}""".Replace("@0@", command);

        var (rows, p) = Run(MongoLogStream.SlowOps, MongoLogProcessor.DefaultGroupCap, LineForm.Rewritten, line);

        Assert.Equal(1, p.Counts.Kept);
        Assert.Equal(expected, Assert.Single(rows).GetProperty("command").GetString());
    }

    /// <summary>
    /// The S3 export reorders records within a batch by up to 38 s (PLAN_S3.md §2.2): the same disorder that opens a
    /// later part of a minute in an Admin API file ships as one row from the export.
    /// </summary>
    [Fact]
    public void Lines_out_of_order_by_40_seconds_ship_as_one_part_from_the_S3_export_and_two_from_the_API()
    {
        string[] times = ["2026-10-19T09:00:50.000+00:00", "2026-10-19T09:01:30.000+00:00", "2026-10-19T09:00:51.000+00:00"];

        // The reader lets the processor ship closed minutes as it goes; here after every line.
        (List<JsonElement> Rows, MongoLogProcessor Processor) Flushed(LineForm form, Func<string, string> line)
        {
            var buffer = new ArrayBufferWriter<byte>();
            var processor = new MongoLogProcessor(MongoLogStream.Ops, "cluster0-shard-00-02.example.mongodb.net", "REPLICA_PRIMARY", Start, End,
                new RowSink(buffer), MongoLogProcessor.DefaultGroupCap, form);
            foreach (var t in times)
            {
                processor.Process(Encoding.UTF8.GetBytes(line(t)));
                processor.FlushClosedMinutes();
            }
            processor.Finish();
            var rows = Encoding.UTF8.GetString(buffer.WrittenSpan).Split('\n', StringSplitOptions.RemoveEmptyEntries)
                .Select(l => JsonDocument.Parse(l).RootElement.Clone()).ToList();
            return (rows, processor);
        }

        var (api, apiProcessor) = Flushed(LineForm.Api, t => AuthCheck(t));
        var (s3, s3Processor) = Flushed(LineForm.Rewritten, AuthCheckS3);

        Assert.Equal(1, apiProcessor.LateLines);
        Assert.Equal(3, api.Count);
        Assert.Equal(0, s3Processor.LateLines);
        Assert.Equal(2, s3.Count);
        Assert.Equal(2, s3.Single(r => r.GetProperty("minute").GetString()!.StartsWith("2026-10-19T09:00", StringComparison.Ordinal))
            .GetProperty("event_count").GetInt64());
    }

    [Fact]
    public void No_literal_value_reaches_an_ops_row()
    {
        var (rows, _) = Run(MongoLogStream.Ops, AuthCheck("2026-10-19T09:00:01.000+00:00", customer: 987654321));

        Assert.DoesNotContain("987654321", Assert.Single(rows).GetRawText(), StringComparison.Ordinal);
    }

    [Fact]
    public void The_audit_connection_uuid_is_the_server_logs_uuid_string()
    {
        // 22943 writes the connection's UUID as a string; the audit line as base64 of its 16 bytes, big-endian (BSON).
        var id = Guid.Parse("b7072a5a-e39d-40b4-977c-66b4a864954a");
        var base64 = Convert.ToBase64String(id.ToByteArray(bigEndian: true));

        var (ops, _) = Run(MongoLogStream.Ops, AuthCheck("2026-10-19T09:00:01.000+00:00", uuid: base64));
        var (connections, _) = Run(MongoLogStream.Connections, Accepted("2026-10-19T09:00:00.500+00:00", id.ToString()));

        Assert.Equal(id.ToString(), Assert.Single(ops).GetProperty("connection_uuid").GetString());
        Assert.Equal(id.ToString(), Assert.Single(connections).GetProperty("connection_uuid").GetString());
    }

    [Fact]
    public void The_audit_startup_line_states_the_active_filter()
    {
        var (rows, _) = Run(MongoLogStream.Ops, StartupAudit("2026-10-19T09:03:33.829+00:00"));

        var row = Assert.Single(rows);
        Assert.Equal("startup", row.GetProperty("atype").GetString());
        Assert.Equal(1, row.GetProperty("event_count").GetInt64());
        Assert.Equal(1, row.GetProperty("audit_successes").GetInt32());
        Assert.Contains("mms-automation", row.GetProperty("audit_filter").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void Past_the_group_cap_a_minutes_new_keys_go_to_one_other_row_with_their_real_count()
    {
        var lines = Enumerable.Range(0, 10).Select(i => AuthCheck("2026-10-19T09:00:01.000+00:00", user: "user" + i)).ToArray();

        var (rows, p) = Run(MongoLogStream.Ops, groupCap: 3, lines);

        Assert.Equal(4, rows.Count);
        var other = Assert.Single(rows, r => r.GetProperty("user").GetString() == MinuteAggregator.Other);
        Assert.Equal(7, other.GetProperty("event_count").GetInt64());
        Assert.Equal(10, rows.Sum(r => r.GetProperty("event_count").GetInt64()));
        Assert.Equal(7, p.OverflowLines);
    }

    [Fact]
    public void A_line_for_a_minute_already_shipped_opens_its_next_part()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var p = new MongoLogProcessor(MongoLogStream.Ops, "h", "REPLICA_PRIMARY", Start, End, new RowSink(buffer));
        p.Process(Encoding.UTF8.GetBytes(AuthCheck("2026-10-19T09:00:10.000+00:00")));
        p.Process(Encoding.UTF8.GetBytes(AuthCheck("2026-10-19T09:01:05.000+00:00")));
        p.FlushClosedMinutes();
        p.Process(Encoding.UTF8.GetBytes(AuthCheck("2026-10-19T09:00:20.000+00:00")));
        p.Finish();

        var rows = Encoding.UTF8.GetString(buffer.WrittenSpan).Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(l => JsonDocument.Parse(l).RootElement).ToList();
        var minute = rows.Where(r => r.GetProperty("minute").GetString()!.StartsWith("2026-10-19T09:00", StringComparison.Ordinal)).ToList();
        Assert.Equal(new[] { 0, 1 }, minute.Select(r => r.GetProperty("part").GetInt32()).OrderBy(x => x));
        Assert.Equal(1, p.LateLines);
    }

    [Fact]
    public void Slow_queries_carry_their_summed_metrics_and_no_user()
    {
        var (rows, _) = Run(MongoLogStream.SlowOps,
            Slow("2026-10-19T09:02:00.000+00:00", nreturned: 20, duration: 266),
            Slow("2026-10-19T09:02:10.000+00:00", nreturned: 5, duration: 900),
            Noise("2026-10-19T09:02:11.000+00:00"));

        var row = Assert.Single(rows);
        Assert.Equal(2, row.GetProperty("event_count").GetInt64());
        Assert.Equal(25, row.GetProperty("nreturned").GetInt64());
        Assert.Equal(1166, row.GetProperty("duration_ms_sum").GetInt64());
        Assert.Equal(900, row.GetProperty("duration_ms_max").GetInt64());
        Assert.Equal("20627", row.GetProperty("conn").GetString());
        Assert.Equal("find", row.GetProperty("command").GetString());
        Assert.Equal("orders-api", row.GetProperty("app_name").GetString());
        Assert.Equal(64, row.GetProperty("server_shape_hash").GetString()!.Length);
        Assert.False(row.TryGetProperty("user", out _));
    }

    [Fact]
    public void A_slow_querys_command_is_found_among_reordered_keys()
    {
        var reordered = Slow("2026-10-19T09:02:00.000+00:00").Replace("""{"find":"orders","filter":{"customer":3931},"limit":20,"$db":"shop"}""", """{"$db":"shop","limit":20,"filter":{"customer":3931},"find":"orders"}""");

        var (rows, _) = Run(MongoLogStream.SlowOps, reordered);

        Assert.Equal("find", Assert.Single(rows).GetProperty("command").GetString());
    }

    [Fact]
    public void Connection_lines_are_one_row_each()
    {
        var (rows, p) = Run(MongoLogStream.Connections,
            Accepted("2026-10-19T09:00:00.500+00:00", "b7072a5a-e39d-40b4-977c-66b4a864954a"),
            SignIn("2026-10-19T09:00:00.600+00:00"),
            Starting("2026-10-19T09:00:00.100+00:00"),
            Slow("2026-10-19T09:00:01.000+00:00"));

        Assert.Equal(3, rows.Count);
        var signin = Assert.Single(rows, r => r.GetProperty("event").GetString() == "signin");
        Assert.Equal(20588, signin.GetProperty("conn_id").GetInt64());
        Assert.Equal("app_orders", signin.GetProperty("user").GetString());
        Assert.Equal("orders-api", signin.GetProperty("app_name").GetString());
        Assert.Equal("nodejs", signin.GetProperty("driver_name").GetString());
        Assert.Equal("10.1.0.5", signin.GetProperty("client_ip").GetString());
        Assert.Equal(58097, signin.GetProperty("client_port").GetInt32());
        var start = Assert.Single(rows, r => r.GetProperty("event").GetString() == "process_start");
        Assert.Equal(2161, start.GetProperty("pid").GetInt64());
        Assert.Equal(20387, Assert.Single(rows, r => r.GetProperty("event").GetString() == "accepted").GetProperty("conn_id").GetInt64());
        Assert.Equal(1, p.Counts.Filtered);
    }

    [Fact]
    public void Failed_sign_ins_are_counted_per_connection_with_the_reasons_code_and_not_its_message()
    {
        // An unknown user writes two lines for one attempt (the driver's mechanism fallback).
        var (rows, _) = Run(MongoLogStream.SigninFailures,
            Failed("2026-10-19T09:05:00.000+00:00"),
            Failed("2026-10-19T09:05:00.150+00:00"),
            Failed("2026-10-19T09:05:03.000+00:00", conn: 20532));

        Assert.Equal(2, rows.Count);
        Assert.Equal(3, rows.Sum(r => r.GetProperty("event_count").GetInt64()));
        var row = rows[0];
        Assert.Equal("UserNotFound", row.GetProperty("reason").GetString());
        Assert.Equal(11, row.GetProperty("result").GetInt32());
        Assert.Equal("198.51.100.7", row.GetProperty("client_ip").GetString());
        Assert.DoesNotContain("Could not find", row.GetRawText(), StringComparison.Ordinal);
    }

    [Fact]
    public void An_operation_whose_audit_line_cannot_be_parsed_is_still_counted()
    {
        var (rows, p) = Run(MongoLogStream.Ops, """{ "atype" : "authCheck", "ts" : { "$date" : "2026-10-19T09:00:01.000+00:00" }, "param" : { "args" : { "find" : """);

        var row = Assert.Single(rows);
        Assert.Equal(MongoLogProcessor.Unparsed, row.GetProperty("command").GetString());
        Assert.Equal(1, row.GetProperty("event_count").GetInt64());
        Assert.Equal(1, p.Counts.Kept);
        Assert.Equal(0, p.Counts.Malformed);
    }

    [Fact]
    public void A_line_without_a_readable_time_is_counted_once_as_malformed()
    {
        var (rows, p) = Run(MongoLogStream.Ops, """{ "atype" : "authCheck", "ts" : { "$date" : "yesterday" } }""");

        Assert.Empty(rows);
        Assert.Equal(1, p.Counts.Malformed);
        Assert.Equal(1, p.Counts.LinesIn);
    }

    /// <summary>MongoDB documents nest up to 100 levels: a deep query must be shipped, not lost to the parser's depth limit.</summary>
    [Theory]
    [InlineData("""{"a":""", "}")]
    [InlineData("[", "]")]
    public void A_deliberately_deep_query_is_shipped_with_a_deep_shape(string open, string close)
    {
        var filter = string.Concat(Enumerable.Repeat(open, 100)) + "1" + string.Concat(Enumerable.Repeat(close, 100));
        var line = AuthCheck("2026-10-19T09:00:01.000+00:00").Replace("""{ "customer" : 4957 }""", "{ \"customer\" : " + filter + " }");

        var (rows, p) = Run(MongoLogStream.Ops, line);

        var row = Assert.Single(rows);
        Assert.Equal("find", row.GetProperty("command").GetString());
        Assert.Equal("app_orders", row.GetProperty("user").GetString());
        Assert.Contains("?deep", row.GetProperty("shape").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, p.Counts.Kept);
    }

    [Fact]
    public void Audit_lines_that_the_server_log_already_has_are_dropped()
    {
        const string authenticate = """{ "atype" : "authenticate", "ts" : { "$date" : "2026-10-19T09:00:01.000+00:00" }, "users" : [ { "user" : "app_orders", "db" : "admin" } ], "param" : { "user" : "app_orders", "db" : "admin", "mechanism" : "SCRAM-SHA-256" }, "result" : 0 }""";

        var (rows, p) = Run(MongoLogStream.Ops, authenticate);

        Assert.Empty(rows);
        Assert.Equal(1, p.Counts.Filtered);
    }
}
