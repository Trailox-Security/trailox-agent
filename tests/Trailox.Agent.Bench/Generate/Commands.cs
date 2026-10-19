using System.Buffers;
using System.Globalization;
using System.Text;

namespace Trailox.Agent.Bench.Generate;

/// <summary>
/// The driver's envelope around a command: the session (one per connection), the cluster time and its
/// signature (the same for every operation in a second), the key id (fixed). Reused values are what
/// make the real files compress 15x; a fresh random value per operation would make them 7x.
/// </summary>
internal readonly record struct Envelope(Guid Lsid, long ClusterSecond, int Increment, string Signature, long KeyId, long TxnNumber);

/// <summary>
/// Builds the command documents an application sends, with literal values in them, as they appear in
/// a slow-query line's "command" (compact, relaxed extended JSON) and in an audit line's "param.args"
/// (spaced, legacy extended JSON: {"$binary": ..., "$type": ...}, {"$numberLong": ...}), both measured.
/// Every literal string starts with <see cref="LiteralMarker"/>, so a run can prove that no literal
/// reached the shipped rows.
/// </summary>
internal static class Commands
{
    public const string LiteralMarker = "lit-";

    // Read/write mix of the load generator used on the test cluster: 80% reads, 20% writes.
    private static readonly (string Name, int Weight)[] Mix =
    [
        ("find", 35), ("find2", 15), ("aggregate", 10), ("getMore", 5), ("count", 5), ("distinct", 5),
        ("insert", 10), ("update", 8), ("findAndModify", 5), ("delete", 2),
    ];

    private static readonly int TotalWeight = Mix.Sum(m => m.Weight);

    public static readonly string[] FieldPool = ["customer", "status", "region", "sku", "createdAt", "amount", "email", "note", "tier", "score"];

    public static string Pick(Random rng)
    {
        var at = rng.Next(TotalWeight);
        foreach (var (name, weight) in Mix)
        {
            if (at < weight)
            {
                return name;
            }
            at -= weight;
        }
        return Mix[0].Name;
    }

    public static string WireName(string template) => template == "find2" ? "find" : template;

    /// <summary>Writes the command document in the audit form (<paramref name="audit"/>) or the server-log form.</summary>
    public static void Write(ArrayBufferWriter<byte> w, bool audit, string template, string collection, string db, string f1, string f2, in Envelope env, Random rng)
    {
        w.Clear();
        var s = new StringBuilder(512);
        var write = template is "insert" or "update" or "findAndModify" or "delete";
        switch (template)
        {
            case "find":
                s.Append($"{{\"find\":\"{collection}\",\"filter\":{{\"{f1}\":{rng.Next(100000)}}},\"limit\":20,");
                break;
            case "find2":
                s.Append($"{{\"find\":\"{collection}\",\"filter\":{{\"{f1}\":\"{Lit(rng)}\",\"{f2}\":{{\"$gte\":{rng.Next(1000)}}}}},\"sort\":{{\"{f2}\":-1}},\"limit\":50,");
                break;
            case "aggregate":
                s.Append($"{{\"aggregate\":\"{collection}\",\"pipeline\":[{{\"$match\":{{\"{f1}\":{{\"$regex\":\"{Lit(rng)}\"}}}}}},{{\"$group\":{{\"_id\":\"${f2}\",\"n\":{{\"$sum\":1}}}}}}],\"cursor\":{{}},");
                break;
            case "getMore":
                s.Append($"{{\"getMore\":{Long(audit, rng.NextInt64(1L << 40, 1L << 62))},\"collection\":\"{collection}\",\"batchSize\":101,");
                break;
            case "count":
                s.Append($"{{\"count\":\"{collection}\",\"query\":{{\"{f1}\":{{\"$in\":[\"{Lit(rng)}\",\"{Lit(rng)}\",\"{Lit(rng)}\"]}}}},");
                break;
            case "distinct":
                s.Append($"{{\"distinct\":\"{collection}\",\"key\":\"{f1}\",\"query\":{{\"{f2}\":{{\"$lt\":{(rng.NextDouble() * 1000).ToString("F2", CultureInfo.InvariantCulture)}}}}},");
                break;
            case "insert":
                s.Append($"{{\"insert\":\"{collection}\",\"ordered\":true,");
                break;
            case "update":
                s.Append($"{{\"update\":\"{collection}\",\"ordered\":true,");
                break;
            case "findAndModify":
                s.Append($"{{\"findAndModify\":\"{collection}\",\"query\":{{\"_id\":{rng.Next(1000000)}}},\"new\":true,\"update\":{{\"$inc\":{{\"{f2}\":1}}}},\"upsert\":false,");
                break;
            case "delete":
                s.Append($"{{\"delete\":\"{collection}\",\"ordered\":true,");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(template), template, null);
        }

        AppendEnvelope(s, audit, write, db, env);

        // Write statements come after the envelope, as in the measured audit lines. The slow-query line
        // of an insert carries no "documents" at all (measured), so its shape differs from the audit one.
        switch (template)
        {
            case "insert" when audit:
                s.Append($",\"documents\":[{{\"_id\":{Long(true, rng.NextInt64(1L << 50, 1L << 51))},\"{f1}\":\"{Lit(rng)}\",\"{f2}\":{rng.Next(100000)},\"status\":\"new\",\"amount\":1,\"created\":{{\"$date\":\"2026-10-19T11:37:28.062+00:00\"}},\"note\":\"{Note(rng)}\"}}]");
                break;
            case "update":
                s.Append($",\"updates\":[{{\"q\":{{\"_id\":{rng.Next(1000000)}}},\"u\":{{\"$set\":{{\"{f1}\":\"{Lit(rng)}\"}}}},\"upsert\":false}}]");
                break;
            case "delete":
                s.Append($",\"deletes\":[{{\"q\":{{\"{f1}\":\"{Lit(rng)}\"}},\"limit\":1}}]");
                break;
        }
        s.Append('}');

        var compact = Encoding.UTF8.GetBytes(s.ToString());
        if (audit)
        {
            Spaced(compact, w);
        }
        else
        {
            w.Write(compact);
        }
    }

    private static void AppendEnvelope(StringBuilder s, bool audit, bool write, string db, in Envelope env)
    {
        if (audit)
        {
            s.Append($"\"lsid\":{{\"id\":{{\"$binary\":\"{Base64Of(env.Lsid)}\",\"$type\":\"04\"}}}},");
            if (write)
            {
                s.Append($"\"txnNumber\":{{\"$numberLong\":\"{env.TxnNumber}\"}},");
            }
            s.Append($"\"$clusterTime\":{{\"clusterTime\":{{\"$timestamp\":{{\"t\":{env.ClusterSecond},\"i\":{env.Increment}}}}},\"signature\":{{\"hash\":{{\"$binary\":\"{env.Signature}\",\"$type\":\"00\"}},\"keyId\":{{\"$numberLong\":\"{env.KeyId}\"}}}}}},");
        }
        else
        {
            s.Append($"\"lsid\":{{\"id\":{{\"$uuid\":\"{env.Lsid}\"}}}},");
            if (write)
            {
                s.Append($"\"txnNumber\":{env.TxnNumber},");
            }
            s.Append($"\"$clusterTime\":{{\"clusterTime\":{{\"$timestamp\":{{\"t\":{env.ClusterSecond},\"i\":{env.Increment}}}}},\"signature\":{{\"hash\":{{\"$binary\":{{\"base64\":\"{env.Signature}\",\"subType\":\"0\"}}}},\"keyId\":{env.KeyId}}}}},");
        }
        if (write)
        {
            s.Append("\"writeConcern\":{\"w\":\"majority\"},");
        }
        s.Append($"\"$db\":\"{db}\"");
    }

    /// <summary>The audit log's spacing: "{ "a" : 1, "b" : [ 1, 2 ] }", with "{}" and "[]" left as they are.</summary>
    private static void Spaced(ReadOnlySpan<byte> json, ArrayBufferWriter<byte> w)
    {
        var inString = false;
        for (var i = 0; i < json.Length; i++)
        {
            var c = json[i];
            if (inString)
            {
                w.Write([c]);
                if (c == (byte)'\\' && i + 1 < json.Length)
                {
                    w.Write([json[++i]]);
                }
                else if (c == (byte)'"')
                {
                    inString = false;
                }
                continue;
            }
            switch (c)
            {
                case (byte)'"':
                    inString = true;
                    w.Write([c]);
                    break;
                case (byte)'{' or (byte)'[':
                    w.Write([c]);
                    if (i + 1 < json.Length && json[i + 1] != (c == (byte)'{' ? (byte)'}' : (byte)']'))
                    {
                        w.Write(" "u8);
                    }
                    break;
                case (byte)'}' or (byte)']':
                    if (json[i - 1] != (c == (byte)'}' ? (byte)'{' : (byte)'['))
                    {
                        w.Write(" "u8);
                    }
                    w.Write([c]);
                    break;
                case (byte)':':
                    w.Write(" : "u8);
                    break;
                case (byte)',':
                    w.Write(", "u8);
                    break;
                default:
                    w.Write([c]);
                    break;
            }
        }
    }

    private static string Long(bool audit, long value) =>
        audit ? $"{{\"$numberLong\":\"{value}\"}}" : value.ToString(CultureInfo.InvariantCulture);

    public static string Lit(Random rng) => LiteralMarker + rng.Next(1_000_000, 9_999_999).ToString(CultureInfo.InvariantCulture);

    /// <summary>A 200-character free-text field, like the load generator's "note" (random words).</summary>
    private static string Note(Random rng)
    {
        Span<char> note = stackalloc char[200];
        for (var i = 0; i < note.Length; i++)
        {
            note[i] = rng.Next(8) == 0 ? ' ' : (char)('a' + rng.Next(26));
        }
        return LiteralMarker + new string(note);
    }

    public static string Hex(Random rng, int bytes)
    {
        Span<byte> b = stackalloc byte[bytes];
        rng.NextBytes(b);
        return Convert.ToHexStringLower(b);
    }

    public static string Base64(Random rng)
    {
        Span<byte> b = stackalloc byte[20];
        rng.NextBytes(b);
        return Convert.ToBase64String(b);
    }

    public static string Base64Of(Guid id) => Convert.ToBase64String(id.ToByteArray());
}
