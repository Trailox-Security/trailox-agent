using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Trailox.Agent.Engines.MongoDb;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Engines.Oracle;

/// <summary>
/// The <c>ora_audit</c> rows of one window, as the poll's rows arrive. Each record of a grouped action joins its minute's
/// group (<see cref="OraAuditGroups"/>); every other record is a row of its own. Every text ships as its shape, passwords
/// removed first, so no value leaves. One count row closes the read, stamped with the window's start.
/// </summary>
/// <remarks>
/// 🔴 ROWS ARE STREAMED, NEVER COLLECTED: a window can hold a bulk load's records. What this holds does not grow with the
/// window, and the upload's pipe pauses the fetch when the upload falls behind.
/// </remarks>
public sealed class OraPoll
{
    /// <summary>The action of the count row.</summary>
    public const string SummaryAction = "summary";

    private const int HeartbeatRows = 5000;
    private const int FlushRows = 256;

    private static readonly JsonSerializerOptions CountsJson = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    private readonly long _start;
    private readonly bool _shipText;
    private readonly bool _sourceColumn;
    private readonly RowWriter _output;
    private readonly RowSink _sink;
    private readonly OraAuditGroups _groups;
    private readonly OraRecord _record = new();
    private readonly SortedDictionary<string, long> _byAction = new(StringComparer.Ordinal);
    private readonly StringBuilder _text = new();
    private readonly Action _beat;

    private long _records;
    private long _shipped;
    private long _groupRows;
    private long _spilled;

    /// <param name="startMicros">The window's start: the count row's time.</param>
    /// <param name="shipText">The source's <c>storeRawQueryText</c>.</param>
    /// <param name="sourceColumn">The read has the <c>SOURCE</c> column, last.</param>
    /// <param name="beat">Called every few thousand rows; the liveness heartbeat unless a test says.</param>
    public OraPoll(long startMicros, bool shipText, bool sourceColumn, RowWriter output, Action? beat = null)
    {
        _beat = beat ?? TaskHeartbeat.Beat;
        _start = startMicros;
        _shipText = shipText;
        _sourceColumn = sourceColumn;
        _output = output;
        _sink = output.Sink;
        _groups = new OraAuditGroups(WriteGroup);
    }

    public async Task ReadAsync(IOraRows rows, CancellationToken ct)
    {
        while (await rows.ReadAsync(ct))
        {
            Add(rows);
            if (_records % FlushRows == 0)
            {
                await _output.FlushAsync(ct);
            }
            if (_records % HeartbeatRows == 0)
            {
                _beat();
            }
        }
        _groups.Finish();
        WriteSummary();
        await _output.FlushAsync(ct);
    }

    private void Add(IOraRows rows)
    {
        var r = _record;
        r.Micros = ((DateTime)rows.Value(OraSql.ColumnTime)! - DateTime.UnixEpoch).Ticks / 10;
        r.Instance = Number(rows, OraSql.ColumnInstance);
        r.User = Text(rows, OraSql.ColumnUser);
        r.Proxy = Text(rows, OraSql.ColumnProxy);
        r.Action = Text(rows, OraSql.ColumnAction);
        // No code is no proof of success: only the agent's own filter and a real 0 read as success.
        r.ReturnCode = rows.Value(OraSql.ColumnReturnCode) == null ? -1 : Number(rows, OraSql.ColumnReturnCode);
        r.Schema = Text(rows, OraSql.ColumnSchema);
        r.Object = Text(rows, OraSql.ColumnObject);
        r.Program = Text(rows, OraSql.ColumnProgram);
        r.OsUser = Text(rows, OraSql.ColumnOsUser);
        r.UserHost = Text(rows, OraSql.ColumnUserHost);
        r.Terminal = Text(rows, OraSql.ColumnTerminal);
        r.Authentication = Text(rows, OraSql.ColumnAuthentication);
        r.Policies = Text(rows, OraSql.ColumnPolicies);
        r.Privilege = Text(rows, OraSql.ColumnPrivilege);
        r.AuditType = Text(rows, OraSql.ColumnAuditType);
        r.SessionId = Number(rows, OraSql.ColumnSessionId);
        r.EntryId = Number(rows, OraSql.ColumnEntryId);
        r.StatementId = Number(rows, OraSql.ColumnStatementId);
        r.TextLength = Number(rows, OraSql.ColumnTextLength);
        r.DataPump = OraPasswordRedactor.Redact(Text(rows, OraSql.ColumnDataPump));
        r.Text = _shipText ? OraStatementShape.Of(OraPasswordRedactor.Redact(StatementText(rows))) : "";
        r.Truncated = r.TextLength > OraSql.MaxTextChars;

        _records++;
        _byAction[r.Action] = _byAction.GetValueOrDefault(r.Action) + 1;
        if (_sourceColumn && rows.Value(OraSql.ColumnSource) is "FILE")
        {
            _spilled++;
        }
        if (OraAuditGroups.IsGrouped(r.Action))
        {
            _groups.Add(r, r.Text);
            return;
        }
        WriteRow(r);
        _shipped++;
    }

    /// <summary>The text's pieces, joined. The trail stores a text with a NUL after it, which is no part of the statement.</summary>
    private string StatementText(IOraRows rows)
    {
        _text.Clear();
        for (var i = 0; i < OraSql.TextPieces && rows.Value(OraSql.ColumnText + i) is string piece; i++)
        {
            _text.Append(piece);
        }
        while (_text.Length > 0 && _text[^1] == '\0')
        {
            _text.Length--;
        }
        return _text.ToString();
    }

    private static string Text(IOraRows rows, int column) => rows.Value(column) as string ?? "";

    private static long Number(IOraRows rows, int column) => rows.Value(column) switch
    {
        long n => n,
        double d => (long)d,
        string s when long.TryParse(s, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var n) => n,
        _ => 0,
    };

    /// <summary>One row, every column sent. A record is one event of its own, with no hash.</summary>
    private void WriteRow(OraRecord r, long eventCount = 1, string hash = "", string summary = "")
    {
        var w = _sink.Writer;
        w.WriteStartObject();
        RowSink.Time(w, "event_time"u8, r.Micros);
        w.WriteNumber("instance_id"u8, r.Instance);
        w.WriteString("dbusername"u8, r.User);
        w.WriteString("dbproxy_username"u8, r.Proxy);
        w.WriteString("action_name"u8, r.Action);
        w.WriteNumber("return_code"u8, r.ReturnCode);
        w.WriteString("object_schema"u8, r.Schema);
        w.WriteString("object_name"u8, r.Object);
        w.WriteString("client_program_name"u8, r.Program);
        w.WriteString("os_username"u8, r.OsUser);
        w.WriteString("userhost"u8, r.UserHost);
        w.WriteString("terminal"u8, r.Terminal);
        w.WriteString("authentication_type"u8, r.Authentication);
        w.WriteString("unified_audit_policies"u8, r.Policies);
        w.WriteString("system_privilege_used"u8, r.Privilege);
        w.WriteString("audit_type"u8, r.AuditType);
        w.WriteNumber("sessionid"u8, r.SessionId);
        w.WriteNumber("entry_id"u8, r.EntryId);
        w.WriteNumber("statement_id"u8, r.StatementId);
        w.WriteString("sql_text"u8, r.Text);
        w.WriteNumber("sql_text_length"u8, r.TextLength);
        w.WriteNumber("truncated"u8, r.Truncated ? 1 : 0);
        w.WriteString("dp_text_parameters1"u8, r.DataPump);
        w.WriteString("summary"u8, summary);
        w.WriteNumber("event_count"u8, eventCount);
        w.WriteString("shape_hash"u8, hash);
        w.WriteEndObject();
        _sink.EndRow();
    }

    /// <summary>One row per minute group: the first record's session, the shape as its text, and how many records.</summary>
    private void WriteGroup(MinuteGroup g)
    {
        var l = g.Labels;
        long Whole(int label) => l[label].Length == 0 ? 0 : long.Parse(l[label], CultureInfo.InvariantCulture);
        WriteRow(new OraRecord
        {
            Micros = g.FirstMicros,
            Instance = Whole(OraAuditGroups.Instance),
            User = l[OraAuditGroups.User],
            Proxy = l[OraAuditGroups.Proxy],
            Action = l[OraAuditGroups.Action],
            ReturnCode = Whole(OraAuditGroups.ReturnCode),
            Schema = l[OraAuditGroups.Schema],
            Object = l[OraAuditGroups.Object],
            Program = l[OraAuditGroups.Program],
            OsUser = l[OraAuditGroups.OsUser],
            UserHost = l[OraAuditGroups.UserHost],
            Terminal = l[OraAuditGroups.Terminal],
            Authentication = l[OraAuditGroups.Authentication],
            Policies = l[OraAuditGroups.Policies],
            Privilege = l[OraAuditGroups.Privilege],
            AuditType = l[OraAuditGroups.AuditType],
            SessionId = Whole(OraAuditGroups.SessionId),
            EntryId = Whole(OraAuditGroups.EntryId),
            StatementId = Whole(OraAuditGroups.StatementId),
            Text = _shipText ? l[OraAuditGroups.Shape] : "",
            TextLength = Whole(OraAuditGroups.TextLength),
            Truncated = l[OraAuditGroups.Truncated].Length > 0,
        }, g.Count, l[OraAuditGroups.Hash]);
        _groupRows++;
    }

    /// <summary>The count row: what was read, by action, and what the groups did with it.</summary>
    private void WriteSummary() => WriteRow(new OraRecord { Micros = _start, Action = SummaryAction }, summary: JsonSerializer.Serialize(new
    {
        records = _records,
        shipped = _shipped,
        by_action = _byAction,
        file_records = _sourceColumn ? _spilled : (long?)null,
        audit_records = _groups.Records,
        audit_rows = _groupRows,
        audit_shapeless = _groups.ShapelessRecords,
        audit_late = _groups.LateRecords,
    }, CountsJson));
}
