using System.Data;
using Npgsql;
using Trailox.Agent.Config;
using Trailox.Agent.Protocol;

namespace Trailox.Agent.Engines.Redshift;

/// <summary>
/// One Redshift endpoint over the Postgres wire protocol. A connection is opened per statement
/// and closed with the row stream: the agent runs a handful of statements per cycle and a pooled
/// idle connection would only keep a Serverless workgroup awake.
/// </summary>
public sealed class RsSource : IEngineSource
{
    private readonly EndpointConfig _endpoint;
    private readonly string _connectionString;

    public RsSource(EndpointConfig endpoint)
    {
        _endpoint = endpoint;
        _connectionString = new NpgsqlConnectionStringBuilder
        {
            Host = endpoint.Host,
            Port = endpoint.Port,
            Database = endpoint.Options[RedshiftEngine.DatabaseOption],
            Username = endpoint.Username,
            Password = endpoint.Password,
            SslMode = SslMode.Require,
            // Npgsql 10 probes GSS encryption by default and, on a distroless image without a
            // Kerberos library, fails the open before the password is ever sent.
            GssEncryptionMode = GssEncryptionMode.Disable,
            ApplicationName = "trailox-agent",
            Pooling = false,
            // A Serverless workgroup that has auto-paused answers its first connection only once
            // compute resumes, which can take longer than a 30-second connect timeout (the first
            // probe then fails with "Exception while reading from stream"). Two minutes covers
            // the resume without hiding a dead host.
            Timeout = 120,
            CommandTimeout = 1800,
        }.ConnectionString;
    }

    public async Task<EndpointCaps> ProbeAsync(CancellationToken ct)
    {
        await using var conn = await OpenAsync(ct);
        var version = await SoftScalarAsync(conn, RsRawSelects.ProbeVersion, ct) ?? "unknown";
        var sessions = _endpoint.CollectSessionLog && await ProbeConnectionLogAsync(conn, ct);
        return new EndpointCaps
        {
            Version = version.Length > 64 ? version[..64] : version,
            Columns = new List<string>(),
            HasSessionLog = sessions,
            TextReadable = true,
            EarliestEventMicros = await SoftLongAsync(conn, RsRawSelects.ProbeEarliestEvent, ct),
            EarliestSessionMicros = sessions ? await SoftLongAsync(conn, RsRawSelects.ProbeEarliestSession, ct) : 0,
        };
    }

    public async Task<RowStream?> OpenAsync(AgentTask task, CancellationToken ct)
    {
        var sql = RsRawSelects.ForTask(_endpoint, task);
        if (sql == null)
        {
            return null;
        }
        var conn = await OpenAsync(ct);
        try
        {
            var cmd = new NpgsqlCommand(sql, conn);
            var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
            var body = new ReaderNdjsonStream(reader);
            return new RowStream(body, new Owner(conn, cmd));
        }
        catch (NpgsqlException ex)
        {
            await conn.DisposeAsync();
            throw new RsException(ex);
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }

    private async Task<NpgsqlConnection> OpenAsync(CancellationToken ct)
    {
        var conn = new NpgsqlConnection(_connectionString);
        try
        {
            await conn.OpenAsync(ct);
            await using var off = new NpgsqlCommand(RsRawSelects.DisableResultCache, conn);
            await off.ExecuteNonQueryAsync(ct);
            return conn;
        }
        catch (NpgsqlException ex)
        {
            await conn.DisposeAsync();
            throw new RsException(ex);
        }
    }

    private static async Task<bool> ProbeConnectionLogAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        try
        {
            await using var cmd = new NpgsqlCommand(RsRawSelects.ProbeConnectionLog, conn);
            await cmd.ExecuteScalarAsync(ct);
            return true;
        }
        catch (NpgsqlException)
        {
            return false;
        }
    }

    private static async Task<string?> SoftScalarAsync(NpgsqlConnection conn, string sql, CancellationToken ct)
    {
        try
        {
            await using var cmd = new NpgsqlCommand(sql, conn);
            return (await cmd.ExecuteScalarAsync(ct))?.ToString();
        }
        catch (NpgsqlException)
        {
            return null;
        }
    }

    private static async Task<long> SoftLongAsync(NpgsqlConnection conn, string sql, CancellationToken ct) =>
        long.TryParse(await SoftScalarAsync(conn, sql, ct), out var n) ? n : 0;

    private sealed class Owner : IDisposable
    {
        private readonly NpgsqlConnection _conn;
        private readonly NpgsqlCommand _cmd;
        public Owner(NpgsqlConnection conn, NpgsqlCommand cmd) { _conn = conn; _cmd = cmd; }
        public void Dispose() { _cmd.Dispose(); _conn.Dispose(); }
    }
}

/// <summary>The cluster refused or failed a statement; the driver's message carries the SQLSTATE.</summary>
public sealed class RsException : SourceException
{
    public RsException(NpgsqlException inner) : base("Redshift: " + inner.Message)
    {
    }
}
