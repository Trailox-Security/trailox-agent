using System.Globalization;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace Trailox.Agent.Engines.Oracle;

/// <summary>
/// ODP.NET Core behind <see cref="IOraDriver"/>. 🔴 THE ONLY FILE THAT NAMES THE DRIVER: a test holds that, so that the
/// driver can be replaced here alone.
/// </summary>
internal sealed class OdpDriver : IOraDriver
{
    /// <summary>The most one round trip fetches. A row's size counts the longest value each column could hold, so the rows per fetch vary.</summary>
    private const long FetchBytes = 4 << 20;

    private const int ConnectTimeoutSeconds = 30;

    /// <summary>ORA-01013: what the server answers to a cancelled call, and the driver's own command timeout.</summary>
    private const int Cancelled = 1013;

    public async Task<IOraSession> OpenAsync(OraTarget target, CancellationToken ct)
    {
        OracleConnection? connection = null;
        try
        {
            connection = new OracleConnection(ConnectionString(target));
            await connection.OpenAsync(ct);
            return new Session(connection);
        }
        catch (Exception ex)
        {
            await QuietlyAsync(connection);
            throw Translate(ex) ?? ex;
        }
    }

    private static string ConnectionString(OraTarget target) => new OracleConnectionStringBuilder
    {
        DataSource = target.Descriptor,
        UserID = target.User,
        Password = target.Password,
        Pooling = false,
        ConnectionTimeout = ConnectTimeoutSeconds,
        PersistSecurityInfo = false,
        SelfTuning = false,
        StatementCacheSize = 0,
        MetadataPooling = false,
        HAEvents = false,
    }.ConnectionString;

    /// <summary>The driver's command timeout for a limit: whole seconds, never shorter than the limit above the seam.</summary>
    internal static int TimeoutSeconds(TimeSpan? limit) => limit is { } set ? (int)Math.Ceiling(set.TotalSeconds) : OraSql.CommandTimeoutSeconds;

    /// <summary>The seam's failure for a driver's; null for a cancellation, which stays one.</summary>
    private static OraDriverException? Translate(Exception ex) => ex switch
    {
        OperationCanceledException => null,
        OracleException oracle => new OraDriverException(oracle.Number, WithCause(oracle), oracle.Number == Cancelled),
        _ => new OraDriverException(0, WithCause(ex)),
    };

    /// <summary>A cancelled token breaks the call on the server too, whether or not the driver's own async call stops at it.</summary>
    /// <remarks>
    /// 🔴 NOTHING MAY ESCAPE THE CANCEL. It runs on the limit's timer thread, where an exception ends the process and no report
    ///    of the task is sent. The call is abandoned above the seam either way.
    /// </remarks>
    private static CancellationTokenRegistration CancelsOn(CancellationToken ct, OracleCommand command) =>
        ct.Register(static c =>
        {
            try
            {
                ((OracleCommand)c!).Cancel();
            }
            catch (Exception)
            {
            }
        }, command);

    /// <summary>A refused TLS connection has a generic message: the certificate's reason is in the exception inside.</summary>
    private static string WithCause(Exception ex) =>
        ex.InnerException is { Message.Length: > 0 } inner ? $"{ex.Message} ({inner.Message})" : ex.Message;

    /// <summary>Closing is no part of a read: a failure here must not replace the read's own result.</summary>
    private static async ValueTask QuietlyAsync(IAsyncDisposable? disposable)
    {
        try
        {
            if (disposable != null)
            {
                await disposable.DisposeAsync();
            }
        }
        catch (Exception ex) when (ex is OracleException or InvalidOperationException or IOException or ObjectDisposedException)
        {
        }
    }

    private sealed class Session : IOraSession
    {
        private readonly OracleConnection _connection;

        public Session(OracleConnection connection) => _connection = connection;

        public async Task ExecuteAsync(string sql, CancellationToken ct)
        {
            using var command = Command(sql, Array.Empty<OraBind>(), null);
            using var cancel = CancelsOn(ct, command);
            try
            {
                await command.ExecuteNonQueryAsync(ct);
            }
            catch (Exception ex) when (Translate(ex) is { } failure)
            {
                throw failure;
            }
        }

        public async Task<IOraRows> QueryAsync(string sql, IReadOnlyList<OraBind> binds, CancellationToken ct, TimeSpan? limit = null)
        {
            var command = Command(sql, binds, limit);
            try
            {
                using var cancel = CancelsOn(ct, command);
                return new Rows(command, await command.ExecuteReaderAsync(ct));
            }
            catch (Exception ex)
            {
                command.Dispose();
                throw Translate(ex) ?? ex;
            }
        }

        private OracleCommand Command(string sql, IReadOnlyList<OraBind> binds, TimeSpan? limit)
        {
            var command = new OracleCommand(sql, _connection)
            {
                BindByName = true,
                CommandTimeout = TimeoutSeconds(limit),
                FetchSize = FetchBytes,
                // A LONG column (a partition's HIGH_VALUE) whole, in its row: the dictionary's views have no key to fetch it by later.
                InitialLONGFetchSize = -1,
                AddToStatementCache = false,
            };
            foreach (var bind in binds)
            {
                command.Parameters.Add(Parameter(bind));
            }
            return command;
        }

        private static OracleParameter Parameter(OraBind bind) => bind.Value switch
        {
            DateTime at => new OracleParameter(bind.Name, OracleDbType.TimeStamp) { Value = at },
            long number => new OracleParameter(bind.Name, OracleDbType.Int64) { Value = number },
            string text => new OracleParameter(bind.Name, OracleDbType.Varchar2) { Value = text },
            _ => throw new ArgumentException($"bind {bind.Name}: a {bind.Value.GetType().Name} is not a value the engine binds"),
        };

        public ValueTask DisposeAsync() => QuietlyAsync(_connection);
    }

    private sealed class Rows : IOraRows
    {
        private readonly OracleCommand _command;
        private readonly OracleDataReader _reader;

        public Rows(OracleCommand command, OracleDataReader reader)
        {
            _command = command;
            _reader = reader;
            var columns = new string[reader.FieldCount];
            for (var i = 0; i < columns.Length; i++)
            {
                columns[i] = reader.GetName(i).ToLowerInvariant();
            }
            Columns = columns;
        }

        public IReadOnlyList<string> Columns { get; }

        public async Task<bool> ReadAsync(CancellationToken ct)
        {
            using var cancel = CancelsOn(ct, _command);
            try
            {
                return await _reader.ReadAsync(ct);
            }
            catch (Exception ex) when (Translate(ex) is { } failure)
            {
                throw failure;
            }
        }

        public object? Value(int column)
        {
            try
            {
                if (_reader.IsDBNull(column))
                {
                    return null;
                }
                // Through the driver's own types: a NUMBER's plain value is a decimal, and a statistic can be too long for one.
                return _reader.GetOracleValue(column) switch
                {
                    OracleDecimal number => Number(number),
                    OracleString text => text.Value,
                    OracleDate date => date.Value,
                    OracleTimeStamp at => at.Value,
                    var other => Convert.ToString(other, CultureInfo.InvariantCulture),
                };
            }
            catch (OracleException ex)
            {
                throw Translate(ex)!;
            }
        }

        private static object Number(OracleDecimal number)
        {
            if (!number.IsInt)
            {
                return number.ToDouble();
            }
            var digits = number.ToString();
            return long.TryParse(digits, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var whole) ? whole : digits;
        }

        public async ValueTask DisposeAsync()
        {
            await QuietlyAsync(_reader);
            await QuietlyAsync(_command);
        }
    }
}
