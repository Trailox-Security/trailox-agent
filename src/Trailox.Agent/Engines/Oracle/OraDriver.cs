namespace Trailox.Agent.Engines.Oracle;

/// <summary>
/// The seam between the Oracle engine and its driver. Everything above it, the SQL, the redactor, the grouping and the
/// probe's decisions, works on plain rows and values, so another driver replaces <see cref="OdpDriver"/> and nothing else.
/// </summary>
public interface IOraDriver
{
    /// <summary>A new session, signed in. Throws <see cref="OraDriverException"/>; a cancellation stays a cancellation.</summary>
    Task<IOraSession> OpenAsync(OraTarget target, CancellationToken ct);
}

/// <summary>One signed-in session, one statement at a time. Every call stops at its token; disposing never throws.</summary>
public interface IOraSession : IAsyncDisposable
{
    /// <summary>A statement with no rows: the session's own settings.</summary>
    Task ExecuteAsync(string sql, CancellationToken ct);

    /// <summary>Runs a query. Its rows are fetched as <see cref="IOraRows.ReadAsync"/> asks for them, never all at once.</summary>
    Task<IOraRows> QueryAsync(string sql, IReadOnlyList<OraBind> binds, CancellationToken ct);
}

/// <summary>The rows of one query, one at a time. Disposing never throws.</summary>
public interface IOraRows : IAsyncDisposable
{
    /// <summary>The column names, in lower case.</summary>
    IReadOnlyList<string> Columns { get; }

    /// <summary>The next row, fetched from the server when none is left here. Stops at its token.</summary>
    Task<bool> ReadAsync(CancellationToken ct);

    /// <summary>
    /// A value of the current row: null, a <see cref="string"/>, a <see cref="long"/>, a <see cref="double"/> or a
    /// <see cref="DateTime"/>. A whole number too long for a long comes as its digits, in a string.
    /// </summary>
    object? Value(int column);
}

/// <summary>A named bind: a <see cref="string"/>, a <see cref="long"/>, or a <see cref="DateTime"/> that binds as a timestamp.</summary>
public readonly record struct OraBind(string Name, object Value);

/// <summary>Where and as whom to sign in. Never printed with its password.</summary>
public sealed class OraTarget
{
    public OraTarget(string host, int port, string serviceName, bool tls, string user, string password)
    {
        Host = host;
        Port = port;
        ServiceName = serviceName;
        Tls = tls;
        User = user;
        Password = password;
    }

    public string Host { get; }
    public int Port { get; }
    public string ServiceName { get; }
    public bool Tls { get; }
    public string User { get; }
    public string Password { get; }

    /// <summary>
    /// The connect descriptor. Host and service name reach it only after <see cref="OracleEngine.Validate"/> has checked
    /// them, so neither can add a clause. With TLS the server's certificate must match the host.
    /// </summary>
    public string Descriptor =>
        $"(DESCRIPTION=(ADDRESS=(PROTOCOL={(Tls ? "tcps" : "tcp")})(HOST={Host})(PORT={Port}))"
        + $"(CONNECT_DATA=(SERVICE_NAME={ServiceName}))"
        + (Tls ? "(SECURITY=(SSL_SERVER_DN_MATCH=TRUE))" : "")
        + ")";

    public override string ToString() => $"{User}@{Host}:{Port}/{ServiceName}";
}

/// <summary>The driver's failure, as plain values: Oracle's error number, 0 when it has none, and its message.</summary>
public sealed class OraDriverException : Exception
{
    public OraDriverException(int number, string message, bool timedOut = false) : base(message)
    {
        Number = number;
        TimedOut = timedOut;
    }

    public int Number { get; }

    /// <summary>The call was stopped for taking too long.</summary>
    public bool TimedOut { get; }
}
