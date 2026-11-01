namespace Trailox.Agent.Engines.Oracle;

/// <summary>
/// Holds every call on a session to one limit, a fetch as much as an execute, for any driver.
/// </summary>
/// <remarks>
/// 🔴 A DRIVER'S COMMAND TIMEOUT MAY COVER THE EXECUTE ALONE, and a scan can spend its time in a later fetch. Without this a
///    read would be stopped by nothing but the profile, whose limits the server enforces only with <c>RESOURCE_LIMIT</c> on,
///    and one long fetch gives the liveness probe no sign of life. Each call has its own limit, so a long window of quick
///    fetches runs, and the time the upload holds the fetch back does not count.
/// </remarks>
internal sealed class OraBoundedSession : IOraSession
{
    private readonly IOraSession _session;
    private readonly TimeSpan _limit;

    public OraBoundedSession(IOraSession session, TimeSpan limit)
    {
        _session = session;
        _limit = limit;
    }

    public Task ExecuteAsync(string sql, CancellationToken ct) => CallAsync(async token =>
    {
        await _session.ExecuteAsync(sql, token);
        return true;
    }, _limit, ct);

    public async Task<IOraRows> QueryAsync(string sql, IReadOnlyList<OraBind> binds, CancellationToken ct) =>
        new Rows(await CallAsync(token => _session.QueryAsync(sql, binds, token), _limit, ct), _limit);

    public ValueTask DisposeAsync() => _session.DisposeAsync();

    /// <summary>
    /// One call, abandoned at the limit even if the driver does not stop at its token: disposing the session then ends it.
    /// </summary>
    private static async Task<T> CallAsync<T>(Func<CancellationToken, Task<T>> call, TimeSpan limit, CancellationToken ct)
    {
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(limit);
        try
        {
            return await call(bounded.Token).WaitAsync(bounded.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new OraDriverException(0, $"the call took more than {limit.TotalSeconds:0} seconds and was stopped", timedOut: true);
        }
    }

    private sealed class Rows : IOraRows
    {
        private readonly IOraRows _rows;
        private readonly TimeSpan _limit;

        public Rows(IOraRows rows, TimeSpan limit)
        {
            _rows = rows;
            _limit = limit;
        }

        public IReadOnlyList<string> Columns => _rows.Columns;

        public Task<bool> ReadAsync(CancellationToken ct) => CallAsync(_rows.ReadAsync, _limit, ct);

        public object? Value(int column) => _rows.Value(column);

        public ValueTask DisposeAsync() => _rows.DisposeAsync();
    }
}
