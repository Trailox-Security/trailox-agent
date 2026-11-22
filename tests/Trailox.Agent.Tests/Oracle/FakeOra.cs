using Trailox.Agent.Engines.Oracle;

namespace Trailox.Agent.Tests.Oracle;

/// <summary>Plays the server behind the driver seam: each statement answers from a script, and every call is recorded.</summary>
internal sealed class FakeOra : IOraDriver
{
    private readonly Dictionary<string, Func<IReadOnlyList<OraBind>, FakeRows>> _answers = new(StringComparer.Ordinal);

    public int Opens { get; private set; }

    /// <summary>Thrown by the next opens, as a server that refuses the sign-in.</summary>
    public Exception? OpenFails { get; set; }

    /// <summary>Closing a session never finishes, as on a connection still held by a call.</summary>
    public bool CloseHangs { get; set; }

    public List<string> Statements { get; } = new();

    public List<IReadOnlyList<OraBind>> Binds { get; } = new();

    /// <summary>The limit each query came with, in the order of <see cref="Binds"/>.</summary>
    public List<TimeSpan?> Limits { get; } = new();

    public int OpenSessions { get; private set; }

    public int MostSessions { get; private set; }

    public void Answer(string sql, params object?[][] rows) => _answers[sql] = _ => new FakeRows(rows);

    public void Answer(string sql, Func<IReadOnlyList<OraBind>, FakeRows> rows) => _answers[sql] = rows;

    public void Fails(string sql, int number, string message = "refused", bool timedOut = false) =>
        _answers[sql] = _ => throw new OraDriverException(number, $"ORA-{number:D5}: {message}", timedOut);

    public Task<IOraSession> OpenAsync(OraTarget target, CancellationToken ct)
    {
        Opens++;
        if (OpenFails != null)
        {
            throw OpenFails;
        }
        OpenSessions++;
        MostSessions = Math.Max(MostSessions, OpenSessions);
        return Task.FromResult<IOraSession>(new Session(this));
    }

    private sealed class Session : IOraSession
    {
        private readonly FakeOra _fake;

        public Session(FakeOra fake) => _fake = fake;

        public Task ExecuteAsync(string sql, CancellationToken ct)
        {
            _fake.Statements.Add(sql);
            return Task.CompletedTask;
        }

        public Task<IOraRows> QueryAsync(string sql, IReadOnlyList<OraBind> binds, CancellationToken ct, TimeSpan? limit = null)
        {
            _fake.Statements.Add(sql);
            _fake.Binds.Add(binds);
            _fake.Limits.Add(limit);
            if (!_fake._answers.TryGetValue(sql, out var answer))
            {
                return Task.FromResult<IOraRows>(new FakeRows(Array.Empty<object?[]>()));
            }
            return Task.FromResult<IOraRows>(answer(binds));
        }

        public ValueTask DisposeAsync()
        {
            _fake.OpenSessions--;
            return _fake.CloseHangs ? new ValueTask(Task.Delay(Timeout.Infinite)) : ValueTask.CompletedTask;
        }
    }
}

internal sealed class FakeRows : IOraRows
{
    private readonly IEnumerator<object?[]> _rows;

    public FakeRows(IEnumerable<object?[]> rows, params string[] columns)
    {
        _rows = rows.GetEnumerator();
        Columns = columns;
    }

    public IReadOnlyList<string> Columns { get; private set; }

    /// <summary>Thrown when the rows run out, as a read stopped part of the way.</summary>
    public Exception? FailsAtEnd { get; init; }

    /// <summary>When the rows run out the fetch hangs: until its token, or for good when <see cref="DeafToToken"/>.</summary>
    public bool HangsAtEnd { get; init; }

    public bool DeafToToken { get; init; }

    /// <summary>Closing these rows never finishes, as on a connection still held by a call.</summary>
    public bool DisposeHangs { get; init; }

    /// <summary>When the rows run out the fetch takes this long, and then the read ends.</summary>
    public TimeSpan EndsAfter { get; init; }

    /// <summary>Rows read so far.</summary>
    public int Read { get; private set; }

    public Task<bool> ReadAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (!_rows.MoveNext())
        {
            if (EndsAfter > TimeSpan.Zero)
            {
                return Task.Delay(EndsAfter, ct).ContinueWith(_ => false, ct, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Default);
            }
            if (HangsAtEnd)
            {
                var heard = DeafToToken ? CancellationToken.None : ct;
                return Task.Delay(Timeout.Infinite, heard).ContinueWith(_ => false, heard, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Default);
            }
            return FailsAtEnd != null ? throw FailsAtEnd : Task.FromResult(false);
        }
        Read++;
        if (Columns.Count == 0)
        {
            Columns = Enumerable.Range(0, _rows.Current.Length).Select(i => $"c{i}").ToArray();
        }
        return Task.FromResult(true);
    }

    public object? Value(int column) => _rows.Current[column];

    public ValueTask DisposeAsync()
    {
        _rows.Dispose();
        return DisposeHangs ? new ValueTask(Task.Delay(Timeout.Infinite)) : ValueTask.CompletedTask;
    }
}
