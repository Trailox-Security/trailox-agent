using System.IO.Pipelines;
using Trailox.Agent.Engines.MongoDb;

namespace Trailox.Agent.Engines;

/// <summary>Rows on their way to the upload: the sink, and the pipe it writes to, flushed as it fills so a task holds little.</summary>
public sealed class RowWriter
{
    private const int FlushBytes = 256 * 1024;

    private readonly PipeWriter? _pipe;

    public RowWriter(RowSink sink, PipeWriter? pipe = null)
    {
        Sink = sink;
        _pipe = pipe;
    }

    public RowSink Sink { get; }

    public async ValueTask FlushAsync(CancellationToken ct)
    {
        if (_pipe != null && _pipe.UnflushedBytes > FlushBytes)
        {
            await _pipe.FlushAsync(ct);
        }
    }
}

/// <summary>
/// The rows of one task through a pipe the upload drains, so what the task holds does not grow with the window or the
/// catalog. Shared by the engines whose rows are made while the upload runs.
/// </summary>
public static class StreamedRows
{
    /// <remarks>
    /// 🔴 THE FIRST BYTES ARE AWAITED BEFORE THE UPLOAD. A failure before any row - "not configured", no server reachable,
    ///    a rejected password - is then thrown here, as the task's own failure, with its flags. Thrown inside the upload,
    ///    it would reach the runner wrapped by the HTTP client, and a missing optional grant would be listed among the
    ///    agent's errors.
    /// </remarks>
    public static async Task<RowStream> OpenAsync(Func<RowWriter, CancellationToken, Task> produce, CancellationToken ct)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: 8 << 20, resumeWriterThreshold: 4 << 20));
        var producer = Task.Run(async () =>
        {
            try
            {
                await produce(new RowWriter(new RowSink(pipe.Writer), pipe.Writer), cts.Token);
                await pipe.Writer.CompleteAsync();
            }
            catch (Exception ex)
            {
                await pipe.Writer.CompleteAsync(ex);
            }
        }, CancellationToken.None);
        try
        {
            // Nothing is consumed: the upload reads these bytes again.
            var first = await pipe.Reader.ReadAsync(ct);
            pipe.Reader.AdvanceTo(first.Buffer.Start);
        }
        catch
        {
            new Producer(cts, producer).Dispose();
            throw;
        }
        // Quiet by design: a log file with few sign-ins, or a catalog database with many tables, yields few rows for minutes.
        return new RowStream(pipe.Reader.AsStream(), new Producer(cts, producer)) { KeepAlive = true };
    }

    /// <summary>Stops the producer when the upload is done or abandoned.</summary>
    private sealed class Producer : IDisposable
    {
        private readonly CancellationTokenSource _cts;
        private readonly Task _task;

        public Producer(CancellationTokenSource cts, Task task)
        {
            _cts = cts;
            _task = task;
        }

        public void Dispose()
        {
            _cts.Cancel();
            _ = _task.ContinueWith(t => _ = t.Exception, TaskScheduler.Default);
            _cts.Dispose();
        }
    }
}
