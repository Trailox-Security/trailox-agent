using System.IO.Pipelines;
using System.Text;
using Trailox.Agent.Engines;
using Trailox.Agent.Runner;

namespace Trailox.Agent.Tests;

/// <summary>One read's rows as several bodies: where a body ends, and what is left for the next one.</summary>
public class RowPartsTests
{
    private static readonly TimeSpan Long = TimeSpan.FromMinutes(10);

    private static string Row(int n) => $"{{\"n\":{n:D4}}}\n";

    private static Stream Rows(int count) => new MemoryStream(Encoding.UTF8.GetBytes(string.Concat(Enumerable.Range(0, count).Select(Row))));

    /// <summary>Reads the way an upload does, with a small buffer so that a body's size does not hang on the reader's.</summary>
    private static async Task<string> BodyAsync(RowParts parts, int bufferBytes = 7)
    {
        var body = parts.Next();
        var text = new StringBuilder();
        var buffer = new byte[bufferBytes];
        int read;
        while ((read = await body.ReadAsync(buffer)) > 0)
        {
            text.Append(Encoding.UTF8.GetString(buffer, 0, read));
        }
        return text.ToString();
    }

    private static async Task<List<string>> BodiesAsync(RowParts parts)
    {
        var bodies = new List<string>();
        while (await parts.MoreAsync())
        {
            bodies.Add(await BodyAsync(parts));
        }
        return bodies;
    }

    [Fact]
    public async Task Bodies_hold_whole_rows_under_the_size_and_every_row_goes_once_in_order()
    {
        var rowBytes = Row(0).Length;

        var bodies = await BodiesAsync(new RowParts(Rows(10), (rowBytes * 3) + 1, Long, CancellationToken.None));

        Assert.Equal(new[] { 3, 3, 3, 1 }, bodies.Select(b => b.Length / rowBytes));
        Assert.All(bodies, b => Assert.EndsWith("\n", b));
        Assert.Equal(string.Concat(Enumerable.Range(0, 10).Select(Row)), string.Concat(bodies));
    }

    [Fact]
    public async Task A_body_exactly_the_size_is_full_and_the_next_rows_go_on()
    {
        var rowBytes = Row(0).Length;

        var bodies = await BodiesAsync(new RowParts(Rows(4), rowBytes * 2, Long, CancellationToken.None));

        Assert.Equal(new[] { rowBytes * 2, rowBytes * 2 }, bodies.Select(b => b.Length));
    }

    /// <summary>It could go nowhere else: a body always carries a row, so the rows always move.</summary>
    [Fact]
    public async Task A_row_longer_than_the_size_goes_alone()
    {
        var bodies = await BodiesAsync(new RowParts(Rows(3), 1, Long, CancellationToken.None));

        Assert.Equal(new[] { Row(0), Row(1), Row(2) }, bodies);
    }

    [Fact]
    public async Task No_rows_make_no_body()
    {
        Assert.Empty(await BodiesAsync(new RowParts(new MemoryStream(), 100, Long, CancellationToken.None)));
    }

    [Fact]
    public async Task A_stream_with_no_last_newline_still_ships_its_last_row()
    {
        var bytes = Encoding.UTF8.GetBytes(Row(0) + "{\"n\":1}");

        var bodies = await BodiesAsync(new RowParts(new MemoryStream(bytes), 1000, Long, CancellationToken.None));

        Assert.Equal(Row(0) + "{\"n\":1}", string.Concat(bodies));
    }

    /// <summary>The pipe may hand over a row cut in two: the rest must arrive before the row goes out.</summary>
    [Fact]
    public async Task A_row_that_arrives_in_two_reads_goes_whole()
    {
        var pipe = new Pipe();
        var parts = new RowParts(pipe.Reader.AsStream(), 1000, Long, CancellationToken.None);

        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes("{\"n\":"));
        var more = parts.MoreAsync();
        await Task.Delay(50);
        Assert.False(more.IsCompleted);
        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes("1}\n"));
        await pipe.Writer.CompleteAsync();

        Assert.True(await more);
        Assert.Equal("{\"n\":1}\n", await BodyAsync(parts));
        Assert.False(await parts.MoreAsync());
    }

    /// <summary>A fetch can send nothing for minutes, and the other side ends an upload held open too long, with every row in it.</summary>
    [Fact]
    public async Task A_body_ends_at_its_time_while_the_rows_are_quiet_and_the_next_rows_go_in_the_next_body()
    {
        var pipe = new Pipe();
        var parts = new RowParts(pipe.Reader.AsStream(), 1000, TimeSpan.FromMilliseconds(100), CancellationToken.None);
        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes(Row(0)));

        Assert.True(await parts.MoreAsync());
        Assert.Equal(Row(0), await BodyAsync(parts).WaitAsync(TimeSpan.FromSeconds(10)));

        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes(Row(1)));
        await pipe.Writer.CompleteAsync();
        Assert.True(await parts.MoreAsync());
        Assert.Equal(Row(1), await BodyAsync(parts));
        Assert.False(await parts.MoreAsync());
    }

    /// <summary>A request's token ends with its request, and the read it began goes on for the next body.</summary>
    [Fact]
    public async Task A_read_begun_inside_a_body_outlives_that_bodys_request()
    {
        var pipe = new Pipe();
        var parts = new RowParts(pipe.Reader.AsStream(), 1000, TimeSpan.FromMilliseconds(50), CancellationToken.None);
        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes(Row(0)));
        Assert.True(await parts.MoreAsync());
        using var request = new CancellationTokenSource();
        var body = parts.Next();
        var buffer = new byte[64];
        Assert.Equal(Row(0).Length, await body.ReadAsync(buffer, request.Token));
        Assert.Equal(0, await body.ReadAsync(buffer, request.Token));

        request.Cancel();
        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes(Row(1)));
        await pipe.Writer.CompleteAsync();

        Assert.True(await parts.MoreAsync());
        Assert.Equal(Row(1), await BodyAsync(parts));
    }

    /// <summary>A body that opened on a row always carries it, however little time it was given.</summary>
    [Fact]
    public async Task A_body_past_its_time_still_carries_its_first_row()
    {
        var bodies = await BodiesAsync(new RowParts(Rows(2), 1000, TimeSpan.Zero, CancellationToken.None));

        Assert.All(bodies, b => Assert.NotEmpty(b));
        Assert.Equal(Row(0) + Row(1), string.Concat(bodies));
    }

    /// <summary>A full body has nothing to wait for, whatever the read is doing: its upload ends at once.</summary>
    [Fact]
    public async Task A_full_body_ends_without_waiting_for_more_rows()
    {
        var pipe = new Pipe();
        var parts = new RowParts(pipe.Reader.AsStream(), Row(0).Length, Long, CancellationToken.None);
        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes(Row(0)));
        Assert.True(await parts.MoreAsync());

        Assert.Equal(Row(0), await BodyAsync(parts).WaitAsync(TimeSpan.FromSeconds(10)));
    }

    /// <summary>No upload may stand open through a quiet read, so the next body waits for its first row.</summary>
    [Fact]
    public async Task The_next_body_waits_for_rows()
    {
        var pipe = new Pipe();
        var parts = new RowParts(pipe.Reader.AsStream(), 1, Long, CancellationToken.None);
        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes(Row(0)));
        Assert.True(await parts.MoreAsync());
        await BodyAsync(parts);

        var more = parts.MoreAsync();
        await Task.Delay(50);
        Assert.False(more.IsCompleted);

        await pipe.Writer.CompleteAsync();
        Assert.False(await more);
    }

    /// <summary>The source's failure must come out as it went in, for its reason to reach Trailox.</summary>
    [Fact]
    public async Task A_read_that_fails_between_bodies_throws_its_own_failure()
    {
        var pipe = new Pipe();
        var parts = new RowParts(pipe.Reader.AsStream(), 1, Long, CancellationToken.None);
        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes(Row(0)));
        Assert.True(await parts.MoreAsync());
        await BodyAsync(parts);

        await pipe.Writer.CompleteAsync(new SourceException("stopped") { TooLargeAtSource = true });

        var failure = await Assert.ThrowsAsync<SourceException>(parts.MoreAsync);
        Assert.True(failure.TooLargeAtSource);
    }

    [Fact]
    public async Task A_read_that_fails_inside_a_body_throws_from_that_body()
    {
        var pipe = new Pipe();
        var parts = new RowParts(pipe.Reader.AsStream(), 1000, Long, CancellationToken.None);
        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes(Row(0)));
        Assert.True(await parts.MoreAsync());
        await pipe.Writer.CompleteAsync(new SourceException("stopped"));

        await Assert.ThrowsAsync<SourceException>(() => BodyAsync(parts));
    }

    [Fact]
    public async Task A_row_too_long_to_hold_is_a_failure_and_no_endless_wait()
    {
        var rows = new MemoryStream(new byte[RowParts.MaxRowBytes * 2]);

        await Assert.ThrowsAsync<InvalidDataException>(new RowParts(rows, 1000, Long, CancellationToken.None).MoreAsync);
    }
}
