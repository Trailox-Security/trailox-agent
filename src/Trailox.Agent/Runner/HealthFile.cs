namespace Trailox.Agent.Runner;

/// <summary>
/// Liveness for a distroless image: the loop touches a file after each successful step and the
/// <c>healthcheck</c> subcommand checks its age. No shell, no curl, no port.
/// </summary>
public sealed class HealthFile
{
    public static readonly TimeSpan MaxAge = TimeSpan.FromMinutes(5);
    public string Path { get; }

    public HealthFile(string path)
    {
        Path = path;
    }

    public void Touch()
    {
        try
        {
            File.WriteAllText(Path, DateTime.UtcNow.ToString("O"));
        }
        catch (Exception)
        {
            // A read-only /tmp would be a deployment mistake, not a reason to stop collecting.
        }
    }

    public bool IsHealthy()
    {
        try
        {
            if (!File.Exists(Path))
            {
                return false;
            }
            return DateTime.TryParse(File.ReadAllText(Path), null, System.Globalization.DateTimeStyles.RoundtripKind, out var at)
                   && DateTime.UtcNow - at.ToUniversalTime() < MaxAge;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
