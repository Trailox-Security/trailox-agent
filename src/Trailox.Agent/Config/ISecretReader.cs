namespace Trailox.Agent.Config;

/// <summary>Where credentials come from: the environment or a file. The YAML only names them.</summary>
public interface ISecretReader
{
    string? FromEnvironment(string variable);

    /// <summary>The file's content, trailing newlines removed. Throws IOException when unreadable.</summary>
    string FromFile(string path);
}

public sealed class EnvironmentSecretReader : ISecretReader
{
    public string? FromEnvironment(string variable) => Environment.GetEnvironmentVariable(variable);

    public string FromFile(string path) => File.ReadAllText(path).TrimEnd('\r', '\n');
}
