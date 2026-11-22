namespace Trailox.Agent.Config;

/// <summary>Where credentials come from: the environment or a file. The YAML only names them.</summary>
public interface ISecretReader
{
    /// <summary>A variable's value, or null. Also what a <c>${NAME}</c> in agent.yaml reads (1.5.0).</summary>
    string? FromEnvironment(string variable);

    /// <summary>The file's content, trailing newlines removed. Throws IOException when unreadable.</summary>
    string FromFile(string path);
}

public sealed class EnvironmentSecretReader : ISecretReader
{
    public string? FromEnvironment(string variable) => Environment.GetEnvironmentVariable(variable);

    public string FromFile(string path) => File.ReadAllText(path).TrimEnd('\r', '\n');
}
