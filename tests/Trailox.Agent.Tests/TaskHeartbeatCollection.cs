namespace Trailox.Agent.Tests;

/// <summary>
/// The liveness heartbeat (<see cref="Trailox.Agent.Runner.TaskHeartbeat"/>) is one per process. The test classes that attach
/// a file to it are in this collection, so they never run at the same time: a class attaching its file, or detaching it,
/// while another waits for a beat would take that beat from the other's file.
/// </summary>
[CollectionDefinition(Name)]
public sealed class TaskHeartbeatCollection
{
    public const string Name = "TaskHeartbeat";
}
