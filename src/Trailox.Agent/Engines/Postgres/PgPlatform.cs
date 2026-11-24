namespace Trailox.Agent.Engines.Postgres;

/// <summary>
/// The platforms the probe reports, and what each keeps for itself: roles and databases that run the service, not the
/// customer's work. The counters, the statement texts and the catalog leave them out.
/// </summary>
public static class PgPlatform
{
    public const string Rds = "rds";
    public const string Aurora = "aurora";

    /// <summary>Azure Database for PostgreSQL flexible server (<see cref="PgSql.AzureMarker"/>).</summary>
    public const string Azure = "azure";

    public const string SelfManaged = "self_managed";

    /// <summary>
    /// The platform's own roles. On Azure, <c>azuresu</c>: the service's superuser, which signs in by certificate many
    /// times a minute and fills <c>pg_stat_statements</c> with its own statements.
    /// </summary>
    public static string[] Users(string platform) => platform == Azure ? new[] { "azuresu" } : Array.Empty<string>();

    /// <summary>
    /// The platform's own databases. <c>rdsadmin</c> (RDS's, which takes no connection) everywhere, as before; on Azure
    /// also <c>azure_sys</c> and <c>azure_maintenance</c>, where only <c>azuresu</c> works.
    /// </summary>
    public static string[] Databases(string platform) =>
        platform == Azure ? new[] { "rdsadmin", "azure_sys", "azure_maintenance" } : new[] { "rdsadmin" };
}
