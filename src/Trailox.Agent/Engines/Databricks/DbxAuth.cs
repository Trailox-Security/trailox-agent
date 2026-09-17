using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace Trailox.Agent.Engines.Databricks;

/// <summary>
/// OAuth client-credentials at the workspace: the service principal's application id and secret
/// (HTTP Basic) for a one-hour bearer token, refreshed a few minutes before it expires. The
/// secret never leaves this process and is never logged.
/// </summary>
public sealed class DbxAuth
{
    private static readonly TimeSpan RefreshMargin = TimeSpan.FromMinutes(5);

    private readonly HttpClient _http;
    private readonly string _host;
    private readonly string _basic;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private string? _token;
    private DateTime _expiresAtUtc = DateTime.MinValue;

    public DbxAuth(HttpClient http, string host, string clientId, string secret)
    {
        _http = http;
        _host = host;
        _basic = Convert.ToBase64String(Encoding.UTF8.GetBytes(clientId + ":" + secret));
    }

    public async Task<string> TokenAsync(CancellationToken ct)
    {
        if (_token != null && DateTime.UtcNow < _expiresAtUtc - RefreshMargin)
        {
            return _token;
        }
        await _gate.WaitAsync(ct);
        try
        {
            if (_token != null && DateTime.UtcNow < _expiresAtUtc - RefreshMargin)
            {
                return _token;
            }
            using var request = new HttpRequestMessage(HttpMethod.Post, $"https://{_host}/oidc/v1/token")
            {
                Content = new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    ["grant_type"] = "client_credentials",
                    ["scope"] = "all-apis",
                }),
            };
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", _basic);
            using var response = await _http.SendAsync(request, ct);
            var body = await response.Content.ReadAsStringAsync(ct);
            if (!response.IsSuccessStatusCode)
            {
                throw new DbxException((int)response.StatusCode,
                    "the workspace refused the service principal's credentials (check username = application id, the OAuth secret, and that the principal has Databricks SQL access): " + Trim(body));
            }
            using var doc = JsonDocument.Parse(body);
            _token = doc.RootElement.GetProperty("access_token").GetString() ?? throw new DbxException(200, "token response without access_token");
            var seconds = doc.RootElement.TryGetProperty("expires_in", out var e) && e.TryGetInt32(out var n) ? n : 3600;
            _expiresAtUtc = DateTime.UtcNow.AddSeconds(seconds);
            return _token;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Never echo a token body: the failure text is enough and the success text is a credential.</summary>
    private static string Trim(string body) => body.Length > 300 ? body[..300] : body;
}

/// <summary>The workspace refused or failed a request. Engine-specific, so the runner can still treat it as a source failure.</summary>
public sealed class DbxException : SourceException
{
    public int StatusCode { get; }

    public DbxException(int statusCode, string message) : base($"Databricks answered HTTP {statusCode}: {message}")
    {
        StatusCode = statusCode;
    }
}
