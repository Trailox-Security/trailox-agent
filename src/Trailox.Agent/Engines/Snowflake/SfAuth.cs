using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Trailox.Agent.Engines.Snowflake;

/// <summary>
/// Key-pair JWT for the SQL API. The token is signed here with the customer's private key and
/// lives for at most an hour (Snowflake's ceiling); a fresh one is minted a few minutes before
/// that. The key never leaves this process and neither it nor a token is ever logged.
/// </summary>
public sealed class SfAuth : IDisposable
{
    private static readonly TimeSpan Lifetime = TimeSpan.FromMinutes(59);
    private static readonly TimeSpan RefreshMargin = TimeSpan.FromMinutes(5);

    private readonly RSA _key;
    private readonly string _issuer;
    private readonly string _subject;
    private readonly object _gate = new();
    private string? _token;
    private DateTime _expiresAtUtc = DateTime.MinValue;

    public SfAuth(string account, string user, string privateKeyPem)
    {
        _key = RSA.Create();
        try
        {
            _key.ImportFromPem(privateKeyPem);
        }
        catch (Exception ex) when (ex is ArgumentException or CryptographicException)
        {
            _key.Dispose();
            // Never include the input: it is the private key.
            throw new SfException(0, "the private key could not be read - it must be an UNENCRYPTED PKCS#8 PEM (-----BEGIN PRIVATE KEY-----)");
        }
        _subject = SnowflakeEngine.JwtAccount(account) + "." + user.ToUpperInvariant();
        _issuer = _subject + "." + Fingerprint(_key);
    }

    /// <summary>SHA256:&lt;base64 of the SHA-256 of the DER public key&gt;, exactly as DESC USER shows RSA_PUBLIC_KEY_FP.</summary>
    public static string Fingerprint(RSA key) =>
        "SHA256:" + Convert.ToBase64String(SHA256.HashData(key.ExportSubjectPublicKeyInfo()));

    public string Token()
    {
        lock (_gate)
        {
            if (_token == null || DateTime.UtcNow >= _expiresAtUtc - RefreshMargin)
            {
                var now = DateTimeOffset.UtcNow;
                _token = Sign(now);
                _expiresAtUtc = now.UtcDateTime + Lifetime;
            }
            return _token;
        }
    }

    internal string Sign(DateTimeOffset now)
    {
        var header = B64(JsonSerializer.SerializeToUtf8Bytes(new { alg = "RS256", typ = "JWT" }));
        var payload = B64(JsonSerializer.SerializeToUtf8Bytes(new
        {
            iss = _issuer,
            sub = _subject,
            iat = now.ToUnixTimeSeconds(),
            exp = (now + Lifetime).ToUnixTimeSeconds(),
        }));
        var signingInput = header + "." + payload;
        var signature = _key.SignData(Encoding.ASCII.GetBytes(signingInput), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return signingInput + "." + B64(signature);
    }

    internal static string B64(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    public void Dispose() => _key.Dispose();
}

/// <summary>Snowflake refused or failed a request. Engine-specific, so the runner can still treat it as a source failure.</summary>
public sealed class SfException : SourceException
{
    public int StatusCode { get; }

    public SfException(int statusCode, string message)
        : base(statusCode == 0 ? "Snowflake: " + message : $"Snowflake answered HTTP {statusCode}: {message}")
    {
        StatusCode = statusCode;
    }
}
