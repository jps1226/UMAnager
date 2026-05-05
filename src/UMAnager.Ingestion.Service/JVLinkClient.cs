namespace UMAnager.Ingestion.Service;

using System.Runtime.InteropServices;
using UMAnager.Common;

/// <summary>
/// Wrapper around the JV-Link COM object for safe, typed access.
/// JV-Link is a 32-bit InprocServer COM component; this project must target x86.
/// </summary>
public sealed class JVLinkClient : IDisposable
{
    private readonly ILogger<JVLinkClient> _logger;
    private dynamic? _jvLink;
    private bool _disposed;

    public JVLinkClient(ILogger<JVLinkClient> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Initialize JV-Link. Must be called once at startup.
    /// </summary>
    public void Initialize(string softwareId)
    {
        ThrowIfDisposed();

        try
        {
            // Create COM object: JVDTLab.JVLink (CLSID {2AB1774D-0C41-11D7-916F-0003479BEB3F})
            var jvLinkType = Type.GetTypeFromProgID("JVDTLab.JVLink");
            if (jvLinkType == null)
                throw new JVLinkException(0, "JV-Link COM object not found. Ensure JV-Link is installed and registered.");

            _jvLink = Activator.CreateInstance(jvLinkType)
                ?? throw new JVLinkException(0, "Failed to instantiate JV-Link COM object");

            int result = (int)_jvLink.JVInit(softwareId);
            _logger.LogInformation("JVInit returned {Result}", result);

            if (result != 0)
                throw new JVLinkException(result, $"JVInit failed with code {result}");

            _logger.LogInformation("JV-Link initialized successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize JV-Link");
            throw;
        }
    }

    /// <summary>
    /// Open a data request and wait for download completion with retry logic.
    /// Automatically retries on -502 (Download Failure) with exponential backoff.
    /// Returns when downloadCount files have been downloaded.
    /// </summary>
    public async Task<JVOpenResult> OpenAndWaitForDownloadAsync(
        string dataSpec,
        string fromTime,
        int option,
        CancellationToken cancellationToken = default)
    {
        const int maxRetries = 3;
        int retryDelayMs = 30_000; // Start with 30 seconds

        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            try
            {
                return await OpenAndWaitForDownloadInternalAsync(dataSpec, fromTime, option, cancellationToken);
            }
            catch (JVLinkException ex) when (ex.ErrorCode == JVLinkErrorCode.CommunicationError)
            {
                attempt++;
                if (attempt >= maxRetries)
                {
                    _logger.LogError("Download failed with -502 after {Retries} attempts", maxRetries);
                    throw;
                }

                _logger.LogWarning(
                    "Download failed with -502 (network error). Retrying in {DelaySeconds}s (attempt {Attempt}/{Max})",
                    retryDelayMs / 1000, attempt, maxRetries);

                await Task.Delay(retryDelayMs, cancellationToken);
                retryDelayMs *= 2; // Exponential backoff
            }
        }

        throw new JVLinkException(-1, "Unexpected retry loop exit");
    }

    /// <summary>
    /// Internal implementation of OpenAndWaitForDownload (called with retry wrapper).
    /// Returns when downloadCount files have been downloaded.
    /// </summary>
    private async Task<JVOpenResult> OpenAndWaitForDownloadInternalAsync(
        string dataSpec,
        string fromTime,
        int option,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        try
        {
            _logger.LogInformation("JVOpen({DataSpec}, {FromTime}, option={Option})", dataSpec, fromTime, option);

            // Parameters are passed by reference for out params
            dynamic readCount = 0;
            dynamic downloadCount = 0;
            dynamic lastFileTimestamp = "";

            int result = (int)_jvLink!.JVOpen(dataSpec, fromTime, option, readCount, downloadCount, lastFileTimestamp);

            // Cast dynamic results to proper types for logging
            int rc = (int)readCount;
            int dc = (int)downloadCount;
            string ts = (string)lastFileTimestamp;

            _logger.LogInformation(
                "JVOpen returned {Result}: readCount={ReadCount}, downloadCount={DownloadCount}, lastFileTimestamp={LastFileTimestamp}",
                result, rc, dc, ts);

            if (result != 0)
                throw new JVLinkException(result, $"JVOpen failed with code {result}");

            // If no files need downloading, skip the wait
            if (downloadCount == 0)
            {
                _logger.LogInformation("No files to download (cached)");
                return new JVOpenResult
                {
                    ReadCount = readCount,
                    DownloadCount = downloadCount,
                    LastFileTimestamp = lastFileTimestamp
                };
            }

            // Poll JVStatus until download completes
            await WaitForDownloadCompletionAsync(downloadCount, cancellationToken);

            return new JVOpenResult
            {
                ReadCount = rc,
                DownloadCount = dc,
                LastFileTimestamp = ts
            };
        }
        catch (JVLinkException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "JVOpen failed");
            throw new JVLinkException(0, "JVOpen failed", ex);
        }
    }

    /// <summary>
    /// Read the next record. Returns byte array, filename, and status code.
    /// Status: > 0 = bytes read, -1 = file boundary, 0 = EOF, -502 = download failure (retryable), < -1 = other error
    /// </summary>
    public JVGetsResult ReadRecord()
    {
        ThrowIfDisposed();

        try
        {
            dynamic buffData = new byte[110000];
            dynamic buffSize = 110000;
            dynamic fileName = "";

            int result = (int)_jvLink!.JVGets(buffData, buffSize, fileName);

            // -502 is retryable; other negative codes are errors
            if (result < -1 && result != -502)
                throw new JVLinkException(result, $"JVGets failed with code {result}");

            // Trim the buffer to actual size returned
            byte[] data = result > 0 ? ((byte[])buffData).Take(result).ToArray() : [];
            string fn = (string)fileName;

            return new JVGetsResult
            {
                StatusCode = result,
                Data = data,
                FileName = fn ?? ""
            };
        }
        catch (JVLinkException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "JVGets failed");
            throw new JVLinkException(0, "JVGets failed", ex);
        }
    }

    /// <summary>
    /// Get the current file's timestamp (set during JVGets read).
    /// </summary>
    public string GetCurrentFileTimestamp()
    {
        ThrowIfDisposed();
        return _jvLink?.m_CurrentFileTimestamp ?? "";
    }

    /// <summary>
    /// Close the download and reading session.
    /// </summary>
    public void Close()
    {
        if (_jvLink == null) return;

        try
        {
            int result = (int)_jvLink.JVClose();
            _logger.LogInformation("JVClose returned {Result}", result);

            if (result != 0)
                _logger.LogWarning("JVClose returned non-zero code {Result}", result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "JVClose failed");
        }
    }

    /// <summary>
    /// Cancel an in-progress download.
    /// </summary>
    public void Cancel()
    {
        if (_jvLink == null) return;

        try
        {
            _jvLink.JVCancel();
            _logger.LogInformation("JVCancel issued");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "JVCancel failed");
        }
    }

    private async Task WaitForDownloadCompletionAsync(int downloadCount, CancellationToken cancellationToken)
    {
        const int pollIntervalMs = 300;
        int maxWaitMs = 300_000; // 5 minutes max
        var deadline = DateTime.UtcNow.AddMilliseconds(maxWaitMs);

        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                int status = (int)_jvLink!.JVStatus();

                if (status < 0)
                    throw new JVLinkException(status, $"JVStatus returned error code {status}");

                _logger.LogDebug("Download progress: {Status}/{Total}", status, downloadCount);

                if (status >= downloadCount)
                {
                    _logger.LogInformation("Download complete: {Status}/{Total}", status, downloadCount);
                    return;
                }
            }
            catch (JVLinkException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "JVStatus failed");
                throw new JVLinkException(0, "JVStatus failed", ex);
            }

            await Task.Delay(pollIntervalMs, cancellationToken);
        }

        throw new TimeoutException($"Download did not complete within {maxWaitMs}ms");
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(JVLinkClient));
    }

    public void Dispose()
    {
        if (_disposed) return;

        Close();
        if (_jvLink != null)
            Marshal.ReleaseComObject(_jvLink);

        _disposed = true;
        GC.SuppressFinalize(this);
    }

    ~JVLinkClient() => Dispose();
}

public record JVOpenResult
{
    public int ReadCount { get; init; }
    public int DownloadCount { get; init; }
    public string LastFileTimestamp { get; init; } = "";
}

public record JVGetsResult
{
    public int StatusCode { get; init; }
    public byte[] Data { get; init; } = [];
    public string FileName { get; init; } = "";
}
