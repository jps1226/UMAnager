namespace UMAnager.Ingestion.Service;

using System.Runtime.InteropServices;
using System.Text;
using UMAnager.Common;

/// <summary>
/// Wrapper around the JV-Link COM object via dynamic COM dispatch.
///
/// JV-Link is a 32-bit InprocServer COM component. The project MUST target x86 with
/// RuntimeIdentifier=win-x86 to ensure in-process COM loading. If the process runs as
/// x64, Windows loads JV-Link in a COM surrogate (dllhost.exe) as out-of-process,
/// causing RPC_E_SERVERFAULT on method calls.
///
/// JVRead semantics (per JRA-VAN SDK docs):
///   - buff: caller pre-allocates a string of null chars; JV-Link fills it with record data
///   - size: input parameter specifying the max buffer length
///   - Returns: bytes read (>0), -1 (file boundary), 0 (EOF), negative (error)
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
            // Trim whitespace—JVInit rejects sid with leading spaces (error -103)
            softwareId = (softwareId ?? "UNKNOWN").Trim();
            if (string.IsNullOrEmpty(softwareId))
                softwareId = "UNKNOWN";

            // Log process architecture — JV-Link requires 32-bit in-process loading
            _logger.LogInformation("Process architecture: Is64Bit={Is64Bit}, PID={PID}",
                Environment.Is64BitProcess, Environment.ProcessId);

            // CP932 (Shift-JIS) encoding is not available by default in .NET Core
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

            // Create COM object via dynamic dispatch (requires win-x86 RID for in-process loading)
            try
            {
                Type jvLinkType = Type.GetTypeFromProgID("JVDTLab.JVLink")
                    ?? throw new InvalidOperationException("JVDTLab.JVLink ProgID not found in registry");
                _jvLink = Activator.CreateInstance(jvLinkType);
            }
            catch (Exception ex)
            {
                throw new JVLinkException(0, "JV-Link COM object not found. Ensure JV-Link is installed and registered.", ex);
            }

            int result = _jvLink!.JVInit(softwareId);
            _logger.LogInformation("JVInit returned {Result} with sid='{SoftwareId}'", result, softwareId);

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

            // Typed interop: JVOpen(string, string, int, ref int, ref int, out string)
            int readCount = 0;
            int downloadCount = 0;

            int result = _jvLink!.JVOpen(dataSpec, fromTime, option, ref readCount, ref downloadCount, out string? lastFileTimestamp);

            _logger.LogInformation(
                "JVOpen returned {Result}: readCount={ReadCount}, downloadCount={DownloadCount}, lastFileTimestamp={LastFileTimestamp}",
                result, readCount, downloadCount, lastFileTimestamp);

            // -1 = NoData: no new files since FromTime; return immediately with empty result
            if (result == -1)
            {
                _logger.LogInformation("JVOpen: no new data since fromTime={FromTime}", fromTime);
                return new JVOpenResult
                {
                    ReadCount = 0,
                    DownloadCount = 0,
                    LastFileTimestamp = lastFileTimestamp ?? "",
                    NoData = true
                };
            }

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
                ReadCount = readCount,
                DownloadCount = downloadCount,
                LastFileTimestamp = lastFileTimestamp
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
    /// Status: > 0 = bytes read, -1 = file boundary, 0 = EOF, negative = error
    ///
    /// Per JRA-VAN SDK: buff is a caller-allocated string buffer that JV-Link fills with
    /// record data. The size parameter is the maximum length of that buffer (input only).
    /// JV-Link manages the buffer content internally; the caller must pre-allocate it.
    /// </summary>
    public JVGetsResult ReadRecord()
    {
        ThrowIfDisposed();

        try
        {
            // JVRead per JRA-VAN SDK spec:
            //   buff  — caller pre-allocates; JV-Link fills with record bytes
            //   size  — input: max buffer length (not returned by JV-Link)
            //   filename — output: name of the file currently being read
            // VB.NET canonical form: Dim buff As String = New String(vbNullChar, buffSize)
            const int bufferSize = 16384;  // generously covers any record type
            string buff = new string('\0', bufferSize);
            string fileName = "";

            // Use dynamic dispatch so size is passed by value (not out),
            // matching the actual COM signature. Typed interop (tlbimp) emits
            // incorrect 'out' modifiers which cause in-process access violations.
            // buff is ref so JV-Link can write record data into the pre-allocated buffer.
            int result = _jvLink!.JVRead(ref buff, bufferSize, ref fileName);

            if (result < -1)
                throw new JVLinkException(result, $"JVRead failed with code {result}");

            byte[] data = [];
            if (result > 0)
            {
                // JV-Link writes raw CP932 bytes into the pre-allocated string buffer.
                // The bytes are stored as individual characters (each byte becomes a Unicode char).
                // Extract only the first 'result' bytes from the buffer.
                // We extract them without re-encoding, since they're already raw bytes masked as characters.
                byte[] extracted = new byte[result];
                for (int i = 0; i < result && i < buff.Length; i++)
                {
                    extracted[i] = (byte)(buff[i] & 0xFF);
                }
                data = extracted;
                _logger.LogDebug("JVRead: {Bytes} bytes, file={File}", data.Length, fileName);
            }

            return new JVGetsResult
            {
                StatusCode = result,
                Data = data,
                FileName = fileName ?? ""
            };
        }
        catch (JVLinkException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "JVRead failed");
            throw new JVLinkException(0, "JVRead failed", ex);
        }
    }

    /// <summary>
    /// Get the current file's timestamp (set during JVGets read).
    /// </summary>
    public string GetCurrentFileTimestamp()
    {
        ThrowIfDisposed();
        return _jvLink?.m_CurrentFileTimeStamp ?? "";
    }

    /// <summary>
    /// Skip the current file and move to the next one in the stream.
    /// Used to skip unwanted record types (e.g., RA, KS) when reading DIFN.
    /// </summary>
    public void Skip()
    {
        ThrowIfDisposed();

        try
        {
            _jvLink!.JVSkip();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "JVSkip failed");
        }
    }

    /// <summary>
    /// Close the download and reading session.
    /// </summary>
    public void Close()
    {
        if (_jvLink == null) return;

        try
        {
            int result = _jvLink.JVClose();
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
                int status = _jvLink!.JVStatus();

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
    public bool NoData { get; init; }
}

public record JVGetsResult
{
    public int StatusCode { get; init; }
    public byte[] Data { get; init; } = [];
    public string FileName { get; init; } = "";
}
