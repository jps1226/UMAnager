// ============================================================
// FILE: ResultsFetchService.cs
// LAYER: Service
// PURPOSE: Enqueues STREAM_RESULTS commands (JVRTOpen 0B12, per-day). One command per call —
//          0B12 returns all venues for the date, so no per-race iteration.
// KEY DEPENDENCIES: SidecarBridge.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Text.Json;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Enqueues STREAM_RESULTS commands to the Sidecar (JVRTOpen("0B12", yyyyMMdd)).
/// 0B12's provision unit is per-day, so this is a single command per call — no per-race iteration.
/// </summary>
public sealed class ResultsFetchService
{
    private static readonly TimeZoneInfo JstZone = TimeZoneInfo.FindSystemTimeZoneById("Tokyo Standard Time");

    private readonly SidecarBridge _bridge;
    private readonly ILogger<ResultsFetchService> _logger;

    public ResultsFetchService(SidecarBridge bridge, ILogger<ResultsFetchService> logger)
    {
        _bridge = bridge;
        _logger = logger;
    }

    public enum EnqueueResult { Enqueued, SidecarDisconnected }

    public Task<(EnqueueResult, string)> EnqueueForTodayAsync(CancellationToken ct)
    {
        var today = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, JstZone).ToString("yyyyMMdd");
        return EnqueueForDateAsync(today, ct);
    }

    public async Task<(EnqueueResult, string)> EnqueueForDateAsync(string yyyyMmDd, CancellationToken ct)
    {
        if (!_bridge.IsConnected) return (EnqueueResult.SidecarDisconnected, yyyyMmDd);

        var payload = JsonSerializer.Serialize(new
        {
            command = "STREAM_RESULTS",
            race_date = yyyyMmDd,
        });

        await _bridge.CommandQueue.Writer.WriteAsync(payload, ct);
        _logger.LogInformation("[ResultsFetch] Enqueued STREAM_RESULTS for {Date}.", yyyyMmDd);
        return (EnqueueResult.Enqueued, yyyyMmDd);
    }
}
