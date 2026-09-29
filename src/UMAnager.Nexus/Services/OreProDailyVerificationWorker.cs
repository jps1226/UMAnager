// ============================================================
// FILE: OreProDailyVerificationWorker.cs
// LAYER: Background service
// PURPOSE: Retry an end-of-day public OrePro check that was temporarily unavailable.
//          It only reads the public profile endpoint; it never logs in or touches OrePro bets.
// ============================================================
using System.Globalization;

namespace UMAnager.Nexus.Services;

public sealed class OreProDailyVerificationWorker : BackgroundService
{
    private static readonly TimeSpan RetryInterval = TimeSpan.FromMinutes(30);

    private readonly OreProDailyVerificationService _verification;
    private readonly ILogger<OreProDailyVerificationWorker> _logger;

    public OreProDailyVerificationWorker(
        OreProDailyVerificationService verification,
        ILogger<OreProDailyVerificationWorker> logger)
    {
        _verification = verification;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Let the web app and normal end-of-day trigger initialize first. This worker only retries
        // dates that the first check recorded as unavailable/unconfigured.
        await Task.Delay(TimeSpan.FromMinutes(2), stoppingToken);
        using var timer = new PeriodicTimer(RetryInterval);
        do
        {
            foreach (var dateKey in await _verification.GetPendingDateKeysAsync())
            {
                if (!DateTime.TryParseExact(dateKey, "yyyy-MM-dd", CultureInfo.InvariantCulture,
                        DateTimeStyles.None, out var parsed))
                    continue;
                try
                {
                    await _verification.VerifyAsync(DateTime.SpecifyKind(parsed, DateTimeKind.Utc), stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "[OreProVerify] Retry worker failed for {Date}.", dateKey);
                }
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }
}
