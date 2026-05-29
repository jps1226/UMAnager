using System.IO.Pipes;
using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;
using UMAnager.Nexus.Data.Entities;
using UMAnager.Nexus.Models;
using UMAnager.Nexus.Services;
using UMAnager.Nexus.Services.Parsing;

namespace UMAnager.Nexus.Pipes;

public sealed class NexusPipeServer : BackgroundService
{
    private readonly SidecarBridge _bridge;
    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly AppStateService _appState;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<NexusPipeServer> _logger;

    // Races where a partial-top-5 follow-up tick has already been scheduled this process.
    // Cleared on restart (acceptable — JRA's 4-5 placings usually publish within ~90s of 1-3).
    private static readonly HashSet<string> s_partialFollowupScheduled = new();
    private static readonly object s_partialFollowupLock = new();

    public NexusPipeServer(
        SidecarBridge bridge,
        IDbContextFactory<AppDbContext> dbFactory,
        AppStateService appState,
        IServiceScopeFactory scopeFactory,
        ILogger<NexusPipeServer> logger)
    {
        _bridge       = bridge;
        _dbFactory    = dbFactory;
        _appState     = appState;
        _scopeFactory = scopeFactory;
        _logger       = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Yield immediately so the host finishes starting before we block on pipe creation.
        // Without this, a synchronous throw in the NamedPipeServerStream constructor
        // propagates back into Host.StartAsync and crashes the process.
        await Task.Yield();

        while (!stoppingToken.IsCancellationRequested)
        {
            await using var pipe = new NamedPipeServerStream(
                PipeEnvelope.PipeName,
                PipeDirection.InOut,
                1,
                PipeTransmissionMode.Byte,
                PipeOptions.Asynchronous);

            _logger.LogInformation("[Nexus] Waiting for Sidecar on pipe '{Pipe}'...", PipeEnvelope.PipeName);

            try
            {
                await pipe.WaitForConnectionAsync(stoppingToken);
                _logger.LogInformation("[Nexus] Sidecar connected. Sending INIT command...");

                var initPayload = Encoding.UTF8.GetBytes("{\"command\":\"INIT\"}");
                await new PipeEnvelope(PipeMessageType.Command, initPayload).WriteAsync(pipe, stoppingToken);

                var response = await PipeEnvelope.ReadAsync(pipe, stoppingToken);
                if (response.Type == PipeMessageType.Status)
                {
                    var json = Encoding.UTF8.GetString(response.Payload);
                    using var doc = JsonDocument.Parse(json);
                    var root = doc.RootElement;
                    _bridge.JvLinkVersion = root.TryGetProperty("jvlink_version", out var v) ? v.GetString() ?? "Unknown" : "Unknown";
                    _bridge.InitResult    = root.TryGetProperty("init_result",    out var r) ? r.GetInt32()  : -1;
                    _logger.LogInformation("[Nexus] Sidecar INIT: Version={Version}, InitResult={Result}",
                        _bridge.JvLinkVersion, _bridge.InitResult);
                }

                // Run command forwarder and record receiver concurrently on the open pipe.
                // WhenAny: if either task ends (pipe disconnect or stream complete), we clean up.
                using var pipeCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                var forwardTask = ForwardCommandsAsync(pipe, pipeCts.Token);
                var receiveTask = ReceiveRecordsAsync(pipe, pipeCts.Token);
                await Task.WhenAny(forwardTask, receiveTask);
                await pipeCts.CancelAsync();
                await Task.WhenAll(forwardTask.ContinueWith(_ => { }), receiveTask.ContinueWith(_ => { }));
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[Nexus] Pipe session error. Restarting listener.");
                _bridge.InitResult      = -1;
                _bridge.JvLinkVersion   = "Disconnected";
                _bridge.IngestionStatus = "Idle";
                await Task.Delay(3000, stoppingToken);
            }
        }
    }

    private async Task ForwardCommandsAsync(NamedPipeServerStream pipe, CancellationToken ct)
    {
        try
        {
            await foreach (var json in _bridge.CommandQueue.Reader.ReadAllAsync(ct))
            {
                if (!pipe.IsConnected) break;
                var envelope = new PipeEnvelope(PipeMessageType.Command, Encoding.UTF8.GetBytes(json));
                await envelope.WriteAsync(pipe, ct);
                _logger.LogInformation("[Nexus] Forwarded command to Sidecar: {Json}", json);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Nexus] Command forwarder stopped.");
        }
    }

    private async Task ReceiveRecordsAsync(NamedPipeServerStream pipe, CancellationToken ct)
    {
        var batch = new List<RawStagingRecord>(500);
        long totalFlushed = 0;

        try
        {
            while (!ct.IsCancellationRequested && pipe.IsConnected)
            {
                PipeEnvelope msg;
                try
                {
                    msg = await PipeEnvelope.ReadAsync(pipe, ct);
                }
                catch (EndOfStreamException)
                {
                    break;
                }

                if (msg.Type == PipeMessageType.RawRecord)
                {
                    var recType = Encoding.ASCII.GetString(msg.Payload[..2]);
                    batch.Add(new RawStagingRecord
                    {
                        RecordType = recType,
                        RawBytes   = msg.Payload,
                        ReceivedAt = DateTime.UtcNow,
                    });

                    if (batch.Count >= 500)
                        totalFlushed += await FlushBatchAsync(batch, totalFlushed, ct);
                }
                else if (msg.Type == PipeMessageType.Status)
                {
                    var json = Encoding.UTF8.GetString(msg.Payload);
                    using var doc = JsonDocument.Parse(json);
                    var root = doc.RootElement;

                    var eventType = root.TryGetProperty("event_type", out var ev) ? ev.GetString() : null;
                    if (eventType is "STREAM_DIFN_COMPLETE" or "STREAM_TOKU_COMPLETE" or "STREAM_ODDS_COMPLETE" or "STREAM_RESULTS_COMPLETE")
                    {
                        if (batch.Count > 0)
                            totalFlushed += await FlushBatchAsync(batch, totalFlushed, ct);

                        var recordCount  = root.TryGetProperty("record_count",  out var rc) ? rc.GetInt32() : -1;
                        var skippedCount = root.TryGetProperty("skipped_count", out var sc) ? sc.GetInt32() : 0;

                        _bridge.StagedRecordCount = (int)totalFlushed;
                        _bridge.IngestionStatus   = recordCount < 0 ? "Error" : "Complete";

                        // Persist the JV-Link cursor returned in last_file_timestamp so the next
                        // Option=2 call resumes exactly where this one stopped.
                        if (eventType == "STREAM_TOKU_COMPLETE"
                            && recordCount >= 0
                            && root.TryGetProperty("last_file_timestamp", out var lts))
                        {
                            var cursor = lts.GetString();
                            if (!string.IsNullOrWhiteSpace(cursor))
                            {
                                await _appState.SetStringAsync(AppStateService.Keys.TokuFileCursor, cursor);
                                _logger.LogInformation("[Nexus] TOKU file cursor advanced to {Cursor}", cursor);
                            }
                        }

                        // After DIFN stream completes, parse newly-staged UM records and stamp
                        // last_um_refresh so the weekly auto-refresh cadence resets correctly.
                        if (eventType == "STREAM_DIFN_COMPLETE" && recordCount > 0)
                        {
                            _ = Task.Run(async () =>
                            {
                                using var scope = _scopeFactory.CreateScope();
                                var svc = scope.ServiceProvider.GetRequiredService<DifnRecordParsingService>();
                                await svc.ParseAllRecordsAsync(CancellationToken.None);
                                var appState = scope.ServiceProvider.GetRequiredService<AppStateService>();
                                await appState.SetTimestampAsync(AppStateService.Keys.LastUmRefresh, DateTime.UtcNow);
                                _logger.LogInformation("[Nexus] DIFN auto-parse complete, last_um_refresh stamped.");
                            });
                        }

                        // After TOKU stream completes, parse newly-staged RA + SE records so
                        // upcoming races appear in the UI. Then nudge the orchestrator to
                        // re-evaluate phase immediately — the tick that triggered this pull
                        // evaluated phase BEFORE the new data landed, so a fresh AWAITING_POSTS
                        // → AWAITING_ODDS transition would otherwise wait up to a full
                        // posts_poll_interval (1h) before firing its Discord ping.
                        if (eventType == "STREAM_TOKU_COMPLETE" && recordCount > 0)
                        {
                            _ = Task.Run(async () =>
                            {
                                using var scope = _scopeFactory.CreateScope();
                                var svc = scope.ServiceProvider.GetRequiredService<Services.Parsing.DifnRecordParsingService>();
                                await svc.ParseAllRecordsAsync(CancellationToken.None);
                                _logger.LogInformation("[Nexus] TOKU auto-parse complete.");

                                var orchestrator = scope.ServiceProvider.GetRequiredService<LiveOrchestrator>();
                                orchestrator.RequestForceTick();
                            });
                        }

                        // After odds stream completes, immediately apply O1 records to race_entries
                        // and broadcast the touched races to any connected SignalR clients.
                        if (eventType == "STREAM_ODDS_COMPLETE" && recordCount > 0)
                        {
                            _ = Task.Run(async () =>
                            {
                                using var scope = _scopeFactory.CreateScope();
                                var svc = scope.ServiceProvider.GetRequiredService<OddsApplyService>();
                                var result = await svc.ApplyAllPendingAsync(CancellationToken.None);
                                if (result.TouchedRaceIds.Count > 0)
                                {
                                    var broadcaster = scope.ServiceProvider.GetRequiredService<LiveBroadcastService>();
                                    await broadcaster.BroadcastOddsAsync(result.TouchedRaceIds, CancellationToken.None);
                                }

                                // Notify Discord the first time odds reach ≥50% coverage on an
                                // upcoming race date. Idempotent via app_state.odds_notified_dates.
                                await NotifyOddsFirstAppearedAsync(scope);

                                // Same reasoning as TOKU above: the orchestrator tick that
                                // pulled these odds evaluated phase BEFORE the data landed, so
                                // an AWAITING_ODDS → RACES_POPULATED flip would otherwise wait
                                // up to a full poll interval. Force-tick wakes it immediately.
                                var orchestrator = scope.ServiceProvider.GetRequiredService<LiveOrchestrator>();
                                orchestrator.RequestForceTick();
                            });
                        }

                        // After results stream completes, parse newly-staged RA + SE so finish positions
                        // land in race_entries. ParseAllRecordsAsync is idempotent (uses !IsProcessed)
                        // and no-ops the UM step (0B12 emits no UM records). Snapshot the affected
                        // race IDs from unprocessed RA records before parsing so we can broadcast.
                        if (eventType == "STREAM_RESULTS_COMPLETE" && recordCount > 0)
                        {
                            _ = Task.Run(async () =>
                            {
                                using var scope = _scopeFactory.CreateScope();
                                // RA record raceId is bytes 11..26 (0-indexed), length 16.
                                List<string> raceIds;
                                List<string> hrRaceIds;
                                await using (var snapshotDb = await _dbFactory.CreateDbContextAsync(CancellationToken.None))
                                {
                                    var raRaw = await snapshotDb.RawStagingRecords
                                        .Where(r => r.RecordType == "RA" && !r.IsProcessed)
                                        .Select(r => r.RawBytes)
                                        .ToListAsync(CancellationToken.None);
                                    raceIds = raRaw
                                        .Where(b => b.Length >= 27)
                                        .Select(b => Encoding.ASCII.GetString(b, 11, 16).Trim())
                                        .Where(s => !string.IsNullOrWhiteSpace(s))
                                        .Distinct()
                                        .ToList();

                                    // HR payout records arrive in a separate batch after RA/SE finish
                                    // records. Capture their raceIds so DayRecapNotifier can fire once
                                    // ResultsJson is populated, even when raceIds (RA-only) is empty.
                                    var hrRaw = await snapshotDb.RawStagingRecords
                                        .Where(r => r.RecordType == "HR" && !r.IsProcessed)
                                        .Select(r => r.RawBytes)
                                        .ToListAsync(CancellationToken.None);
                                    hrRaceIds = hrRaw
                                        .Where(b => b.Length >= 27)
                                        .Select(b => Encoding.ASCII.GetString(b, 11, 16).Trim())
                                        .Where(s => !string.IsNullOrWhiteSpace(s))
                                        .Distinct()
                                        .ToList();
                                }

                                var svc = scope.ServiceProvider.GetRequiredService<Services.Parsing.DifnRecordParsingService>();
                                await svc.ParseAllRecordsAsync(CancellationToken.None);

                                if (raceIds.Count > 0)
                                {
                                    var broadcaster = scope.ServiceProvider.GetRequiredService<LiveBroadcastService>();
                                    await broadcaster.BroadcastResultsAsync(raceIds, CancellationToken.None);

                                    // Evaluate marks against the freshly-parsed finishes and ping
                                    // Discord on any newly-won bets. Idempotent — already-notified
                                    // races are tracked in app_state.
                                    var betWin = scope.ServiceProvider.GetRequiredService<Services.BetWinNotifier>();
                                    await betWin.EvaluateAndNotifyAsync(raceIds, CancellationToken.None);
                                }

                                // Phase 16: once a JST race-day's results are fully in, fire a
                                // single Discord recap with hit counts + estimated ¥ won.
                                // Uses RA ∪ HR raceIds: RA records carry finish positions, HR records
                                // carry payouts (ResultsJson). Both sets are needed to satisfy the
                                // ≥80%-finished + all-ResultsJson gate in DayRecapNotifier.
                                var recapIds = raceIds.Union(hrRaceIds).ToList();
                                if (recapIds.Count > 0)
                                {
                                    var dayRecap = scope.ServiceProvider.GetRequiredService<Services.DayRecapNotifier>();
                                    await dayRecap.EvaluateAndNotifyAsync(recapIds, CancellationToken.None);
                                }

                                if (raceIds.Count > 0)
                                {
                                    // Phase 9: refresh sire_performance MV after new finishes land.
                                    // CONCURRENTLY — readers never block, ~sub-second on ~10K rows.
                                    // Fire-and-forget so a slow refresh can't stall the pipe.
                                    var sirePerf = scope.ServiceProvider.GetRequiredService<Services.SirePerformanceService>();
                                    _ = Task.Run(async () =>
                                    {
                                        try { await sirePerf.RefreshAsync(CancellationToken.None); }
                                        catch (Exception ex) { _logger.LogError(ex, "[Nexus] sire_performance refresh failed."); }
                                    });

                                    // Phase 8: refresh jockey/trainer rolling stats. Same fire-and-forget pattern.
                                    var jtStats = scope.ServiceProvider.GetRequiredService<Services.JockeyTrainerStatsService>();
                                    _ = Task.Run(async () =>
                                    {
                                        try { await jtStats.RefreshAsync(CancellationToken.None); }
                                        catch (Exception ex) { _logger.LogError(ex, "[Nexus] jockey/trainer stats refresh failed."); }
                                    });

                                    // JRA publishes finishers 1-3 (the official umaban) immediately,
                                    // then the rest of the field a few seconds-to-minutes later. If
                                    // any race in this batch has top-3 but is missing some of 4-5,
                                    // schedule a single force-tick ~90s out to grab the full order
                                    // before the next regular 5-min tick.
                                    await ScheduleFollowupForPartialResultsAsync(scope, raceIds);
                                }
                            });
                        }

                        _logger.LogInformation(
                            "[Nexus] {EventType} complete. Staged={Staged}, SidecarSent={Sent}, SkippedFiles={Skipped}",
                            eventType, totalFlushed, recordCount, skippedCount);

                        // Reset per-stream accumulators but keep the pipe open — the Sidecar's main
                        // loop is waiting for the next command on this same connection. Tearing the
                        // pipe down here would kill the Sidecar (it doesn't reconnect).
                        batch.Clear();
                        totalFlushed = 0;
                    }
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[Nexus] Record receiver stopped unexpectedly.");
            _bridge.IngestionStatus = "Error";
        }
        finally
        {
            if (batch.Count > 0)
            {
                try { await FlushBatchAsync(batch, totalFlushed, CancellationToken.None); }
                catch (Exception ex) { _logger.LogError(ex, "[Nexus] Final batch flush failed."); }
            }
        }
    }

    private async Task ScheduleFollowupForPartialResultsAsync(IServiceScope scope, List<string> raceIds)
    {
        // A race is "partial" when finishers 1-3 exist but the entry count exceeds the
        // count of populated FinishPos values — i.e. the rest of the field isn't filled
        // in yet. We don't insist on exactly 5; small fields (≤4 starters) still benefit
        // from one follow-up to fill positions 2+ if 1 is published alone.
        await using var db = await _dbFactory.CreateDbContextAsync();
        var stats = await db.RaceEntries.AsNoTracking()
            .Where(e => raceIds.Contains(e.RaceId))
            .GroupBy(e => e.RaceId)
            .Select(g => new
            {
                RaceId   = g.Key,
                Total    = g.Count(),
                Finished = g.Count(e => e.FinishPos != null && e.FinishPos > 0),
                HasTop3  = g.Count(e => e.FinishPos >= 1 && e.FinishPos <= 3) >= 3,
            })
            .ToListAsync();

        var partials = new List<string>();
        lock (s_partialFollowupLock)
        {
            foreach (var s in stats)
            {
                if (!s.HasTop3) continue;
                if (s.Finished >= s.Total) continue; // already complete
                if (!s_partialFollowupScheduled.Add(s.RaceId)) continue; // already scheduled
                partials.Add(s.RaceId);
            }
        }

        if (partials.Count == 0) return;

        _logger.LogInformation("[Nexus] Partial results detected for {Count} race(s); scheduling follow-up tick in 90s. Races: {Ids}",
            partials.Count, string.Join(",", partials));

        // Fire-and-forget delayed force-tick. Uses the root provider (not the per-event
        // scope, which disposes when the parse Task.Run lambda exits) so the orchestrator
        // singleton stays resolvable when the delay fires.
        var sp = _scopeFactory.CreateScope().ServiceProvider;
        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(90));
                var orchestrator = sp.GetRequiredService<LiveOrchestrator>();
                orchestrator.RequestForceTick();
                _logger.LogInformation("[Nexus] Partial-results follow-up tick requested.");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[Nexus] Partial-results follow-up failed.");
            }
        });
    }

    private static readonly TimeZoneInfo JstZone = TimeZoneInfo.FindSystemTimeZoneById("Tokyo Standard Time");

    private async Task NotifyOddsFirstAppearedAsync(IServiceScope scope)
    {
        try
        {
            var jstTodayDt = DateTime.SpecifyKind(
                TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, JstZone).Date, DateTimeKind.Utc);

            await using var db = await _dbFactory.CreateDbContextAsync(CancellationToken.None);

            // Find upcoming dates where ≥50% of entries now have odds.
            var coverage = await db.RaceEntries
                .Join(db.Races, e => e.RaceId, r => r.RaceId, (e, r) => new { e.Odds, r.RaceDate, r.TrackCode })
                .Where(x => x.RaceDate >= jstTodayDt)
                .GroupBy(x => x.RaceDate)
                .Select(g => new { Date = g.Key, Total = g.Count(), WithOdds = g.Count(x => x.Odds != null) })
                .Where(x => x.Total > 0 && x.WithOdds * 2 >= x.Total)
                .ToListAsync(CancellationToken.None);

            if (coverage.Count == 0) return;

            var notifiedRaw = await _appState.GetStringAsync(AppStateService.Keys.OddsNotifiedDates) ?? "[]";
            var notified    = JsonSerializer.Deserialize<List<string>>(notifiedRaw) ?? new();
            var discord     = scope.ServiceProvider.GetRequiredService<IDiscordNotifier>();
            var changed     = false;

            foreach (var d in coverage)
            {
                var dateKey = d.Date.ToString("yyyy-MM-dd");
                if (notified.Contains(dateKey)) continue;

                var tracks = await db.Races
                    .Where(r => r.RaceDate == d.Date)
                    .Select(r => r.TrackCode)
                    .Distinct().OrderBy(t => t)
                    .ToListAsync(CancellationToken.None);

                await discord.NotifyOddsAvailableAsync(
                    dateKey, tracks.Where(t => t != null).Cast<string>());

                notified.Add(dateKey);
                changed = true;
            }

            if (changed)
                await _appState.SetStringAsync(
                    AppStateService.Keys.OddsNotifiedDates,
                    JsonSerializer.Serialize(notified));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Nexus] Odds-arrival notification check failed.");
        }
    }

    private async Task<int> FlushBatchAsync(List<RawStagingRecord> batch, long previousTotal, CancellationToken ct)
    {
        await using var ctx = await _dbFactory.CreateDbContextAsync(ct);
        ctx.RawStagingRecords.AddRange(batch);
        await ctx.SaveChangesAsync(ct);
        var count = batch.Count;
        var newTotal = previousTotal + count;
        batch.Clear();

        // Update real-time progress
        _bridge.StagedRecordCount = (int)newTotal;

        _logger.LogInformation("[Nexus] Flushed {Count} records to raw_staging (total: {Total}).",
            count, newTotal);
        return count;
    }
}
