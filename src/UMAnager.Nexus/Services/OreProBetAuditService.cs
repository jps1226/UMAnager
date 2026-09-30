// ============================================================
// FILE: OreProBetAuditService.cs
// LAYER: Service (singleton)
// PURPOSE: Durable, redacted audit trail for each OrePro Apply attempt. It preserves
//          what UMAnager intended to bet, the relevant OrePro confirmation evidence,
//          and UMAnager's own settled result before transient marks are cleared.
// ============================================================
using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace UMAnager.Nexus.Services;

/// <summary>
/// Stores an append-only record of each Apply attempt separately from the transient marks blob.
/// This is accounting/audit evidence only: it never changes a bet, retries a submit, or writes to OrePro.
/// </summary>
public sealed class OreProBetAuditService
{
    public const string StateKey = "orepro_bet_audit_records";
    private const int MaxRecords = 5_000;
    private static readonly SemaphoreSlim Gate = new(1, 1);
    private static readonly Regex SensitiveJsonField = new(
        "\\\"(?<field>cookie|set-cookie|authorization|token|session|password)\\\"\\s*:\\s*\\\"(?:\\\\.|[^\\\"])*\\\"",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private readonly AppStateService _state;
    private readonly SunkCostService _sunkCost;
    private readonly ILogger<OreProBetAuditService> _logger;

    public OreProBetAuditService(AppStateService state, SunkCostService sunkCost, ILogger<OreProBetAuditService> logger)
    {
        _state = state;
        _sunkCost = sunkCost;
        _logger = logger;
    }

    public async Task RecordAttemptAsync(
        string raceId,
        string route,
        object intended,
        object oreProEvidence,
        bool submitted,
        string status,
        string? message,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(raceId)) return;
        var raceDate = TryGetJstDate(raceId);
        var record = new OreProBetAuditRecord(
            AuditId: Guid.NewGuid().ToString("N"),
            RaceId: raceId,
            RaceDate: raceDate,
            AttemptedAtUtc: DateTime.UtcNow,
            Route: route,
            Submitted: submitted,
            Status: status,
            Message: message,
            Intended: RedactedElement(intended),
            OreProEvidence: RedactedElement(oreProEvidence),
            Settlement: null);

        await Gate.WaitAsync(ct);
        try
        {
            var records = await LoadUnsafeAsync();
            records.Add(record);
            if (records.Count > MaxRecords)
                records = records.OrderByDescending(r => r.AttemptedAtUtc).Take(MaxRecords).OrderBy(r => r.AttemptedAtUtc).ToList();
            await SaveUnsafeAsync(records);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[OreProAudit] Failed recording Apply attempt for {RaceId}", raceId);
        }
        finally
        {
            Gate.Release();
        }
    }

    /// <summary>
    /// Freezes UMAnager's own result calculation while the completed day's marks/frozen lines are still
    /// available. It intentionally does not overwrite or infer an OrePro settlement.
    /// </summary>
    public async Task FinalizeDayAsync(DateTime jstRaceDate, CancellationToken ct = default)
    {
        var date = DateTime.SpecifyKind(jstRaceDate.Date, DateTimeKind.Utc);
        var dateKey = date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var recaps = await _sunkCost.GetPerRaceRecapAsync(date, applyResetCutoff: false, ct);
        var recapByRace = recaps.ToDictionary(r => r.RaceId, StringComparer.Ordinal);

        await Gate.WaitAsync(ct);
        try
        {
            var records = await LoadUnsafeAsync();
            var changed = false;
            for (var i = 0; i < records.Count; i++)
            {
                var record = records[i];
                if (!record.Submitted || record.Settlement is not null || !string.Equals(record.RaceDate, dateKey, StringComparison.Ordinal))
                    continue;

                if (recapByRace.TryGetValue(record.RaceId, out var recap))
                {
                    records[i] = record with
                    {
                        Settlement = new OreProBetAuditSettlement(
                            SettledAtUtc: DateTime.UtcNow,
                            OutcomeAvailable: true,
                            StakedYen: recap.Staked,
                            WonYen: recap.Won,
                            NetYen: recap.Net,
                            HitLabels: recap.HitLabels,
                            Imported: recap.Imported,
                            Note: "UMAnager's frozen-plan/JRA-results calculation at day settlement; not an OrePro receipt.")
                    };
                }
                else
                {
                    records[i] = record with
                    {
                        Settlement = new OreProBetAuditSettlement(
                            SettledAtUtc: DateTime.UtcNow,
                            OutcomeAvailable: false,
                            StakedYen: 0,
                            WonYen: 0,
                            NetYen: 0,
                            HitLabels: Array.Empty<string>(),
                            Imported: false,
                            Note: "No UMAnager frozen-plan result was available when the day settled.")
                    };
                }
                changed = true;
            }
            if (changed) await SaveUnsafeAsync(records);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[OreProAudit] Failed finalizing Apply audits for {Date}", dateKey);
        }
        finally
        {
            Gate.Release();
        }
    }

    public async Task<IReadOnlyList<OreProBetAuditRecord>> GetAsync(string? jstDate = null)
    {
        await Gate.WaitAsync();
        try
        {
            var records = await LoadUnsafeAsync();
            return records
                .Where(r => string.IsNullOrWhiteSpace(jstDate) || string.Equals(r.RaceDate, jstDate, StringComparison.Ordinal))
                .OrderBy(r => r.AttemptedAtUtc)
                .ToArray();
        }
        finally
        {
            Gate.Release();
        }
    }

    private async Task<List<OreProBetAuditRecord>> LoadUnsafeAsync()
    {
        var raw = await _state.GetStringAsync(StateKey);
        if (string.IsNullOrWhiteSpace(raw)) return new();
        try { return JsonSerializer.Deserialize<List<OreProBetAuditRecord>>(raw) ?? new(); }
        catch (JsonException) { return new(); }
    }

    private Task SaveUnsafeAsync(List<OreProBetAuditRecord> records)
        => _state.SetStringAsync(StateKey, JsonSerializer.Serialize(records));

    private static string? TryGetJstDate(string raceId)
        => raceId.Length >= 8 && DateTime.TryParseExact(raceId[..8], "yyyyMMdd", CultureInfo.InvariantCulture,
            DateTimeStyles.None, out var date)
            ? date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
            : null;

    private static JsonElement RedactedElement(object value)
    {
        var json = JsonSerializer.Serialize(value);
        var redacted = SensitiveJsonField.Replace(json, "\"${field}\":\"[REDACTED]\"");
        using var doc = JsonDocument.Parse(redacted);
        return doc.RootElement.Clone();
    }
}

public sealed record OreProBetAuditRecord(
    string AuditId,
    string RaceId,
    string? RaceDate,
    DateTime AttemptedAtUtc,
    string Route,
    bool Submitted,
    string Status,
    string? Message,
    JsonElement Intended,
    JsonElement OreProEvidence,
    OreProBetAuditSettlement? Settlement);

public sealed record OreProBetAuditSettlement(
    DateTime SettledAtUtc,
    bool OutcomeAvailable,
    int StakedYen,
    int WonYen,
    int NetYen,
    IReadOnlyList<string> HitLabels,
    bool Imported,
    string Note);
