// ============================================================
// FILE: BetReminderNotifier.cs
// LAYER: Service — scheduled through LiveOrchestrator ticks
// PURPOSE: Sends one Discord reminder at 4:00 PM and one at 6:30 PM Eastern on Friday/Saturday
//          when the corresponding Saturday/Sunday JST race card has no locked bets yet.
// ============================================================
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data;

namespace UMAnager.Nexus.Services;

public sealed class BetReminderNotifier
{
    private const string SentStateKey = "bet_reminder_sent_slots";
    private static readonly TimeZoneInfo EasternZone =
        TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time");
    private static readonly TimeZoneInfo TokyoZone =
        TimeZoneInfo.FindSystemTimeZoneById("Tokyo Standard Time");

    private readonly IDbContextFactory<AppDbContext> _dbFactory;
    private readonly AppStateService _state;
    private readonly IDiscordNotifier _discord;
    private readonly ILogger<BetReminderNotifier> _logger;

    public BetReminderNotifier(
        IDbContextFactory<AppDbContext> dbFactory,
        AppStateService state,
        IDiscordNotifier discord,
        ILogger<BetReminderNotifier> logger)
    {
        _dbFactory = dbFactory;
        _state = state;
        _discord = discord;
        _logger = logger;
    }

    public async Task EvaluateAndNotifyAsync(DateTime utcNow, CancellationToken ct = default)
    {
        var nowUtc = DateTime.SpecifyKind(utcNow, DateTimeKind.Utc);
        var eastern = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, EasternZone);
        if (eastern.DayOfWeek is not (DayOfWeek.Friday or DayOfWeek.Saturday)) return;

        var slots = new[]
        {
            (Name: "4:00 PM", Time: new TimeSpan(16, 0, 0)),
            (Name: "6:30 PM", Time: new TimeSpan(18, 30, 0)),
        };
        var dueSlots = slots.Where(s => eastern.TimeOfDay >= s.Time).ToList();
        if (dueSlots.Count == 0) return;

        var jstNow = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, TokyoZone);
        var raceDateKey = jstNow.ToString("yyyy-MM-dd");
        var raceDate = DateTime.SpecifyKind(jstNow.Date, DateTimeKind.Utc);

        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        var raceIds = await db.Races.AsNoTracking()
            .Where(r => r.RaceDate == raceDate)
            .Select(r => r.RaceId)
            .ToListAsync(ct);
        if (raceIds.Count == 0) return; // No card loaded; do not nag before the app knows the card.

        var marksBlob = await _state.GetStringAsync("user_marks_blob");
        var lockedIds = LoadLockedRaceIds(marksBlob);
        // This reminder is an initial nudge: once any bet is locked for the card,
        // stop reminding rather than claiming that no bets exist.
        if (raceIds.Any(id => lockedIds.Contains(id))) return;

        var sent = await LoadSentSlotsAsync();
        foreach (var slot in dueSlots)
        {
            var key = $"{raceDateKey}|{slot.Name}";
            if (sent.Contains(key)) continue;

            var delivered = await _discord.NotifyBetReminderAsync(raceDateKey, slot.Name, ct);
            if (!delivered)
            {
                _logger.LogWarning("[BetReminder] {Slot} reminder for {Date} was not delivered; will retry.",
                    slot.Name, raceDateKey);
                continue;
            }

            sent.Add(key);
            _logger.LogInformation("[BetReminder] Sent {Slot} reminder for {Date}.", slot.Name, raceDateKey);
        }

        await SaveSentSlotsAsync(sent);
    }

    private static HashSet<string> LoadLockedRaceIds(string? raw)
    {
        var locked = new HashSet<string>();
        if (string.IsNullOrWhiteSpace(raw)) return locked;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            if (!doc.RootElement.TryGetProperty("raceMeta", out var meta)
                || meta.ValueKind != JsonValueKind.Object) return locked;

            foreach (var prop in meta.EnumerateObject())
            {
                if (prop.Value.ValueKind == JsonValueKind.Object
                    && prop.Value.TryGetProperty("lockStateAtSave", out var state)
                    && state.ValueKind == JsonValueKind.True)
                    locked.Add(prop.Name);
            }
        }
        catch (JsonException) { }
        return locked;
    }

    private async Task<HashSet<string>> LoadSentSlotsAsync()
    {
        var raw = await _state.GetStringAsync(SentStateKey);
        if (string.IsNullOrWhiteSpace(raw)) return new();
        try
        {
            var values = JsonSerializer.Deserialize<string[]>(raw);
            return values is null ? new() : new HashSet<string>(values);
        }
        catch (JsonException) { return new(); }
    }

    private Task SaveSentSlotsAsync(HashSet<string> values)
        => _state.SetStringAsync(SentStateKey, JsonSerializer.Serialize(values.ToArray()));
}
