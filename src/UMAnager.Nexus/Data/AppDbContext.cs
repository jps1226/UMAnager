// ============================================================
// FILE: AppDbContext.cs
// LAYER: Data (EF Core DbContext)
// PURPOSE: EF context for the EF-managed tables (raw_staging, horses, races, race_entries,
//          app_state, app_settings, breeding_horses, odds_history, vote_history). Applies
//          all IEntityTypeConfiguration from Data/Configurations.
// CAUTION: jockeys, trainers, user_horse_lists, and the sire_performance MV are raw-SQL
//          objects — NOT DbSets here. Schema evolves via inline DDL in Program.cs, not migrations.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using Microsoft.EntityFrameworkCore;
using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Data;

public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<RawStagingRecord> RawStagingRecords => Set<RawStagingRecord>();
    public DbSet<Horse> Horses => Set<Horse>();
    public DbSet<Race> Races => Set<Race>();
    public DbSet<RaceEntry> RaceEntries => Set<RaceEntry>();
    public DbSet<AppState> AppState => Set<AppState>();
    public DbSet<AppSetting> AppSettings => Set<AppSetting>();
    public DbSet<BreedingHorse> BreedingHorses => Set<BreedingHorse>();
    public DbSet<OddsHistory> OddsHistory => Set<OddsHistory>();
    public DbSet<VoteHistory> VoteHistory => Set<VoteHistory>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.ApplyConfigurationsFromAssembly(typeof(AppDbContext).Assembly);
}
