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

    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.ApplyConfigurationsFromAssembly(typeof(AppDbContext).Assembly);
}
