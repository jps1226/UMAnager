using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Data.Configurations;

public class RaceEntryConfiguration : IEntityTypeConfiguration<RaceEntry>
{
    public void Configure(EntityTypeBuilder<RaceEntry> builder)
    {
        builder.ToTable("race_entries");

        builder.HasKey(e => e.Id);

        builder.Property(e => e.Id)
            .UseIdentityAlwaysColumn();

        builder.Property(e => e.RaceId)
            .HasMaxLength(16)
            .IsRequired();

        builder.Property(e => e.HorseId)
            .HasMaxLength(10)
            .IsRequired();

        builder.Property(e => e.PostPosition);

        builder.Property(e => e.Bracket);

        builder.Property(e => e.Weight);

        builder.Property(e => e.HorseWeight);

        builder.Property(e => e.JockeyName)
            .HasMaxLength(256);

        builder.Property(e => e.Odds)
            .HasPrecision(10, 2);

        builder.Property(e => e.PrevOdds)
            .HasPrecision(10, 2);

        builder.Property(e => e.FavRank);

        builder.Property(e => e.FinishPos);

        builder.Property(e => e.PerformanceJson)
            .HasColumnType("jsonb");

        builder.Property(e => e.DataStatus)
            .HasColumnType("smallint")
            .HasDefaultValue((short)0);

        builder.Property(e => e.LastModified)
            .HasColumnType("date");

        builder.Property(e => e.UpdatedAt)
            .HasDefaultValueSql("now()")
            .ValueGeneratedOnAdd();

        // Indexes
        builder.HasIndex(e => e.RaceId);
        builder.HasIndex(e => e.HorseId);
        builder.HasIndex(e => new { e.RaceId, e.HorseId }).IsUnique();
    }
}
