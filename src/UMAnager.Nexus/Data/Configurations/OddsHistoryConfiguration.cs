using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Data.Configurations;

public class OddsHistoryConfiguration : IEntityTypeConfiguration<OddsHistory>
{
    public void Configure(EntityTypeBuilder<OddsHistory> builder)
    {
        builder.ToTable("odds_history");

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

        builder.Property(e => e.Odds)
            .HasPrecision(10, 2)
            .IsRequired();

        builder.Property(e => e.FavRank);

        builder.Property(e => e.CapturedAt)
            .HasDefaultValueSql("now()");

        // Read path is always "all snapshots for one race, ordered by time".
        builder.HasIndex(e => new { e.RaceId, e.CapturedAt });
    }
}
