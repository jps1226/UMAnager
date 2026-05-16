using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Data.Configurations;

public class RaceConfiguration : IEntityTypeConfiguration<Race>
{
    public void Configure(EntityTypeBuilder<Race> builder)
    {
        builder.ToTable("races");

        builder.HasKey(r => r.RaceId);

        builder.Property(r => r.RaceId)
            .HasMaxLength(16)
            .IsRequired();

        builder.Property(r => r.RaceDate)
            .IsRequired();

        builder.Property(r => r.TrackCode)
            .HasMaxLength(2);

        builder.Property(r => r.RaceNumber);

        builder.Property(r => r.NameJa)
            .HasMaxLength(256);

        builder.Property(r => r.Distance);

        builder.Property(r => r.Surface)
            .HasMaxLength(10);  // 'turf' or 'dirt'

        builder.Property(r => r.SortTime);

        builder.Property(r => r.ResultsJson)
            .HasColumnType("jsonb");

        builder.Property(r => r.DataStatus)
            .HasColumnType("smallint")
            .HasDefaultValue((short)0);

        builder.Property(r => r.LastModified)
            .HasColumnType("date");

        builder.Property(r => r.HistoryRefreshed)
            .HasDefaultValue(false);

        builder.Property(r => r.LastUpdated)
            .HasDefaultValueSql("now()")
            .ValueGeneratedOnAdd();

        builder.HasIndex(r => r.RaceId).IsUnique();
        builder.HasIndex(r => r.RaceDate);
    }
}
