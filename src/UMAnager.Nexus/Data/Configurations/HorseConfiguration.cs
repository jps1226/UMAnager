using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Data.Configurations;

public class HorseConfiguration : IEntityTypeConfiguration<Horse>
{
    public void Configure(EntityTypeBuilder<Horse> builder)
    {
        builder.ToTable("horses");

        builder.HasKey(h => h.HorseId);

        builder.Property(h => h.HorseId)
            .HasMaxLength(10)
            .IsRequired();

        builder.Property(h => h.NameJa)
            .HasMaxLength(256)
            .IsRequired();

        builder.Property(h => h.NameEn)
            .HasMaxLength(256);

        builder.Property(h => h.BirthYear);

        builder.Property(h => h.SireId)
            .HasMaxLength(10);

        builder.Property(h => h.DamId)
            .HasMaxLength(10);

        builder.Property(h => h.BmsId)
            .HasMaxLength(10);

        builder.Property(h => h.PedigreeJson)
            .HasColumnType("jsonb");

        builder.Property(h => h.LastUpdated)
            .HasDefaultValueSql("now()")
            .ValueGeneratedOnAdd();

        builder.HasIndex(h => h.HorseId).IsUnique();
    }
}
