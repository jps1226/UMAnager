using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Data.Configurations;

public class BreedingHorseConfiguration : IEntityTypeConfiguration<BreedingHorse>
{
    public void Configure(EntityTypeBuilder<BreedingHorse> builder)
    {
        builder.ToTable("breeding_horses");

        builder.HasKey(b => b.HansyokuNum);

        builder.Property(b => b.HansyokuNum)
            .HasMaxLength(10)
            .IsRequired();

        builder.Property(b => b.NameJa)
            .HasMaxLength(256)
            .IsRequired();

        builder.Property(b => b.NameEn)
            .HasMaxLength(256);

        builder.Property(b => b.LastUpdated)
            .HasDefaultValueSql("now()")
            .ValueGeneratedOnAdd();
    }
}
