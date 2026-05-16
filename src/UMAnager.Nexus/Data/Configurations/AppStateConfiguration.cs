using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Data.Configurations;

public class AppStateConfiguration : IEntityTypeConfiguration<AppState>
{
    public void Configure(EntityTypeBuilder<AppState> builder)
    {
        builder.ToTable("app_state");

        builder.HasKey(a => a.Key);

        builder.Property(a => a.Key)
            .HasMaxLength(100)
            .IsRequired();

        builder.Property(a => a.Value)
            .HasColumnType("text");

        builder.Property(a => a.UpdatedAt)
            .HasDefaultValueSql("now()")
            .ValueGeneratedOnAdd();
    }
}
