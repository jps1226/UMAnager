using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Data.Configurations;

public class AppSettingConfiguration : IEntityTypeConfiguration<AppSetting>
{
    public void Configure(EntityTypeBuilder<AppSetting> builder)
    {
        builder.ToTable("app_settings");

        builder.HasKey(s => s.Key);

        builder.Property(s => s.Key)
            .HasMaxLength(100)
            .IsRequired();

        builder.Property(s => s.Value)
            .HasColumnType("text");

        builder.Property(s => s.UpdatedAt)
            .HasDefaultValueSql("now()")
            .ValueGeneratedOnAdd();
    }
}
