using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Data.Configurations;

internal sealed class RawStagingRecordConfiguration : IEntityTypeConfiguration<RawStagingRecord>
{
    public void Configure(EntityTypeBuilder<RawStagingRecord> b)
    {
        b.ToTable("raw_staging");
        b.HasKey(x => x.Id);
        b.Property(x => x.Id).UseIdentityAlwaysColumn();
        b.Property(x => x.RecordType).HasMaxLength(2).IsRequired();
        b.Property(x => x.RawBytes).IsRequired();
        b.Property(x => x.ReceivedAt).HasDefaultValueSql("now()");
        b.Property(x => x.IsProcessed).HasDefaultValue(false);
        b.HasIndex(x => new { x.RecordType, x.IsProcessed });
    }
}
