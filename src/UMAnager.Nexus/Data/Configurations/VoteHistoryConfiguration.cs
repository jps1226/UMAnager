using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using UMAnager.Nexus.Data.Entities;

namespace UMAnager.Nexus.Data.Configurations;

public class VoteHistoryConfiguration : IEntityTypeConfiguration<VoteHistory>
{
    public void Configure(EntityTypeBuilder<VoteHistory> builder)
    {
        builder.ToTable("vote_history");

        builder.HasKey(e => e.Id);

        builder.Property(e => e.Id)
            .UseIdentityAlwaysColumn();

        builder.Property(e => e.HorseId)
            .HasMaxLength(10)
            .IsRequired();

        builder.Property(e => e.RaceId)
            .HasMaxLength(16)
            .IsRequired();

        builder.Property(e => e.Mark)
            .HasMaxLength(4)
            .IsRequired();

        builder.Property(e => e.VotedAt)
            .IsRequired();

        // Unique per race+horse so re-applying updates rather than duplicating.
        builder.HasIndex(e => new { e.RaceId, e.HorseId })
            .IsUnique();

        // Badge lookup: "how many times has this horse been voted?"
        builder.HasIndex(e => e.HorseId);
    }
}
