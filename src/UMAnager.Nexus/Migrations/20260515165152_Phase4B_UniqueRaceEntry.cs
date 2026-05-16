using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace UMAnager.Nexus.Migrations
{
    /// <inheritdoc />
    public partial class Phase4B_UniqueRaceEntry : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_race_entries_RaceId_HorseId",
                table: "race_entries");

            migrationBuilder.CreateIndex(
                name: "IX_race_entries_RaceId_HorseId",
                table: "race_entries",
                columns: new[] { "RaceId", "HorseId" },
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_race_entries_RaceId_HorseId",
                table: "race_entries");

            migrationBuilder.CreateIndex(
                name: "IX_race_entries_RaceId_HorseId",
                table: "race_entries",
                columns: new[] { "RaceId", "HorseId" });
        }
    }
}
