using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace UMAnager.Nexus.Migrations
{
    /// <inheritdoc />
    public partial class DropRaceEntryForeignKeys : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_race_entries_horses_HorseId",
                table: "race_entries");

            migrationBuilder.DropForeignKey(
                name: "FK_race_entries_races_RaceId",
                table: "race_entries");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddForeignKey(
                name: "FK_race_entries_horses_HorseId",
                table: "race_entries",
                column: "HorseId",
                principalTable: "horses",
                principalColumn: "HorseId",
                onDelete: ReferentialAction.SetNull);

            migrationBuilder.AddForeignKey(
                name: "FK_race_entries_races_RaceId",
                table: "race_entries",
                column: "RaceId",
                principalTable: "races",
                principalColumn: "RaceId",
                onDelete: ReferentialAction.Cascade);
        }
    }
}
