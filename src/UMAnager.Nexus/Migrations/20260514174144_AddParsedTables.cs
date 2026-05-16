using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace UMAnager.Nexus.Migrations
{
    /// <inheritdoc />
    public partial class AddParsedTables : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "horses",
                columns: table => new
                {
                    HorseId = table.Column<string>(type: "character varying(10)", maxLength: 10, nullable: false),
                    NameJa = table.Column<string>(type: "character varying(256)", maxLength: 256, nullable: false),
                    NameEn = table.Column<string>(type: "character varying(256)", maxLength: 256, nullable: true),
                    BirthYear = table.Column<int>(type: "integer", nullable: true),
                    SireId = table.Column<string>(type: "character varying(10)", maxLength: 10, nullable: true),
                    DamId = table.Column<string>(type: "character varying(10)", maxLength: 10, nullable: true),
                    BmsId = table.Column<string>(type: "character varying(10)", maxLength: 10, nullable: true),
                    PedigreeJson = table.Column<string>(type: "jsonb", nullable: true),
                    LastUpdated = table.Column<DateTime>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_horses", x => x.HorseId);
                });

            migrationBuilder.CreateTable(
                name: "races",
                columns: table => new
                {
                    RaceId = table.Column<string>(type: "character varying(16)", maxLength: 16, nullable: false),
                    RaceDate = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    TrackCode = table.Column<string>(type: "character varying(2)", maxLength: 2, nullable: true),
                    RaceNumber = table.Column<int>(type: "integer", nullable: true),
                    NameJa = table.Column<string>(type: "character varying(256)", maxLength: 256, nullable: true),
                    Distance = table.Column<int>(type: "integer", nullable: true),
                    Surface = table.Column<string>(type: "character varying(10)", maxLength: 10, nullable: true),
                    SortTime = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ResultsJson = table.Column<string>(type: "jsonb", nullable: true),
                    HistoryRefreshed = table.Column<bool>(type: "boolean", nullable: false, defaultValue: false),
                    LastUpdated = table.Column<DateTime>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_races", x => x.RaceId);
                });

            migrationBuilder.CreateTable(
                name: "race_entries",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityAlwaysColumn),
                    RaceId = table.Column<string>(type: "character varying(16)", maxLength: 16, nullable: false),
                    HorseId = table.Column<string>(type: "character varying(10)", maxLength: 10, nullable: false),
                    PostPosition = table.Column<int>(type: "integer", nullable: true),
                    Bracket = table.Column<int>(type: "integer", nullable: true),
                    Weight = table.Column<int>(type: "integer", nullable: true),
                    HorseWeight = table.Column<int>(type: "integer", nullable: true),
                    JockeyName = table.Column<string>(type: "character varying(256)", maxLength: 256, nullable: true),
                    Odds = table.Column<decimal>(type: "numeric(10,2)", precision: 10, scale: 2, nullable: true),
                    FavRank = table.Column<int>(type: "integer", nullable: true),
                    FinishPos = table.Column<int>(type: "integer", nullable: true),
                    PerformanceJson = table.Column<string>(type: "jsonb", nullable: true),
                    UpdatedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_race_entries", x => x.Id);
                    table.ForeignKey(
                        name: "FK_race_entries_horses_HorseId",
                        column: x => x.HorseId,
                        principalTable: "horses",
                        principalColumn: "HorseId",
                        onDelete: ReferentialAction.SetNull);
                    table.ForeignKey(
                        name: "FK_race_entries_races_RaceId",
                        column: x => x.RaceId,
                        principalTable: "races",
                        principalColumn: "RaceId",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_horses_HorseId",
                table: "horses",
                column: "HorseId",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_race_entries_HorseId",
                table: "race_entries",
                column: "HorseId");

            migrationBuilder.CreateIndex(
                name: "IX_race_entries_RaceId",
                table: "race_entries",
                column: "RaceId");

            migrationBuilder.CreateIndex(
                name: "IX_race_entries_RaceId_HorseId",
                table: "race_entries",
                columns: new[] { "RaceId", "HorseId" });

            migrationBuilder.CreateIndex(
                name: "IX_races_RaceDate",
                table: "races",
                column: "RaceDate");

            migrationBuilder.CreateIndex(
                name: "IX_races_RaceId",
                table: "races",
                column: "RaceId",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "race_entries");

            migrationBuilder.DropTable(
                name: "horses");

            migrationBuilder.DropTable(
                name: "races");
        }
    }
}
