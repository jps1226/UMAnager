using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace UMAnager.Nexus.Migrations
{
    /// <inheritdoc />
    public partial class Phase5A_BreedingHorses : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "breeding_horses",
                columns: table => new
                {
                    HansyokuNum = table.Column<string>(type: "character varying(10)", maxLength: 10, nullable: false),
                    NameJa = table.Column<string>(type: "character varying(256)", maxLength: 256, nullable: false),
                    LastUpdated = table.Column<DateTime>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_breeding_horses", x => x.HansyokuNum);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(name: "breeding_horses");
        }
    }
}
