using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace UMAnager.Nexus.Migrations
{
    /// <inheritdoc />
    public partial class Phase5_1_AppSettings : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "app_settings",
                columns: table => new
                {
                    Key = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: false),
                    Value = table.Column<string>(type: "text", nullable: true),
                    UpdatedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_app_settings", x => x.Key);
                });

            // NOTE: scaffolder also wanted to create breeding_horses here — pre-existing snapshot/DB
            // drift from before this migration. Table already exists in production DB. Leaving the
            // snapshot's view of breeding_horses intact so it stays in the model, but skipping the
            // CreateTable here so this migration applies cleanly to existing databases.
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "app_settings");
        }
    }
}
