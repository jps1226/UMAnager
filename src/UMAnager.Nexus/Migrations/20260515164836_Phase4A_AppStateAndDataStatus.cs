using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace UMAnager.Nexus.Migrations
{
    /// <inheritdoc />
    public partial class Phase4A_AppStateAndDataStatus : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<short>(
                name: "DataStatus",
                table: "races",
                type: "smallint",
                nullable: false,
                defaultValue: (short)0);

            migrationBuilder.AddColumn<DateOnly>(
                name: "LastModified",
                table: "races",
                type: "date",
                nullable: true);

            migrationBuilder.AddColumn<short>(
                name: "DataStatus",
                table: "race_entries",
                type: "smallint",
                nullable: false,
                defaultValue: (short)0);

            migrationBuilder.AddColumn<DateOnly>(
                name: "LastModified",
                table: "race_entries",
                type: "date",
                nullable: true);

            migrationBuilder.CreateTable(
                name: "app_state",
                columns: table => new
                {
                    Key = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: false),
                    Value = table.Column<string>(type: "text", nullable: true),
                    UpdatedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_app_state", x => x.Key);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "app_state");

            migrationBuilder.DropColumn(
                name: "DataStatus",
                table: "races");

            migrationBuilder.DropColumn(
                name: "LastModified",
                table: "races");

            migrationBuilder.DropColumn(
                name: "DataStatus",
                table: "race_entries");

            migrationBuilder.DropColumn(
                name: "LastModified",
                table: "race_entries");
        }
    }
}
