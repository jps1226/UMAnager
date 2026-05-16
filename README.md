# UMAnager v2.0

A high-performance Japanese thoroughbred racing analysis and betting platform built on JRA-VAN's JV-Link data feed.

This is a ground-up clean-room rewrite of UMAnager v1.0 (Python, archived on the `v1.0-python-archive` branch). v2.0 uses a split-process architecture to escape the 32-bit limitations of JV-Link's COM interface while keeping a modern, fast .NET backend.

## Architecture

| Process | Bitness | Role |
|:---|:---|:---|
| **Nexus** (`src/UMAnager.Nexus`) | x64 | ASP.NET Core 8 — parses raw records with `Span<T>`, persists to PostgreSQL, serves the dashboard, broadcasts SignalR. |
| **Sidecar** (`src/UMAnager.Sidecar`) | x86 | .NET 8 console — hosts JV-Link COM on an STA thread, streams raw records over a Named Pipe to Nexus. |
| **Tray** (`src/UMAnager.Tray`) | any | WinForms launcher and status indicator. |

The two processes communicate over a **persistent Named Pipe** with a 4-byte-length + 2-byte-type binary envelope. See `CLAUDE.md` for the full spec.

## Prerequisites

- Windows 10/11
- .NET 8 SDK (with both x64 and x86 runtimes installed)
- PostgreSQL 15+
- A licensed **JRA-VAN DataLab subscription** and the JV-Link SDK installed
- The following JV-Link SDK files dropped into the repo root (they are gitignored — not redistributable):
  - `JVData_Struct.cs`
  - `JVDTLab.IDL`

## Setup

1. Copy the example settings files and fill in real values:
   ```
   cp src/UMAnager.Nexus/appsettings.example.json   src/UMAnager.Nexus/appsettings.json
   cp src/UMAnager.Sidecar/appsettings.example.json src/UMAnager.Sidecar/appsettings.json
   ```
2. Create the Postgres database and run EF Core migrations from `src/UMAnager.Nexus`.
3. Launch via `launch-services.ps1` or the tray app.

## Project Documentation

- **`CLAUDE.md`** — master specification, architecture, phase roadmap, and ground rules. Start here.
- **`BACKEND_API_SPEC.md`** — HTTP API contract that the frontend depends on.
- **`jra_van_offsets.md`** — JRA-VAN record byte-offset reference.
- **`current_state.md`** / **`dev_log.md`** — live project status and session history.
- **`ORACLE_ANSWERS.md`** / **`LIBRARIAN_ANSWERS.md`** — Q&A archive from the JRA-VAN documentation oracle and the kmy-keiba reference project.

## Status

Phases 0–4 complete. Phase 5 (live SignalR pipeline) in progress.
