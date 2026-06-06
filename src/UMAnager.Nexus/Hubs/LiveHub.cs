// ============================================================
// FILE: LiveHub.cs
// LAYER: Realtime (SignalR hub, mapped at /hubs/live)
// PURPOSE: Empty marker hub. Clients connect and receive "OddsUpdated" / "ResultsUpdated"
//          messages pushed server-side by LiveBroadcastService — no client-callable methods.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using Microsoft.AspNetCore.SignalR;

namespace UMAnager.Nexus.Hubs;

public sealed class LiveHub : Hub
{
}
