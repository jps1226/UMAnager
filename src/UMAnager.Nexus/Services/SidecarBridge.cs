// ============================================================
// FILE: SidecarBridge.cs
// LAYER: Service (singleton — shared state between API/orchestrator and the pipe server)
// PURPOSE: The mailbox between Nexus and Sidecar. CommandQueue (bounded channel, DropOldest)
//          holds STREAM_* commands; IsConnected/JvLinkVersion/IngestionStatus/StagedRecordCount
//          surface live connection + ingest state.
// CALLED BY: NexusPipeServer (drains queue, sets state), JvLinkController, fetch services, LiveOrchestrator.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Threading.Channels;

namespace UMAnager.Nexus.Services;

public sealed class SidecarBridge
{
    public string JvLinkVersion { get; set; } = "Disconnected";
    public int    InitResult    { get; set; } = -1;
    public bool   IsConnected   => InitResult == 0;

    public Channel<string> CommandQueue { get; } =
        Channel.CreateBounded<string>(new BoundedChannelOptions(8)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
        });

    public int    StagedRecordCount { get; set; }
    public string IngestionStatus   { get; set; } = "Idle"; // "Idle" | "Streaming" | "Complete" | "Error"
}
