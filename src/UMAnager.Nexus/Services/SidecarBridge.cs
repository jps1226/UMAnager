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

    // Last STREAM_* command forwarded to the Sidecar while IngestionStatus is Streaming. This lets the
    // watchdog distinguish a weekly master-data pull (STREAM_DIFN) from live odds/results/card fetches.
    public string? ActiveStreamCommand { get; set; }

    // "Idle" | "Streaming" | "Complete" | "Error". The setter auto-stamps StreamingSinceUtc on the
    // Idle→Streaming edge and clears it when leaving Streaming, so the LiveOrchestrator watchdog can
    // detect a Sidecar that hung mid-stream WITHOUT ever sending a completion (which would otherwise
    // leave ingest stuck "Streaming" forever — dead for the rest of an unattended weekend). T1-3.
    private string _ingestionStatus = "Idle";
    public string IngestionStatus
    {
        get => _ingestionStatus;
        set
        {
            if (value == "Streaming")
            {
                if (_ingestionStatus != "Streaming") StreamingSinceUtc = DateTime.UtcNow;
            }
            else
            {
                StreamingSinceUtc = null;
            }
            _ingestionStatus = value;
        }
    }

    /// <summary>UTC time the current "Streaming" state began; null when not streaming. Drives the watchdog.</summary>
    public DateTime? StreamingSinceUtc { get; private set; }
}
