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

    // In-flight STREAM_* commands: forwarded to the Sidecar but not yet acknowledged with a completion.
    // The Sidecar's command loop is strictly sequential (one command at a time, exactly one completion
    // per command on every path — success, bad input, and exception), so this is a FIFO and its HEAD is
    // the command actually executing right now.
    //
    // 2026-07-31: this used to be a single "last command forwarded" field, which was wrong whenever more
    // than one command was in flight — the forwarder drains the queue into the pipe back-to-back, so the
    // field held the LAST command sent while the Sidecar was still working on the FIRST. The watchdog
    // then blamed the wrong command: a hung STREAM_DIFN was reported as a hung STREAM_RTCARD, so the
    // "back off the weekly UM refresh" branch never fired and the orchestrator re-enqueued the same
    // hanging pull every tick — a permanent 10-minute Sidecar kill/restart loop.
    private readonly object _inFlightLock = new();
    private readonly Queue<string> _inFlight = new();

    /// <summary>The STREAM_* command the Sidecar is currently executing (FIFO head), or null if idle.</summary>
    public string? ActiveStreamCommand
    {
        get { lock (_inFlightLock) return _inFlight.Count > 0 ? _inFlight.Peek() : null; }
    }

    /// <summary>Record that a STREAM_* command has been handed to the Sidecar.</summary>
    public void MarkCommandForwarded(string command)
    {
        lock (_inFlightLock) _inFlight.Enqueue(command);
    }

    /// <summary>Retire the oldest in-flight command on a STREAM_*_COMPLETE.</summary>
    public void MarkCommandCompleted()
    {
        lock (_inFlightLock) { if (_inFlight.Count > 0) _inFlight.Dequeue(); }
    }

    /// <summary>Drop all in-flight tracking — the Sidecar died or the pipe session ended, so nothing it
    /// was working on will ever complete.</summary>
    public void ClearInFlight()
    {
        lock (_inFlightLock) _inFlight.Clear();
    }

    /// <summary>True if <paramref name="command"/> is anywhere in the in-flight queue, not just at the
    /// head. The watchdog uses this so a wedge is still attributed correctly even if head-tracking is
    /// somehow off — belt-and-braces against exactly the misattribution this class had before.</summary>
    public bool IsInFlight(string command)
    {
        lock (_inFlightLock)
            return _inFlight.Any(c => string.Equals(c, command, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>Snapshot of everything in flight, oldest first — for diagnostics/logging.</summary>
    public IReadOnlyList<string> InFlightCommands
    {
        get { lock (_inFlightLock) return _inFlight.ToArray(); }
    }

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
