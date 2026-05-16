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
