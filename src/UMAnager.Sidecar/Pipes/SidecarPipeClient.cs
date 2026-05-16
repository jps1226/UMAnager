using System.IO.Pipes;
using System.Text;
using System.Text.Json;
using UMAnager.Sidecar.Models;

namespace UMAnager.Sidecar.Pipes;

internal sealed class SidecarPipeClient
{
    private readonly NamedPipeClientStream _pipe;

    public SidecarPipeClient()
    {
        _pipe = new NamedPipeClientStream(".", PipeEnvelope.PipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
    }

    public async Task ConnectAsync(CancellationToken ct)
    {
        Console.WriteLine("[Sidecar] Connecting to Nexus pipe...");
        await _pipe.ConnectAsync(ct);
        Console.WriteLine("[Sidecar] Connected to Nexus.");
    }

    public async Task<string> WaitForCommandAsync(CancellationToken ct)
    {
        var envelope = await PipeEnvelope.ReadAsync(_pipe, ct);
        return Encoding.UTF8.GetString(envelope.Payload);
    }

    public async Task SendStatusAsync(string jvlinkVersion, int initResult, CancellationToken ct)
    {
        var payload = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new
        {
            jvlink_version = jvlinkVersion,
            init_result    = initResult,
        }));
        await new PipeEnvelope(PipeMessageType.Status, payload).WriteAsync(_pipe, ct);
    }

    public async Task<string> WaitForNextCommandAsync(CancellationToken ct)
    {
        var envelope = await PipeEnvelope.ReadAsync(_pipe, ct);
        return Encoding.UTF8.GetString(envelope.Payload);
    }

    public async Task SendRawRecordAsync(byte[] rawBytes, CancellationToken ct)
        => await new PipeEnvelope(PipeMessageType.RawRecord, rawBytes).WriteAsync(_pipe, ct);

    public async Task SendStreamCompleteAsync(int recordCount, int skippedCount, CancellationToken ct)
    {
        var payload = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new
        {
            event_type    = "STREAM_DIFN_COMPLETE",
            record_count  = recordCount,
            skipped_count = skippedCount,
        }));
        await new PipeEnvelope(PipeMessageType.Status, payload).WriteAsync(_pipe, ct);
    }

    public async Task SendTokuCompleteAsync(int recordCount, int skippedCount, string lastFileTimestamp, CancellationToken ct)
    {
        var payload = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new
        {
            event_type          = "STREAM_TOKU_COMPLETE",
            record_count        = recordCount,
            skipped_count       = skippedCount,
            last_file_timestamp = lastFileTimestamp,
        }));
        await new PipeEnvelope(PipeMessageType.Status, payload).WriteAsync(_pipe, ct);
    }

    public async Task SendOddsCompleteAsync(int recordCount, int skippedCount, CancellationToken ct)
    {
        var payload = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new
        {
            event_type    = "STREAM_ODDS_COMPLETE",
            record_count  = recordCount,
            skipped_count = skippedCount,
        }));
        await new PipeEnvelope(PipeMessageType.Status, payload).WriteAsync(_pipe, ct);
    }

    public void Dispose() => _pipe.Dispose();
}
