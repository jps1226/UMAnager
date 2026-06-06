// ============================================================
// FILE: PipeEnvelope.cs  (Nexus)
// LAYER: IPC (binary wire format)
// PURPOSE: The named-pipe message envelope: [4B length LE][2B type LE][payload].
//          PipeMessageType = Command(1) / RawRecord(2) / Status(3). PipeName = "UMAnager_IPC".
// CAUTION: Sidecar has a byte-identical copy (Sidecar/Models/PipeEnvelope.cs) — keep both in sync.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Buffers.Binary;
using System.IO.Pipes;

namespace UMAnager.Nexus.Models;

public enum PipeMessageType : ushort
{
    Command   = 0x0001,
    RawRecord = 0x0002,
    Status    = 0x0003,
}

public record PipeEnvelope(PipeMessageType Type, byte[] Payload)
{
    public const string PipeName = "UMAnager_IPC";

    public static async Task<PipeEnvelope> ReadAsync(PipeStream stream, CancellationToken ct)
    {
        var header = new byte[6];
        await stream.ReadExactlyAsync(header, ct);
        var len  = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(0, 4));
        var type = (PipeMessageType)BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(4, 2));
        var payload = new byte[len];
        if (len > 0)
            await stream.ReadExactlyAsync(payload, ct);
        return new(type, payload);
    }

    public async Task WriteAsync(PipeStream stream, CancellationToken ct)
    {
        var header = new byte[6];
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(0, 4), Payload.Length);
        BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(4, 2), (ushort)Type);
        await stream.WriteAsync(header, ct);
        if (Payload.Length > 0)
            await stream.WriteAsync(Payload, ct);
        await stream.FlushAsync(ct);
    }
}
