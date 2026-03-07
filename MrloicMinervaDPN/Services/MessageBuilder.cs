using System.IO;
using System.Text;

namespace MrloicMinervaDPN.Services;

/// <summary>Builds binary wire messages sent from the worker client to the Minerva coordinator.</summary>
internal static class MessageBuilder
{
    public static byte[] BuildRegister(uint version, uint concurrency, string token)
    {
        using var ms = new MemoryStream(1 + 4 + 4 + 4 + Encoding.UTF8.GetByteCount(token));
        BinaryCodec.WriteU8(ms, MinervaProtocol.MsgRegister);
        BinaryCodec.WriteU32(ms, version);
        BinaryCodec.WriteU32(ms, concurrency);
        BinaryCodec.WriteStr(ms, token);
        return ms.ToArray();
    }

    public static byte[] BuildGetChunks(uint count)
    {
        using var ms = new MemoryStream(5);
        BinaryCodec.WriteU8(ms, MinervaProtocol.MsgGetChunks);
        BinaryCodec.WriteU32(ms, count);
        return ms.ToArray();
    }

    /// <summary>
    /// Builds just the header for an UPLOAD_SUBCHUNK message (without the payload bytes).
    /// Use with <see cref="WsHelper.SendAsync(System.Net.WebSockets.ClientWebSocket,System.Threading.SemaphoreSlim,System.ReadOnlyMemory{byte}[],System.Threading.CancellationToken)"/>
    /// to send header + payload as two WebSocket frames, avoiding a large intermediate allocation.
    /// </summary>
    public static byte[] BuildUploadSubchunkHeader(string chunkId, string fileId, int payloadLen)
    {
        using var ms = new MemoryStream(1 + 4 + System.Text.Encoding.UTF8.GetByteCount(chunkId)
                                          + 4 + System.Text.Encoding.UTF8.GetByteCount(fileId) + 4);
        BinaryCodec.WriteU8(ms, MinervaProtocol.MsgUploadSubchunk);
        BinaryCodec.WriteStr(ms, chunkId);
        BinaryCodec.WriteStr(ms, fileId);
        BinaryCodec.WriteU32(ms, (uint)payloadLen); // length prefix for payload
        return ms.ToArray();
    }

    public static byte[] BuildDetachChunk(string chunkId)
    {
        using var ms = new MemoryStream(1 + 4 + chunkId.Length);
        BinaryCodec.WriteU8(ms, MinervaProtocol.MsgDetachChunk);
        BinaryCodec.WriteStr(ms, chunkId);
        return ms.ToArray();
    }
}
