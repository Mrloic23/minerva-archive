using System;
using System.Buffers;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

namespace MrloicMinervaDPN.Services;

/// <summary>Low-level WebSocket send/receive helpers.</summary>
internal static class WsHelper
{
    private const int ReceiveBufSize = 64 * 1024;

    /// <summary>Accumulates all WebSocket frames until EndOfMessage and returns the full payload.</summary>
    public static async Task<byte[]> ReceiveFullAsync(ClientWebSocket ws, CancellationToken ct)
    {
        using var ms = new MemoryStream();
        var buf = ArrayPool<byte>.Shared.Rent(ReceiveBufSize);
        try
        {
            WebSocketReceiveResult result;
            do
            {
                result = await ws.ReceiveAsync(buf, ct);
                if (result.MessageType == WebSocketMessageType.Close)
                    throw new WebSocketException("WebSocket closed by server");
                ms.Write(buf, 0, result.Count);
            }
            while (!result.EndOfMessage);
        }
        finally { ArrayPool<byte>.Shared.Return(buf); }
        return ms.ToArray();
    }

    /// <summary>Sends a single binary WebSocket message under a send lock.</summary>
    public static async Task SendAsync(ClientWebSocket ws, SemaphoreSlim lk, byte[] data, CancellationToken ct)
    {
        await lk.WaitAsync(ct);
        try { await ws.SendAsync(data, WebSocketMessageType.Binary, endOfMessage: true, ct); }
        finally { lk.Release(); }
    }

    /// <summary>
    /// Sends multiple segments as one binary WebSocket message under a send lock.
    /// Frames are sent with <c>endOfMessage=false</c> for all but the last segment.
    /// This avoids merging large payloads into a single intermediate byte array.
    /// </summary>
    public static async Task SendAsync(ClientWebSocket ws, SemaphoreSlim lk,
        ReadOnlyMemory<byte>[] segments, CancellationToken ct)
    {
        await lk.WaitAsync(ct);
        try
        {
            for (int i = 0; i < segments.Length; i++)
                await ws.SendAsync(segments[i], WebSocketMessageType.Binary,
                    endOfMessage: i == segments.Length - 1, ct);
        }
        finally { lk.Release(); }
    }
}
