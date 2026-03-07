using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MrloicMinervaDPN.Services;

// ── Top-level protocol types ──────────────────────────────────────────────

internal readonly record struct ChunkInfo(
    string ChunkId, string FileId, string Url, long Start, long End);

internal sealed class WsResponse
{
    public static readonly WsResponse Ok = new() { IsOk = true };
    public bool IsOk     { get; init; }
    public bool IsError  { get; init; }
    public bool IsChunks { get; init; }
    public string?          Error  { get; init; }
    public List<ChunkInfo>? Chunks { get; init; }
}

// ── Protocol constants, normalisation, registration, dispatch ─────────────

/// <summary>
/// Minerva coordinator WebSocket binary protocol.
///
/// Client to Server:
///   REGISTER(0):        u8(0)  | u32(version) | u32(max_concurrent) | str(token)
///   GET_CHUNKS(2):      u8(2)  | u32(count)
///   UPLOAD_SUBCHUNK(1): u8(1)  | str(chunk_id) | str(file_id) | bytes(payload)
///   DETACH_CHUNK(3):    u8(3)  | str(chunk_id)
///
/// Server to Client:
///   REGISTER_RESPONSE(128): u8(128) | str(worker_id)
///   CHUNK_RESPONSE(129):    u8(129) | u32(n) | [str|str|str|u64|u64] x n
///   OK_RESPONSE(131):       u8(131) | u32(kv_count) | [str(key)|str(value)] x n
///   ERROR_RESPONSE(130):    u8(130) | u32(kv_count) | [str(key)|str(value)] x n
/// </summary>
internal static class MinervaProtocol
{
    // ── Protocol constants ────────────────────────────────────────────────
    public const uint   ServerVersion  = 4;
    /// <summary>Sub-chunk payload size mandated by the server. Do not change.</summary>
    public const int    SubchunkSize   = 996_147;
    public const double RetryDelaySecs = 5.0;

    // Message type bytes — client to server
    public const byte MsgRegister       = 0;
    public const byte MsgUploadSubchunk = 1;
    public const byte MsgGetChunks      = 2;
    public const byte MsgDetachChunk    = 3;

    // Message type bytes — server to client
    public const byte MsgRegisterResponse = 128;
    public const byte MsgChunkResponse    = 129;
    public const byte MsgErrorResponse    = 130;
    public const byte MsgOkResponse       = 131;

    // ── URL normalisation (mirrors Python's ChunkInfo.normalize_url) ──────
    public static string NormalizeUrl(string url)
    {
        try
        {
            var uri = new Uri(url);
            var decoded = Uri.UnescapeDataString(uri.AbsolutePath);
            var encoded = string.Join("/", decoded.Split('/').Select(Uri.EscapeDataString));
            return $"{uri.Scheme}://{uri.Authority}/{encoded.TrimStart('/')}{uri.Query}";
        }
        catch { return url; }
    }

    // ── Registration response decoder ─────────────────────────────────────
    /// <summary>Parses a REGISTER_RESPONSE frame and returns the worker_id, or throws on error.</summary>
    public static string ParseRegisterResponse(byte[] raw)
    {
        int pos = 0;
        var type = BinaryCodec.ReadU8(raw, ref pos);
        if (type == MsgErrorResponse)
        {
            var kv = BinaryCodec.ReadKv(raw, ref pos);
            throw new Exception($"Registration rejected: {kv.GetValueOrDefault("error", "unknown")}");
        }
        if (type != MsgRegisterResponse)
            throw new Exception($"Unexpected message type {type} during registration");
        var workerId = BinaryCodec.ReadStr(raw, ref pos);
        if (string.IsNullOrEmpty(workerId))
            throw new Exception("Server returned empty worker_id");
        return workerId;
    }

    // ── Incoming message dispatcher ───────────────────────────────────────
    /// <summary>
    /// Decodes a raw server message and resolves the matching pending future,
    /// keyed by <paramref name="workerId"/> for CHUNK_RESPONSE or by chunk_id for OK/ERROR.
    /// </summary>
    public static void DispatchMessage(
        byte[] raw, string workerId,
        ConcurrentDictionary<string, TaskCompletionSource<WsResponse>> futures,
        Action<string> log)
    {
        if (raw.Length == 0) return;
        int pos = 0;
        var type = BinaryCodec.ReadU8(raw, ref pos);

        if (type == MsgChunkResponse)
        {
            var count = (int)BinaryCodec.ReadU32(raw, ref pos);
            var chunks = new List<ChunkInfo>(count);
            for (int i = 0; i < count; i++)
            {
                var cid   = BinaryCodec.ReadStr(raw, ref pos);
                var fid   = BinaryCodec.ReadStr(raw, ref pos);
                var url   = NormalizeUrl(BinaryCodec.ReadStr(raw, ref pos));
                var start = (long)BinaryCodec.ReadU64(raw, ref pos);
                var end   = (long)BinaryCodec.ReadU64(raw, ref pos);
                chunks.Add(new ChunkInfo(cid, fid, url, start, end));
            }
            if (futures.TryRemove(workerId, out var tcs))
                tcs.TrySetResult(new WsResponse { IsChunks = true, Chunks = chunks });
        }
        else if (type is MsgOkResponse or MsgErrorResponse)
        {
            var kv = BinaryCodec.ReadKv(raw, ref pos);
            kv.TryGetValue("chunk_id", out var chunkId);
            var resp = type == MsgOkResponse
                ? WsResponse.Ok
                : new WsResponse { IsError = true, Error = kv.GetValueOrDefault("error", "server error") };
            if (!string.IsNullOrEmpty(chunkId) && futures.TryRemove(chunkId, out var tcs))
                tcs.TrySetResult(resp);
        }
        else if (type != MsgRegisterResponse)
        {
            log($"Received unrecognised message type {type}");
        }
    }
}
