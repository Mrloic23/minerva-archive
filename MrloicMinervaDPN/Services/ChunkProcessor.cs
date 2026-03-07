using System;
using System.Collections.Concurrent;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using MrloicMinervaDPN.Models;

namespace MrloicMinervaDPN.Services;

/// <summary>Downloads an assigned byte range and streams it to the coordinator as sub-chunk messages.</summary>
internal static class ChunkProcessor
{
    // A single shared client avoids socket exhaustion from repeated construction.
    private static readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(300) };
    /// <summary>
    /// Downloads the assigned byte range from <see cref="ChunkInfo.Url"/> using an HTTP Range
    /// request, then streams the data back to the coordinator as UPLOAD_SUBCHUNK messages,
    /// waiting for an OK/ERROR ACK from the server after each sub-chunk before sending the next.
    /// </summary>
    public static async Task ProcessChunkAsync(
        ChunkInfo chunk,
        ClientWebSocket ws,
        SemaphoreSlim sendLock,
        ConcurrentDictionary<string, TaskCompletionSource<WsResponse>> futures,
        JobProgressViewModel vm,
        CancellationToken ct)
    {
        var total = chunk.End - chunk.Start;

        using var req  = new HttpRequestMessage(HttpMethod.Get, chunk.Url);
        req.Headers.Range = new RangeHeaderValue(chunk.Start, chunk.End - 1); // inclusive
        req.Headers.Add("User-Agent", $"HyperscrapeWorker/v{MinervaProtocol.ServerVersion}");

        using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct);
        resp.EnsureSuccessStatusCode();

        await using var stream = await resp.Content.ReadAsStreamAsync(ct);
        var buf = new byte[MinervaProtocol.SubchunkSize];
        long progress = 0;
        int read;

        while ((read = await stream.ReadAsync(buf, ct)) > 0)
        {
            UIHelper.UI(() =>
            {
                vm.Status = JobStatus.Downloading;
                vm.BytesTransferred = progress;
                vm.TotalBytes = total;
                vm.UpdateSpeed(progress);
            });

            // Register a future keyed by chunk_id so the receiver can resolve it,
            // send the sub-chunk, then block until we get the server's ACK.
            var tcs = new TaskCompletionSource<WsResponse>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            futures[chunk.ChunkId] = tcs;

            // Send header + payload as two WebSocket frames to avoid merging into one large byte[].
            var header = MessageBuilder.BuildUploadSubchunkHeader(chunk.ChunkId, chunk.FileId, read);
            await WsHelper.SendAsync(ws, sendLock,
                [header.AsMemory(), buf.AsMemory(0, read)], ct);

            WsResponse ack;
            try { ack = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(60), ct); }
            catch { futures.TryRemove(chunk.ChunkId, out _); throw; }

            if (ack.IsError)
                throw new InvalidOperationException($"Server rejected sub-chunk: {ack.Error}");

            progress += read;
            UIHelper.UI(() =>
            {
                vm.Status = JobStatus.Uploading;
                vm.BytesTransferred = progress;
                vm.TotalBytes = total;
                vm.UpdateSpeed(progress);
            });
        }
    }
}
