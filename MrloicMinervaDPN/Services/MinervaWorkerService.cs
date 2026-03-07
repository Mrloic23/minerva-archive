using System;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MrloicMinervaDPN.Models;

namespace MrloicMinervaDPN.Services;

public sealed class MinervaWorkerService
{
    // ── Public surface ────────────────────────────────────────────────────
    public ObservableCollection<JobProgressViewModel> ActiveJobs { get; } = [];

    // ── Main entry point ──────────────────────────────────────────────────
    public async Task RunAsync(
        WorkerSettings settings, string token,
        Action<string> log, CancellationToken ct,
        Action<long>? onSuccess = null, Action? onFail = null)
    {
        // Convert https:// to wss:// for the coordinator WebSocket endpoint
        var wsUrl = settings.ServerUrl
            .Replace("https://", "wss://")
            .Replace("http://", "ws://")
            .TrimEnd('/') + "/worker";

        while (!ct.IsCancellationRequested)
        {
            log("Connecting to coordinator...");
            using var ws = new ClientWebSocket();
            try { await ws.ConnectAsync(new Uri(wsUrl), ct); }
            catch (OperationCanceledException) { return; }
            catch (Exception ex)
            {
                log($"Connection failed ({ex.Message}), retrying in {MinervaProtocol.RetryDelaySecs}s...");
                await Task.Delay(TimeSpan.FromSeconds(MinervaProtocol.RetryDelaySecs), ct);
                continue;
            }

            // ── Register ──────────────────────────────────────────────────
            var sendLock = new SemaphoreSlim(1, 1);
            string workerId;
            try
            {
                await WsHelper.SendAsync(ws, sendLock,
                    MessageBuilder.BuildRegister(MinervaProtocol.ServerVersion, (uint)settings.Concurrency, token), ct);
                var raw = await WsHelper.ReceiveFullAsync(ws, ct);
                workerId = MinervaProtocol.ParseRegisterResponse(raw);
            }
            catch (OperationCanceledException) { return; }
            catch (Exception ex)
            {
                log($"Registration failed ({ex.Message}), retrying in {MinervaProtocol.RetryDelaySecs}s...");
                await Task.Delay(TimeSpan.FromSeconds(MinervaProtocol.RetryDelaySecs), ct);
                continue;
            }

            log($"Registered as {workerId}. Running {settings.Concurrency} concurrent workers...");

            // Response futures keyed by workerId (chunk responses) or chunkId (subchunk ACKs)
            var futures = new ConcurrentDictionary<string, TaskCompletionSource<WsResponse>>();
            var jobCh = Channel.CreateBounded<ChunkInfo>(
                new BoundedChannelOptions(settings.Concurrency * 2)
                    { FullMode = BoundedChannelFullMode.Wait });

            // linkCts: cancelled on WebSocket disconnect so all tasks unwind, then outer loop reconnects
            using var linkCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

            // ── Receiver ─────────────────────────────────────────────────
            var receiverTask = Task.Run(async () =>
            {
                try
                {
                    while (!linkCts.Token.IsCancellationRequested)
                    {
                        var raw = await WsHelper.ReceiveFullAsync(ws, linkCts.Token);
                        if (raw.Length == 0) continue;
                        MinervaProtocol.DispatchMessage(raw, workerId, futures, log);
                    }
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) { log($"Receiver error: {ex.Message}"); }
                finally { linkCts.Cancel(); }
            });

            // ── Producer ──────────────────────────────────────────────────
            var producerTask = Task.Run(async () =>
            {
                try
                {
                    while (!linkCts.Token.IsCancellationRequested)
                    {
                        // Back off when the job channel is already full
                        if (jobCh.Reader.Count >= settings.Concurrency)
                        {
                            await Task.Delay(1000, linkCts.Token);
                            continue;
                        }

                        var freeSlots = (uint)Math.Max(0, settings.Concurrency - jobCh.Reader.Count);
                        if (freeSlots == 0) { await Task.Delay(1000, linkCts.Token); continue; }

                        var tcs = new TaskCompletionSource<WsResponse>(
                            TaskCreationOptions.RunContinuationsAsynchronously);
                        futures[workerId] = tcs;
                        await WsHelper.SendAsync(ws, sendLock, MessageBuilder.BuildGetChunks(freeSlots), linkCts.Token);

                        WsResponse resp;
                        try { resp = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(30), linkCts.Token); }
                        catch { futures.TryRemove(workerId, out _); throw; }

                        if (resp.IsError)
                        {
                            log($"GetChunks error: {resp.Error}");
                            await Task.Delay(TimeSpan.FromSeconds(MinervaProtocol.RetryDelaySecs), linkCts.Token);
                            continue;
                        }

                        if (resp.Chunks is { Count: > 0 } chunks)
                        {
                            log($"Received {chunks.Count} chunk(s)");
                            foreach (var c in chunks)
                                await jobCh.Writer.WriteAsync(c, linkCts.Token);
                        }
                        else
                        {
                            // Server has no jobs available right now
                            await Task.Delay(TimeSpan.FromSeconds(MinervaProtocol.RetryDelaySecs), linkCts.Token);
                        }
                    }
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) { log($"Producer error: {ex.Message}"); linkCts.Cancel(); }
                finally { jobCh.Writer.TryComplete(); }
            });

            // ── Worker pool ───────────────────────────────────────────────
            var workerTasks = Enumerable.Range(0, settings.Concurrency).Select(_ => Task.Run(async () =>
            {
                await foreach (var chunk in jobCh.Reader.ReadAllAsync(CancellationToken.None))
                {
                    var vm = new JobProgressViewModel();

                    // Extract a display label from the URL
                    var rawName = chunk.Url.Split('?')[0].Split('/').Last();
                    var label = Uri.UnescapeDataString(rawName);
                    if (label.Length > 60) label = "..." + label[^57..];

                    UIHelper.UI(() =>
                    {
                        vm.Label = label;
                        vm.MaxAttempts = settings.Retries;
                        ActiveJobs.Add(vm);
                    });

                    bool success = false;
                    for (int attempt = 1; attempt <= settings.Retries
                                        && !linkCts.Token.IsCancellationRequested; attempt++)
                    {
                        try
                        {
                            long total = chunk.End - chunk.Start;
                            UIHelper.UI(() =>
                            {
                                vm.CurrentAttempt = attempt;
                                vm.Status = JobStatus.Downloading;
                                vm.BytesTransferred = 0;
                                vm.TotalBytes = total;
                                vm.ErrorMessage = "";
                                vm.ResetSpeed();
                            });

                            await ChunkProcessor.ProcessChunkAsync(chunk, ws, sendLock, futures, vm, linkCts.Token);
                            UIHelper.UI(() => { vm.Status = JobStatus.Done; vm.SpeedText = ""; });
                            onSuccess?.Invoke(chunk.End - chunk.Start);
                            log($"OK [{label}]");
                            success = true;
                            break;
                        }
                        catch (OperationCanceledException) { break; }
                        catch (WebSocketException ex)
                        {
                            // WebSocket is dead — break and let the outer loop reconnect
                            log($"WS error on chunk [{label}]: {ex.Message}");
                            linkCts.Cancel();
                            break;
                        }
                        catch (Exception ex)
                        {
                            log($"Chunk attempt {attempt}/{settings.Retries} failed [{label}]: {ex.Message}");
                            UIHelper.UI(() =>
                            {
                                vm.Status = JobStatus.Retrying;
                                vm.SpeedText = "";
                                vm.ErrorMessage = ex.Message;
                            });
                            if (attempt < settings.Retries)
                                await Task.Delay(TimeSpan.FromSeconds(MinervaProtocol.RetryDelaySecs * attempt), linkCts.Token)
                                    .ConfigureAwait(false);
                        }
                    }

                    if (!success)
                    {
                        UIHelper.UI(() => { vm.Status = JobStatus.Failed; vm.SpeedText = ""; });
                        if (!ct.IsCancellationRequested) onFail?.Invoke();
                        // Tell the server to free the chunk for reassignment
                        try { await WsHelper.SendAsync(ws, sendLock, MessageBuilder.BuildDetachChunk(chunk.ChunkId), CancellationToken.None); }
                        catch { }
                    }

                    await Task.Delay(TimeSpan.FromSeconds(3), CancellationToken.None);
                    UIHelper.UI(() => ActiveJobs.Remove(vm));
                }
            })).ToArray();

            // Wait for any task to exit, then tear down and reconnect
            await Task.WhenAny([receiverTask, producerTask, .. workerTasks]);
            linkCts.Cancel();
            try { await Task.WhenAll([receiverTask, producerTask, .. workerTasks]); } catch { }

            log("Disconnected from coordinator.");
            if (!ct.IsCancellationRequested)
            {
                log($"Reconnecting in {MinervaProtocol.RetryDelaySecs}s...");
                await Task.Delay(TimeSpan.FromSeconds(MinervaProtocol.RetryDelaySecs), ct);
            }
        }

        log("Worker stopped.");
    }

    /// <summary>Safe to call from any thread. Stopping is handled via the CancellationToken.</summary>
    public Task StopAsync() => Task.CompletedTask;
}
