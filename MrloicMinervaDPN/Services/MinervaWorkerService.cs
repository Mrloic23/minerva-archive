using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Aria2NET;
using MrloicMinervaDPN.Models;

namespace MrloicMinervaDPN.Services;
public sealed class MinervaWorkerService
{
    private const string Version = "1.2.4";
    private const int MaxRetries = 3;
    private const int RetryDelaySeconds = 5;
    private const long UploadChunkSize = 8 * 1024 * 1024;

    private static readonly int[] RetriableStatusCodes = [408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524];

    private Process? _aria2cProc;
    private Aria2NetClient? _aria2c;
    private string _aria2cSecret = "";

    public ObservableCollection<JobProgressViewModel> ActiveJobs { get; } = [];

    private static int FindFreePort()
    {
        var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        int port = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    private async Task<bool> TryStartAria2cDaemonAsync(int conns, bool autoInstall, Action<string> log, CancellationToken ct)
    {
        var aria2cPath = await Aria2cInstaller.EnsureAsync(log, autoInstall, ct);
        if (aria2cPath is null) return false;

        int port = FindFreePort();
        _aria2cSecret = Guid.NewGuid().ToString("N");
        var psi = new ProcessStartInfo(aria2cPath)
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        psi.ArgumentList.Add("--enable-rpc=true");
        psi.ArgumentList.Add($"--rpc-listen-port={port}");
        psi.ArgumentList.Add($"--rpc-secret={_aria2cSecret}");
        psi.ArgumentList.Add("--rpc-listen-all=false");
        psi.ArgumentList.Add("--quiet=true");
        psi.ArgumentList.Add($"--max-connection-per-server={conns}");
        psi.ArgumentList.Add($"--split={conns}");
        psi.ArgumentList.Add("--auto-file-renaming=false");
        psi.ArgumentList.Add("--allow-overwrite=true");
        try
        {
            _aria2cProc = Process.Start(psi);
            if (_aria2cProc is null) return false;
        }
        catch { return false; }

        var client = new Aria2NetClient($"http://127.0.0.1:{port}/jsonrpc", _aria2cSecret, retryCount: 1);
        for (int i = 0; i < 20; i++)
        {
            try { await client.GetVersionAsync(ct); _aria2c = client; return true; }
            catch { await Task.Delay(250, ct).ConfigureAwait(false); }
        }
        try { _aria2cProc?.Kill(); } catch { }
        _aria2cProc = null;
        return false;
    }

    private void StopAria2cDaemon()
    {
        var client = _aria2c;
        _aria2c = null;
        // Fire-and-forget the RPC shutdown; then give the process a moment to exit on its own.
        try { client?.ShutdownAsync().GetAwaiter().GetResult(); } catch { }
        if (_aria2cProc is { HasExited: false })
        {
            try { if (!_aria2cProc.WaitForExit(2000)) _aria2cProc.Kill(); }
            catch { }
        }
        _aria2cProc = null;
    }

    /// <summary>Cancels any running work and asynchronously kills the aria2c daemon.
    /// Safe to call more than once and from any thread.</summary>
    public Task StopAsync()
    {
        _aria2c = null;  // prevent new submissions
        return Task.Run(StopAria2cDaemon);
    }

    public void Stop() => StopAria2cDaemon();

    private static void AddWorkerHeaders(HttpClient client, string token)
    {
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
        client.DefaultRequestHeaders.Add("X-Minerva-Worker-Version", Version);
    }

    private static bool IsRetriable(int code) =>
        Array.IndexOf(RetriableStatusCodes, code) >= 0;

    private static double RetrySleep(int attempt, double cap = 25.0) =>
        Math.Min(cap, 0.85 * attempt + Random.Shared.NextDouble() * 1.25);

    private static string SanitizePart(string part)
    {
        const string bad = "<>:\"/\\|?*";
        var sb = new StringBuilder(part.Length);
        foreach (var ch in part)
            sb.Append(bad.Contains(ch) || ch < 32 ? '_' : ch);
        var cleaned = sb.ToString().Trim().TrimEnd('.');
        return string.IsNullOrEmpty(cleaned) ? "_" : cleaned;
    }

    private static string LocalPathForJob(string tempDir, string url, string destPath)
    {
        var uri = new Uri(url);
        var host = SanitizePart(uri.Host);
        var decoded = Uri.UnescapeDataString(destPath).TrimStart('/');
        var segments = decoded.Split('/');
        var parts = new string[segments.Length];
        for (int i = 0; i < segments.Length; i++)
            parts[i] = SanitizePart(segments[i]);
        var allParts = new string[parts.Length + 2];
        allParts[0] = tempDir;
        allParts[1] = host;
        parts.CopyTo(allParts, 2);
        return Path.Combine(allParts);
    }

    private async Task DownloadFileAsync(string url, string dest, int aria2cConns, long knownSize,
        CancellationToken ct, Action<long, long, long>? onProgress = null)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
        if (_aria2c is { } aria2c)
        {
            string? gid = null;
            try
            {
                gid = await aria2c.AddUriAsync(
                    [url],
                    new Dictionary<string, object>
                    {
                        { "dir",                      Path.GetDirectoryName(dest)! },
                        { "out",                      Path.GetFileName(dest) },
                        { "max-connection-per-server", aria2cConns.ToString() },
                        { "split",                    aria2cConns.ToString() },
                        { "min-split-size",           "1M" },
                        { "allow-overwrite",          "true" },
                        { "auto-file-renaming",       "false" },
                    }, cancellationToken: ct);

                while (true)
                {
                    ct.ThrowIfCancellationRequested();
                    await Task.Delay(400, ct);
                    var status = await aria2c.TellStatusAsync(gid, ct);
                    onProgress?.Invoke(status.CompletedLength, status.TotalLength, status.DownloadSpeed);
                    if (status.Status == "complete") break;
                    if (status.Status == "error")
                        throw new InvalidOperationException(
                            $"aria2c error: {status.ErrorMessage ?? status.ErrorCode ?? "unknown"}");
                }
                try { await aria2c.RemoveDownloadResultAsync(gid); } catch { }
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                if (gid != null)
                {
                    try { await aria2c.ForceRemoveAsync(gid); } catch { }
                    try { await aria2c.RemoveDownloadResultAsync(gid); } catch { }
                }
                throw;
            }
        }

        // Fallback: plain HTTP streaming
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(300) };
        using var resp = await client.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, ct);
        resp.EnsureSuccessStatusCode();
        var total = resp.Content.Headers.ContentLength ?? knownSize;
        await using var fs = new FileStream(dest, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 20, true);
        var buf = new byte[1 << 20];
        long received = 0;
        var stream = await resp.Content.ReadAsStreamAsync(ct);
        int read;
        while ((read = await stream.ReadAsync(buf, ct)) > 0)
        {
            await fs.WriteAsync(buf.AsMemory(0, read), ct);
            received += read;
            onProgress?.Invoke(received, total, 0);
        }
    }

    /// <summary>
    /// Downloads <paramref name="url"/> into memory when the content fits within
    /// <paramref name="inMemThreshold"/>, or streams to <paramref name="fallbackLocalPath"/> otherwise.
    /// Returns <c>(memData, null)</c> for an in-memory result, or <c>(null, fallbackLocalPath)</c>
    /// when the file was written to disk.
    /// </summary>
    private async Task<(byte[]? memData, string? localPath)> DownloadSmartAsync(
        string url, string fallbackLocalPath, int aria2cConns, long knownSize,
        long inMemThreshold, CancellationToken ct, Action<long, long, long>? onProgress = null)
    {
        // aria2c always writes to disk — honour that and skip the in-memory path.
        bool canUseAria2c = _aria2c != null && (inMemThreshold <= 0 || knownSize == 0 || knownSize > inMemThreshold);

        if (inMemThreshold <= 0 || (knownSize > 0 && knownSize > inMemThreshold) || canUseAria2c)
        {
            await DownloadFileAsync(url, fallbackLocalPath, aria2cConns, knownSize, ct, onProgress);
            return (null, fallbackLocalPath);
        }

        // Open the HTTP response so we can inspect Content-Length before committing.
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(300) };
        using var resp = await client.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, ct);
        resp.EnsureSuccessStatusCode();

        var contentLength = resp.Content.Headers.ContentLength;
        long total = contentLength ?? knownSize;

        // Content-Length is known and exceeds threshold — stream directly to disk.
        if (contentLength.HasValue && contentLength.Value > inMemThreshold)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(fallbackLocalPath)!);
            await using var fsDirect = new FileStream(fallbackLocalPath, FileMode.Create,
                FileAccess.Write, FileShare.None, 1 << 20, true);
            var bufD = new byte[1 << 20];
            long rxD = 0;
            await using var stD = await resp.Content.ReadAsStreamAsync(ct);
            int rD;
            while ((rD = await stD.ReadAsync(bufD, ct)) > 0)
            {
                await fsDirect.WriteAsync(bufD.AsMemory(0, rD), ct);
                rxD += rD;
                onProgress?.Invoke(rxD, total, 0);
            }
            return (null, fallbackLocalPath);
        }

        // Stream into memory; if we exceed the threshold mid-download, spill buffered
        // bytes to disk transparently so the caller never needs to retry the download.
        var ms = new MemoryStream(total > 0 ? (int)Math.Min(total, inMemThreshold) : 64 * 1024);
        FileStream? spill = null;
        var buf = new byte[1 << 20];
        long received = 0;
        await using var stream = await resp.Content.ReadAsStreamAsync(ct);
        int read;
        while ((read = await stream.ReadAsync(buf, ct)) > 0)
        {
            received += read;
            onProgress?.Invoke(received, total, 0);

            if (spill is null && received <= inMemThreshold)
            {
                await ms.WriteAsync(buf.AsMemory(0, read), ct);
            }
            else if (spill is null)
            {
                // Exceeded the threshold — flush the in-memory buffer to disk and continue there.
                Directory.CreateDirectory(Path.GetDirectoryName(fallbackLocalPath)!);
                spill = new FileStream(fallbackLocalPath, FileMode.Create, FileAccess.Write,
                    FileShare.None, 1 << 20, true);
                ms.Position = 0;
                await ms.CopyToAsync(spill, ct);
                ms.Dispose();
                await spill.WriteAsync(buf.AsMemory(0, read), ct);
            }
            else
            {
                await spill.WriteAsync(buf.AsMemory(0, read), ct);
            }
        }

        if (spill is not null)
        {
            await spill.FlushAsync(ct);
            await spill.DisposeAsync();
            return (null, fallbackLocalPath);
        }

        return (ms.ToArray(), null);
    }

    private async Task UploadStreamAsync(
        string uploadServerUrl, string token, int fileId, Stream src, long fileSize,
        Action<long, long> onProgress, CancellationToken ct)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(300) };
        AddWorkerHeaders(client, token);

        // 1. Start session
        string? sessionId = null;
        for (int attempt = 1; attempt <= 12; attempt++)
        {
            try
            {
                var resp = await client.PostAsync($"{uploadServerUrl}/api/upload/{fileId}/start",
                    new ByteArrayContent([]), ct);
                if ((int)resp.StatusCode == 426)
                    throw new InvalidOperationException("Worker update required");
                if (IsRetriable((int)resp.StatusCode))
                {
                    if (attempt == 12) resp.EnsureSuccessStatusCode();
                    await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt)), ct);
                    continue;
                }
                resp.EnsureSuccessStatusCode();
                using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct));
                sessionId = doc.RootElement.GetProperty("session_id").GetString();
                break;
            }
            catch (HttpRequestException ex) when (attempt < 12 && (ex.StatusCode is null || IsRetriable((int)ex.StatusCode.Value)))
            {
                await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt)), ct);
            }
        }
        if (sessionId is null) throw new InvalidOperationException("Failed to create upload session");

        // 2. Send chunks
        long sent = 0;
        using var sha = SHA256.Create();
        var buf = new byte[UploadChunkSize];
        int read;
        while ((read = await src.ReadAsync(buf, ct)) > 0)
        {
            var chunk = buf[..read];
            sha.TransformBlock(chunk, 0, chunk.Length, null, 0);

            for (int attempt = 1; attempt <= 30; attempt++)
            {
                try
                {
                    long chunkSent = 0;
                    using var ms = new MemoryStream(chunk, 0, chunk.Length, writable: false);
                    var progressStream = new ProgressStream(ms, written =>
                    {
                        chunkSent += written;
                        onProgress(sent + chunkSent, fileSize);
                    });
                    var content = new StreamContent(progressStream);
                    content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
                    var resp = await client.PostAsync(
                        $"{uploadServerUrl}/api/upload/{fileId}/chunk?session_id={sessionId}",
                        content, ct);
                    if ((int)resp.StatusCode == 426)
                        throw new InvalidOperationException("Worker update required");
                    if (IsRetriable((int)resp.StatusCode))
                    {
                        if (attempt == 30) resp.EnsureSuccessStatusCode();
                        await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt, 20)), ct);
                        continue;
                    }
                    resp.EnsureSuccessStatusCode();
                    break;
                }
                catch (HttpRequestException ex) when (attempt < 30 && (ex.StatusCode is null || IsRetriable((int)ex.StatusCode.Value)))
                {
                    await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt, 20)), ct);
                }
            }

            sent += read;
        }

        // 3. Finish
        sha.TransformFinalBlock([], 0, 0);
        var hash = Convert.ToHexStringLower(sha.Hash!);
        for (int attempt = 1; attempt <= 12; attempt++)
        {
            try
            {
                var resp = await client.PostAsync(
                    $"{uploadServerUrl}/api/upload/{fileId}/finish?session_id={sessionId}&expected_sha256={hash}",
                    new ByteArrayContent([]), ct);
                if ((int)resp.StatusCode == 426)
                    throw new InvalidOperationException("Worker update required");
                if (IsRetriable((int)resp.StatusCode))
                {
                    if (attempt == 12) resp.EnsureSuccessStatusCode();
                    await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt, 20)), ct);
                    continue;
                }
                resp.EnsureSuccessStatusCode();
                break;
            }
            catch (HttpRequestException ex) when (attempt < 12 && (ex.StatusCode is null || IsRetriable((int)ex.StatusCode.Value)))
            {
                await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt, 20)), ct);
            }
        }
    }

    private async Task ReportJobAsync(string serverUrl, string token, int fileId, string status,
        long? bytesDownloaded = null, string? error = null, CancellationToken ct = default)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        AddWorkerHeaders(client, token);

        var body = JsonSerializer.Serialize(new
        {
            file_id = fileId,
            status,
            bytes_downloaded = bytesDownloaded,
            error,
        });

        for (int attempt = 1; attempt <= 20; attempt++)
        {
            try
            {
                var content = new StringContent(body, Encoding.UTF8, "application/json");
                var resp = await client.PostAsync($"{serverUrl}/api/jobs/report", content, ct);
                if ((int)resp.StatusCode == 401) throw new InvalidOperationException("Token expired. Please log in again.");
                if ((int)resp.StatusCode == 409 && status == "completed")
                {
                    if (attempt == 20) resp.EnsureSuccessStatusCode();
                    await Task.Delay(TimeSpan.FromMilliseconds(250 + attempt * 100), ct);
                    continue;
                }
                if (IsRetriable((int)resp.StatusCode))
                {
                    if (attempt == 20) resp.EnsureSuccessStatusCode();
                    await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt, 20)), ct);
                    continue;
                }
                resp.EnsureSuccessStatusCode();
                return;
            }
            catch (HttpRequestException) when (attempt < 20)
            {
                await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt, 20)), ct);
            }
        }
    }

    private static string ExceptionSummary(Exception ex) => ex switch
    {
        HttpRequestException { StatusCode: { } code } => $"HTTP {(int)code} {code}",
        HttpRequestException => "connection error",
        _ => ex.Message.Split('\n')[0].Trim()
    };

    private static string BuildErrorMessage(Exception? ex, int totalAttempts)
    {
        if (ex is null) return "Unknown error";
        return $"Failed after {totalAttempts} attempt{(totalAttempts != 1 ? "s" : "")}: {ExceptionSummary(ex)}";
    }

    private async Task ProcessJobAsync(WorkerSettings settings, string token,
        JobProgressViewModel vm, JsonElement job, Action<string> log, CancellationToken ct,
        Action<long>? onSuccess = null, Action? onFail = null)
    {
        var fileId = job.GetProperty("file_id").GetInt32();
        var url = job.GetProperty("url").GetString()!;
        var destPath = job.GetProperty("dest_path").GetString()!;
        long knownSize = job.TryGetProperty("size", out var sz) && sz.ValueKind != JsonValueKind.Null
            ? sz.GetInt64() : 0;

        void UI(Action a) => Avalonia.Threading.Dispatcher.UIThread.Post(a);

        UI(() =>
        {
            var decoded = Uri.UnescapeDataString(destPath);
            vm.Label = decoded.Length <= 60 ? decoded : "..." + decoded[^57..];
            vm.MaxAttempts = MaxRetries;
            vm.Status = JobStatus.Downloading;
        });

        // Always compute a local path as the disk fallback. DownloadSmartAsync will decide
        // at download time (using Content-Length) whether to use memory or disk.
        var localPath = LocalPathForJob(settings.TempDir, url, destPath);
        Exception? lastErr = null;
        bool uploaded = false;
        bool fileReady = false;   // true once download has succeeded; skip re-download on upload retries
        long fileSize = 0;
        byte[]? inMemData = null;

        for (int attempt = 1; attempt <= MaxRetries && !ct.IsCancellationRequested; attempt++)
        {
            try
            {
                UI(() => { vm.CurrentAttempt = attempt; });

                // ── Download phase (skipped on upload retries) ──────────────────────
                if (!fileReady)
                {
                    UI(() =>
                    {
                        vm.Status = JobStatus.Downloading;
                        vm.BytesTransferred = 0;
                        vm.TotalBytes = knownSize;
                        vm.ResetSpeed();
                    });

                    Action<long, long, long> dlProgress = (rx, total, speedBps) => UI(() =>
                    {
                        vm.BytesTransferred = rx;
                        if (total > 0) vm.TotalBytes = total;
                        if (speedBps > 0) vm.ForceSetSpeed(speedBps);
                        else vm.UpdateSpeed(rx);
                    });

                    (inMemData, _) = await DownloadSmartAsync(url, localPath,
                        settings.Aria2cConnections, knownSize,
                        settings.InMemoryThresholdBytes, ct, dlProgress);
                    fileSize = inMemData is not null ? inMemData.LongLength : new FileInfo(localPath).Length;
                    fileReady = true;
                }

                // ── Upload phase ────────────────────────────────────────────────────
                UI(() =>
                {
                    vm.Status = JobStatus.Uploading;
                    vm.BytesTransferred = 0;
                    vm.TotalBytes = fileSize;
                    vm.ResetSpeed();
                });

                var ulThrottle = Stopwatch.StartNew();
                Action<long, long> ulProgressTracked = (s, t) =>
                {
                    if (ulThrottle.Elapsed.TotalMilliseconds < 200) return;
                    ulThrottle.Restart();
                    UI(() =>
                    {
                        vm.BytesTransferred = s;
                        vm.TotalBytes = t;
                        vm.UpdateSpeed(s);
                    });
                };

                if (inMemData is not null)
                {
                    using var ms = new MemoryStream(inMemData);
                    await UploadStreamAsync(settings.UploadServerUrl, token, fileId, ms, fileSize, ulProgressTracked, ct);
                }
                else
                {
                    await using var fs = new FileStream(localPath, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 20, true);
                    await UploadStreamAsync(settings.UploadServerUrl, token, fileId, fs, fileSize, ulProgressTracked, ct);
                }

                uploaded = true;
                break;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                lastErr = ex;
                var msg = $"Attempt {attempt}/{MaxRetries}: {ExceptionSummary(ex)}";
                log($"FAIL [{vm.Label}] {msg}");
                UI(() => { vm.ErrorMessage = msg; vm.SpeedText = ""; });
                // Only discard the local file if the download itself failed (partial write).
                // If the download succeeded but the upload failed, keep the file for the next attempt.
                if (!fileReady && inMemData is null && File.Exists(localPath)) File.Delete(localPath);
                if (attempt < MaxRetries)
                {
                    UI(() => { vm.CurrentAttempt = attempt; vm.Status = JobStatus.Retrying; });
                    await Task.Delay(TimeSpan.FromSeconds(RetryDelaySeconds * attempt), ct);
                }
            }
        }

        if (!uploaded)
        {
            if (inMemData is null && File.Exists(localPath)) try { File.Delete(localPath); } catch { }
            var errMsg = BuildErrorMessage(lastErr, MaxRetries);
            log($"FAIL [{vm.Label}] {errMsg}");
            UI(() => { vm.Status = JobStatus.Failed; vm.SpeedText = ""; vm.ErrorMessage = errMsg; });
            try { await ReportJobAsync(settings.ServerUrl, token, fileId, "failed", error: errMsg, ct: ct); } catch { }
            onFail?.Invoke();
            return;
        }

        UI(() => { vm.Status = JobStatus.Done; vm.BytesTransferred = fileSize; vm.SpeedText = ""; });
        if (inMemData is null && !settings.KeepFiles && File.Exists(localPath)) File.Delete(localPath);
        onSuccess?.Invoke(fileSize);

        try { await ReportJobAsync(settings.ServerUrl, token, fileId, "completed", bytesDownloaded: fileSize, ct: ct); }
        catch { /* best-effort */ }
    }

    public async Task RunAsync(WorkerSettings settings, string token,
        Action<string> log, CancellationToken ct,
        Action<long>? onSuccess = null, Action? onFail = null)
    {
        Directory.CreateDirectory(settings.TempDir);
        var seen = new ConcurrentDictionary<int, byte>();
        var channel = Channel.CreateBounded<JsonElement>(
            new BoundedChannelOptions(settings.Concurrency * 2) { FullMode = BoundedChannelFullMode.Wait });

        bool aria2cOk = await TryStartAria2cDaemonAsync(settings.Aria2cConnections, settings.AutoInstallAria2c, log, ct);
        log($"Starting — concurrency={settings.Concurrency}, aria2c={aria2cOk}");

        // Producer
        var producer = Task.Run(async () =>
        {
            using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
            AddWorkerHeaders(client, token);
            bool noJobsWarned = false;

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var resp = await client.GetAsync(
                        $"{settings.ServerUrl}/api/jobs?count={Math.Min(4, settings.BatchSize)}", ct);
                    if ((int)resp.StatusCode == 401) { log("Token expired. Log in again."); break; }
                    resp.EnsureSuccessStatusCode();
                    using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct));
                    var jobs = doc.RootElement.GetProperty("jobs");

                    if (jobs.GetArrayLength() == 0)
                    {
                        if (!noJobsWarned) { log("No jobs available, waiting..."); noJobsWarned = true; }
                        await Task.Delay(TimeSpan.FromSeconds(12 + Random.Shared.NextDouble() * 8), ct);
                        continue;
                    }
                    noJobsWarned = false;
                    foreach (var job in jobs.EnumerateArray())
                    {
                        var fid = job.GetProperty("file_id").GetInt32();
                        if (!seen.TryAdd(fid, 0)) continue;
                        await channel.Writer.WriteAsync(job.Clone(), ct);
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    log($"Server error: {ex.Message}. Retrying in 10s...");
                    await Task.Delay(TimeSpan.FromSeconds(6 + Random.Shared.NextDouble() * 4), ct);
                }
            }
            channel.Writer.TryComplete();
        }, ct);

        // Consumers
        var sem = new SemaphoreSlim(settings.Concurrency);
        var workerTasks = new List<Task>();

        await foreach (var job in channel.Reader.ReadAllAsync(ct))
        {
            await sem.WaitAsync(ct);
            var vm = new JobProgressViewModel();
            Avalonia.Threading.Dispatcher.UIThread.Post(() => ActiveJobs.Add(vm));
            var captured = job;
            var task = Task.Run(async () =>
            {
                try
                {
                    await ProcessJobAsync(settings, token, vm, captured, log, ct, onSuccess, onFail);
                    log($"{(vm.Status == JobStatus.Done ? "OK" : "FAIL")} {vm.Label}");
                }
                finally
                {
                    sem.Release();
                    seen.TryRemove(captured.GetProperty("file_id").GetInt32(), out _);
                    if (vm.Status == JobStatus.Done)
                        await Task.Delay(TimeSpan.FromSeconds(3), CancellationToken.None);
                    Avalonia.Threading.Dispatcher.UIThread.Post(() => ActiveJobs.Remove(vm));
                }
            }, CancellationToken.None);
            workerTasks.Add(task);
        }

        await Task.WhenAll(workerTasks);
        StopAria2cDaemon();
        log("Worker stopped.");
    }
}
