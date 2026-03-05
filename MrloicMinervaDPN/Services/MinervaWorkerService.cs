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
/// <summary>Thrown when the upload server permanently rejects a file (e.g. HTTP 422).
/// Retrying will not help — the job should be marked failed immediately.</summary>
public sealed class PermanentUploadException(string message) : Exception(message);

public sealed class MinervaWorkerService
{
    private const string Version = "1.2.4";
    private const int MaxRetries = 3;
    private const int RetryDelaySeconds = 5;
    private const long UploadChunkSize = 8 * 1024 * 1024;

    private static readonly HashSet<int> RetriableStatusCodes = [408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524];
    /// <summary>Retry delays for deferred jobs. Once the last value is reached it repeats indefinitely.</summary>
    private static readonly TimeSpan[] DeferSchedule =
    [
        TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2)
    ];

    /// <summary>Tracks total bytes of downloaded-but-not-yet-uploaded files that are cached
    /// while the upload server is unavailable, preventing unbounded disk usage.</summary>
    private sealed class CacheTracker(long maxBytes)
    {
        private long _used;
        public long MaxBytes => maxBytes;
        public long Used => Volatile.Read(ref _used);

        /// <summary>Atomically reserves <paramref name="bytes"/>.
        /// Returns false (without reserving) when maxBytes is set and the limit would be exceeded.</summary>
        public bool TryReserve(long bytes)
        {
            if (maxBytes <= 0) { Interlocked.Add(ref _used, bytes); return true; }
            while (true)
            {
                var cur = Volatile.Read(ref _used);
                if (cur + bytes > maxBytes) return false;
                if (Interlocked.CompareExchange(ref _used, cur + bytes, cur) == cur) return true;
            }
        }
        public void Release(long bytes) => Interlocked.Add(ref _used, -bytes);
    }

    private Process? _aria2cProc;
    private Aria2NetClient? _aria2c;
    private string _aria2cSecret = "";
    private readonly SessionStore _sessions = new();

    public ObservableCollection<JobProgressViewModel> ActiveJobs { get; } = [];

    private static int FindFreePort()
    {
        var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        int port = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    private async Task<bool> TryStartAria2cDaemonAsync(int maxConcurrent, int conns, bool autoInstall, Action<string> log, CancellationToken ct)
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
        psi.ArgumentList.Add($"--max-concurrent-downloads={maxConcurrent}");
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
        _aria2c = null;
        var proc = _aria2cProc;
        _aria2cProc = null;
        if (proc is { HasExited: false })
            try { proc.Kill(); } catch { }
    }

    /// <summary>Kills the aria2c daemon. Safe to call from any thread.</summary>
    public Task StopAsync() { StopAria2cDaemon(); return Task.CompletedTask; }

    private static void AddWorkerHeaders(HttpClient client, string token)
    {
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
        client.DefaultRequestHeaders.Add("X-Minerva-Worker-Version", Version);
    }

    private static bool IsRetriable(int code) => RetriableStatusCodes.Contains(code);

    /// <summary>Codes that mean the upload session no longer exists server-side and we should
    /// open a fresh one. 422 is intentionally excluded — it means the content itself was
    /// rejected (e.g. SHA mismatch, illegal file), not that the session is missing.</summary>
    private static bool IsSessionGone(int code) => code is 404 or 410 or 412;

    /// <summary>Reads the response body and throws an <see cref="InvalidOperationException"/>
    /// whose message includes the HTTP status and any "detail" field from the JSON body.
    /// Always throws; return type is Task only so callers can write <c>await ThrowHttpErrorAsync(…)</c>.</summary>
    private static async Task ThrowHttpErrorAsync(HttpResponseMessage resp, CancellationToken ct)
    {
        string body = "";
        try { body = (await resp.Content.ReadAsStringAsync(ct)).Trim(); } catch { }
        if (!string.IsNullOrEmpty(body))
        {
            try
            {
                using var doc = JsonDocument.Parse(body);
                if (doc.RootElement.TryGetProperty("detail", out var d))
                    body = d.ValueKind == JsonValueKind.String ? d.GetString()! : d.GetRawText();
            }
            catch { }
        }
        var msg = $"HTTP {(int)resp.StatusCode} {resp.StatusCode}";
        if (!string.IsNullOrEmpty(body)) msg += $": {body}";
        // 4xx that are not session-related are permanent — the server will not accept this
        // file regardless of how many times we retry. Signal this to callers.
        var sc = (int)resp.StatusCode;
        if (sc >= 400 && sc < 500 && !IsSessionGone(sc) && !IsRetriable(sc))
            throw new PermanentUploadException(msg);
        throw new InvalidOperationException(msg);
    }

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
        // aria2c always writes to disk — use the disk path when aria2c is running,
        // the threshold is disabled, or the file is known to exceed the threshold.
        if (_aria2c != null || inMemThreshold <= 0 || (knownSize > 0 && knownSize > inMemThreshold))
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
        Action<long, long> onProgress, SessionStore sessions, CancellationToken ct)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(300) };
        AddWorkerHeaders(client, token);

        const int maxSessionRestarts = 5;
        for (int sessionAttempt = 1; sessionAttempt <= maxSessionRestarts; sessionAttempt++)
        {
            // Rewind so a session restart always uploads from the beginning.
            src.Seek(0, SeekOrigin.Begin);
            bool sessionExpired = false;

            // 1. Start session — reuse a persisted session ID if one exists for this file,
            // so repeated attempts never open a duplicate session (which causes HTTP 412).
            string? sessionId = sessions.Get(fileId);
            if (sessionId is null)
            {
                for (int attempt = 1; attempt <= 12; attempt++)
                {
                    try
                    {
                        var resp = await client.PostAsync($"{uploadServerUrl}/api/upload/{fileId}/start",
                            new ByteArrayContent([]), ct);
                        if ((int)resp.StatusCode == 426)
                            throw new InvalidOperationException("Worker update required");
                        if ((int)resp.StatusCode == 412)
                        {
                            try
                            {
                                var body = await resp.Content.ReadAsStringAsync(ct);
                                using var doc = JsonDocument.Parse(body);
                                var root = doc.RootElement;
                                // Accept either {"session_id": "..."} or {"detail": {"session_id": "..."}}
                                if (root.TryGetProperty("session_id", out var sidEl) && sidEl.GetString() is { } sid && sid.Length > 0)
                                { sessionId = sid; sessions.Set(fileId, sessionId); break; }
                                if (root.TryGetProperty("detail", out var detailEl) && detailEl.ValueKind == JsonValueKind.Object &&
                                    detailEl.TryGetProperty("session_id", out var sid2El) && sid2El.GetString() is { } sid2 && sid2.Length > 0)
                                { sessionId = sid2; sessions.Set(fileId, sessionId); break; }
                            }
                            catch { }
                            // No session_id in response — wait for the server-side session to expire.
                            if (attempt == 12) throw new InvalidOperationException($"HTTP 412: server has an active session for file {fileId} and did not return its ID");
                            await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt, 30)), ct);
                            continue;
                        }
                        if (IsRetriable((int)resp.StatusCode))
                        {
                            if (attempt == 12) await ThrowHttpErrorAsync(resp, ct);
                            await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt)), ct);
                            continue;
                        }
                        if (!resp.IsSuccessStatusCode) await ThrowHttpErrorAsync(resp, ct);
                        using var sdoc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct));
                        sessionId = sdoc.RootElement.GetProperty("session_id").GetString();
                        if (sessionId is not null) sessions.Set(fileId, sessionId);
                        break;
                    }
                    catch (HttpRequestException ex) when (attempt < 12 && (ex.StatusCode is null || IsRetriable((int)ex.StatusCode.Value)))
                    {
                        await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt)), ct);
                    }
                }
            }
            if (sessionId is null) throw new InvalidOperationException("Failed to create upload session");

            // 2. Send chunks
            long sent = 0;
            using var sha = SHA256.Create();
            var buf = new byte[UploadChunkSize];
            int read;
            while (!sessionExpired && (read = await src.ReadAsync(buf, ct)) > 0)
            {
                sha.TransformBlock(buf, 0, read, null, 0);

                for (int attempt = 1; attempt <= 30; attempt++)
                {
                    try
                    {
                        long chunkSent = 0;
                        using var ms = new MemoryStream(buf, 0, read, writable: false);
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
                        var sc = (int)resp.StatusCode;
                        // Session is gone server-side — discard it and restart from /start.
                        if (IsSessionGone(sc)) { sessions.Remove(fileId); sessionExpired = true; break; }
                        if (IsRetriable(sc))
                        {
                            if (attempt == 30) await ThrowHttpErrorAsync(resp, ct);
                            await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt, 20)), ct);
                            continue;
                        }
                        // Any other 4xx/5xx (e.g. 422 UnprocessableEntity) is a permanent
                        // rejection — throw immediately with the server's error body.
                        if (!resp.IsSuccessStatusCode) await ThrowHttpErrorAsync(resp, ct);
                        break;
                    }
                    catch (HttpRequestException ex) when (attempt < 30 && (ex.StatusCode is null || IsRetriable((int)ex.StatusCode.Value)))
                    {
                        await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt, 20)), ct);
                    }
                }

                if (!sessionExpired)
                    sent += read;
            }

            if (sessionExpired)
            {
                if (sessionAttempt == maxSessionRestarts)
                    throw new InvalidOperationException("Upload session expired repeatedly; giving up.");
                await Task.Delay(TimeSpan.FromSeconds(RetrySleep(sessionAttempt)), ct);
                continue;
            }

            // 3. Finish
            sha.TransformFinalBlock([], 0, 0);
            var hash = Convert.ToHexStringLower(sha.Hash!);
            bool finishExpired = false;
            for (int attempt = 1; attempt <= 12; attempt++)
            {
                try
                {
                    var resp = await client.PostAsync(
                        $"{uploadServerUrl}/api/upload/{fileId}/finish?session_id={sessionId}&expected_sha256={hash}",
                        new ByteArrayContent([]), ct);
                    if ((int)resp.StatusCode == 426)
                        throw new InvalidOperationException("Worker update required");
                    var fsc = (int)resp.StatusCode;
                    // Session gone — restart from /start.
                    if (IsSessionGone(fsc)) { sessions.Remove(fileId); finishExpired = true; break; }
                    if (IsRetriable(fsc))
                    {
                        if (attempt == 12) await ThrowHttpErrorAsync(resp, ct);
                        await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt, 20)), ct);
                        continue;
                    }
                    // Permanent rejection (e.g. 422 = SHA mismatch or invalid content).
                    // Do NOT restart the session — throw with full server detail for logging.
                    if (!resp.IsSuccessStatusCode) await ThrowHttpErrorAsync(resp, ct);
                    break;
                }
                catch (HttpRequestException ex) when (attempt < 12 && (ex.StatusCode is null || IsRetriable((int)ex.StatusCode.Value)))
                {
                    await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt, 20)), ct);
                }
            }

            if (finishExpired)
            {
                if (sessionAttempt == maxSessionRestarts)
                    throw new InvalidOperationException("Upload session expired repeatedly; giving up.");
                await Task.Delay(TimeSpan.FromSeconds(RetrySleep(sessionAttempt)), ct);
                continue;
            }

            sessions.Remove(fileId); // Clean up persisted session on success.
            return; // Upload complete.
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

    /// <summary>Returns true when <paramref name="ex"/> was caused by a retriable server-side HTTP error.</summary>
    private static bool IsServerError(Exception? ex) =>
        ex is HttpRequestException { StatusCode: { } sc } && IsRetriable((int)sc);

    private enum DownloadOutcome { Sent, Deferred, Failed }

    /// <summary>Payload passed from a download worker to an upload worker.</summary>
    private sealed record DownloadedJob(
        WorkerSettings Settings, string Token, JobProgressViewModel Vm, JsonElement Job,
        byte[]? InMemData, string? LocalPath, long FileSize,
        Action<string> Log, Action<long>? OnSuccess, Action? OnFail,
        CacheTracker Cache);

    /// <summary>
    /// Download phase: retries up to MaxRetries times.
    /// On success writes to <paramref name="uploadWriter"/> and returns true.
    /// On total failure reports to the server and returns false.
    /// </summary>
    private async Task<DownloadOutcome> DownloadPhaseAsync(
        WorkerSettings settings, string token, JobProgressViewModel vm, JsonElement job,
        Action<string> log, CancellationToken ct,
        ChannelWriter<DownloadedJob> uploadWriter,
        Action<long>? onSuccess, Action? onFail, CacheTracker cache)
    {
        var fileId   = job.GetProperty("file_id").GetInt32();
        var url      = job.GetProperty("url").GetString()!;
        var destPath = job.GetProperty("dest_path").GetString()!;
        long knownSize = job.TryGetProperty("size", out var sz) && sz.ValueKind != JsonValueKind.Null
            ? sz.GetInt64() : 0;

        static void UI(Action a) => Avalonia.Threading.Dispatcher.UIThread.Post(a);
        UI(() =>
        {
            var decoded = Uri.UnescapeDataString(destPath);
            vm.Label = decoded.Length <= 60 ? decoded : "..." + decoded[^57..];
            vm.MaxAttempts = MaxRetries;
            vm.Status = JobStatus.Downloading;
        });

        var localPath = LocalPathForJob(settings.TempDir, url, destPath);
        Exception? lastErr = null;

        for (int attempt = 1; attempt <= MaxRetries && !ct.IsCancellationRequested; attempt++)
        {
            try
            {
                UI(() =>
                {
                    vm.CurrentAttempt = attempt;
                    vm.Status = JobStatus.Downloading;
                    vm.BytesTransferred = 0;
                    vm.TotalBytes = knownSize;
                    vm.ResetSpeed();
                });

                Action<long, long, long> dlProgress = (rx, total, speedBps) => UI(() =>
                {
                    vm.BytesTransferred = rx;
                    if (total > 0) vm.TotalBytes = total;
                    if (speedBps > 0) vm.ForceSetSpeed(speedBps); else vm.UpdateSpeed(rx);
                });

                var (inMemData, _) = await DownloadSmartAsync(url, localPath,
                    settings.Aria2cConnections, knownSize,
                    settings.InMemoryThresholdBytes, ct, dlProgress);

                long fileSize = inMemData is not null ? inMemData.LongLength : new FileInfo(localPath).Length;

                UI(() => { vm.Status = JobStatus.Queued; vm.SpeedText = ""; });
                await uploadWriter.WriteAsync(
                    new DownloadedJob(settings, token, vm, job,
                        inMemData, inMemData is null ? localPath : null,
                        fileSize, log, onSuccess, onFail, cache), CancellationToken.None);
                return DownloadOutcome.Sent;
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                lastErr = ex;
                var msg = $"Download attempt {attempt}/{MaxRetries}: {ExceptionSummary(ex)}";
                log($"DL-FAIL [{vm.Label}] {msg}");
                UI(() => { vm.ErrorMessage = msg; vm.SpeedText = ""; });
                if (File.Exists(localPath)) try { File.Delete(localPath); } catch { }
                if (attempt < MaxRetries)
                {
                    UI(() => { vm.Status = JobStatus.Retrying; });
                    await Task.Delay(TimeSpan.FromSeconds(RetryDelaySeconds * attempt), ct);
                }
            }
        }

        // Server-side errors are deferred for retry by the caller; other errors are permanent.
        if (IsServerError(lastErr))
            return DownloadOutcome.Deferred;

        var errMsg = BuildErrorMessage(lastErr, MaxRetries);
        log($"DL-FAIL [{vm.Label}] {errMsg}");
        UI(() => { vm.Status = JobStatus.Failed; vm.SpeedText = ""; vm.ErrorMessage = errMsg; });
        try { await ReportJobAsync(settings.ServerUrl, token, fileId, "failed", error: errMsg, ct: ct); } catch { }
        onFail?.Invoke();
        return DownloadOutcome.Failed;
    }

    /// <summary>
    /// Upload phase: retries up to MaxRetries times, then reports result to the server.
    /// </summary>
    private async Task UploadPhaseAsync(DownloadedJob dl, CancellationToken ct)
    {
        var (settings, token, vm, job, inMemData, localPath, fileSize, log, onSuccess, onFail, cache) = dl;
        var fileId = job.GetProperty("file_id").GetInt32();
        void UI(Action a) => Avalonia.Threading.Dispatcher.UIThread.Post(a);
        Exception? lastErr = null;
        Action<long, long> ulProgress = (s, t) => UI(() => { vm.BytesTransferred = s; vm.TotalBytes = t; vm.UpdateSpeed(s); });

        // The loop guard intentionally omits a ct check so that already-downloaded files
        // always get an upload attempt even when the worker is stopping.
        // Individual HTTP calls and retry delays still honour ct.
        for (int attempt = 1; attempt <= MaxRetries; attempt++)
        {
            try
            {
                UI(() =>
                {
                    vm.CurrentAttempt = attempt;
                    vm.Status = JobStatus.Uploading;
                    vm.BytesTransferred = 0;
                    vm.TotalBytes = fileSize;
                    vm.ResetSpeed();
                });

                log($"Uploading [{vm.Label}] attempt {attempt}/{MaxRetries}…");

                if (inMemData is not null)
                {
                    using var ms = new MemoryStream(inMemData);
                    await UploadStreamAsync(settings.UploadServerUrl, token, fileId, ms, fileSize, ulProgress, _sessions, ct);
                }
                else
                {
                    await using var fs = new FileStream(localPath!, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 20, true);
                    await UploadStreamAsync(settings.UploadServerUrl, token, fileId, fs, fileSize, ulProgress, _sessions, ct);
                }

                UI(() => { vm.Status = JobStatus.Done; vm.BytesTransferred = fileSize; vm.SpeedText = ""; });
                if (inMemData is null && !settings.KeepFiles && localPath != null && File.Exists(localPath))
                    File.Delete(localPath);
                onSuccess?.Invoke(fileSize);
                log($"OK (upload) [{vm.Label}]");
                try { await ReportJobAsync(settings.ServerUrl, token, fileId, "completed", bytesDownloaded: fileSize, ct: CancellationToken.None); } catch { }
                return;
            }
            catch (OperationCanceledException) { throw; }
            catch (PermanentUploadException ex)
            {
                // Server permanently rejected this file — no point retrying at all.
                var msg = ExceptionSummary(ex);
                log($"UL-FAIL [{vm.Label}] Permanent rejection: {msg}");
                UI(() => { vm.Status = JobStatus.Failed; vm.SpeedText = ""; vm.ErrorMessage = msg; });
                if (inMemData is null && localPath != null && File.Exists(localPath))
                    try { File.Delete(localPath); } catch { }
                try { await ReportJobAsync(settings.ServerUrl, token, fileId, "failed", error: msg, ct: ct); } catch { }
                onFail?.Invoke();
                return;
            }
            catch (Exception ex)
            {
                lastErr = ex;
                var msg = $"Upload attempt {attempt}/{MaxRetries}: {ExceptionSummary(ex)}";
                log($"UL-FAIL [{vm.Label}] {msg}");
                UI(() => { vm.ErrorMessage = msg; vm.SpeedText = ""; });
                if (attempt < MaxRetries)
                {
                    UI(() => { vm.Status = JobStatus.Retrying; });
                    await Task.Delay(TimeSpan.FromSeconds(RetryDelaySeconds * attempt), ct);
                }
            }
        }

        // If all fast retries failed with a server error, hold the downloaded file and
        // keep retrying with increasing delays until the server recovers, the worker is
        // stopped, or adding this file to the cache would exceed the configured limit.
        if (IsServerError(lastErr))
        {
            if (!cache.TryReserve(fileSize))
            {
                var limitMb = cache.MaxBytes / (1024 * 1024);
                log($"UL-FAIL [{vm.Label}] Cache full ({limitMb} MB limit) — dropping file");
            }
            else
            {
                try
                {
                    int d = 0;
                    while (!ct.IsCancellationRequested)
                    {
                        var delay = DeferSchedule[Math.Min(d, DeferSchedule.Length - 1)];
                        var waitMsg = $"Server error — retrying in {delay.TotalMinutes:0}m";
                        log($"UL-DEFER [{vm.Label}] {waitMsg}");
                        UI(() => { vm.Status = JobStatus.Retrying; vm.SpeedText = ""; vm.ErrorMessage = waitMsg; });
                        try { await Task.Delay(delay, ct); } catch (OperationCanceledException) { break; }
                        try
                        {
                            UI(() => { vm.Status = JobStatus.Uploading; vm.BytesTransferred = 0; vm.TotalBytes = fileSize; vm.ResetSpeed(); });
                            if (inMemData is not null)
                            {
                                using var ms = new MemoryStream(inMemData);
                                await UploadStreamAsync(settings.UploadServerUrl, token, fileId, ms, fileSize, ulProgress, _sessions, ct);
                            }
                            else
                            {
                                await using var fs = new FileStream(localPath!, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 20, true);
                                await UploadStreamAsync(settings.UploadServerUrl, token, fileId, fs, fileSize, ulProgress, _sessions, ct);
                            }
                            UI(() => { vm.Status = JobStatus.Done; vm.BytesTransferred = fileSize; vm.SpeedText = ""; });
                            if (inMemData is null && !settings.KeepFiles && localPath != null && File.Exists(localPath))
                                File.Delete(localPath);
                            onSuccess?.Invoke(fileSize);
                            log($"OK (deferred upload) [{vm.Label}]");
                            try { await ReportJobAsync(settings.ServerUrl, token, fileId, "completed", bytesDownloaded: fileSize, ct: CancellationToken.None); } catch { }
                            return;
                        }
                        catch (OperationCanceledException) { break; }
                        catch (PermanentUploadException ex)
                        {
                            lastErr = ex;
                            log($"UL-DEFER-FAIL [{vm.Label}] Permanent rejection: {ExceptionSummary(ex)}");
                            break; // Stop deferring — server will never accept this file.
                        }
                        catch (Exception ex)
                        {
                            lastErr = ex;
                            log($"UL-DEFER-FAIL [{vm.Label}] retry {d + 1}: {ExceptionSummary(ex)}");
                            if (!IsServerError(ex)) break; // Non-server error — stop deferring
                            d++;
                        }
                    }
                }
                finally { cache.Release(fileSize); }
                if (ct.IsCancellationRequested) return;
            }
        }

        if (inMemData is null && localPath != null && File.Exists(localPath))
            try { File.Delete(localPath); } catch { }
        var errMsg = BuildErrorMessage(lastErr, MaxRetries);
        log($"UL-FAIL [{vm.Label}] {errMsg}");
        UI(() => { vm.Status = JobStatus.Failed; vm.SpeedText = ""; vm.ErrorMessage = errMsg; });
        try { await ReportJobAsync(settings.ServerUrl, token, fileId, "failed", error: errMsg, ct: ct); } catch { }
        onFail?.Invoke();
    }

    public async Task RunAsync(WorkerSettings settings, string token,
        Action<string> log, CancellationToken ct,
        Action<long>? onSuccess = null, Action? onFail = null)
    {
        Directory.CreateDirectory(settings.TempDir);
        var seen = new ConcurrentDictionary<int, byte>();
        // Tracks bytes currently reserved by deferred upload jobs.
        var cache = new CacheTracker(settings.MaxCacheSizeBytes);
        // Jobs that failed to download due to server errors are re-queued here with a notBefore timestamp.
        var deferredQueue = new ConcurrentQueue<(JsonElement Job, DateTimeOffset NotBefore, int DeferCount)>();
        var deferralCounts = new ConcurrentDictionary<int, int>();

        // Channel 1: raw jobs from the API → download workers
        var jobChannel = Channel.CreateBounded<JsonElement>(
            new BoundedChannelOptions(settings.Concurrency * 2) { FullMode = BoundedChannelFullMode.Wait });

        // Channel 2: downloaded jobs → upload workers
        // Capacity must be at least the download pool size so finished downloads never
        // block waiting for an upload slot, which would stall the download concurrency.
        var uploadChannel = Channel.CreateBounded<DownloadedJob>(
            new BoundedChannelOptions(Math.Max(settings.Concurrency, settings.UploadConcurrency * 2))
                { FullMode = BoundedChannelFullMode.Wait });

        bool aria2cOk = await TryStartAria2cDaemonAsync(settings.Concurrency, settings.Aria2cConnections, settings.AutoInstallAria2c, log, ct);
        log($"Starting — download={settings.Concurrency}, upload={settings.UploadConcurrency}, aria2c={aria2cOk}");

        // Producer: fetch jobs from the API and push into jobChannel
        var producer = Task.Run(async () =>
        {
            using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
            AddWorkerHeaders(client, token);
            bool noJobsWarned = false;

            while (!ct.IsCancellationRequested)
            {
                // Re-inject any deferred download jobs whose wait period has elapsed.
                while (deferredQueue.TryPeek(out var pending) && pending.NotBefore <= DateTimeOffset.UtcNow)
                {
                    if (deferredQueue.TryDequeue(out var deferred))
                    {
                        var fid = deferred.Job.GetProperty("file_id").GetInt32();
                        if (seen.TryAdd(fid, 0))
                            await jobChannel.Writer.WriteAsync(deferred.Job, ct);
                    }
                }

                try
                {
                    var resp = await client.GetAsync(
                        $"{settings.ServerUrl}/api/jobs?count={settings.BatchSize}", ct);
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
                        await jobChannel.Writer.WriteAsync(job.Clone(), ct);
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    log($"Server error: {ex.Message}. Retrying in 10s...");
                    await Task.Delay(TimeSpan.FromSeconds(6 + Random.Shared.NextDouble() * 4), ct);
                }
            }
            jobChannel.Writer.TryComplete();
        }, ct);

        // Download workers: pull from jobChannel, download, push to uploadChannel
        var downloadWorkers = new Task[settings.Concurrency];
        for (int i = 0; i < settings.Concurrency; i++)
        {
            downloadWorkers[i] = Task.Run(async () =>
            {
                await foreach (var job in jobChannel.Reader.ReadAllAsync(ct))
                {
                    var vm = new JobProgressViewModel();
                    Avalonia.Threading.Dispatcher.UIThread.Post(() => ActiveJobs.Add(vm));
                    var outcome = DownloadOutcome.Failed;
                    try
                    {
                        outcome = await DownloadPhaseAsync(settings, token, vm, job, log, ct,
                            uploadChannel.Writer, onSuccess, onFail, cache);
                    }
                    catch (OperationCanceledException) { }
                    finally
                    {
                        if (outcome != DownloadOutcome.Sent)
                        {
                            var fid = job.GetProperty("file_id").GetInt32();
                            if (outcome == DownloadOutcome.Deferred && !ct.IsCancellationRequested)
                            {
                                // Retry indefinitely: advance through DeferSchedule then hold at the last delay.
                                var dCount = deferralCounts.AddOrUpdate(fid, 1, (_, v) => v + 1);
                                var delay = DeferSchedule[Math.Min(dCount - 1, DeferSchedule.Length - 1)];
                                deferredQueue.Enqueue((job, DateTimeOffset.UtcNow + delay, dCount));
                                log($"DL-DEFER [{vm.Label}] Server error — retrying in {delay.TotalMinutes:0}m");
                                seen.TryRemove(fid, out _); // allow producer to re-inject when ready
                                Avalonia.Threading.Dispatcher.UIThread.Post(() =>
                                    vm.ErrorMessage = $"Server error — retrying in {delay.TotalMinutes:0}m");
                            }
                            else
                            {
                                seen.TryRemove(fid, out _);
                            }
                            if (!ct.IsCancellationRequested)
                                await Task.Delay(TimeSpan.FromSeconds(3), CancellationToken.None);
                            Avalonia.Threading.Dispatcher.UIThread.Post(() => ActiveJobs.Remove(vm));
                        }
                    }
                }
            }, ct);
        }

        // Close the upload channel once all download workers are done
        _ = Task.WhenAll(downloadWorkers).ContinueWith(_ => uploadChannel.Writer.TryComplete());

        // Upload workers: pull from uploadChannel, upload, report
        var uploadWorkers = new Task[settings.UploadConcurrency];
        for (int i = 0; i < settings.UploadConcurrency; i++)
        {
            uploadWorkers[i] = Task.Run(async () =>
            {
                await foreach (var dl in uploadChannel.Reader.ReadAllAsync(CancellationToken.None))
                {
                    try { await UploadPhaseAsync(dl, ct); }
                    catch (OperationCanceledException) { }
                    catch (Exception ex) { log($"Upload worker error: {ex.Message}"); }
                    finally
                    {
                        seen.TryRemove(dl.Job.GetProperty("file_id").GetInt32(), out _);
                        if (dl.Vm.Status == JobStatus.Done && !ct.IsCancellationRequested)
                            await Task.Delay(TimeSpan.FromSeconds(3), CancellationToken.None);
                        Avalonia.Threading.Dispatcher.UIThread.Post(() => ActiveJobs.Remove(dl.Vm));
                    }
                }
            }, ct);
        }

        try { await Task.WhenAll([producer, ..downloadWorkers, ..uploadWorkers]); }
        catch (OperationCanceledException) { }

        StopAria2cDaemon();
        log("Worker stopped.");
    }
}