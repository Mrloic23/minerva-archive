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
using MrloicMinervaDPN.Models;

namespace MrloicMinervaDPN.Services
{

public sealed class WorkerSettings
{
    public string ServerUrl { get; set; } = "https://api.minerva-archive.org";
    public string UploadServerUrl { get; set; } = "https://gate.minerva-archive.org";
    public int Concurrency { get; set; } = 2;
    public int BatchSize { get; set; } = 10;
    public int Aria2cConnections { get; set; } = 8;
    public string TempDir { get; set; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".minerva-dpn", "tmp");
    public bool KeepFiles { get; set; } = false;
    /// <summary>Files whose known size is at or below this threshold are kept entirely in
    /// RAM — never written to <see cref="TempDir"/>. 0 = always write to disk.</summary>
    public long InMemoryThresholdBytes { get; set; } = 0;
}

public sealed class MinervaWorkerService
{
    private const string Version = "1.2.4";
    private const int MaxRetries = 3;
    private const int RetryDelaySeconds = 5;
    private const long UploadChunkSize = 8 * 1024 * 1024;
    private const long Aria2cSizeThreshold = 5 * 1024 * 1024;

    private static readonly int[] RetriableStatusCodes = [408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524];

    private readonly bool _hasAria2c = Which("aria2c") is not null;

    public ObservableCollection<JobProgressViewModel> ActiveJobs { get; } = [];

    private static string? Which(string exe)
    {
        var pathExt = Environment.GetEnvironmentVariable("PATHEXT") ?? "";
        var path = Environment.GetEnvironmentVariable("PATH") ?? "";
        foreach (var dir in path.Split(Path.PathSeparator))
        {
            var candidate = Path.Combine(dir, exe);
            if (File.Exists(candidate)) return candidate;
            foreach (var ext in pathExt.Split(';'))
            {
                var withExt = candidate + ext;
                if (File.Exists(withExt)) return withExt;
            }
        }
        return null;
    }

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
        CancellationToken ct, Action<long, long>? onProgress = null)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
        bool useAria2c = _hasAria2c && (knownSize == 0 || knownSize >= Aria2cSizeThreshold);
        if (useAria2c)
        {
            var psi = new ProcessStartInfo("aria2c",
                $"--max-connection-per-server={aria2cConns} --split={aria2cConns} " +
                $"--min-split-size=1M --dir \"{Path.GetDirectoryName(dest)}\" " +
                $"--out \"{Path.GetFileName(dest)}\" --auto-file-renaming=false " +
                "--allow-overwrite=true --console-log-level=warn --retry-wait=3 " +
                $"--max-tries=5 --timeout=60 --connect-timeout=15 \"{url}\"")
            {
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi) ?? throw new InvalidOperationException("Failed to start aria2c");
            // Poll written bytes for progress while aria2c runs (best-effort)
            if (onProgress != null && knownSize > 0)
            {
                _ = Task.Run(async () =>
                {
                    while (!proc.HasExited)
                    {
                        try { if (File.Exists(dest)) onProgress(new FileInfo(dest).Length, knownSize); }
                        catch { }
                        await Task.Delay(400).ConfigureAwait(false);
                    }
                });
            }
            await proc.WaitForExitAsync(ct);
            if (proc.ExitCode != 0)
            {
                var err = await proc.StandardError.ReadToEndAsync(ct);
                throw new InvalidOperationException($"aria2c exit {proc.ExitCode}: {err[..Math.Min(200, err.Length)]}");
            }
        }
        else
        {
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
                onProgress?.Invoke(received, total);
            }
        }
    }

    /// <summary>Downloads a URL fully into memory and returns the bytes.
    /// Only used when the file is below <see cref="WorkerSettings.InMemoryThresholdBytes"/>.</summary>
    private static async Task<byte[]> DownloadToMemoryAsync(string url, long knownSize,
        CancellationToken ct, Action<long, long>? onProgress = null)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(300) };
        using var resp = await client.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, ct);
        resp.EnsureSuccessStatusCode();
        var total = resp.Content.Headers.ContentLength ?? knownSize;
        var ms = knownSize > 0 ? new MemoryStream((int)knownSize) : new MemoryStream();
        var buf = new byte[1 << 20];
        long received = 0;
        await using var stream = await resp.Content.ReadAsStreamAsync(ct);
        int read;
        while ((read = await stream.ReadAsync(buf, ct)) > 0)
        {
            await ms.WriteAsync(buf.AsMemory(0, read), ct);
            received += read;
            onProgress?.Invoke(received, total);
        }
        return ms.ToArray();
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
            catch (HttpRequestException) when (attempt < 12)
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
                    var content = new ByteArrayContent(chunk);
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
                catch (HttpRequestException) when (attempt < 30)
                {
                    await Task.Delay(TimeSpan.FromSeconds(RetrySleep(attempt, 20)), ct);
                }
            }

            sent += read;
            onProgress(sent, fileSize);
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
            catch (HttpRequestException) when (attempt < 12)
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

    private static string BuildErrorMessage(Exception? ex, int totalAttempts)
    {
        if (ex is null) return "Unknown error";
        var msg = ex.Message.Split('\n')[0].Trim();
        return $"Failed after {totalAttempts} attempt{(totalAttempts != 1 ? "s" : "")}: {msg}";
    }

    private async Task ProcessJobAsync(WorkerSettings settings, string token,
        JobProgressViewModel vm, JsonElement job, CancellationToken ct)
    {
        var fileId = job.GetProperty("file_id").GetInt32();
        var url = job.GetProperty("url").GetString()!;
        var destPath = job.GetProperty("dest_path").GetString()!;
        long knownSize = job.TryGetProperty("size", out var sz) && sz.ValueKind != JsonValueKind.Null
            ? sz.GetInt64() : 0;

        void UI(Action a) => Avalonia.Threading.Dispatcher.UIThread.Post(a);

        UI(() =>
        {
            vm.Label = destPath.Length <= 60 ? destPath : "..." + destPath[^57..];
            vm.MaxAttempts = MaxRetries;
            vm.Status = JobStatus.Downloading;
        });

        // Files that fit within the threshold are kept entirely in RAM.
        // aria2c always writes to disk, so in-memory only applies to HTTP downloads.
        bool useInMemory = settings.InMemoryThresholdBytes > 0
            && knownSize > 0
            && knownSize <= settings.InMemoryThresholdBytes;

        var localPath = useInMemory ? null : LocalPathForJob(settings.TempDir, url, destPath);
        Exception? lastErr = null;
        bool uploaded = false;
        long fileSize = 0;

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

                Action<long, long> dlProgress = (rx, total) => UI(() =>
                {
                    vm.BytesTransferred = rx;
                    if (total > 0) vm.TotalBytes = total;
                    vm.UpdateSpeed(rx);
                });

                byte[]? inMemData = null;
                if (useInMemory)
                {
                    inMemData = await DownloadToMemoryAsync(url, knownSize, ct, dlProgress);
                    fileSize = inMemData.LongLength;
                }
                else
                {
                    await DownloadFileAsync(url, localPath!, settings.Aria2cConnections, knownSize, ct, dlProgress);
                    fileSize = new FileInfo(localPath!).Length;
                }

                UI(() =>
                {
                    vm.Status = JobStatus.Uploading;
                    vm.BytesTransferred = 0;
                    vm.TotalBytes = fileSize;
                    vm.ResetSpeed();
                });

                Action<long, long> ulProgress = (s, t) => UI(() =>
                {
                    vm.BytesTransferred = s;
                    vm.TotalBytes = t;
                    vm.UpdateSpeed(s);
                });

                if (useInMemory)
                {
                    using var ms = new MemoryStream(inMemData!);
                    await UploadStreamAsync(settings.UploadServerUrl, token, fileId, ms, fileSize, ulProgress, ct);
                }
                else
                {
                    await using var fs = new FileStream(localPath!, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 20, true);
                    await UploadStreamAsync(settings.UploadServerUrl, token, fileId, fs, fileSize, ulProgress, ct);
                }

                uploaded = true;
                break;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                lastErr = ex;
                var msg = $"Attempt {attempt}/{MaxRetries}: {ex.Message.Split('\n')[0].Trim()}";
                UI(() => { vm.ErrorMessage = msg; vm.SpeedText = ""; });
                if (!useInMemory && localPath != null && File.Exists(localPath)) File.Delete(localPath);
                if (attempt < MaxRetries)
                {
                    UI(() => { vm.CurrentAttempt = attempt; vm.Status = JobStatus.Retrying; });
                    await Task.Delay(TimeSpan.FromSeconds(RetryDelaySeconds * attempt), ct);
                }
            }
        }

        if (!uploaded)
        {
            var errMsg = BuildErrorMessage(lastErr, MaxRetries);
            UI(() => { vm.Status = JobStatus.Failed; vm.SpeedText = ""; vm.ErrorMessage = errMsg; });
            try { await ReportJobAsync(settings.ServerUrl, token, fileId, "failed", error: errMsg, ct: ct); } catch { }
            return;
        }

        UI(() => { vm.Status = JobStatus.Done; vm.BytesTransferred = fileSize; vm.SpeedText = ""; });
        if (!useInMemory && !settings.KeepFiles && localPath != null && File.Exists(localPath)) File.Delete(localPath);

        try { await ReportJobAsync(settings.ServerUrl, token, fileId, "completed", bytesDownloaded: fileSize, ct: ct); }
        catch { /* best-effort */ }
    }

    public async Task RunAsync(WorkerSettings settings, string token,
        Action<string> log, CancellationToken ct)
    {
        Directory.CreateDirectory(settings.TempDir);
        var seen = new ConcurrentDictionary<int, byte>();
        var channel = Channel.CreateBounded<JsonElement>(
            new BoundedChannelOptions(settings.Concurrency * 2) { FullMode = BoundedChannelFullMode.Wait });

        log($"Starting — concurrency={settings.Concurrency}, aria2c={_hasAria2c}");

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
                    await ProcessJobAsync(settings, token, vm, captured, ct);
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
        log("Worker stopped.");
    }
}

} // namespace MrloicMinervaDPN.Services
