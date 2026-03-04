using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace MrloicMinervaDPN.Services;

/// <summary>Locates or downloads aria2c, caching it under LocalApplicationData.</summary>
static class Aria2cInstaller
{
    private const string Aria2Version = "1.37.0";

    private static readonly string CacheDir = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "MrloicMinervaDPN", "aria2c");

    public static string ExeName =>
        RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "aria2c.exe" : "aria2c";

    /// <summary>
    /// Returns the path to aria2c if already available (cache dir or PATH),
    /// or downloads + installs it. Returns null if the platform is unsupported
    /// or the download fails.
    /// </summary>
    public static async Task<string?> EnsureAsync(Action<string> log, bool autoInstall, CancellationToken ct)
    {
        // 1. Already cached from a previous run?
        var cached = Path.Combine(CacheDir, ExeName);
        if (File.Exists(cached)) return cached;

        // 2. Already on PATH?
        var onPath = FindOnPath();
        if (onPath != null) return onPath;

        if (!autoInstall)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                log("aria2c not found. Install manually: 'sudo apt install aria2' (Ubuntu/Debian) or 'sudo yum install aria2' (CentOS/RHEL) or 'sudo pacman -S aria2' (Arch).");
            }
            else
            {
                log("aria2c not found and auto-install is disabled. " +
                    "Install aria2 manually for parallel/multi-connection downloads.");
            }
            return null;
        }

        // 3. Determine platform download.
        if (!TryGetDownload(out var url, out var isTarBz2, out var entryInZip))
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                log("aria2c: auto-install not available for Linux. " +
                    "Install manually: 'sudo apt install aria2' (Ubuntu/Debian) or 'sudo yum install aria2' (CentOS/RHEL) or 'sudo pacman -S aria2' (Arch).");
            }
            else
            {
                log("aria2c: auto-install not supported on this platform. " +
                    "Install aria2 manually for parallel/multi-connection downloads.");
            }
            return null;
        }

        log($"aria2c not found — downloading v{Aria2Version}…");
        Directory.CreateDirectory(CacheDir);

        var archiveName = isTarBz2 ? "aria2c.tar.bz2" : "aria2c.zip";
        var archivePath = Path.Combine(CacheDir, archiveName);
        var tmpBin = cached + ".tmp";

        try
        {
            // Download archive.
            using var http = new HttpClient();
            http.DefaultRequestHeaders.Add("User-Agent", "MrloicMinervaDPN");
            using var resp = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, ct);
            resp.EnsureSuccessStatusCode();

            await using (var fs = new FileStream(archivePath, FileMode.Create, FileAccess.Write, FileShare.None))
                await resp.Content.CopyToAsync(fs, ct);

            // Extract binary.
            if (isTarBz2)
            {
                var extractDir = Path.Combine(CacheDir, "extract_tmp");
                if (Directory.Exists(extractDir)) Directory.Delete(extractDir, true);
                Directory.CreateDirectory(extractDir);

                int code = await RunProcessAsync("tar", ["xf", archivePath, "-C", extractDir], ct);
                if (code != 0) throw new InvalidOperationException($"tar exited with code {code}");

                var found = FindRecursive(extractDir, "aria2c")
                    ?? throw new InvalidOperationException("aria2c binary not found in archive");

                File.Move(found, tmpBin, overwrite: true);
                Directory.Delete(extractDir, true);
            }
            else
            {
                using var zip = ZipFile.OpenRead(archivePath);
                var entry = zip.GetEntry(entryInZip!)
                    ?? throw new InvalidOperationException($"Entry '{entryInZip}' not in archive");
                await using var es = entry.Open();
                await using var fs = new FileStream(tmpBin, FileMode.Create, FileAccess.Write);
                await es.CopyToAsync(fs, ct);
            }

            // Atomic rename.
            if (File.Exists(cached)) File.Delete(cached);
            File.Move(tmpBin, cached);

            // Mark executable on Unix.
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                await RunProcessAsync("chmod", ["+x", cached], ct);

            File.Delete(archivePath);
            log($"aria2c v{Aria2Version} installed to {CacheDir}");
            return cached;
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            log($"aria2c auto-install failed: {ex.Message.Split('\n')[0].Trim()}");
            try { if (File.Exists(archivePath)) File.Delete(archivePath); } catch { }
            try { if (File.Exists(tmpBin)) File.Delete(tmpBin); } catch { }
            return null;
        }
    }

    private static bool TryGetDownload(out string url, out bool isTarBz2, out string? entryInZip)
    {
        var arch = RuntimeInformation.OSArchitecture;
        isTarBz2 = false;
        entryInZip = null;

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && arch == Architecture.X64)
        {
            var stem = $"aria2-{Aria2Version}-win-64bit-build1";
            url = $"https://github.com/aria2/aria2/releases/download/release-{Aria2Version}/{stem}.zip";
            entryInZip = $"{stem}/aria2c.exe";
            return true;
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            // aria2 does not provide pre-compiled Linux binaries
            // Users should install via package manager: apt install aria2, yum install aria2, etc.
            url = "";
            return false;
        }

        url = "";
        return false;
    }

    private static string? FindOnPath()
    {
        var path = Environment.GetEnvironmentVariable("PATH") ?? "";
        var pathExt = Environment.GetEnvironmentVariable("PATHEXT") ?? "";
        foreach (var dir in path.Split(Path.PathSeparator))
        {
            var candidate = Path.Combine(dir, ExeName);
            if (File.Exists(candidate)) return candidate;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                foreach (var ext in pathExt.Split(';'))
                    if (File.Exists(candidate + ext)) return candidate + ext;
            }
        }
        return null;
    }

    private static string? FindRecursive(string dir, string name)
    {
        foreach (var f in Directory.EnumerateFiles(dir, name, SearchOption.AllDirectories))
            return f;
        return null;
    }

    private static async Task<int> RunProcessAsync(string exe, string[] args, CancellationToken ct)
    {
        var psi = new ProcessStartInfo(exe) { UseShellExecute = false, CreateNoWindow = true };
        foreach (var a in args) psi.ArgumentList.Add(a);
        using var proc = Process.Start(psi) ?? throw new InvalidOperationException($"Failed to start {exe}");
        await proc.WaitForExitAsync(ct);
        return proc.ExitCode;
    }
}
