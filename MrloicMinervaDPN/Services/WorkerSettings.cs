using System;
using System.IO;

namespace MrloicMinervaDPN.Services;

public sealed class WorkerSettings
{
    public string ServerUrl { get; set; } = "https://api.minerva-archive.org";
    public string UploadServerUrl { get; set; } = "https://gate.minerva-archive.org";
    public int Concurrency { get; set; } = 2;
    public int BatchSize { get; set; } = 10;
    public int Aria2cConnections { get; set; } = 8;
    /// <summary>When true, aria2c is automatically downloaded on first run if not found in PATH.</summary>
    public bool AutoInstallAria2c { get; set; } = true;
    public string TempDir { get; set; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".minerva-dpn", "tmp");
    public bool KeepFiles { get; set; } = false;
    /// <summary>Files whose known size is at or below this threshold are kept entirely in
    /// RAM — never written to <see cref="TempDir"/>. 0 = always write to disk.</summary>
    public long InMemoryThresholdBytes { get; set; } = 0;
}
