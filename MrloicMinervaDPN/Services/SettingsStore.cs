using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MrloicMinervaDPN.Services;

public sealed class PersistedSettings
{
    public string  ServerUrl            { get; set; } = "https://api.minerva-archive.org";
    public string  UploadServerUrl      { get; set; } = "https://gate.minerva-archive.org";
    public decimal Concurrency          { get; set; } = 4;
    public decimal UploadConcurrency    { get; set; } = 4;
    public decimal BatchSize            { get; set; } = 4;
    public decimal Aria2cConnections    { get; set; } = 16;
    public bool    AutoInstallAria2c    { get; set; } = true;
    public string  TempDir              { get; set; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".minerva-dpn", "tmp");
    public bool    KeepFiles            { get; set; } = false;
    public decimal InMemoryThresholdMb  { get; set; } = 0;
    public decimal MaxCacheSizeMb       { get; set; } = 10240;
}

public static class SettingsStore
{
    private static readonly string FilePath = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
        ".minerva-dpn", "settings.json");

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>Loads persisted settings, falling back to defaults on any error.</summary>
    public static PersistedSettings Load()
    {
        try
        {
            if (File.Exists(FilePath))
            {
                var json = File.ReadAllText(FilePath);
                return JsonSerializer.Deserialize<PersistedSettings>(json, JsonOpts)
                       ?? new PersistedSettings();
            }
        }
        catch { /* corrupt / unreadable — use defaults */ }
        return new PersistedSettings();
    }

    /// <summary>Saves settings to disk. Silently ignores I/O errors.</summary>
    public static void Save(PersistedSettings settings)
    {
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(FilePath)!);
            File.WriteAllText(FilePath, JsonSerializer.Serialize(settings, JsonOpts));
        }
        catch { }
    }
}
