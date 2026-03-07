using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MrloicMinervaDPN.Services;

public sealed class PersistedSettings
{
    public string  ServerUrl   { get; set; } = "https://firehose.minerva-archive.org";
    public decimal Concurrency { get; set; } = 2;
    public decimal Retries     { get; set; } = 5;
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
