using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;

namespace MrloicMinervaDPN.Services;

public sealed class SessionStore
{
    private static readonly string FilePath = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
        ".minerva-dpn", "sessions.json");

    private readonly object _lock = new();
    private readonly Dictionary<int, string> _map;

    public SessionStore()
    {
        try
        {
            if (File.Exists(FilePath))
            {
                var json = File.ReadAllText(FilePath);
                _map = JsonSerializer.Deserialize<Dictionary<int, string>>(json) ?? [];
                return;
            }
        }
        catch { }
        _map = [];
    }

    public string? Get(int fileId)
    {
        lock (_lock) return _map.TryGetValue(fileId, out var s) ? s : null;
    }

    public void Set(int fileId, string sessionId)
    {
        lock (_lock) { _map[fileId] = sessionId; Flush(); }
    }

    public void Remove(int fileId)
    {
        lock (_lock) { if (_map.Remove(fileId)) Flush(); }
    }

    private void Flush()
    {
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(FilePath)!);
            File.WriteAllText(FilePath, JsonSerializer.Serialize(_map));
        }
        catch { }
    }
}
