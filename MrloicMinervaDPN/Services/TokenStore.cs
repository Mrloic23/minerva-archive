using System;
using System.IO;

namespace MrloicMinervaDPN.Services;

public static class TokenStore
{
    private static readonly string TokenFile = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
        ".minerva-dpn", "token");

    public static void Save(string token)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(TokenFile)!);
        File.WriteAllText(TokenFile, token);
    }

    public static string? Load()
    {
        if (!File.Exists(TokenFile)) return null;
        var t = File.ReadAllText(TokenFile).Trim();
        return string.IsNullOrEmpty(t) ? null : t;
    }

    public static void Delete()
    {
        if (File.Exists(TokenFile)) File.Delete(TokenFile);
    }
}
