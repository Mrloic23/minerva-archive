using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;

namespace MrloicMinervaDPN.Services;

public record UpdateInfo(bool IsAvailable, string LatestVersion, string HtmlUrl);

public static class UpdateService
{
    private const string ApiUrl = "https://api.github.com/repos/Mrloic23/minerva-archive/releases/latest";

    public static async Task<UpdateInfo> CheckAsync(Version currentVersion)
    {
        using var client = new HttpClient();
        client.DefaultRequestHeaders.UserAgent.Add(
            new ProductInfoHeaderValue("MrloicMinervaDPN", currentVersion.ToString()));

        var response = await client.GetAsync(ApiUrl);
        response.EnsureSuccessStatusCode();

        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var root = doc.RootElement;

        var tagName = root.GetProperty("tag_name").GetString() ?? "";
        var htmlUrl = root.GetProperty("html_url").GetString() ?? "";

        var versionStr = tagName.TrimStart('v');
        if (!Version.TryParse(versionStr, out var latestVersion))
            return new UpdateInfo(false, tagName, htmlUrl);

        return new UpdateInfo(latestVersion > currentVersion, tagName, htmlUrl);
    }
}
