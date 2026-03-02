using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace MrloicMinervaDPN.Services;

public static class AuthService
{
    public static async Task<string> LoginAsync(string serverUrl, Action<string> log, CancellationToken ct)
    {
        var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var listener = new HttpListener();
        listener.Prefixes.Add("http://127.0.0.1:19283/");
        try
        {
            listener.Start();
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Cannot start local listener on port 19283: {ex.Message}", ex);
        }

        var callbackUrl = "http://127.0.0.1:19283/";
        var loginUrl = $"{serverUrl}/auth/discord/login?worker_callback={Uri.EscapeDataString(callbackUrl)}";
        log($"Opening browser for Discord login...");
        log($"If it doesn't open: {loginUrl}");
        Process.Start(new ProcessStartInfo(loginUrl) { UseShellExecute = true });

        using var reg = ct.Register(() =>
        {
            tcs.TrySetCanceled();
            listener.Stop();
        });

        _ = Task.Run(async () =>
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    HttpListenerContext ctx;
                    try
                    {
                        ctx = await listener.GetContextAsync().WaitAsync(ct);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    var query = ctx.Request.Url?.Query ?? "";
                    var token = ParseQueryParam(query, "token");
                    if (!string.IsNullOrEmpty(token))
                    {
                        var html = "<h1>Logged in! You can close this tab.</h1>"u8.ToArray();
                        ctx.Response.StatusCode = 200;
                        ctx.Response.ContentType = "text/html";
                        ctx.Response.ContentLength64 = html.Length;
                        await ctx.Response.OutputStream.WriteAsync(html, ct);
                        ctx.Response.Close();
                        tcs.TrySetResult(token);
                        break;
                    }
                    else
                    {
                        ctx.Response.StatusCode = 400;
                        ctx.Response.Close();
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                tcs.TrySetException(ex);
            }
            finally
            {
                try { listener.Stop(); } catch { /* ignore */ }
            }
        }, ct);

        return await tcs.Task;
    }

    private static string? ParseQueryParam(string query, string key)
    {
        query = query.TrimStart('?');
        foreach (var part in query.Split('&'))
        {
            var kv = part.Split('=', 2);
            if (kv.Length == 2 && string.Equals(Uri.UnescapeDataString(kv[0]), key, StringComparison.OrdinalIgnoreCase))
                return Uri.UnescapeDataString(kv[1]);
        }
        return null;
    }
}
