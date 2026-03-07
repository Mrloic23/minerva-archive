using System;

namespace MrloicMinervaDPN.Services;

/// <summary>Convenience helpers for dispatching work to the Avalonia UI thread.</summary>
internal static class UIHelper
{
    /// <summary>Posts <paramref name="a"/> to the Avalonia UI thread (fire-and-forget).</summary>
    public static void UI(Action a) => Avalonia.Threading.Dispatcher.UIThread.Post(a);
}
