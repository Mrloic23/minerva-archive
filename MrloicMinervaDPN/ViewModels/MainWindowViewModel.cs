using System;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using MrloicMinervaDPN.Models;
using MrloicMinervaDPN.Services;

namespace MrloicMinervaDPN.ViewModels;

public partial class MainWindowViewModel : ViewModelBase
{
    private CancellationTokenSource? _loginCts;
    private CancellationTokenSource? _workerCts;
    private readonly MinervaWorkerService _workerService = new();
    private DispatcherTimer? _speedTimer;

    // ── Auth ───────────────────────────────────────────────────────────────
    [ObservableProperty] private bool _isLoggedIn;
    [ObservableProperty] private bool _isLoggingIn;

    // ── Settings ───────────────────────────────────────────────────────────
    [ObservableProperty] private string _serverUrl = "https://api.minerva-archive.org";
    [ObservableProperty] private string _uploadServerUrl = "https://gate.minerva-archive.org";
    [ObservableProperty] private decimal _concurrency = 2;
    [ObservableProperty] private decimal _batchSize = 10;
    [ObservableProperty] private decimal _aria2cConnections = 8;
    [ObservableProperty] private bool _autoInstallAria2c = true;
    [ObservableProperty] private string _tempDir = System.IO.Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".minerva-dpn", "tmp");
    [ObservableProperty] private bool _keepFiles;
    /// <summary>Files at or below this size (in MB) are kept only in RAM. 0 = always use disk.</summary>
    [ObservableProperty] private decimal _inMemoryThresholdMb = 0;

    // ── Worker state ───────────────────────────────────────────────────────
    [ObservableProperty] private bool _isRunning;
    [ObservableProperty] private string _logText = "";
    [ObservableProperty] private string _downloadSpeedText = "";
    [ObservableProperty] private string _uploadSpeedText = "";

    public ObservableCollection<JobProgressViewModel> ActiveJobs => _workerService.ActiveJobs;

    public MainWindowViewModel()
    {
        IsLoggedIn = TokenStore.Load() != null;
    }

    // ── Commands ───────────────────────────────────────────────────────────

    [RelayCommand(CanExecute = nameof(CanLogin))]
    private async Task LoginAsync()
    {
        IsLoggingIn = true;
        _loginCts = new CancellationTokenSource();
        try
        {
            var token = await AuthService.LoginAsync(ServerUrl, AppendLog, _loginCts.Token);
            TokenStore.Save(token);
            IsLoggedIn = true;
            AppendLog("Login successful!");
        }
        catch (OperationCanceledException)
        {
            AppendLog("Login cancelled.");
        }
        catch (Exception ex)
        {
            AppendLog($"Login failed: {ex.Message}");
        }
        finally
        {
            IsLoggingIn = false;
            _loginCts = null;
        }
    }
    private bool CanLogin() => !IsLoggedIn && !IsLoggingIn;

    [RelayCommand(CanExecute = nameof(IsLoggingIn))]
    private void CancelLogin()
    {
        _loginCts?.Cancel();
    }

    [RelayCommand(CanExecute = nameof(CanLogout))]
    private void Logout()
    {
        _workerCts?.Cancel();
        TokenStore.Delete();
        IsLoggedIn = false;
        AppendLog("Logged out.");
    }
    private bool CanLogout() => IsLoggedIn;

    [RelayCommand(CanExecute = nameof(CanStart))]
    private async Task StartWorkerAsync()
    {
        var token = TokenStore.Load();
        if (token is null) { AppendLog("Not logged in."); return; }

        _workerCts = new CancellationTokenSource();
        IsRunning = true;

        var settings = new WorkerSettings
        {
            ServerUrl = ServerUrl,
            UploadServerUrl = UploadServerUrl,
            Concurrency = (int)Concurrency,
            BatchSize = (int)BatchSize,
            Aria2cConnections = (int)Aria2cConnections,
            AutoInstallAria2c = AutoInstallAria2c,
            TempDir = TempDir,
            KeepFiles = KeepFiles,
            InMemoryThresholdBytes = (long)(InMemoryThresholdMb * 1024 * 1024),
        };

        _speedTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(1) };
        _speedTimer.Tick += (_, _) =>
        {
            double dl = 0, ul = 0;
            foreach (var j in ActiveJobs)
            {
                if (j.Status == JobStatus.Downloading) dl += j.CurrentSpeedBps;
                else if (j.Status == JobStatus.Uploading)  ul += j.CurrentSpeedBps;
            }
            DownloadSpeedText = dl > 0 ? $"↓ {FormatSpeed(dl)}" : "";
            UploadSpeedText   = ul > 0 ? $"↑ {FormatSpeed(ul)}" : "";
        };
        _speedTimer.Start();

        try
        {
            await _workerService.RunAsync(settings, token, AppendLog, _workerCts.Token);
        }
        catch (OperationCanceledException)
        {
            AppendLog("Worker stopped.");
        }
        catch (Exception ex)
        {
            AppendLog($"Worker error: {ex.Message}");
        }
        finally
        {
            _speedTimer?.Stop();
            _speedTimer = null;
            DownloadSpeedText = "";
            UploadSpeedText = "";
            IsRunning = false;
            _workerCts = null;
        }
    }
    private bool CanStart() => IsLoggedIn && !IsRunning;

    [RelayCommand(CanExecute = nameof(IsRunning))]
    private void StopWorker()
    {
        _workerCts?.Cancel();
        AppendLog("Stopping worker...");
    }

    /// <summary>Called when the main window is closing. Cancels the worker and stops the
    /// aria2c daemon synchronously so the process does not linger after the app exits.</summary>
    public void Shutdown()
    {
        _loginCts?.Cancel();
        _workerCts?.Cancel();
        _workerService.Stop();
    }

    [RelayCommand]
    private void ClearLog() => LogText = "";

    partial void OnIsLoggedInChanged(bool value)
    {
        LoginCommand.NotifyCanExecuteChanged();
        LogoutCommand.NotifyCanExecuteChanged();
        StartWorkerCommand.NotifyCanExecuteChanged();
    }

    partial void OnIsLoggingInChanged(bool value)
    {
        LoginCommand.NotifyCanExecuteChanged();
        CancelLoginCommand.NotifyCanExecuteChanged();
    }

    partial void OnIsRunningChanged(bool value)
    {
        StartWorkerCommand.NotifyCanExecuteChanged();
        StopWorkerCommand.NotifyCanExecuteChanged();
    }

    private static string FormatSpeed(double bps) => bps switch
    {
        >= 1_073_741_824 => $"{bps / 1_073_741_824:F1} GB/s",
        >= 1_048_576     => $"{bps / 1_048_576:F1} MB/s",
        >= 1_024         => $"{bps / 1_024:F0} KB/s",
        _                => $"{bps:F0} B/s",
    };

    private void AppendLog(string message)
    {
        var line = $"[{DateTime.Now:HH:mm:ss}] {message}{Environment.NewLine}";
        Dispatcher.UIThread.Post(() =>
        {
            LogText += line;
        });
    }
}
