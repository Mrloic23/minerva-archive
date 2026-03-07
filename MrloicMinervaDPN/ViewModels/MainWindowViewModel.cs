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
    [ObservableProperty] private string  _serverUrl   = "https://firehose.minerva-archive.org";
    [ObservableProperty] private decimal _concurrency = 2;
    [ObservableProperty] private decimal _retries     = 5;

    // ── Worker state ───────────────────────────────────────────────────────
    [ObservableProperty] private bool _isRunning;
    [ObservableProperty] private string _logText = "";
    [ObservableProperty] private string _downloadSpeedText = "";
    [ObservableProperty] private string _uploadSpeedText = "";

    // ── Update check ───────────────────────────────────────────────────────
    [ObservableProperty] private bool _updateAvailable;
    [ObservableProperty] private string _latestVersion = "";
    [ObservableProperty] private string _updateUrl = "";

    // ── Session counters ──────────────────────────────────────────────────
    [ObservableProperty] private int _filesUploaded;
    [ObservableProperty] private long _totalBytesUploaded;
    [ObservableProperty] private int _filesFailed;

    // ── Live job counts (updated by the speed timer) ───────────────────────
    [ObservableProperty] private int _downloadingCount;
    [ObservableProperty] private int _queuedCount;
    [ObservableProperty] private int _uploadingCount;

    public bool HasSessionStats => FilesUploaded > 0 || FilesFailed > 0;
    public bool HasFailedJobs   => FilesFailed > 0;
    public string FilesUploadedText     => $"{FilesUploaded} chunk{(FilesUploaded != 1 ? "s" : "")} processed";
    public string TotalBytesUploadedText => FormatSize(TotalBytesUploaded);
    public string FilesFailedText       => $"{FilesFailed} failed";

    public ObservableCollection<JobProgressViewModel> ActiveJobs => _workerService.ActiveJobs;

    public MainWindowViewModel()
    {
        var s = SettingsStore.Load();
        ServerUrl   = s.ServerUrl;
        Concurrency = s.Concurrency;
        Retries     = s.Retries;

        IsLoggedIn = TokenStore.Load() != null;
        _ = StartUpdateCheckLoopAsync();
    }

    private async Task StartUpdateCheckLoopAsync()
    {
        await CheckForUpdateAsync();
        using var timer = new PeriodicTimer(TimeSpan.FromMinutes(45));
        while (await timer.WaitForNextTickAsync())
            await CheckForUpdateAsync();
    }

    private async Task CheckForUpdateAsync()
    {
        try
        {
            var current = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version
                          ?? new Version(0, 6, 0);
            var info = await UpdateService.CheckAsync(current);
            if (info.IsAvailable)
            {
                UpdateAvailable = true;
                LatestVersion = info.LatestVersion;
                UpdateUrl = info.HtmlUrl;
            }
        }
        catch { /* silently ignore update-check failures */ }
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
        FilesUploaded = 0;
        TotalBytesUploaded = 0;
        FilesFailed = 0;

        var settings = new WorkerSettings
        {
            ServerUrl   = ServerUrl,
            Concurrency = (int)Concurrency,
            Retries     = (int)Retries,
        };

        _speedTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(1) };
        _speedTimer.Tick += (_, _) =>
        {
            double dl = 0, ul = 0;
            int dlCount = 0, qCount = 0, ulCount = 0;
            foreach (var j in ActiveJobs)
            {
                if (j.Status == JobStatus.Downloading) { dl += j.CurrentSpeedBps; dlCount++; }
                else if (j.Status == JobStatus.Queued)    qCount++;
                else if (j.Status == JobStatus.Uploading) { ul += j.CurrentSpeedBps; ulCount++; }
            }
            DownloadSpeedText = dl > 0 ? $"↓ {FormatSpeed(dl)}" : "";
            UploadSpeedText   = ul > 0 ? $"↑ {FormatSpeed(ul)}" : "";
            DownloadingCount = dlCount;
            QueuedCount      = qCount;
            UploadingCount   = ulCount;
        };
        _speedTimer.Start();

        try
        {
            await _workerService.RunAsync(settings, token, AppendLog, _workerCts.Token,
                onSuccess: bytes => Dispatcher.UIThread.Post(() =>
                {
                    FilesUploaded++;
                    TotalBytesUploaded += bytes;
                }),
                onFail: () => Dispatcher.UIThread.Post(() => FilesFailed++));
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
    public Task ShutdownAsync()
    {
        SettingsStore.Save(new PersistedSettings
        {
            ServerUrl   = ServerUrl,
            Concurrency = Concurrency,
            Retries     = Retries,
        });
        _loginCts?.Cancel();
        _workerCts?.Cancel();
        return _workerService.StopAsync();
    }

    // Keep the parameterless Shutdown for any callers that don't await.
    public void Shutdown() => _ = ShutdownAsync();

    [RelayCommand]
    private void ClearLog() => LogText = "";

    [RelayCommand]
    private void OpenUpdateUrl()
    {
        if (string.IsNullOrEmpty(UpdateUrl)) return;
        System.Diagnostics.Process.Start(
            new System.Diagnostics.ProcessStartInfo(UpdateUrl) { UseShellExecute = true });
    }

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
        if (!value) { DownloadingCount = 0; QueuedCount = 0; UploadingCount = 0; }
    }

    partial void OnFilesUploadedChanged(int value)
    {
        OnPropertyChanged(nameof(HasSessionStats));
        OnPropertyChanged(nameof(FilesUploadedText));
    }

    partial void OnTotalBytesUploadedChanged(long value)
    {
        OnPropertyChanged(nameof(TotalBytesUploadedText));
    }

    partial void OnFilesFailedChanged(int value)
    {
        OnPropertyChanged(nameof(HasSessionStats));
        OnPropertyChanged(nameof(HasFailedJobs));
        OnPropertyChanged(nameof(FilesFailedText));
    }

    private static string FormatSize(long bytes) => bytes switch
    {
        >= 1_073_741_824 => $"{bytes / 1_073_741_824.0:F1} GB",
        >= 1_048_576     => $"{bytes / 1_048_576.0:F1} MB",
        >= 1_024         => $"{bytes / 1_024.0:F0} KB",
        _                => $"{bytes} B",
    };

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
