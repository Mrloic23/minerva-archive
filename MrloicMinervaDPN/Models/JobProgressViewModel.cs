using System;
using System.Diagnostics;
using Avalonia.Media;
using CommunityToolkit.Mvvm.ComponentModel;

namespace MrloicMinervaDPN.Models;

public enum JobStatus { Pending, Downloading, Queued, Uploading, Retrying, Done, Failed }

public partial class JobProgressViewModel : ObservableObject
{
    [ObservableProperty] private string _label = "";
    [ObservableProperty] private JobStatus _status = JobStatus.Pending;
    [ObservableProperty] private long _bytesTransferred;
    [ObservableProperty] private long _totalBytes;
    [ObservableProperty] private string _errorMessage = "";
    [ObservableProperty] private string _speedText = "";
    [ObservableProperty] private int _currentAttempt;
    [ObservableProperty] private int _maxAttempts;

    // Speed tracking internals
    private long _lastSpeedBytes;
    private readonly Stopwatch _speedWatch = Stopwatch.StartNew();

    /// <summary>Most-recently computed transfer rate in bytes/sec. Updated by <see cref="UpdateSpeed"/>.</summary>
    public double CurrentSpeedBps { get; private set; }

    public string StatusText => Status switch
    {
        JobStatus.Pending     => "Pending",
        JobStatus.Downloading => "Downloading",
        JobStatus.Queued      => "Queued",
        JobStatus.Uploading   => "Uploading",
        JobStatus.Retrying    => CurrentAttempt > 0 && MaxAttempts > 0
                                    ? $"Retrying {CurrentAttempt}/{MaxAttempts}"
                                    : "Retrying",
        JobStatus.Done        => "Done",
        JobStatus.Failed      => "FAILED",
        _                     => "Unknown",
    };

    public double Progress => TotalBytes > 0 ? (double)BytesTransferred / TotalBytes * 100 : 0;

    public string TotalSizeText => TotalBytes > 0 ? FormatSize(TotalBytes) : "";
    public string TransferText =>
        (BytesTransferred > 0, TotalBytes > 0) switch
        {
            (true,  true)  => $"{FormatSize(BytesTransferred)} / {FormatSize(TotalBytes)}",
            (false, true)  => $"— / {FormatSize(TotalBytes)}",
            (true,  false) => $"{FormatSize(BytesTransferred)} / ?",
            _              => "",
        };

    public bool HasError => !string.IsNullOrEmpty(ErrorMessage) &&
                            (Status is JobStatus.Failed or JobStatus.Retrying);
    public bool IsActive => Status is JobStatus.Downloading or JobStatus.Uploading;
    public bool IsFailed => Status == JobStatus.Failed;
    public bool IsRetrying => Status == JobStatus.Retrying;
    public bool IsQueued => Status == JobStatus.Queued;
    /// <summary>True when status needs no special color (Pending, Downloading, Uploading, Done).</summary>
    public bool IsNormal => !IsFailed && !IsRetrying && !IsQueued;

    private static readonly SolidColorBrush BrushGreen   = new(Color.Parse("#4CAF50"));
    private static readonly SolidColorBrush BrushBlue    = new(Color.Parse("#42A5F5"));
    private static readonly SolidColorBrush BrushOrange  = new(Color.Parse("#FF9800"));
    private static readonly SolidColorBrush BrushRed     = new(Color.Parse("#EF5350"));
    private static readonly SolidColorBrush BrushDefault = new(Color.Parse("#555555"));

    public ISolidColorBrush StatusColor => Status switch
    {
        JobStatus.Downloading or JobStatus.Uploading or JobStatus.Done => BrushGreen,
        JobStatus.Queued   => BrushBlue,
        JobStatus.Retrying => BrushOrange,
        JobStatus.Failed   => BrushRed,
        _                  => BrushDefault,
    };

    /// <summary>Call from progress callbacks to update speed. Thread-safe.</summary>
    public void UpdateSpeed(long bytesNow)
    {
        var elapsed = _speedWatch.Elapsed.TotalSeconds;
        if (elapsed < 0.5) return;                          // update at most every 0.5s
        var delta = bytesNow - _lastSpeedBytes;
        if (delta < 0) delta = bytesNow;                    // phase reset (new file)
        var bps = delta / elapsed;
        _lastSpeedBytes = bytesNow;
        _speedWatch.Restart();
        CurrentSpeedBps = bps;
        SpeedText = FormatSpeed(bps);
    }

    /// <summary>Directly sets the speed from an externally-measured rate (e.g. per-chunk
    /// upload timing). Bypasses the time gate used by <see cref="UpdateSpeed"/>.</summary>
    public void ForceSetSpeed(double bps)
    {
        CurrentSpeedBps = bps;
        SpeedText = FormatSpeed(bps);
    }

    public void ResetSpeed()
    {
        _lastSpeedBytes = 0;
        _speedWatch.Restart();
        CurrentSpeedBps = 0;
        SpeedText = "";
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

    partial void OnStatusChanged(JobStatus value)
    {
        OnPropertyChanged(nameof(StatusText));
        OnPropertyChanged(nameof(StatusColor));
        OnPropertyChanged(nameof(HasError));
        OnPropertyChanged(nameof(IsActive));
        OnPropertyChanged(nameof(IsFailed));
        OnPropertyChanged(nameof(IsRetrying));
        OnPropertyChanged(nameof(IsQueued));
        OnPropertyChanged(nameof(IsNormal));
    }

    partial void OnBytesTransferredChanged(long value)
    {
        OnPropertyChanged(nameof(Progress));
        OnPropertyChanged(nameof(TransferText));
    }

    partial void OnTotalBytesChanged(long value)
    {
        OnPropertyChanged(nameof(Progress));
        OnPropertyChanged(nameof(TotalSizeText));
        OnPropertyChanged(nameof(TransferText));
    }
    partial void OnErrorMessageChanged(string value) => OnPropertyChanged(nameof(HasError));
    partial void OnCurrentAttemptChanged(int value) => OnPropertyChanged(nameof(StatusText));
}
