using System;
using System.Diagnostics;
using CommunityToolkit.Mvvm.ComponentModel;

namespace MrloicMinervaDPN.Models;

public enum JobStatus { Pending, Downloading, Uploading, Retrying, Done, Failed }

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
        JobStatus.Uploading   => "Uploading",
        JobStatus.Retrying    => CurrentAttempt > 0 && MaxAttempts > 0
                                    ? $"Retrying {CurrentAttempt}/{MaxAttempts}"
                                    : "Retrying",
        JobStatus.Done        => "Done",
        JobStatus.Failed      => "FAILED",
        _                     => "Unknown",
    };

    public double Progress => TotalBytes > 0 ? (double)BytesTransferred / TotalBytes * 100 : 0;

    public bool HasError => !string.IsNullOrEmpty(ErrorMessage) &&
                            (Status is JobStatus.Failed or JobStatus.Retrying);
    public bool IsActive => Status is JobStatus.Downloading or JobStatus.Uploading;
    public bool IsFailed => Status == JobStatus.Failed;
    public bool IsRetrying => Status == JobStatus.Retrying;
    /// <summary>True when status needs no special color (Pending, Downloading, Uploading, Done).</summary>
    public bool IsNormal => !IsFailed && !IsRetrying;

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

    public void ResetSpeed()
    {
        _lastSpeedBytes = 0;
        _speedWatch.Restart();
        CurrentSpeedBps = 0;
        SpeedText = "";
    }

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
        OnPropertyChanged(nameof(HasError));
        OnPropertyChanged(nameof(IsActive));
        OnPropertyChanged(nameof(IsFailed));
        OnPropertyChanged(nameof(IsRetrying));
        OnPropertyChanged(nameof(IsNormal));
    }

    partial void OnBytesTransferredChanged(long value) => OnPropertyChanged(nameof(Progress));
    partial void OnTotalBytesChanged(long value) => OnPropertyChanged(nameof(Progress));
    partial void OnErrorMessageChanged(string value) => OnPropertyChanged(nameof(HasError));
    partial void OnCurrentAttemptChanged(int value) => OnPropertyChanged(nameof(StatusText));
}
