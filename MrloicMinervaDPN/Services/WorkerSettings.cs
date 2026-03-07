namespace MrloicMinervaDPN.Services;

public sealed class WorkerSettings
{
    public string ServerUrl   { get; set; } = "https://firehose.minerva-archive.org";
    public int    Concurrency { get; set; } = 2;
    public int    Retries     { get; set; } = 5;
}
