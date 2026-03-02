using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace MrloicMinervaDPN.Services;

/// <summary>A read-only stream wrapper that fires <paramref name="onRead"/> with the
/// byte count after every successful <see cref="ReadAsync"/> call. Using this with
/// <see cref="System.Net.Http.StreamContent"/> ensures progress callbacks fire as
/// HttpClient reads bytes to send over the network, not during an internal buffer copy.</summary>
sealed class ProgressStream : Stream
{
    private readonly Stream _inner;
    private readonly Action<int> _onRead;

    public ProgressStream(Stream inner, Action<int> onRead)
    {
        _inner = inner;
        _onRead = onRead;
    }

    public override bool CanRead  => true;
    public override bool CanSeek  => _inner.CanSeek;
    public override bool CanWrite => false;
    public override long Length   => _inner.Length;
    public override long Position { get => _inner.Position; set => _inner.Position = value; }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
    {
        int n = await _inner.ReadAsync(buffer, ct);
        if (n > 0) _onRead(n);
        return n;
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
    {
        int n = await _inner.ReadAsync(buffer, offset, count, ct);
        if (n > 0) _onRead(n);
        return n;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        int n = _inner.Read(buffer, offset, count);
        if (n > 0) _onRead(n);
        return n;
    }

    public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override void Flush() { }

    protected override void Dispose(bool disposing)
    {
        if (disposing) _inner.Dispose();
        base.Dispose(disposing);
    }
}
