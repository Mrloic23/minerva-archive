using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace MrloicMinervaDPN.Services;

/// <summary>Little-endian binary wire codec shared by the Minerva protocol decoder and message builders.</summary>
internal static class BinaryCodec
{
    // ── Write helpers ─────────────────────────────────────────────────────
    public static void WriteU8(MemoryStream ms, byte v) => ms.WriteByte(v);

    public static void WriteU32(MemoryStream ms, uint v)
    {
        System.Span<byte> b = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(b, v);
        ms.Write(b);
    }

    public static void WriteStr(MemoryStream ms, string s)
    {
        var maxLen = Encoding.UTF8.GetMaxByteCount(s.Length);
        var buf = ArrayPool<byte>.Shared.Rent(maxLen);
        try
        {
            var written = Encoding.UTF8.GetBytes(s, 0, s.Length, buf, 0);
            WriteU32(ms, (uint)written);
            ms.Write(buf, 0, written);
        }
        finally { ArrayPool<byte>.Shared.Return(buf); }
    }

    public static void WriteRawBytes(MemoryStream ms, byte[] data, int offset, int count)
    {
        WriteU32(ms, (uint)count);
        ms.Write(data, offset, count);
    }

    // ── Read helpers ──────────────────────────────────────────────────────
    public static byte ReadU8(byte[] buf, ref int pos) => buf[pos++];

    public static uint ReadU32(byte[] buf, ref int pos)
    {
        var v = BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(pos));
        pos += 4;
        return v;
    }

    public static ulong ReadU64(byte[] buf, ref int pos)
    {
        var v = BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(pos));
        pos += 8;
        return v;
    }

    public static string ReadStr(byte[] buf, ref int pos)
    {
        var len = (int)ReadU32(buf, ref pos);
        var s = Encoding.UTF8.GetString(buf, pos, len);
        pos += len;
        return s;
    }

    public static Dictionary<string, string> ReadKv(byte[] buf, ref int pos)
    {
        var count = (int)ReadU32(buf, ref pos);
        var dict = new Dictionary<string, string>(count);
        for (int i = 0; i < count; i++)
            dict[ReadStr(buf, ref pos)] = ReadStr(buf, ref pos);
        return dict;
    }
}
