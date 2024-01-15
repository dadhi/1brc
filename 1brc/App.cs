using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Text;
using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using System.Numerics;
using System.Runtime.Intrinsics;

namespace _1brc;

unsafe readonly struct Chunk
{
    public readonly byte* Pointer;
    public readonly int Length;
    public Chunk(byte* pointer, int length)
    {
        Pointer = pointer;
        Length = length;
    }
}

[InlineArray(4)]
public struct Byte4
{
    private byte _element;
}


[StructLayout(LayoutKind.Sequential,
    Size = 8 + 2 + 2 + 2 + 2 + 8 // 28 bytes
)]
public unsafe struct StationTemperatures
{
    // It is tempting to replace NamePtr with 'int' Offset, but it will require the chunk start pointer to be around:
    // - because the unique names may be found in different chunks,
    // - using the global file pointer negates the win - we need the 'long' Offset back.
    public readonly byte* NamePtr; // 8 bytes
    public readonly short NameLen; // 2 bytes
    // public readonly Byte4 NamePrefix4; // 4 bytes
    public short Min;   // 2 bytes
    public short Max;   // 2 bytes
    public short Count; // 2 bytes
    public long Sum;    // 8 bytes

    public StationTemperatures(byte* namePtr, short nameLen, short val)
    {
        NamePtr = namePtr;
        NameLen = nameLen;
        // namePrefix4.CopyTo(NamePrefix4);
        Min = val;
        Max = val;
        Count = 1;
        Sum = val;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool NameEqualTo(in StationTemperatures other) =>
        new ReadOnlySpan<byte>(NamePtr, NameLen).SequenceEqual(new ReadOnlySpan<byte>(other.NamePtr, other.NameLen));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal int NameCompareTo(in StationTemperatures other) =>
        NamePtr == null ? 0 : // provides stable comparison for the empty entries
        new ReadOnlySpan<byte>(NamePtr, NameLen).SequenceCompareTo(new ReadOnlySpan<byte>(other.NamePtr, other.NameLen));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static int CalculateNameHash(byte* namePtr, short nameLen)
    {
        // Here we use the first 4 chars (if ASCII) and the length for a hash.
        // The worst case would be a prefix such as Port/Saint and the same length,
        // which for human geo names is quite rare. 

        // .NET dictionary will obviously slow down with collisions but will still work.
        // If we keep only `*_pointer` the run time is still reasonable ~9 secs.
        // Just using `if (_len > 0) return (_len * 820243) ^ (*_pointer);` gives 5.8 secs.
        // By just returning 0 - the worst possible hash function and linear search - the run time is 12x slower at 56 seconds. 

        // The magic number 820243 is the largest happy prime that contains 2024 from https://prime-numbers.info/list/happy-primes-page-9

        if (nameLen > 3)
            return (nameLen * 820243) ^ (int)*(uint*)namePtr;

        if (nameLen > 1)
            return (int)(uint)*(ushort*)namePtr;

        return (int)(uint)*namePtr;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string NameToString() => new((sbyte*)NamePtr, 0, NameLen, Encoding.UTF8);

    // todo: @perf optimize using stack based StringBuilder, avoid the NameToString materialization
    public override string ToString() => $"{NameToString()} = {Min * 0.1:N1}/{Sum * 0.1 / Count:N1}/{Max * 0.1:N1}";
}

public unsafe struct App : IDisposable
{
    private readonly FileStream _fileStream;
    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _va;
    private readonly SafeMemoryMappedViewHandle _vaHandle;
    private readonly byte* _pointer;
    private readonly long _fileLength;
    private readonly int _initialChunkCount;

    private const int RESULTS_CAPACITY = 1_024 * 16; // for measurements.txt; expecting ~10_000 unique results
    // private const int RESULTS_CAPACITY = 1_024 * 64; // for weather_stations.csv; expecting ~40_000 unique results
    private const int RESULTS_CAPACITY_MASK = RESULTS_CAPACITY - 1;
    private const int RESULTS_MAX_COUNT = RESULTS_CAPACITY - (RESULTS_CAPACITY >> 3);
    private const int MAX_CHUNK_SIZE = int.MaxValue - 100_000;

    public string FilePath { get; }

    public App(string filePath, int? chunkCount = null)
    {
        _initialChunkCount = Math.Max(1, chunkCount ?? Environment.ProcessorCount);
        FilePath = filePath;

        _fileStream = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 1, FileOptions.SequentialScan);
        var fileLength = _fileStream.Length;
        _mmf = MemoryMappedFile.CreateFromFile(FilePath, FileMode.Open);

        byte* ptr = (byte*)0;
        _va = _mmf.CreateViewAccessor(0, fileLength, MemoryMappedFileAccess.Read);
        _vaHandle = _va.SafeMemoryMappedViewHandle;
        _vaHandle.AcquirePointer(ref ptr);

        _pointer = ptr;

        _fileLength = fileLength;
    }

    Chunk[] SplitIntoMemoryChunks()
    {
        var sw = Stopwatch.StartNew();
        Debug.Assert(_fileStream.Position == 0);

        // We want equal chunks not larger than int.MaxValue
        // We want the number of chunks to be a multiple of CPU count, so multiply by 2
        // Otherwise with CPU_N+1 chunks the last chunk will be processed alone.

        var chunkCount = _initialChunkCount;
        var chunkSize = _fileLength / chunkCount;
        while (chunkSize > MAX_CHUNK_SIZE)
        {
            chunkCount *= 2;
            chunkSize = _fileLength / chunkCount;
        }

        long pos = 0;
        var chunks = new Chunk[chunkCount];
        for (int i = 0; i < chunkCount; i++)
        {
            var nextChunkPos = pos + chunkSize;
            int actualChunkLength;
            if (nextChunkPos >= _fileLength) // todo: @wip do we even need to do that?
            {
                actualChunkLength = (int)(_fileLength - pos);
                Debug.Assert(actualChunkLength > 0);
            }
            else
            {
                nextChunkPos = AlignToNewLineOrEof(_fileStream, nextChunkPos);
                actualChunkLength = (int)(nextChunkPos - pos);
            }

            chunks[i] = new(_pointer + pos, actualChunkLength);
            pos = nextChunkPos;
        }

        _fileStream.Position = 0; // don't forget to reset the position

        sw.Stop();
        Console.WriteLine($"Slice and align chunks: {sw.Elapsed}");
        return chunks;
    }

    static long AlignToNewLineOrEof(FileStream fileStream, long newPos)
    {
        fileStream.Position = newPos;

        int c;
        while ((c = fileStream.ReadByte()) >= 0 && c != '\n') { }

        return fileStream.Position;
    }

    const byte VEC_BYTES = 32; // Vector256<byte>.Count;

    static int ChunksProcessed = 0;

#if WIP
    static (StationTemperatures[] results, int count) ProcessChunk(Chunk chunk)
    {
        var sw = Stopwatch.StartNew();
        var ptr = chunk.Pointer;
        var len = chunk.Length;

        var results = new StationTemperatures[RESULTS_CAPACITY]; // todo: @perf find a way to do it on stack - the problem is how to merge those from multiple threads?
        var resultCount = 0;

        var vecSemicols = Vector256.Create((byte)';');
        var vecNewLines = Vector256.Create((byte)'\n');

        var namePos = 0;
        var pos = 0;
        for (; pos < len; pos += VEC_BYTES)
        {
            var vecBytes = Unsafe.ReadUnaligned<Vector256<byte>>(ptr + pos);
            var semicols = Vector256.Equals(vecBytes, vecSemicols).ExtractMostSignificantBits();
            var newLines = Vector256.Equals(vecBytes, vecNewLines).ExtractMostSignificantBits();
            while (semicols != 0)
            {
                var nameLen = BitOperations.TrailingZeroCount(semicols);

                var temperature = ParseTemperature(ptr, namePos + nameLen + 1);
                
                var result = new StationTemperatures(ptr + namePos, (short)nameLen, (short)temperature);
                AddOrMergeResult(results, ref resultCount, ref result);

                var nextNamePos = BitOperations.TrailingZeroCount(newLines) + 1;
                newLines >>= nextNamePos;
                semicols >>= nextNamePos;
                namePos += nextNamePos;
                if (semicols == 0)
                    break;
            }
        }
        if (pos - VEC_BYTES < len) // handling the remainder
        {
        }

        sw.Stop();
        Console.WriteLine($"Chunk {Interlocked.Increment(ref ChunksProcessed)}: {sw.Elapsed}");
        return (results, resultCount);
    }
#else

    static (StationTemperatures[] results, int count) ProcessChunk(Chunk chunk)
    {
        var sw = Stopwatch.StartNew();
        var ptr = chunk.Pointer;
        var len = chunk.Length;

        var resultCount = 0;
        StationTemperatures[] results = null;
        var resentResultCount = 0;
        Span<StationTemperatures> recentResults = stackalloc StationTemperatures[512];

        var vecSemicols = Vector256.Create((byte)';');

        var posOfNextSemicolon = -1;
        // loop line by line, line is either terminated by '\n' or EOF
        var pos = 0;
        while (pos < len)
        {
            var namePos = pos;
            int nameLen;
            if (posOfNextSemicolon != -1)
            {
                nameLen = posOfNextSemicolon - pos;
                posOfNextSemicolon = -1;
            }
            else
            {
                while (true) // simplify and invert the loop to read in Vectors
                {
                    if (pos + VEC_BYTES > len) // handle the small remainder at the end of file without SIMD (note that it may be more than 1 line)
                    {
                        nameLen = FindSemicolonIndexFallback(ptr, len, pos);
                        break;
                    }

                    var vecBytes = Unsafe.ReadUnaligned<Vector256<byte>>(ptr + pos);
                    var semicolsMask = Vector256.Equals(vecBytes, vecSemicols).ExtractMostSignificantBits();
                    if (semicolsMask != 0)
                    {
                        nameLen = BitOperations.TrailingZeroCount(semicolsMask);

                        // look for the next semicolon in the same vector, because the vector is 32 bytes wide and usually accommodates the 2 lines
                        semicolsMask >>= nameLen + 1;
                        if (semicolsMask != 0)
                            posOfNextSemicolon = pos + nameLen + 1 + BitOperations.TrailingZeroCount(semicolsMask);
                        break;
                    }
                    pos += VEC_BYTES;
                }
            }

            // var namePrefix4 = Unsafe.ReadUnaligned<Byte4>(ptr + namePos);
#if DEBUG
            // var p4 = new ReadOnlySpan<byte>(namePrefix4);
            var n = ToString(ptr + namePos, nameLen);
#endif
            pos += nameLen + 1;
            var temperature = ParseTemperatureAndPosAfterEol(ptr, len, ref pos);

            // var m = 0;
            var l = 0;
            var r = resentResultCount - 1;
            while (true)
            {
                if (l >= r)
                {
                    for (var i = resentResultCount; i > l; --i)
                        recentResults[i] = recentResults[i - 1];
                    recentResults[l] = new StationTemperatures(ptr + namePos, (short)nameLen, temperature);
                    ++resentResultCount;
                    if (resentResultCount >= 512)
                    {
                        MergeRecentResults(ref resultCount, ref results, resentResultCount, recentResults);
                        resentResultCount = 0; // we don't need to clear the recents, because we will overwrite it, and the count controls the valid entries
                    }
                    break;
                }

                var m = (l + r) / 2;
                ref var res = ref recentResults[m];
#if DEBUG
                var s = ToString(res.NamePtr, res.NameLen);
#endif
                var cmpRes = CompareName(res.NamePtr, res.NameLen, ptr + namePos, nameLen);
                if (cmpRes == 0)
                {
                    res.Min = Math.Min(res.Min, temperature);
                    res.Max = Math.Max(res.Max, temperature);
                    res.Sum += temperature;
                    res.Count++;
                    break;
                }
                if (cmpRes > 0)
                    r = m;
                else
                    l = m + 1;
            }
        }

        sw.Stop();
        Console.WriteLine($"Chunk {Interlocked.Increment(ref ChunksProcessed)}: {sw.Elapsed}");

        return (results, resultCount);
    }

    private static void MergeRecentResults(
        ref int resultCount, ref StationTemperatures[] results,
        int resentResultCount, Span<StationTemperatures> recentResults)
    {
        if (results == null)
        {
            results = new StationTemperatures[resentResultCount];
            recentResults.CopyTo(results);
            resultCount = resentResultCount;
            return;
        }

        var mergedResults = new StationTemperatures[resentResultCount + resultCount];
        int i = 0, j = 0, k = 0;
        while (i < resentResultCount & j < resultCount)
        {
            ref var rr = ref recentResults[i];
            ref var result = ref results[j];
            var cmp = CompareName(rr.NamePtr, rr.NameLen, result.NamePtr, result.NameLen);
            if (cmp == 0)
            {
                ref var mr = ref mergedResults[k];
                mr = rr;
                mr.Min = Math.Min(mr.Min, result.Min);
                mr.Max = Math.Max(mr.Max, result.Max);
                mr.Sum += result.Sum;
                mr.Count += result.Count;
                i++;
                j++;
                k++;
                continue;
            }
            if (cmp < 0)
            {
                mergedResults[k] = rr;
                ++i;
            }
            else
            {
                mergedResults[k] = result;
                ++j;
            }
            k++;
        }

        while (i < resentResultCount)
        {
            mergedResults[k] = recentResults[i++];
            k++;
        }

        while (j < resultCount)
        {
            mergedResults[k] = results[j++];
            k++;
        }

        results = mergedResults;
        resultCount = k;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CompareName(byte* ptr, int len, byte* otherPtr, int otherLen) =>
        new ReadOnlySpan<byte>(ptr, len).SequenceCompareTo(new ReadOnlySpan<byte>(otherPtr, otherLen));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string ToString(byte* namePtr, int nameLen) =>
        new((sbyte*)namePtr, 0, nameLen, Encoding.UTF8);


#endif
    private static int FindSemicolonIndexFallback(byte* ptr, int len, int pos)
    {
        int semicolonIndex = new ReadOnlySpan<byte>(ptr + pos, len - pos).IndexOf((byte)';');
        Debug.Assert(semicolonIndex != -1, """
            Semicolon is not found - means the file is not well-formed. 
            We assume that it is not the case, and the chunks and the end of the file are aligned correctly (because we did it).
        """);
        return semicolonIndex;
    }

#if WIP
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ParseTemperature(byte* pointer, int pos)
    {
        // explicitly handle the temperature patterns of '(-)d.d(.*)\n' and '(-)dd.d(.*)\n'
        var b0 = pointer[pos];
        var sign = 1;
        if (b0 == '-')
        {
            sign = -1;
            b0 = pointer[++pos];
        }

        var b1 = pointer[pos + 1];
        var b2 = pointer[pos + 2];
        var b3 = pointer[pos + 3];

        if (b1 != '.')
            return sign * ((b0 - '0') * 100 + (b1 - '0') * 10 + (b3 - '0'));
        return sign * ((b0 - '0') * 10 + (b2 - '0'));
    }

#else
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static short ParseTemperatureAndPosAfterEol(byte* pointer, int length, ref int pos)
    {
        // explicitly handle the temperature patterns of '(-)d.d(.*)\n' and '(-)dd.d(.*)\n'
        var b0 = pointer[pos];
        var sign = 1;
        if (b0 == '-')
        {
            sign = -1;
            b0 = pointer[++pos];
        }

        var b1 = pointer[pos + 1];
        var b2 = pointer[pos + 2];
        var b3 = pointer[pos + 3];

        // todo: @perf try SWAR (SIMD within a register), see Daniel Lemire's blog 'SWAR explained: parsing eight digits')
        int val;
        if (b1 == '.')
            val = sign * ((b0 - '0') * 10 + (b2 - '0'));
        else
            val = sign * ((b0 - '0') * 100 + (b1 - '0') * 10 + (b3 - '0'));

        pos += 4;
        while (b3 != '\n' & pos < length) // skip the remaining symbols until the end of line (or out of length) - still here to work with weather_stations.csv
            b3 = pointer[pos++];

        return (short)val;
    }
#endif

    public (StationTemperatures[] results, int resultCount) Process()
    {
        var chunks =
            SplitIntoMemoryChunks()
#if !DEBUG
            .AsParallel()
#endif
            .Select(ProcessChunk)
            .ToList();

        var sw = Stopwatch.StartNew();

        // todo: @perf merge the results in parallel
        var (totalResults, totalCount) = chunks[0];
        for (int chunk = 1; chunk < chunks.Count; chunk++)
        {
            var (chunkResults, chunkCount) = chunks[chunk];
            MergeRecentResults(ref totalCount, ref totalResults,
                chunkCount, new Span<StationTemperatures>(chunkResults, 0, chunkCount));
        }

        sw.Stop();
        Console.WriteLine($"Aggregating chunk results: {sw.Elapsed}");

        return (totalResults, totalCount);
    }

    public void ProcessAndPrintResults()
    {
        var (results, resultCount) = Process();

        var sw = Stopwatch.StartNew();

        var lineCount = 0;
        var sb = new StringBuilder(1024 * 32);
        sb.Append("{ ");
        for (var i = 0; i < resultCount; i++)
        {
            var result = results[i];
            if (lineCount != 0)
                sb.Append(", ");
            sb.Append(result);
            lineCount += result.Count;
        }

        // Console.OutputEncoding = Encoding.UTF8;
        // Console.WriteLine(sb);

        sw.Stop();
        Console.WriteLine($"Console output: {sw.Elapsed}");

        if (resultCount != 1_000_000_000)
            Console.WriteLine($"Total line count: {lineCount:N0}");
        Console.WriteLine($"Total unique results: {resultCount:N0}");
    }

    public void Dispose()
    {
        var sw = Stopwatch.StartNew();

        _vaHandle.Dispose();
        _va.Dispose();
        _mmf.Dispose();
        _fileStream.Dispose();

        sw.Stop();
        Console.WriteLine($"Free resources: {sw.Elapsed}");
    }
}
