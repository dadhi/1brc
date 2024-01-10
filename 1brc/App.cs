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

[StructLayout(LayoutKind.Sequential,
    Size = 4 + 8 + 2 + 2 + 2 + 2 + 8 // 28 bytes
)]
public unsafe struct StationTemperatures
{
    // It is tempting to replace NamePtr with 'int' Offset, but it will require the chunk start pointer to be around:
    // - because the unique names may be found in different chunks,
    // - using the global file pointer negates the win - we need the 'long' Offset back.
    public readonly int NameHash;  // 4 bytes
    public readonly byte* NamePtr; // 8 bytes
    public readonly short NameLen; // 2 bytes
    public short Min;   // 2 bytes
    public short Max;   // 2 bytes
    public short Count; // 2 bytes
    public long Sum;    // 8 bytes

    public StationTemperatures(byte* namePtr, short nameLen, short val)
    {
        NameHash = CalculateNameHash(namePtr, nameLen);
        NamePtr = namePtr;
        NameLen = nameLen;
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

    private const int RESULTS_CAPACITY = 1_024 << 3; // for measurements.txt;
    // private const int RESULTS_CAPACITY = 1_024 << 6; // for weather_stations.csv;
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
        Debug.WriteLine($"CHUNKS {sw.Elapsed}");
        return chunks;
    }

    static long AlignToNewLineOrEof(FileStream fileStream, long newPos)
    {
        fileStream.Position = newPos;

        int c;
        while ((c = fileStream.ReadByte()) >= 0 && c != '\n') { }

        return fileStream.Position;
    }

    const byte SEMICOLON = (byte)';';

    const byte VEC_BYTES = 32; // Vector256<byte>.Count;

    static (StationTemperatures[] results, int count) ProcessChunk(Chunk chunk)
    {
        var results = new StationTemperatures[RESULTS_CAPACITY]; // todo: @perf find a way to do it on stack - the problem is how to merge those from multiple threads?
        var resultCount = 0;

        var vecSemicolon = new Vector<byte>(SEMICOLON);
        var vecZero = Vector<byte>.Zero;

        var ptr = chunk.Pointer;
        var len = chunk.Length;

        // loop line by line, line is either terminated by '\n' or EOF
        var pos = 0;
        while (pos < len)
        {
            var namePos = pos;
            int semicolonIndex;
            while (true)
            {
                if (len < pos + VEC_BYTES) // handle the small remainder at the end of file without SIMD (note that it may be more than 1 line)
                {
                    semicolonIndex = FindSemicolonIndexFallback(ptr, len, pos);
                    break;
                }

                // todo: @perf use it as a top loop to read the bytes, then find the semicolons and line endings.
                var vecBytes = Unsafe.ReadUnaligned<Vector<byte>>(ptr + pos);
                var vecEqSemicolon = Vector.Equals(vecBytes, vecSemicolon);
                if (!vecEqSemicolon.Equals(vecZero))
                {
                    var foundMask = vecEqSemicolon.AsVector256().ExtractMostSignificantBits();
                    semicolonIndex = BitOperations.TrailingZeroCount(foundMask);
                    break;
                }
                pos += VEC_BYTES;
            }

            pos += semicolonIndex + 1;

            var temperature = ParseTemperatureAndPosAfterEol(ptr, len, ref pos);
            var result = new StationTemperatures(ptr + namePos, (short)semicolonIndex, temperature);
            AddOrMergeResult(results, ref resultCount, ref result);
        }

        return (results, resultCount);
    }

    private static int FindSemicolonIndexFallback(byte* ptr, int len, int pos)
    {
        int semicolonIndex = new ReadOnlySpan<byte>(ptr + pos, len - pos).IndexOf(SEMICOLON);
        Debug.Assert(semicolonIndex != -1, """
            Semicolon is not found - means the file is not well-formed. 
            We assume that it is not the case, and the chunks and the end of the file are aligned correctly.
        """);
        return semicolonIndex;
    }

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

        // todo: @perf try SWAR SIMD (see Daniel Lemire's blog 'SWAR explained: parsing eight digits')
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

    private static void AddOrMergeResult(StationTemperatures[] results, ref int count, ref StationTemperatures result)
    {
        var hash = result.NameHash;
        var index = hash & RESULTS_CAPACITY_MASK;
        var probe = 0;
        while (true)
        {
            ref var res = ref results[index];
            if (res.NamePtr == null)
            {
                ++count;
                Debug.Assert(count <= RESULTS_MAX_COUNT, "Unexpectedly too many unique results. Increase the capacity.");
                res = result;
                break;
            }
            if (res.NameHash == hash) // check the hash first, no need to load the actual string from memory
            {
                if (res.NameEqualTo(result)) // todo: @perf store the hash and fast scan with SIMD to compare before comparing the name
                {
                    res.Sum += result.Sum;
                    res.Count++;
                    res.Min = Math.Min(res.Min, result.Min);
                    res.Max = Math.Max(res.Max, result.Max);
                    break;
                }
            }

            ++probe;

            // 7 quadratic probes vs. 26 for linear ! (measurements.txt)
            index = (index + (probe * probe)) & RESULTS_CAPACITY_MASK; // quadratic probing using probe with wrap-around

            // index = (index + probe) & RESULTS_CAPACITY_MASK; // linear probing with wrap-around
        }
#if DEBUG
        if (_probes.Count < probe + 1)
            _probes.Add(1);
        else
            ++_probes[probe];
#endif
    }
#if DEBUG
    static List<int> _probes = new(); // accumulating the number of probes to analyze the @perf - observability ftw :)
#endif

    public (StationTemperatures[] results, int resultCount) Process() =>
        SplitIntoMemoryChunks()
#if !DEBUG
            .AsParallel()
#endif
            .Select(ProcessChunk)
            .ToList()
            .Aggregate((total, chunk) =>
            {
                var (totalResults, totalCount) = total;
                var (chunkResults, _) = chunk;
                for (int i = 0; i < chunkResults.Length; i++)
                {
                    ref var chunkResult = ref chunkResults[i];
                    if (chunkResult.NamePtr != null)    // todo: @perf speed-up the scan with SIMD
                        AddOrMergeResult(totalResults, ref totalCount, ref chunkResult);
                }

                return (totalResults, totalCount);
            });

    public void PrintResult()
    {
        var (results, resultCount) = Process();

        // todo: @perf the idea to explore - use insertion sort when adding to results and merge sort here at the end
        Array.Sort(results, static (x, y) => x.NameCompareTo(y));


        // todo: @perf use faster console output with StreamWriter and Flush at the end
        var sw = Stopwatch.StartNew();
        Console.OutputEncoding = Encoding.UTF8;
        Console.Write("{");

        var lineCount = 0;
        var many = false;
        foreach (var result in results)
        {
            if (result.NamePtr == null) // todo: @wip re-check if we need to do that
                continue;
            if (many)
                Console.Write(", ");
            many = true;
            Console.Write(result);
            lineCount += result.Count;
        }

        Console.WriteLine("}");
        sw.Stop();
        Console.WriteLine($"Console output took: {sw.Elapsed}");

        if (resultCount != 1_000_000_000)
            Console.WriteLine($"Total line count: {lineCount:N0}");
        Console.WriteLine($"Total unique results: {resultCount:N0}");
    }

    public void Dispose()
    {
        _vaHandle.Dispose();
        _va.Dispose();
        _mmf.Dispose();
        _fileStream.Dispose();
    }
}