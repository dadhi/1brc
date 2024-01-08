using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Text;
using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;
using static System.Runtime.InteropServices.CollectionsMarshal;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using System.Numerics;
using System.Runtime.Intrinsics;

namespace _1brc
{

    [StructLayout(LayoutKind.Sequential,
        Size = 16 // bytes: Sum + Count + Min + Max
    )]
    public struct Summary
    {
        public long Sum;
        public int Cnt;
        public short Min;
        public short Max;
        public override string ToString() => $"{Min / 10.0:N1}/{(double)Sum / Cnt / 10.0:N1}/{Max / 10.0:N1}";
    }

    public unsafe class App : IDisposable
    {
        private readonly FileStream _fileStream;
        private readonly MemoryMappedFile _mmf;
        private readonly MemoryMappedViewAccessor _va;
        private readonly SafeMemoryMappedViewHandle _vaHandle;
        private readonly byte* _pointer;
        private readonly long _fileLength;

        private readonly int _initialChunkCount;

        private const int DICT_INIT_CAPACITY = 10_000;
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

        public List<Utf8Span> SplitIntoMemoryChunks()
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

            List<Utf8Span> chunks = new(capacity: chunkCount); // todo: use array

            long pos = 0;
            while (true)
            {
                var nextChunkPos = pos + chunkSize;
                if (nextChunkPos >= _fileLength)
                {
                    chunks.Add(new(_pointer + pos, (int)(_fileLength - pos)));
                    break;
                }

                nextChunkPos = AlignToNewLine(_fileStream, nextChunkPos);

                chunks.Add(new(_pointer + pos, (int)(nextChunkPos - pos)));

                pos = nextChunkPos;
            }

            _fileStream.Position = 0;

            sw.Stop();
            Debug.WriteLine($"CHUNKS {sw.Elapsed}");
            return chunks;
        }

        private static long AlignToNewLine(FileStream fileStream, long newPos)
        {
            fileStream.Position = newPos;

            int c;
            while ((c = fileStream.ReadByte()) >= 0 && c != '\n') { }

            return fileStream.Position;
        }

        const byte SEMICOLON = (byte)';';

        const byte VEC_BYTES = 32; // Vector256 - Avx2.IsSupported

        public static Dictionary<Utf8Span, Summary> ProcessChunk(Utf8Span span)
        {
            var result = new Dictionary<Utf8Span, Summary>(DICT_INIT_CAPACITY);

            var vecSemicolon = new Vector<byte>(SEMICOLON);
            var vecZero = Vector<byte>.Zero;

            var pointer = span.Pointer;
            var remains = span.Remains;
            while (remains > 0)
            {
                // we don't expect the first byte to be ';' it should be some name at least - so start from the second byte
                var pos = 1;
                var semicolonIndex = 0;
                if (Avx2.IsSupported) // hot-path
                {
                    while (true)
                    {
                        if (remains < pos + VEC_BYTES) // handle the small remainder without SIMD
                        {
                            semicolonIndex = new ReadOnlySpan<byte>(pointer + pos, remains - pos).IndexOf(SEMICOLON);
                            if (semicolonIndex == -1) // todo: @perf convert to Debug.Assert if we assume a well formed input
                                return result; // if semicolon is not found, then the last record is not complete or empty - se we're done
                            break;
                        }

                        var vecBytes = Unsafe.ReadUnaligned<Vector<byte>>(pointer + pos);
                        var vecEqSemicolon = Vector.Equals(vecBytes, vecSemicolon);
                        if (!vecEqSemicolon.Equals(vecZero))
                        {
                            var foundMask = Avx2.MoveMask(vecEqSemicolon.AsVector256());
                            semicolonIndex = BitOperations.TrailingZeroCount((uint)foundMask);
                            break;
                        }
                        pos += VEC_BYTES;
                    }
                }
                else
                {
                    semicolonIndex = new ReadOnlySpan<byte>(pointer + pos, remains - pos).IndexOf(SEMICOLON);
                    if (semicolonIndex == -1)
                        return result;
                }

                pos += semicolonIndex + 1;

                int number = 0;
                // read first 2 bytes together to handle the first being the '-' sign
                var s = pointer[pos];
                var b = pointer[pos + 1];
                if (s != '-')
                    number = ((s - '0') * 10) + (b - '0');
                else
                    number = -(b - '0');
                ++pos;
                while ((b = pointer[++pos]) != '.')
                    number = (number * 10) + (b - '0');

                // read the fractional part after the dot and store it as part of number x10
                b = pointer[++pos];
                number = (number * 10) + (b - '0');

                // ignore any other digits or symbols until new line (it is a rare case because we expect 1 fractionak only)
                while (pointer[++pos] != '\n') {}

                ref var res = ref GetValueRefOrAddDefault(result, new(pointer, semicolonIndex), out var exists);
                if (exists)
                {
                    res.Sum += number;
                    res.Cnt++;
                    res.Min = (short)Math.Min(res.Min, number);
                    res.Max = (short)Math.Max(res.Max, number);
                }
                else
                {
                    res.Sum = number;
                    res.Cnt = 1;
                    res.Min = (short)number;
                    res.Max = (short)number;
                }

                // advance to the next line
                pointer += pos + 1;
                remains -= pos + 1;
            }

            return result;
        }

        public Dictionary<Utf8Span, Summary> Process() =>
            SplitIntoMemoryChunks()
                .AsParallel()
#if DEBUG
                .WithDegreeOfParallelism(1)
#endif
                .Select(ProcessChunk)
                .ToList()
                .Aggregate((result, chunk) =>
                {
                    foreach (KeyValuePair<Utf8Span, Summary> pair in chunk)
                    {
                        ref var summary = ref GetValueRefOrAddDefault(result, pair.Key, out bool exists);
                        if (exists)
                        {
                            var other = pair.Value;
                            summary.Sum += other.Sum;
                            summary.Cnt += other.Cnt;
                            summary.Min = Math.Min(summary.Min, other.Min);
                            summary.Max = Math.Max(summary.Max, other.Max);

                        }
                        else
                            summary = pair.Value;
                    }

                    return result;
                });

        public void PrintResult()
        {
            var result = Process();

            long count = 0;
            // Console.OutputEncoding = Encoding.UTF8;
            // Console.Write("{");
            var line = 0;
            foreach (var pair in result
                         .Select(x => (Name: x.Key.ToString(), x.Value))
                         .OrderBy(x => x.Name, StringComparer.Ordinal))
            {
                count += pair.Value.Cnt;
                // Console.Write($"{pair.Name} = {pair.Value}");
                line++;
                // if (line < result.Count)
                // Console.Write(", ");
            }

            // Console.WriteLine("}");

            if (count != 1_000_000_000)
                Console.WriteLine($"Total row count {count:N0}");
        }

        public void Dispose()
        {
            _vaHandle.Dispose();
            _va.Dispose();
            _mmf.Dispose();
            _fileStream.Dispose();
        }
    }
}