using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Text;
using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;
using static System.Runtime.InteropServices.CollectionsMarshal;
using System.Buffers.Text;
using System.Runtime.CompilerServices;

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

        public static Dictionary<Utf8Span, Summary> ProcessChunk(Utf8Span span)
        {
            var result = new Dictionary<Utf8Span, Summary>(DICT_INIT_CAPACITY);

            while (span.Length > 0)
            {
                var byteAt = span.Pointer;

                var separatorIdx = span.IndexOf(0, (byte)';');

                var numPos = separatorIdx + 1;
                var b = byteAt[numPos];
                int sign = 1;
                if (b == '-')
                {
                    sign = -1;
                    ++numPos;
                }
                int num = 0;
                while (true)
                {
                    b = byteAt[numPos++];
                    if (b == '.')
                        break;
                    num = (num * 10) + (b - '0');
                }

                // store the fractional part as part of number X 10
                b = byteAt[numPos];
                num = (num * 10) + (b - '0');

                num *= sign;

                // ignore any other digits, symbols until new line (it is a rare case because we expect 1 fractionak only)
                while (byteAt[++numPos] != '\n') {}

                span = new(byteAt + numPos + 1, span.Length - numPos - 1);

                ref var res = ref GetValueRefOrAddDefault(result, new(byteAt, separatorIdx), out var exists);

                if (exists)
                {
                    res.Sum += num;
                    res.Cnt++;
                    res.Min = (short)Math.Min(res.Min, num);
                    res.Max = (short)Math.Max(res.Max, num);
                }
                else
                {
                    res.Sum = num;
                    res.Cnt = 1;
                    res.Min = (short)num;
                    res.Max = (short)num;
                }
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