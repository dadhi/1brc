using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Text;
using Microsoft.Win32.SafeHandles;
using static System.Runtime.InteropServices.CollectionsMarshal;

namespace _1brc
{
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
            while ((c = fileStream.ReadByte()) >= 0 && c != '\n') {}

            return fileStream.Position;
        }

        public static Dictionary<Utf8Span, Summary> ProcessChunk(Utf8Span remaining)
        {
            var result = new Dictionary<Utf8Span, Summary>(DICT_INIT_CAPACITY);

            while (remaining.Length > 0)
            {
                var separatorIdx = remaining.IndexOf(0, (byte)';');

                var dotIdx = remaining.IndexOf(separatorIdx + 1, (byte)'.');
                
                var nlIdx = remaining.IndexOf(dotIdx + 1, (byte)'\n');
                        
                ref var it = ref GetValueRefOrAddDefault(result, new(remaining.Pointer, separatorIdx), out var exists);

                int intPart = remaining.ParseInt(separatorIdx + 1, dotIdx - separatorIdx - 1);

                if (exists)
                    it.Apply(intPart);
                else
                {
                    it.Sum = intPart;
                    it.Count = 1;
                    it.Min = (short)intPart;
                    it.Max = (short)intPart;
                }
                
                remaining = remaining.AdvanceUnsafe(nlIdx + 1);
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
                            summary.Merge(pair.Value);
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
                count += pair.Value.Count;
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