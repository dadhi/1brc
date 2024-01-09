using System.Diagnostics;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using _1brc;

if (!Avx2.IsSupported || !Vector256<byte>.IsSupported)
{
    Console.WriteLine("AVX2 is not supported but required for this program. There fallback impl. is possible but is not implemented for simplicity.");
    Environment.Exit(1);
}

var sw = Stopwatch.StartNew();

var stationsFile = @"C:\oss\1brc\weather_stations.csv";
// var stations = @"C:\oss\measurements.txt";

var path = args.Length > 0 ? args[0] : stationsFile;

using var app = new App(path);
app.PrintResult();

sw.Stop();
Console.WriteLine($"Processed in {sw.Elapsed}");
Environment.Exit(0);
