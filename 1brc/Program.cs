using System.Diagnostics;

namespace _1brc;

/*
# Results on my machine:

$ dotnet run -c Release --project ./1brc
Total row count 44,691

## No more tuples for chunks

Processed in 00:00:00.1270662

*/

class Program
{
    static void Main(string[] args)
    {
        var sw = Stopwatch.StartNew();
        var path = args.Length > 0 ? args[0] : @"C:\oss\measurements.txt"; // @"C:\oss\1brc\weather_stations.csv"
        using var app = new App(path);
        // Console.WriteLine($"Chunk count: {app.SplitIntoMemoryChunks().Count}");
        app.PrintResult();
        sw.Stop();
        Console.WriteLine($"Processed in {sw.Elapsed}");
        Environment.Exit(0);
    }
}