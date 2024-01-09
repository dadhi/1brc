/*
## 1. Baseline with no Dictionary:

### weather_stations.csv

private const int RESULTS_CAPACITY = 1_024 << 6; // for weather_stations.csv;

Console output took: 00:00:03.8005088
Total line count: 44,294
Total unique results: 41,342
Processed in: 00:00:04.1373231


### measurements.txt

todo: @perf reduce memory - robin hood to the rescue :)
private const int RESULTS_CAPACITY = 1_024 << 3; // for measurements.txt;

Console output took: 00:00:00.0496668
Total line count: 12,503,143
Total unique results: 413
Processed in: 00:00:00.9950907

*/

using System.Diagnostics;
using _1brc;

var sw = Stopwatch.StartNew();

// var input = @"C:\oss\1brc\weather_stations.csv";
var input = @"C:\oss\measurements.txt";

var path = args.Length > 0 ? args[0] : input;

using var app = new App(path);
app.PrintResult();

sw.Stop();
Console.WriteLine($"Processed in: {sw.Elapsed}");
Environment.Exit(0);
