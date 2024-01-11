/*

## weather_stations.csv

### 1 Baseline with custom map

private const int RESULTS_CAPACITY = 1_024 << 6; // for weather_stations.csv;

Console output: 00:00:03.8005088
Total line count: 44,294
Total unique results: 41,342
Processed in: 00:00:04.1373231

### 2 Calculating the hash once and storing it in entry; quadratic probing instead of linear. 

Some spead-up

Console output: 00:00:02.9858855
Total line count: 44,294
Total unique results: 41,343
Processed in: 00:00:03.0633131


## measurements.txt

### 1 Baseline with custom map

todo: @perf reduce memory - robin hood to the rescue :)
private const int RESULTS_CAPACITY = 1_024 << 3; // for measurements.txt;

Console output: 00:00:00.0496668
Total line count: 12,503,143
Total unique results: 413
Processed in: 00:00:00.9950907


### 2 Calculating the hash once and storing it in entry; quadratic probing instead of linear. 

No difference.

Aggregating chunk results: 00:00:00.0048256 - nothing
Console output: 00:00:00.0685998 - small something
Total line count: 12,503,143
Total unique results: 413
Processed in: 00:00:01.0713925

*/

using System.Diagnostics;
using _1brc;

var sw = Stopwatch.StartNew();

// var input = @"C:\oss\1brc\weather_stations.csv";
var input = @"C:\oss\measurements.txt";

var path = args.Length > 0 ? args[0] : input;

using (var app = new App(path))
    app.PrintResult();

sw.Stop();
Console.WriteLine($"Processed in: {sw.Elapsed}");
Environment.Exit(0);
