namespace bm;

using BenchmarkDotNet.Attributes;
using System.Numerics;

/*
## Baseline

BenchmarkDotNet v0.13.12, Windows 11 (10.0.22631.2861/23H2/2023Update/SunValley3)
11th Gen Intel Core i7-1185G7 3.00GHz, 1 CPU, 8 logical and 4 physical cores
.NET SDK 8.0.100
  [Host]     : .NET 8.0.0 (8.0.23.53103), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI
  DefaultJob : .NET 8.0.0 (8.0.23.53103), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI

| Method      | Mean      | Error    | StdDev    | Median    | Ratio | RatioSD | Allocated | Alloc Ratio |
|------------ |----------:|---------:|----------:|----------:|------:|--------:|----------:|------------:|
| MinMaxNaive | 370.66 us | 7.402 us | 12.162 us | 368.85 us |  5.08 |    0.33 |         - |          NA |
| MinMaxILP   | 340.03 us | 6.675 us |  7.687 us | 338.54 us |  4.65 |    0.27 |         - |          NA |
| MinMaxSimd  |  72.86 us | 1.490 us |  4.201 us |  71.28 us |  1.00 |    0.00 |         - |          NA |
*/
[MemoryDiagnoser]
public class MinMax
{
    private readonly int[] _data = new int[512 * 1024];

    [GlobalSetup]
    public void Setup()
    {
        var random = new Random(42);
        for (int i = 0; i < _data.Length; ++i)
            _data[i] = random.Next();
    }

    [Benchmark]
    public (int, int) MinMaxNaive()
    {
        int max = int.MinValue, min = int.MaxValue;
        foreach (var i in _data)
        {
            min = Math.Min(min, i);
            max = Math.Max(max, i);
        }
        return (min, max);
    }

    [Benchmark]
    public (int, int) MinMaxILP()
    {
        int max1 = int.MinValue, max2 = int.MinValue, min1 = int.MaxValue, min2 = int.MaxValue;
        for (int i = 0; i < _data.Length; i += 2)
        {
            int d1 = _data[i], d2 = _data[i + 1];
            min1 = Math.Min(min1, d1);
            min2 = Math.Min(min2, d2);
            max1 = Math.Max(max1, d1);
            max2 = Math.Max(max2, d2);
        }
        return (Math.Min(min1, min2), Math.Max(max1, max2));
    }

    [Benchmark(Baseline = true)]
    public (int, int) MinMaxSimd()
    {
        Vector<int> vmin = new Vector<int>(int.MaxValue), vmax = new Vector<int>(int.MinValue);
        int vecSize = Vector<int>.Count;
        for (int i = 0; i < _data.Length; i += vecSize)
        {
            Vector<int> vdata = new Vector<int>(_data, i);
            Vector<int> minMask = Vector.LessThan(vdata, vmin);
            Vector<int> maxMask = Vector.GreaterThan(vdata, vmax);
            vmin = Vector.ConditionalSelect(minMask, vdata, vmin);
            vmax = Vector.ConditionalSelect(maxMask, vdata, vmax);
        }
        int min = int.MaxValue, max = int.MinValue;
        for (int i = 0; i < vecSize; ++i)
        {
            min = Math.Min(min, vmin[i]);
            max = Math.Max(max, vmax[i]);
        }
        return (min, max);
    }
}
