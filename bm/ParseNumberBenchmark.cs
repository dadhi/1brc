namespace bm;

using BenchmarkDotNet.Attributes;
using System.Runtime.CompilerServices;
using System.Numerics;
using System.Text.Unicode;
using System.Text;
using System.Runtime.InteropServices.Marshalling;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

/*
## Baseline

BenchmarkDotNet v0.13.12, Windows 11 (10.0.22631.2861/23H2/2023Update/SunValley3)
11th Gen Intel Core i7-1185G7 3.00GHz, 1 CPU, 8 logical and 4 physical cores
.NET SDK 8.0.100
  [Host]     : .NET 8.0.0 (8.0.23.53103), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI
  DefaultJob : .NET 8.0.0 (8.0.23.53103), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI

| Method      | Mean     | Error     | StdDev    | Ratio | RatioSD | Allocated | Alloc Ratio |
|------------ |---------:|----------:|----------:|------:|--------:|----------:|------------:|
| Pattern     | 8.950 ns | 0.2020 ns | 0.4600 ns |  1.00 |    0.00 |         - |          NA |
| TableLookup | 9.027 ns | 0.2095 ns | 0.3669 ns |  1.01 |    0.07 |         - |          NA |

*/
[MemoryDiagnoser]
public class ParseNumberBenchmark
{
    static readonly string[] _strs = ["35.6\n", "-6.1\n", "8.6\n", "-23.1\n"];

    static readonly byte[][] _bstrs = _strs.Select(Encoding.UTF8.GetBytes).ToArray();

    [Benchmark(Baseline = true)]
    public int SIMD()
    {
        var sum = 0;
        var zero = Vector64.Create((byte)'0');
        var nine = Vector64.Create((byte)'9');
        foreach (var s in _bstrs)
        {
            // var pos = 0;

            ref var b = ref MemoryMarshal.GetArrayDataReference(s);
            var vec = Vector64.LoadUnsafe(ref b);
            var dig = Vector64.Subtract(vec, zero);
            var dig2 = Vector64.ConditionalSelect(Vector64.GreaterThan(dig, nine), Vector64<byte>.Zero, dig);

            // var b0 = s[pos];    //
            // var b1 = s[pos + 1];
            // var b2 = s[pos + 2];
            // var b3 = s[pos + 3];

            // var sign = 1;
            // if (b0 == '-')
            // {
            //     sign = -1;
            //     b0 = s[++pos];
            // }


            // int val;
            // if (b1 == '.')
            //     val = sign * ((b0 - '0') * 10 + (b2 - '0'));
            // else
            //     val = sign * ((b0 - '0') * 100 + (b1 - '0') * 10 + (b3 - '0'));
            // sum += val;
        }
        return sum;
    }

    [Benchmark(Baseline = true)]
    public int Pattern_with_branches()
    {
        var sum = 0;
        foreach (var s in _strs)
        {
            var pos = 0;
            var b0 = s[0];
            var sign = 1;
            if (b0 == '-')
            {
                sign = -1;
                b0 = s[++pos];
            }
            var b1 = s[pos + 1];
            var b2 = s[pos + 2];
            var b3 = s[pos + 3];

            int val;
            if (b1 == '.')
                val = sign * ((b0 - '0') * 10 + (b2 - '0'));
            else
                val = sign * ((b0 - '0') * 100 + (b1 - '0') * 10 + (b3 - '0'));
            sum += val;
        }
        return sum;
    }

    [Benchmark]
    public int TableLookup()
    {
        ReadOnlySpan<int> lookup10 = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90];
        ReadOnlySpan<int> lookup100 = [0, 100, 200, 300, 400, 500, 600, 700, 800, 900];

        var sum = 0;
        foreach (var s in _strs)
        {
            var pos = 0;
            var b0 = s[0];
            var sign = 1;
            if (b0 == '-')
            {
                sign = -1;
                b0 = s[++pos];
            }
            var b1 = s[pos + 1];
            var b2 = s[pos + 2];
            var b3 = s[pos + 3];

            int val;
            if (b1 == '.')
                val = sign * (lookup10[b0 - '0'] + (b2 - '0'));
            else
                val = sign * (lookup100[b0 - '0'] + lookup10[b1 - '0'] + (b3 - '0'));
            sum += val;
        }
        return sum;
    }
}
