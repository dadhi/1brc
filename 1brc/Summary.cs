using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace _1brc
{
    [StructLayout(LayoutKind.Sequential, 
        Size = 16 // bytes: Sum + Count + Min + Max
    )]
    public struct Summary
    {
        public long Sum;
        public int Count;
        public short Min;
        public short Max;
        
        public double Average => (double)Sum / Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Apply(int value)
        {
            Sum += value;
            Count++;

            // if (Min > value)
            //     Min = (short)value;
            // var newMin = value < Min;
            // var newMax = value > Max;
            // Min = value * Unsafe.As<bool, byte>(ref newMin) +;

            int deltaMin = Min - value;
            Min = (short)(value + (deltaMin & (deltaMin >> 31)));
            
            int deltaMax = Max - value;
            Max = (short)(Max - (deltaMax & (deltaMax >> 31)));
        }

        public void Merge(Summary other)
        {
            if (other.Min < Min)
                Min = other.Min;
            if (other.Max > Max)
                Max = other.Max;
            Sum += other.Sum;
            Count += other.Count;
        }

        public override string ToString() => $"{Min / 10.0:N1}/{Average / 10.0:N1}/{Max / 10.0:N1}";
    }
}