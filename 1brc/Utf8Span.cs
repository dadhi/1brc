using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;

namespace _1brc
{
    public unsafe readonly struct Utf8Span : IEquatable<Utf8Span>
    {
        internal readonly byte* Pointer;
        internal readonly int Length;

        public Utf8Span(byte* pointer, int length)
        {
            Debug.Assert(length >= 0);
            Pointer = pointer;
            Length = length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte GetAtUnsafe(int idx) => Pointer[idx];

        public ReadOnlySpan<byte> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(Pointer, Length);
        }

        /// <summary>
        /// Slice without bound checks. Use only when the bounds are checked/ensured before the call.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Utf8Span AdvanceUnsafe(int offset) => new(Pointer + offset, Length - offset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(Utf8Span other) => Span.SequenceEqual(other.Span);

        public override bool Equals(object? obj)
        {
            return obj is Utf8Span other && Equals(other);
        }

        public override int GetHashCode()
        {
            // Here we use the first 4 chars (if ASCII) and the length for a hash.
            // The worst case would be a prefix such as Port/Saint and the same length,
            // which for human geo names is quite rare. 

            // .NET dictionary will obviously slow down with collisions but will still work.
            // If we keep only `*_pointer` the run time is still reasonable ~9 secs.
            // Just using `if (_len > 0) return (_len * 820243) ^ (*_pointer);` gives 5.8 secs.
            // By just returning 0 - the worst possible hash function and linear search - the run time is 12x slower at 56 seconds. 

            // The magic number 820243 is the largest happy prime that contains 2024 from https://prime-numbers.info/list/happy-primes-page-9

            if (Length > 3)
                return (Length * 820243) ^ (*(int*)Pointer);

            if (Length > 1)
                return *(short*)Pointer;

            return *Pointer;
        }

        public override string ToString() => new((sbyte*)Pointer, 0, Length, Encoding.UTF8);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ParseInt(int start, int length)
        {
            int sign = 1;
            int value = 0;

            int i = start;
            for (; i < start + length; i++)
            {
                var c = (int)GetAtUnsafe(i);

                if (c == '-')
                    sign = -1;
                else
                    value = value * 10 + (c - '0');
            }

            var fractional = GetAtUnsafe(i + 1) - '0';
            return sign * (value * 10 + fractional); 
        }
        
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int IndexOf(int start, byte value)
        {
            int offset = 0;
            
            if (Avx2.IsSupported)
            {
                Vector<byte> vec = default;
                for (var i = 0; i < int.MaxValue; i++)
                {
                    offset = Vector<byte>.Count * i;
                    if(start + offset >= Length)
                        goto BAIL;
                    var data = Unsafe.ReadUnaligned<Vector<byte>>(Pointer + start + offset);
                    vec = Vector.Equals(data, new Vector<byte>(value));
                    if (!vec.Equals(Vector<byte>.Zero))
                        break;
                }

                var matches = vec.AsVector256();
                var mask = Avx2.MoveMask(matches);
                int tzc = BitOperations.TrailingZeroCount((uint)mask);
                return start + offset + tzc;
                
                BAIL:
                offset -= Vector<byte>.Count;
            }

            start += offset;
            
            int indexOf = AdvanceUnsafe(start).Span.IndexOf(value);
            return indexOf < 0 ? Length : start + indexOf;
        }
    }
}