using System.Diagnostics;
using System.Runtime.CompilerServices;
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
        public ReadOnlySpan<byte> ToSpan() => new(Pointer, Length);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(Utf8Span other) => ToSpan().SequenceEqual(other.ToSpan());

        public override bool Equals(object? obj) => 
            obj is Utf8Span other && Equals(other);

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
                return (Length * 820243) ^ (int)*(uint*)Pointer;

            if (Length > 1)
                return (int)(uint)*(ushort*)Pointer;

            return (int)(uint)*Pointer;
        }

        public override string ToString() => new((sbyte*)Pointer, 0, Length, Encoding.UTF8);
    }
}