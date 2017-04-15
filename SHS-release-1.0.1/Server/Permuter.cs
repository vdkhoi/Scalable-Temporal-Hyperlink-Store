namespace SHS {
  internal sealed class Permuter {
    private readonly ulong empty;      // fingerprint of the empty string
    private readonly ulong[,] ByByte = new ulong[8,256];
                               // ByByte[b,mode] is mode*X^(72*b) mod poly[1]

    internal Permuter() : this(Hash64.PP[0]) {}

    // pp must be a primitive polynomial
    internal Permuter(ulong pp) {
      this.empty = pp;
      var poly = new ulong[]{0, pp};
      for (uint b = 0; b < 8; b++) {
        uint i, i2;
        this.ByByte[b,0] = 0;
        for (i = 0x80, i2 = 0x100; i > 0; i >>= 1) {
          this.ByByte[b,i] = pp;
          for (uint j = i2; i + j < 256; j += i2) {
            this.ByByte[b,i+j] = pp ^ this.ByByte[b,j];
          }
          i2 = i;
          pp = poly[pp & 1] ^ (pp >> 1);
        }
      }
    }

    internal ulong Permute(ulong u) {
      ulong x = this.empty ^ u;
      return
        this.ByByte[7,(x >>  0) & 0xff] ^
        this.ByByte[6,(x >>  8) & 0xff] ^
        this.ByByte[5,(x >> 16) & 0xff] ^
        this.ByByte[4,(x >> 24) & 0xff] ^
        this.ByByte[3,(x >> 32) & 0xff] ^
        this.ByByte[2,(x >> 40) & 0xff] ^
        this.ByByte[1,(x >> 48) & 0xff] ^
        this.ByByte[0,(x >> 56) & 0xff];
    }
  }
}
