public class BitVector {
  private ulong[] bits;

  public BitVector(long n) {
    this.bits = new ulong[((n - 1) / 64) + 1];
  }

  public bool this[long i] {
    get {
      return ((this.bits[i >> 6] >> (int)(i & 0x3f)) & 1) == 1;
    }
    set {
      long p = i >> 6;
      ulong m = (ulong)1 << (int)(i & 0x3f);
      if (value) {
        this.bits[p] |= m;
      } else {
        this.bits[p] &= ~m;
      }
    }
  }

  public void SetAll(bool val) {
    ulong m = val ? ulong.MaxValue : ulong.MinValue;
    for (int i = 0; i < this.bits.Length; i++) this.bits[i] = m;
  }
}
