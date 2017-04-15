using System.IO;

namespace SHS {
  internal class VarByteIntStreamCompressor : IntStreamCompressor {
    private BinaryWriter wr;
    private long pos;
    private int freeBytes;  // in [7..0]
    private ulong word;  // Used to accumulate the next word to write to ob

    internal VarByteIntStreamCompressor() {
      this.wr = null;
      this.pos = 0;
      this.freeBytes = 7;
      this.word = 0;
    }

    internal override void SetWriter(BinaryWriter wr) {
      this.wr = wr;
      this.pos = 0;
      this.freeBytes = 7;
      this.word = 0;
    }

    internal override void PutInt32(int x) {
      this.PutInt64(x);
    }

    internal override void PutUInt32(uint x) {
      this.PutUInt64(x);
    }

    internal override void PutInt64(long x) {
      // Change the encoding of the sign of x
      ulong ux = x < 0 ? (ulong)(~x) << 1 | 1 : (ulong)x << 1;
      this.PutUInt64(ux);
    }

    internal override void PutUInt64(ulong x) {
      // Determine how many bytes we need to represent x
      ulong mask   = 0xff00000000000000UL;
      int numBytes = 8;
      for (; numBytes > 1 && (x & mask) == 0; numBytes--) {
        mask >>= 8;  // "logical shift" -- higher-order bits are 0
      }
      // Does x fit into word?
      if (this.freeBytes < numBytes) {
        // Write out the higher-order bytes that fit; flush; adjust numBytes and x
        numBytes -= this.freeBytes;
        this.word |= (x >> numBytes * 8) << 8;  // Relies on logical shift
        this.FlushWord();
        x &= ((1UL << numBytes * 8) - 1);
      }
      this.freeBytes -= numBytes;
      this.word |= 1UL << this.freeBytes;
      this.word |= x << (this.freeBytes + 1) * 8;
      if (this.freeBytes == 0) this.FlushWord();
    }

    internal override long Align() {
      if (this.freeBytes < 7) {
        this.word |= (ulong)this.freeBytes << 8;
        this.word |= 1 << 7;
        this.FlushWord();
      }
      return this.pos << 3;
    }

    internal override LinkCompression Identify() {
      return LinkCompression.VarByte;
    }

    private void FlushWord() {
      this.wr.Write(this.word);
      this.pos++;
      this.freeBytes = 7;
      this.word = 0;
    }
  }
}
