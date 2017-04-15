using System.IO;

namespace SHS {
  internal class DummyIntStreamCompressor : IntStreamCompressor {
    private BinaryWriter wr;
    private long pos;

    /* Creates a (non-)compressor on top of wr. */
    internal DummyIntStreamCompressor() {
      this.wr = null;
      this.pos = 0L;
    }

    /* Provide implementations for all pure virtual functions of base class. */
    internal override void SetWriter(BinaryWriter wr) {
      this.wr = wr;
      this.pos = 0L;
    }

    internal override void PutInt32(int x) {
      this.wr.Write(x);
      this.pos += sizeof(int);
    }

    internal override void PutUInt32(uint x) {
      this.wr.Write(x);
      this.pos += sizeof(uint);
    }

    internal override void PutInt64(long x) {
      this.wr.Write(x);
      this.pos += sizeof(long);
    }

    internal override void PutUInt64(ulong x) {
      this.wr.Write(x);
      this.pos += sizeof(ulong);
    }

    internal override long Align() {
      return this.pos;
    }

    internal override LinkCompression Identify() {
      return LinkCompression.None;
    }
  }
}
