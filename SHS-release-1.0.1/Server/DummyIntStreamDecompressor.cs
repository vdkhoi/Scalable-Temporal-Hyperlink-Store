using Int32 = System.Int32;
using Int64 = System.Int64;
using UInt32 = System.UInt32;
using UInt64 = System.UInt64;

/* This class has one piece of mutable state, namely "pos", the
   position in the stream.  Clients are responsible for guaranteeing
   thread-safety, either by locking this object or (preferredly) by
   having a separate DummyIntStreamDecompressor object per thread. */

namespace SHS {
  internal class DummyIntStreamDecompressor : IntStreamDecompressor {
    private readonly CachedStream main;
    private UInt64 pos;  // mutable state

    internal DummyIntStreamDecompressor(CachedStream main) {
      this.main = main;
      this.pos = 0;
    }

    internal override void SetPosition(UInt64 pos) {
      this.pos = pos;
    }

    internal override UInt64 GetPosition() {
      return this.pos;
    }

    internal override Int32 GetInt32() {
      Int32 res = this.main.GetInt32(this.pos);
      this.pos += sizeof(Int32);
      return res;
    }

    internal override UInt32 GetUInt32() {
      UInt32 res = this.main.GetUInt32(this.pos);
      this.pos += sizeof(UInt32);
      return res;
    }

    internal override Int64 GetInt64() {
      Int64 res = this.main.GetInt64(this.pos);
      this.pos += sizeof(Int64);
      return res;
    }

    internal override UInt64 GetUInt64() {
      UInt64 res = this.main.GetUInt64(this.pos);
      this.pos += sizeof(UInt64);
      return res;
    }

    internal override bool AtEnd() {
      return this.pos == this.main.Size;
    }
  }
}
