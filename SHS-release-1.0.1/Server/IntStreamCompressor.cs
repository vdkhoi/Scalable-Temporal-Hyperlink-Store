using System;
using System.IO;

namespace SHS {
  internal abstract class IntStreamCompressor {
    internal static IntStreamCompressor New(LinkCompression lc) {
      switch (lc) {
        case LinkCompression.None:
          return new DummyIntStreamCompressor();
        case LinkCompression.VarByte:
          return new VarByteIntStreamCompressor();
        case LinkCompression.VarNybble:
          return new VarNybbleIntStreamCompressor();
        default:
          throw new Exception("Illegal LinkCompression argument");
      }
    }

    internal abstract void SetWriter(BinaryWriter wr);

    /* PutInt32(x) appends a compressed version of x to the underlying stream. */
    internal abstract void PutInt32(int x);

    /* PutUInt32(x) appends a compressed version of x to the underlying stream. */
    internal abstract void PutUInt32(uint x);

    /* PutInt64(x) appends a compressed version of x to the underlying stream. */
    internal abstract void PutInt64(long x);

    /* PutUInt64(x) appends a compressed version of x to the underlying stream. */
    internal abstract void PutUInt64(ulong x);

    /// <summary>
    /// Align flushes out any buffered content to the underlying stream, aligns
    /// the stream position to a position partID and returns partID.  partID has the property
    /// that, assuming the next call is PutInt64(x), a compatible decompressor 
    /// d will be able to recover x by calling: 
    ///     d.SetPosition(partID); x=d.GetInt64();
    /// </summary>
    /// <returns>the position in the stream after alignment</returns>
    internal abstract long Align();

    /* Identify writes an identification of this compressor (plus whatever
       else might be needed) to ob. This identification should be used to
       select a matching decompressor later on. */
    internal abstract LinkCompression Identify();
  }
}
