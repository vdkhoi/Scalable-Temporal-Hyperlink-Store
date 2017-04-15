using Int32 = System.Int32;
using Int64 = System.Int64;
using UInt32 = System.UInt32;
using UInt64 = System.UInt64;

namespace SHS {
  /// <summary>
  /// IntStreamDecompressor is an abstract class encapsulating decompression 
  /// algorithms for integer streams.  The primary  purpose is for decompressing
  /// memory-mapped LinkCell files, which consist mostly of oldNewGap-encoded 
  /// sequences of UIDs.
  /// </summary>
  internal abstract class IntStreamDecompressor {

    /// <summary>
    /// Sets the position of the decrompressor in the stream.
    /// </summary>
    /// <param name="pos">
    /// The byte in the stream to position on. Must be a multiple of 8, which
    /// I insist on in order to avoid extremely expensive unaligned memory 
    /// accesses, since I punt groups of 8 bytes into long values. 
    /// </param>
    internal abstract void SetPosition(UInt64 pos);

    /* GetPosition  */
    /// <summary>
    /// Return the current position of this IntStreamDecompressor.
    /// </summary>
    /// <returns>the current position in the stream of integers</returns>
    internal abstract UInt64 GetPosition();

    /* GetInt32 decompresses a 32-bit signed integer starting from the current
       position of the stream, and advances the stream.  It is a checked error
       if the next value on the stream is not within the proper range of a
       signed 32-bit integer. Likewise, it is a checked error to read past the
       end of the stream.  Conversely, reading in a region of the stream that
       was not written to (because the "Align" method of the compressor that
       created the stream was called) does not lead to a checked error, but
       produces undefined results.  It is up to the client to know where the
       valid regions of the stream are. */
    internal abstract Int32 GetInt32();

    /* GetUInt32 decompresses a 32-bit unsigned integer starting from the
       current position of the stream, and advances the stream.  It is a
       checked error if the next value on the stream is not within the proper
       range of an unsigned 32-bit integer. Likewise, it is a checked error
       to read past the end of the stream.  Conversely, reading in a region
       of the stream that was not written to (because the "Align" method of
       the compressor that created the stream was called) does not lead to a
       checked error, but produces undefined results.  It is up to the client
       to know where the valid regions of the stream are. */
    internal abstract UInt32 GetUInt32();

    /* GetInt64 decompresses a 64-bit signed integer starting from the current
       position of the stream, and advances the stream.  It is a checked error
       to read past the end of the stream.  Conversely, reading in a region of
       the stream that was not written to (because the "Align" method of the
       compressor that created the stream was called) does not lead to a
       checked error, but produces undefined results.  It is up to the client
       to know where the valid regions of the stream are. */
    internal abstract Int64 GetInt64();

    /* GetUInt64 decompresses a 64-bit unsigned integer starting from the
       current position of the stream, and advances the stream.  It is a
       checked error to read past the end of the stream.  Conversely, reading
       in a region of the stream that was not written to (because the "Align"
       method of the compressor that created the stream was called) does not
       lead to a checked error, but produces undefined results.  It is up to
       the client to know where the valid regions of the stream are. */
    internal abstract UInt64 GetUInt64();

    /* AtEnd returns whether the underlying stream has been exhausted. */
    internal abstract bool AtEnd();
  }
}
