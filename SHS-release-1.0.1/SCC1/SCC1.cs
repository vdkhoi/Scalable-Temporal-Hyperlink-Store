using System;
using System.Diagnostics;
using System.IO;
using SHS;

public class SCC {
  private class Frame {
    internal Frame parent;
    internal long uid;
    internal long[] links;
    internal int linkPos;
    internal Frame(Frame parent, long uid, Store shs, Dir dir, BitVector bv) {
      this.parent = parent;
      this.uid = uid;
      this.links = shs.GetLinks(uid, dir);
      this.linkPos = 0;
      bv[shs.UidToLid(uid)] = true;
    }
  }
  public static void Main(string[] args) {
    if (args.Length != 2) {
      Console.Error.WriteLine("Usage: SHS.SCC1 <leader> <store>");
    } else {
      var sw = Stopwatch.StartNew();
      var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
      long numUids = store.NumUrls();
      var bv = new BitVector(numUids);  // All false at creation
      var stk = new LongStack(1 << 23, "scc");
      foreach (long u in store.Uids()) {
        if (!bv[store.UidToLid(u)]) {
          Frame frame = new Frame(null, u, store, Dir.Fwd, bv);
          while (frame != null) {
            while (frame.linkPos < frame.links.Length) {
              long v = frame.links[frame.linkPos++];
              if (!bv[store.UidToLid(v)]) {
                frame = new Frame(frame, v, store, Dir.Fwd, bv);
              }
            }
            stk.Push(frame.uid);
            frame = frame.parent;
          }
        }
      }
      using (var sccWr = new BinaryWriter(new BufferedStream(new FileStream("scc-main.bin", FileMode.Create, FileAccess.Write)))) {
        using (var idxWr = new BinaryWriter(new BufferedStream(new FileStream("scc-index.bin", FileMode.Create, FileAccess.Write)))) {
          long numSCCs = 0;
          long sccPos = 0;
          bv.SetAll(false);
          for (long i = 0; i < numUids; i++) {
            long u = stk.Pop();
            if (!bv[store.UidToLid(u)]) {
              numSCCs++;
              long sccSize = 0;
              Frame frame = new Frame(null, u, store, Dir.Bwd, bv);
              while (frame != null) {
                while (frame.linkPos < frame.links.Length) {
                  long v = frame.links[frame.linkPos++];
                  if (!bv[store.UidToLid(v)]) {
                    frame = new Frame(frame, v, store, Dir.Bwd, bv);
                  }
                }
                sccWr.Write(frame.uid);
                sccSize++;
                frame = frame.parent;
              }
              idxWr.Write(sccSize);
              idxWr.Write(sccPos);
              sccPos += sccSize;
            }
          }
        }
      }
      store.Close();
      Console.WriteLine("Done. Job took {0} seconds.", 0.001 * sw.ElapsedMilliseconds);
    }
  }
}
