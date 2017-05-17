using System;
using System.Diagnostics;
using System.IO;
using SHS;
using System.IO.Compression;

public class PageRankFT {
  public static void Main(string[] args) {
    if (args.Length != 4) {
      Console.Error.WriteLine("Usage: SHS.PageRankFT <leader> <store> <d> <iters>");
    } else {
      var sw = Stopwatch.StartNew();
      Console.ReadLine();
      var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));

      Action<Action> Checkpointed = delegate(Action checkpointedBlock) {
        while (true) {
          try {
            checkpointedBlock();
            store.Checkpoint();
            break;
          } catch (ServerFailure) {
            Console.Error.WriteLine("Restarting from checkpoint");
            // go again
          }
        }
      };

      double d = double.Parse(args[2]);
      int numIters = int.Parse(args[3]);
      long n = store.NumUrls();

      UidState<string> oldScores = null, newScores = null;

      UidState<string> time_revision = store.AllocateUidState<string>();

      Checkpointed(delegate() {
        newScores = store.AllocateUidState<string>();
        oldScores = store.AllocateUidState<string>();
        oldScores.SetAll(uid => Convert.ToString(1.0 / n));
      });

      for (int k = 0; k < numIters; k++) {
        Checkpointed(delegate() {
          var uidBatch = new Batch<long>(50000);
          newScores.SetAll(x => Convert.ToString(d / n));
          foreach (long u in store.Uids()) {
            uidBatch.Add(u);
            if (uidBatch.Full || store.IsLastUid(u)) {
              var linkBatch = store.BatchedGetLinks(uidBatch, Dir.Fwd);
              var newMap = new UidMap(linkBatch);
              var oldSc = oldScores.GetMany(uidBatch);
              var newSc = newScores.GetMany(newMap);
              for (int i = 0; i < uidBatch.Count; i++) {
                var links = linkBatch[i];
                double f = (1.0 - d) * Convert.ToDouble(oldSc[i]) / links.Length;
                foreach (var link in links) {
                  newSc[newMap[link]] = Convert.ToString(Convert.ToDouble(newSc[newMap[link]]) + f);
                }
              }
              newScores.SetMany(newMap, newSc);
              uidBatch.Reset();
            }
          }
        });
        var tmp = newScores; newScores = oldScores; oldScores = tmp;
        Console.WriteLine("Done with iteration {0}", k);
      }
      using (var wr = new BinaryWriter(new BufferedStream(new FileStream("pr-scores.bin", FileMode.Create, FileAccess.Write)))) {
        foreach (var us in newScores.GetAll()) wr.Write(us.val);
      }

      using (var wr = new StreamWriter(new GZipStream(new FileStream("pr-scores.gz", FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
      {
        foreach (var us in newScores.GetAll()) wr.WriteLine(us.val);
      }
      Console.WriteLine("Done. {0} iterations took {1} seconds.", numIters, 0.001 * sw.ElapsedMilliseconds);
    }
  }
}
