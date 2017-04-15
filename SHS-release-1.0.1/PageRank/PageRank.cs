using System;
using System.Diagnostics;
using System.IO;
using SHS;

public class ShsPageRank {
  public static void Main(string[] args) {
    if (args.Length != 4) {
      Console.Error.WriteLine("Usage: SHS.PageRank <leader> <store> <d> <iters>");
    } else {
      var sw = Stopwatch.StartNew();
      var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
      double d = double.Parse(args[2]);
      int numIters = int.Parse(args[3]);
      long n = store.NumUrls();
      using (var wr = new BinaryWriter(new BufferedStream(new FileStream("pr-scores-" + 0 + ".bin", FileMode.Create, FileAccess.Write)))) {
        for (long i = 0; i < n; i++) wr.Write(1.0 / n);
      }
      var scores = store.AllocateUidState<double>();
      var uidBatch = new Batch<long>(50000);
      for (int k = 0; k < numIters; k++) {
        scores.SetAll(x => d / n);
        using (var rd = new BinaryReader(new BufferedStream(new FileStream("pr-scores-" + k + ".bin", FileMode.Open, FileAccess.Read)))) {
          foreach (long u in store.Uids()) {
            uidBatch.Add(u);
            if (uidBatch.Full || store.IsLastUid(u)) {
              var linkBatch = store.BatchedGetLinks(uidBatch, Dir.Fwd);
              var uniqLinks = new UidMap(linkBatch);
              var scoreArr = scores.GetMany(uniqLinks);
              foreach (var links in linkBatch) {
                double f = (1.0 - d) * rd.ReadDouble() / links.Length;
                foreach (var link in links) {
                  scoreArr[uniqLinks[link]] += f;
                }
              }
              scores.SetMany(uniqLinks, scoreArr);
              uidBatch.Reset();
            }
          }
        }
        using (var wr = new BinaryWriter(new BufferedStream(new FileStream("pr-scores-" + (k + 1) + ".bin", FileMode.Create, FileAccess.Write)))) {
          foreach (var us in scores.GetAll()) wr.Write(us.val);
        }
        File.Delete("pr-scores-" + k + ".bin");
        Console.WriteLine("Iteration {0} complete", k);
      }

      
      Console.WriteLine("Done. {0} iterations took {1} seconds.", numIters, 0.001 * sw.ElapsedMilliseconds);
      using (var rd = new BinaryReader(new BufferedStream(new FileStream("pr-scores-" + numIters + ".bin", FileMode.Open, FileAccess.Read))))
      {
          using (var wr = new StreamWriter("pr-scores-" + numIters + ".txt", true))
          {
              for (int i = 0; i < 1000000; i++)
              {
                wr.WriteLine("{0}", Math.Log(rd.ReadDouble()));
              }
          }
      }
    }
  }
}
