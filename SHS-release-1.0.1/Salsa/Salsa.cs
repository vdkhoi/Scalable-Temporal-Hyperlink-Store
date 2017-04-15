using System;
using System.Collections.Generic;
using System.IO;
using SHS;
using System.Diagnostics;

public class Salsa {
  
  public static void Main(string[] args) {
    var shs = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
    int ITERATION_NUM = 10; 
    //using (var rd = new BinaryReader(new BufferedStream(new FileStream(args[2], FileMode.Open, FileAccess.Read)))) {
    using (var rd = new StreamReader(new BufferedStream(new FileStream(args[2], FileMode.Open, FileAccess.Read))))
    {
        
      int bs = int.Parse(args[3]);
      int fs = int.Parse(args[4]);
      while (true) {
        try {
          int queryId = Int32.Parse(rd.ReadLine());
          int numUrls = Int32.Parse(rd.ReadLine());
          var urls = new string[numUrls];
          for (int i = 0; i < numUrls; i++) urls[i] = rd.ReadLine();


          var sw = Stopwatch.StartNew();
          var uids = shs.BatchedUrlToUid(urls);
          var tbl = new UidMap(uids);
          var bwdUids = shs.BatchedSampleLinks(tbl, Dir.Bwd, bs, true);
          var fwdUids = shs.BatchedSampleLinks(tbl, Dir.Fwd, fs, true);
          foreach (long[] x in bwdUids) tbl.Add(x);
          foreach (long[] x in fwdUids) tbl.Add(x);
          long[] srcUids = tbl;
          //long one_hope_retrieval_time = sw.ElapsedTicks;
          //Console.WriteLine("Retrieve 1-hops nodes: {0} from {1} root_nodes in {2} microseconds", srcUids.Length, uids.Length, one_hope_retrieval_time / 10);

          //sw = Stopwatch.StartNew();
          var dstUids = shs.BatchedGetLinks(srcUids, Dir.Fwd);
          
          //long forward_link_of_one_hop = sw.ElapsedTicks;
          
          int n = dstUids.Length;
          //Console.WriteLine("Retrieve forward link of 1-hop nodes: {0} in {1} microseconds", dstUids.Length, forward_link_of_one_hop / 10);

          
          var srcId = new List<int>[n];
          var dstId = new List<int>[n];
          for (int i = 0; i < n; i++) {
            srcId[i] = new List<int>();
            dstId[i] = new List<int>();
          }
          sw = Stopwatch.StartNew();
          for (int i = 0; i < n; i++) {
            int sid = tbl[srcUids[i]];
            for (int j = 0; j < dstUids[i].Length; j++) {
              int did = tbl[dstUids[i][j]];
              if (did != -1) {
                srcId[sid].Add(did);
                dstId[did].Add(sid);
              }
            }
          }

          long end_time = sw.ElapsedTicks;
          Console.WriteLine("SALSA finish in {0} microseconds", end_time / 10);

          int numAuts = 0;
          for (int i = 0; i < n; i++) {
            if (dstId[i].Count > 0) numAuts++;
          }
          double initAut = 1.0 / numAuts;
          var aut = new double[n];
          var tmp = new double[n];
          for (int i = 0; i < n; i++) {
            aut[i] = dstId[i].Count > 0 ? initAut : 0.0;
          }
          for (int k = 0; k < ITERATION_NUM; k++) {
            for (int u = 0; u < n; u++) {
              foreach (var id in dstId[u]) {
                tmp[id] += (aut[u] / dstId[u].Count);
              }
              aut[u] = 0.0;
            }
            for (int u = 0; u < n; u++) {
              foreach (var id in srcId[u]) {
                aut[id] += (tmp[u] / srcId[u].Count);
              }
              tmp[u] = 0.0;
            }
          }
          var scores = new double[urls.Length];
          for (int i = 0; i < scores.Length; i++) {
            scores[i] = uids[i] == -1 ? 0.0 : aut[tbl[uids[i]]];
          }

          //long end_time = sw.ElapsedTicks;

          //Console.WriteLine("SALSA finish in {0} microseconds", end_time / 10);

          for (int i = 0; i < scores.Length; i++) {
            Console.WriteLine("{0}: {1}", urls[i], scores[i]);
          }

          double bestScore = double.MinValue;
          string bestUrl = null;
          for (int i = 0; i < urls.Length; i++) {
            if (scores[i] > bestScore) {
              bestScore = scores[i];
              bestUrl = urls[i];
            }
          }
          System.Console.Error.WriteLine("{0} {1}", queryId, bestUrl);
        } catch (EndOfStreamException) {
          break;
        }
      }
    }
  }
}
