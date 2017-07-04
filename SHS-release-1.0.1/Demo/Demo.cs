using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using SHS;
using System.Net.NetworkInformation;
using System.Net;

namespace SHS {
    public class Demo
    {
        private static void List(Int64 uid, Store store, int dist)
        {
            if (dist == 0)
            {
                Console.WriteLine(store.UidToUrl(uid));
            }
            else
            {
                Int64[] uids = store.GetLinks(uid, dist > 0 ? Dir.Fwd : Dir.Bwd);
                System.Console.WriteLine("Khoi: Retrieved links=" + uids.Length);
                for (int i = 0; i < uids.Length; i++)
                {
                    List(uids[i], store, dist > 0 ? dist - 1 : dist + 1);
                }
            }
        }

        public static string[] root_set = {
        "http://angela-merkel/",
        "http://stern.de/",
        "http://bild.de"
    };

        //public static void Main(string[] args)
        //{
        //    if (args.Length != 4)
        //    {
        //        //Console.WriteLine("Usage: SHS.Demo <servers> <store> <dist> <urlBytes>");

        //        Console.WriteLine("Usage: SHS.Demo <servers> <store> <distance> <url>");
        //    }
        //    else
        //    {
        //        //Console.ReadLine();
        //        //int dist = Int32.Parse(args[2]);
        //        //var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
        //        //Console.WriteLine("Store opened. Number of URL in store = " + store.NumUrls() + ". Number of Links in store = " + store.NumLinks());
        //        //var uid = store.UrlToUid(args[3]);
        //        //Int64[] uids = store.GetLinks(uid, dist > 0 ? Dir.Fwd : Dir.Bwd);
        //        //Console.ReadLine();
        //        List<List<long>> linkRevs = new List<List<long>>();
        //        List<long> linkRev = new List<long>();
        //        linkRev.Add(1);
        //        linkRev.Add(3);
        //        linkRev.Add(4);
        //        linkRev.Add(7);
        //        linkRevs.Add(linkRev);
        //        linkRev = new List<long>();
        //        linkRev.Add(2);
        //        linkRev.Add(3);
        //        linkRev.Add(5);
        //        linkRev.Add(6);
        //        linkRevs.Add(linkRev);
        //        linkRev = new List<long>();
        //        linkRev.Add(1);
        //        linkRev.Add(2);
        //        linkRev.Add(3);
        //        linkRev.Add(4);
        //        linkRev.Add(5);
        //        linkRev.Add(6);
        //        linkRev.Add(7);
        //        var bit_matrix = TempUtils.ConvertTimeSeriesToByteArray(linkRevs, linkRev);
        //        for (int i = 0; i < bit_matrix.Length; i++ )
        //        {
        //            Console.Write(bit_matrix[i]);
        //        }
        //        Console.WriteLine();
        //        List<long> timeStamp = new List<long>();
        //        timeStamp.Add(0);
        //        timeStamp.Add(3);
        //        var result = TempUtils.getListOfUidInTimeStamp(2, timeStamp, bit_matrix, linkRev, 4, 4);
        //        for (int i = 0; i < result.Count; i++)
        //        {
        //            Console.Write("{0}\t", result[i]);
        //        }
        //        Console.ReadLine();
        //    }
        //}

        //  public static void Main(string[] args) {
        //  if (args.Length != 3) {
        //    //Console.WriteLine("Usage: SHS.Demo <servers> <store> <dist> <urlBytes>");

        //    Console.WriteLine("Usage: SHS.Demo <servers> <store> <distance>");
        //  } else {
        //    int dist = Int32.Parse(args[2]);
        //    var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
        //    Console.WriteLine("Store opened. Number of URL in store = " + store.NumUrls() + ". Number of Links in store = " + store.NumLinks());
        //    Console.WriteLine("Number of batch links = {0}", root_set.Length);
        //    store.PrintCacheStats();
        //    //IEnumerable<long> uids = store.Uids();
        //    var sw = Stopwatch.StartNew();
        //    //foreach (var item in uids)
        //    //{
        //    //    System.Console.WriteLine(item);   
        //    //}


        //    /*
        //    long uid = 0;
        //    long duration = 0;
        //    for (int k = 0; k < 10; k++)
        //    {
        //      sw = Stopwatch.StartNew();
        //      uid = store.UrlToUid(args[3]);
        //      duration = sw.ElapsedTicks;

        //      Console.WriteLine("Mapping from URL to UID = {0} at loop {1}: {2} microseconds", uid, k, duration / 10);
        //    }

        //    for (int k = 0; k < 10; k++)
        //    {
        //        sw = Stopwatch.StartNew();
        //        string url = store.UidToUrl(uid);
        //        duration = sw.ElapsedTicks;
        //        Console.WriteLine("Mapping from UID to URL = {0} at loop {1}: {2} microseconds", url, k, duration / 10);
        //    }
        //    */



        //    //long uid = 21485769764;
        //    //Console.WriteLine("Khoi: UUID=" + uid);
        //    //if (uid == -1) {
        //    //  Console.WriteLine("URL {0} not in store. Use {1} instead.", args[3], "https://www.4x7ygr19wxcm9le3q7oq.de/");
        //    //  uid = 675139;
        //    //}
        //    //List(uid, store, dist);




        //    //var sw = Stopwatch.StartNew();
        //    //Int64[][] uids_b1 = store.BatchedGetLinks(manually_test, Dir.Fwd);
        //    //Int64[][] uids_b2 = store.BatchedGetLinks(manually_test, Dir.Bwd);




        //    //System.Console.WriteLine("Time to retrieve and send to console: {0} microseconds", sw.ElapsedMilliseconds * 1000);


        //    long end_time;
        //    //Int64[] uids_1 = store.GetLinks(uid, Dir.Fwd);
        //    //Int64[] uids_2 = store.GetLinks(uid, Dir.Bwd);
        //    //end_time = sw.ElapsedMilliseconds;


        //    //Console.Error.WriteLine("Time to GetLinks: {0} microseconds", sw.ElapsedMilliseconds);

        //    //Console.WriteLine("Frequency = {0}", Stopwatch.Frequency);
        //    //Console.WriteLine("Time to retrieve: {0} milliseconds", end_time);
        //    //sw = Stopwatch.StartNew();
        //    //uids_1 = store.GetLinks(uid, Dir.Fwd);
        //    //uids_2 = store.GetLinks(uid, Dir.Bwd);
        //    //end_time = sw.ElapsedTicks;
        //    //System.Console.WriteLine("Time to retrieve second times: {0} microseconds", end_time / 10);




        //    long[] manually_test = new long[root_set.Length];
        //    sw = Stopwatch.StartNew();
        //    manually_test = store.BatchedUrlToUid(root_set);
        //    end_time = sw.ElapsedTicks;
        //    Console.WriteLine("Time to map root_set URL to UUID using batch processing (loading time): {0} microseconds", end_time / 10);

        //    sw = Stopwatch.StartNew();
        //    manually_test = store.BatchedUrlToUid(root_set);
        //    end_time = sw.ElapsedTicks;
        //    System.Console.WriteLine("Time to map root_set URL to UUID using batch processing (normal value): {0} microseconds", end_time / 10);

        //    for (int i = 0; i < root_set.Length; i++)
        //    {
        //        if (manually_test[i] == -1)
        //            Console.Error.WriteLine("Error at link {0}", i);
        //    }

        //    sw = Stopwatch.StartNew();
        //    long[][] uids_b1 = store.BatchedGetLinks(manually_test, Dir.Fwd);
        //    long[][] uids_b2 = store.BatchedGetLinks(manually_test, Dir.Bwd);
        //    end_time = sw.ElapsedTicks;
        //    System.Console.WriteLine("Time to retrieve root_set links at using batch processing at iteration 0 (loading time): {0} microseconds", end_time / 10);

        //    for (int i = 1; i < 3; i++)
        //    {
        //        sw = Stopwatch.StartNew();
        //        uids_b1 = store.BatchedGetLinks(manually_test, Dir.Fwd);
        //        uids_b2 = store.BatchedGetLinks(manually_test, Dir.Bwd);
        //        end_time = sw.ElapsedTicks;
        //        System.Console.WriteLine("Time to retrieve root_set links at using batch processing at iteration {0} (normal value): {1} microseconds", i, end_time / 10);
        //    }

        //    List<long> uids_batch = new List<long>();

        //    //Console.WriteLine("Here 1 {0}, {1}", uids_b1.Length, uids_b2.Length);

        //    for (int i = 0; i < uids_b1.Length; i++)
        //    {
        //        for (int j = 0; j < uids_b1[i].Length; j++)
        //        {
        //            uids_batch.Add(uids_b1[i][j]);
        //        }
        //    }

        //    //Console.WriteLine("Here 2");


        //    for (int i = 0; i < uids_b2.Length; i++)
        //    {
        //        for (int j = 0; j < uids_b2[i].Length; j++)
        //        {
        //            uids_batch.Add(uids_b2[i][j]);
        //        }
        //    }

        //    long[] uids_batch_array = uids_batch.ToArray();

        //    Console.WriteLine("Total {0} URLs are retrieved as return_set", uids_batch_array.Length);

        //    sw = Stopwatch.StartNew();
        //    string[] url_result = store.BatchedUidToUrl(uids_batch_array);
        //    end_time = sw.ElapsedTicks;
        //    System.Console.WriteLine("Time to map return_set UUID to URL using batch processing first time (loading time): {0} microseconds", end_time / 10);

        //    sw = Stopwatch.StartNew();
        //    url_result = store.BatchedUidToUrl(uids_batch_array);
        //    end_time = sw.ElapsedTicks;
        //    System.Console.WriteLine("Time to map return_set UUID to URL using batch processing second times (normal value): {0} microseconds", end_time / 10);

        //    sw = Stopwatch.StartNew();
        //    url_result = store.BatchedUidToUrl(uids_batch_array);
        //    end_time = sw.ElapsedTicks;
        //    System.Console.WriteLine("Time to map return_set UUID to URL using batch processing third times (normal value): {0} microseconds", end_time / 10);


        //    //Ngung chay
        //    //for(int k = 0; k < 10; k++){
        //    //    sw = Stopwatch.StartNew();
        //    //    uids_1 = store.GetLinks(uid, Dir.Fwd);
        //    //    uids_2 = store.GetLinks(uid, Dir.Bwd);
        //    //    end_time = sw.ElapsedTicks;
        //    //    System.Console.WriteLine("Time to retrieve in {0} loop: {1} microseconds", k, end_time / 10);
        //    //}
        //    //List(uid, store, dist);

        //  }
        //}

        public static void Main(string[] args)
        {
            if (args.Length != 6)
            {
                Console.Error.WriteLine("Usage: SHS.Demo <servers.txt> <store> <dist> <urlBytes> <firsTime> <lastTime>");
            }
            else
            {
                Console.ReadLine();
                int dist = Int32.Parse(args[2]);
                long firstTime = TempUtils.GetDistanceToStartingPoint(args[4]);
                long lastTime  = TempUtils.GetDistanceToStartingPoint(args[5]);
                var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
                long uid = store.UrlToUid(args[3]);
                Console.WriteLine("Uid: {0} \t Duration: {1} - {2}", uid, firstTime, lastTime);
                Int64[] outLinks = store.GetTemporalLinks(uid, firstTime, lastTime, Dir.Fwd);
                //Int64[] outLinks = store.GetLinks(uid, Dir.Fwd);
                for (int i = 0; i < outLinks.Length; i++)
                {
                    Console.WriteLine("{0}", store.UidToUrl(outLinks[i]));
                }
                Console.ReadLine();
            }
        }
    }
}