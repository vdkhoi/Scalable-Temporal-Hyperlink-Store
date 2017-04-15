using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using SHS;
using System.IO.Compression;

// This code is wrote by a wrong understanding about UidMap Class. Need to be careful reconsideration 




public struct node_revision_info
{
    //public long nodeId;
    public int number_of_revision;
    public int outlink_vector_size;
    public DateTime first_time_stamp;
    public long[] time_duration;
    public byte[][] revision_matrix;
}

public class RevisionData
{

    //public static int NUM_RECORDS = 18444488;
    public static int TEMP_BUFF = 10000;
    //public static int NUM_RECORDS = 503101;  //From Jan-2013
    public static int NUM_RECORDS = 1030208;  //From Jan-to-Mar-2013
    //public static int TEMP_BUFF = 47;
    public node_revision_info[] revision_data;
    public int[] pointer = new int[NUM_RECORDS];
    public UidMap nodeMap;
    public RevisionData(string revision_file)
    {
        long[] uuid = null;
        using (var rd = new BinaryReader(new GZipStream(new BufferedStream(new FileStream(revision_file, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress)))
        {
            int i = 0;
            try
            {
                revision_data = new node_revision_info[NUM_RECORDS];
                uuid = new long[NUM_RECORDS];
                for (i = 0; i < NUM_RECORDS; i++)
                {
                    uuid[i] = (long)rd.ReadInt64();
                    //Console.Write("{0} ", uuid[i]);
                    revision_data[i].number_of_revision = rd.ReadInt32();
                    //Console.Write("{0} ", revision_data[i].number_of_revision);
                    revision_data[i].outlink_vector_size = rd.ReadInt32();
                    //Console.Write("{0} ", revision_data[i].outlink_vector_size);
                    revision_data[i].revision_matrix = new byte[revision_data[i].number_of_revision][];
                    revision_data[i].time_duration = new long[revision_data[i].number_of_revision];
                    int outlink_vector_matrix_size_in_byte = (int)Math.Ceiling((double)(revision_data[i].outlink_vector_size / 8.0));
                    //Console.WriteLine("2");
                    for (int j = 0; j < revision_data[i].number_of_revision; j++)
                    {
                        revision_data[i].revision_matrix[j] = new byte[outlink_vector_matrix_size_in_byte];
                        //Console.WriteLine("3 {0}", j);
                        if (j == 0)
                        {
                            //string dt = rd.ReadString();
                            //Console.Write("{0} ", dt);
                            revision_data[i].first_time_stamp = Convert.ToDateTime(rd.ReadString()).AddMilliseconds(-3600000);
                            //revision_data[i].first_time_stamp = rd.ReadString();
                            revision_data[i].time_duration[j] = 0;
                        }
                        else
                        {
                            revision_data[i].time_duration[j] = rd.ReadInt64();
                            //Console.Write("Time{0} ", revision_data[i].time_duration[j]);
                        }

                        for (int k = 0; k < outlink_vector_matrix_size_in_byte; k++)
                        {
                            revision_data[i].revision_matrix[j][k] = rd.ReadByte();
                            //Console.Write("{0} ", revision_data[i].revision_matrix[j][k]);
                        }

                    }
                    //Console.WriteLine();
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Reading error at line: {0}", i);
                Console.Error.WriteLine(e.Message);
            }
        }
        nodeMap = new UidMap(uuid);

        for (int i = 0; i < NUM_RECORDS; i++)
        {
            pointer[nodeMap[uuid[i]]] = i;
        }
    }

    public node_revision_info getNodeInfo(long nodeId)
    {
        int idx = pointer[nodeMap[nodeId]];
        return revision_data[idx];
    }

    public byte link_at_index(byte[] bit_vector, int position)
    {
        int byte_idx = position / 8;
        int bit_idx = 7 - position % 8;
        return (byte)((1 << bit_idx) & bit_vector[byte_idx]);
    }

    public void print_node_revision_info(node_revision_info info)
    {
        Console.Write("{0}, ", info.number_of_revision);
        Console.Write("{0}, ", info.outlink_vector_size);
        Console.Write("{0}, ", info.first_time_stamp.ToString());
        for (int i = 0; i < info.number_of_revision; i++)
        {
            Console.Write("{0}, ", info.first_time_stamp.AddSeconds(info.time_duration[i]).ToString());
            //Console.Write("{0}, ", Convert.ToDateTime(info.first_time_stamp).AddSeconds(info.time_duration[i]));
            for (int j = 0; j < info.revision_matrix[i].Length; j++)
            {
                Console.Write("{0}, ", info.revision_matrix[i][j]);
            }
        }
        Console.WriteLine();
    }

    public SortedDictionary<string, long> getOutlinkInDuration(long nodeId, long[] uids, string[] urls, DateTime start_date, DateTime end_date)
    {
        node_revision_info info = getNodeInfo(nodeId);

        //Console.Write("{0}, ", nodeId);
        //print_node_revision_info(info);

        if (start_date > end_date) return null;

        long dist_start = (long)(start_date - info.first_time_stamp).TotalSeconds;
        long dist_end = (long)(end_date - info.first_time_stamp).TotalSeconds;
        //DateTime first_time_stamp = Convert.ToDateTime(info.first_time_stamp);
        //long dist_start = (first_time_stamp - start_date).Milliseconds;
        //long dist_end = (first_time_stamp - end_date).Milliseconds;
        byte[] union_vector = new byte[(int)Math.Ceiling((double)(info.outlink_vector_size / 8.0))];

        List<long> final_outlink = new List<long>();


        for (int i = 0; i < union_vector.Length; i++)  // byte vector
        {
            union_vector[i] = 0;
        }
        for (int i = 0; i < info.number_of_revision; i++)
        {

            //Console.WriteLine("Current {0}, {1}, {2}, {3}", dist_start, dist_end, info.first_time_stamp.ToString(), info.first_time_stamp.AddSeconds(info.time_duration[i]).ToString());

            if (info.time_duration[i] < dist_end && info.time_duration[i] > dist_start)
            {
                for (int k = 0; k < union_vector.Length; k++)
                {
                    //Console.Write("{0}|{1}={2}", info.revision_matrix[i][k], union_vector[k], (byte)(info.revision_matrix[i][k] | union_vector[k]));
                    union_vector[k] = (byte)(info.revision_matrix[i][k] | union_vector[k]);
                }
                //Console.WriteLine();
            }
        }

        //for (int k = 0; k < union_vector.Length; k++)
        //{
        //    Console.Write("{0}, ", union_vector[k]);
        //}
        //Console.WriteLine();

        SortedDictionary<string, long> dict = new SortedDictionary<string, long>();

        for (int i = 0; i < uids.Length; i++)  // byte vector
        {
            dict.Add(urls[i], uids[i]);
        }

        dict.Values.CopyTo(uids, 0);


        for (int i = 0; i < info.outlink_vector_size; i++)
        {
            if (link_at_index(union_vector, i) == 0)
            {
                dict.Remove(urls[i]);
            }
        }
        return dict;
    }

    public SortedDictionary<string, long>[] BatchGetOutlinkInDuration(long[] nodeId, long[][] uids, string[][] urls, DateTime start_date, DateTime end_date)
    {
        SortedDictionary<string, long>[] result_graph = new SortedDictionary<string, long>[nodeId.Length];
        for (int i = 0; i < nodeId.Length; i++)
        {
            result_graph[i] = getOutlinkInDuration(nodeId[i], uids[i], urls[i], start_date, end_date);

        }
        return result_graph;
    }

    public SortedDictionary<string, long> getInlinkInDuration(long nodeId, long[] bwdUids, long[][] bwdValidateLinks, string[][] bwdValidateUrls, DateTime start_date, DateTime end_date)
    {
        SortedDictionary<string, long> result_inlinks = new SortedDictionary<string, long>(),
                                            temp_uids = new SortedDictionary<string, long>(); ;


        for (int i = 0; i < bwdUids.Length; i++)
        {
            temp_uids = getOutlinkInDuration(bwdUids[i], bwdValidateLinks[i], bwdValidateUrls[i], start_date, end_date);
            for (int j = 0; j < bwdValidateLinks[i].Length; j++)
            {
                if (bwdValidateLinks[i][j] == nodeId)
                {
                    result_inlinks.Add(bwdValidateUrls[i][j], bwdValidateLinks[i][j]);
                    break;
                }
            }
        }
        return result_inlinks;
    }
}

public class HITS
{
    public static int ITERATION_NUM = 10;

    public static int bs = 10;

    public static int fs = 10;

    public static SortedDictionary<long, KeyValuePair<double, double>> computeHITS(UidMap tbl, long[] srcUids, long[][] dstUids)
    {
        int n = dstUids.Length;
        //Console.WriteLine("Retrieve forward link of 1-hop nodes: {0} in {1} microseconds", dstUids.Length, forward_link_of_one_hop / 10);



        var srcId = new List<int>[n];
        var dstId = new List<int>[n];
        for (int i = 0; i < n; i++)
        {
            srcId[i] = new List<int>();
            dstId[i] = new List<int>();
        }
        for (int i = 0; i < n; i++)
        {
            int sid = tbl[srcUids[i]];
            for (int j = 0; j < dstUids[i].Length; j++)
            {
                int did = tbl[dstUids[i][j]];
                if (did != -1)
                {
                    srcId[sid].Add(did);
                    dstId[did].Add(sid);
                }
            }
        }

        //Build complete graph by array pointers



        double initScore = Math.Sqrt(1.0 / n);
        var aut = new double[n];
        var tmp_aut = new double[n];

        var hub = new double[n];
        var tmp_hub = new double[n];

        double norm_aut = 0.0, norm_hub = 0.0;

        for (int i = 0; i < n; i++)
        {
            hub[i] = aut[i] = initScore;
            tmp_aut[i] = tmp_hub[i] = 0.0;
        }


        for (int k = 0; k < ITERATION_NUM; k++)
        {
            norm_aut = norm_hub = 0.0;

            for (int u = 0; u < n; u++)
            {
                foreach (var id in srcId[u])
                {
                    tmp_aut[u] += hub[id];
                }
                norm_aut += Math.Pow(tmp_aut[u], 2.0);
            }
            norm_aut = Math.Sqrt(norm_aut);

            for (int u = 0; u < n; u++)
            {
                foreach (var id in dstId[u])
                {
                    tmp_hub[u] += aut[id];
                }
                norm_hub += Math.Pow(tmp_hub[u], 2.0);
            }
            norm_hub = Math.Sqrt(norm_hub);

            for (int u = 0; u < n; u++)
            {
                aut[u] = tmp_aut[u] / norm_aut;
                hub[u] = tmp_hub[u] / norm_hub;
                tmp_aut[u] = tmp_hub[u] = 0.0;
            }
        }

        //Prepare output data

        SortedDictionary<long, KeyValuePair<double, double>> result_score = new SortedDictionary<long, KeyValuePair<double, double>>();

        for (int i = 0; i < srcUids.Length; i++)
        {
            if (srcUids[i] > -1)
            {
                int idx = tbl[srcUids[i]];
                if (idx > -1)
                    result_score.Add(srcUids[i], new KeyValuePair<double, double>(aut[idx], hub[idx]));
            }
        }
        return result_score;
    }

    public static void Main(string[] args)
    {
        var shs = new Service(args[0]).OpenStore(Guid.Parse(args[1]));

        //using (var rd = new BinaryReader(new BufferedStream(new FileStream(args[2], FileMode.Open, FileAccess.Read)))) {
        RevisionData info = new RevisionData(args[1]);
        using (var rd = new StreamReader(new BufferedStream(new FileStream(args[2], FileMode.Open, FileAccess.Read))))
        {

            DateTime d1 = Convert.ToDateTime(args[3]);
            DateTime d2 = Convert.ToDateTime(args[4]);

            try
            {
                int queryId = Int32.Parse(rd.ReadLine());
                int numUrls = Int32.Parse(rd.ReadLine());
                var urls = new string[numUrls];
                for (int i = 0; i < numUrls; i++) urls[i] = rd.ReadLine();



                var sw = Stopwatch.StartNew();
                var uids = shs.BatchedUrlToUid(urls);
                var tbl = new UidMap(uids, true);


                long[] bwdUids = tbl;
                var bwdLinks = shs.BatchedSampleLinks(bwdUids, Dir.Bwd, bs, true);
                SortedDictionary<string, long> temp = new SortedDictionary<string,long>();
                for (int i = 0; i < bwdUids.Length; i++ )
                {
                    var bwdValidateUids = shs.BatchedSampleLinks(bwdLinks[i], Dir.Fwd, fs, true);
                    for (int j = 0; j < bwdValidateUids.Length; j++) {
                        string[] validateUrls = shs.BatchedUidToUrl(bwdValidateUids[j]);
                        temp = info.getOutlinkInDuration(bwdLinks[i][j], bwdValidateUids[j], validateUrls, d1, d2);
                        if (temp.ContainsValue(bwdUids[i])) {

                        }
                    }
                    var bwdValidateUrls = shs.BatchedUidToUrl(bwdLinks[i]);
                    //info.getInlinkInDuration(bwdUids[i], bwdLinks[i], )
                }
                



                var fwdUids = shs.BatchedSampleLinks(tbl, Dir.Fwd, fs, true);
                var fwdUrls = shs.BatchedUidToUrl(tbl);
                


                foreach (long[] x in bwdLinks) tbl.Add(x);
                foreach (long[] x in fwdUids) tbl.Add(x);
                long[] srcUids = tbl;
                string[] return_urls = shs.BatchedUidToUrl(srcUids);




                //Console.Error.WriteLine("Length in Archive {0}", tbl.GetSize());
                //var extTbl = tbl.Subtract(new UidMap(uids, true));
                //Console.Error.WriteLine("Length in Archive {0}", extTbl.GetSize());

                //long one_hope_retrieval_time = sw.ElapsedTicks;
                //Console.WriteLine("Retrieve 1-hops nodes: {0} from {1} root_nodes in {2} microseconds", srcUids.Length, uids.Length, one_hope_retrieval_time / 10);

                //sw = Stopwatch.StartNew();
                var dstUids = shs.BatchedGetLinks(srcUids, Dir.Fwd);



                //long forward_link_of_one_hop = sw.ElapsedTicks;

                SortedDictionary<long, KeyValuePair<double, double>> return_score = computeHITS(tbl, srcUids, dstUids);



                //long[] extUids = extTbl;
                //var extUrls = shs.BatchedUidToUrl(extUids);

                long end_time = sw.ElapsedTicks;
                Console.WriteLine("HITS finish in {0} microseconds with {1} links", end_time / 10, tbl.GetSize());


                /*
                int menu = 0;

                while ((menu = Int32.Parse(Console.ReadLine())) > 0)
                {
                    try { 
                        Console.WriteLine("You choose {0}.", menu);
                        switch (menu)
                        {
                            case 1:
                                Console.Error.WriteLine("Num of URLs: {0}", tbl.GetSize());
                                tbl.PrintList();
                                break;
                            case 2:
                                 Console.Error.WriteLine("Num of extend URLs: {0}", extTbl.GetSize());
                                 extTbl.PrintList();
                                 break;
                            case 3:
                                for (int i = 0; i < uids.Length; i++)
                                {
                                    if (uids[i] > -1)
                                    {
                                        int idx = tbl[uids[i]];
                                        Console.WriteLine("{0}\t{1}\t{2}", aut[idx], hub[idx], urls[i]);
                                    }
                                }
                                break;
                            case 4:
                                Console.Error.WriteLine("Num of extend URLs: {0}", extUids.Length);
                                for (int i = 0; i < extUrls.Length; i++)
                                {
                                    if (extUids[i] > -1)
                                    {
                                        int idx = tbl[extUids[i]];
                                        Console.WriteLine("{0}\t{1}\t{2}", aut[idx], hub[idx], extUrls[i]);
                                    }
                                }
                                break;
                            case 5:
                                Console.Error.WriteLine("Num of UIDS: {0}", uids.Length);
                                for (int i = 0; i < uids.Length; i++)
                                {
                                    Console.WriteLine("{0}", uids[i]);
                                }
                                break;
                            case 6:
                                Console.Error.WriteLine("Mapping UID to URL");
                                long uid = Int64.Parse(Console.ReadLine());
                                Console.WriteLine("{0}", shs.UidToUrl(uid));
                                break;
                            case 7:
                                Console.Error.WriteLine("Mapping URL to UID");
                                string url = Console.ReadLine();
                                Console.WriteLine("{0}", shs.UrlToUid(url));
                                break;
                            default:
                                Console.WriteLine("What?");
                                break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine(ex.ToString());
                    }
                }

                */

                //Output the result scores to screen
                var result_urls = shs.BatchedUidToUrl(srcUids);
                for (int i = 0; i < srcUids.Length; i++)
                {

                    if (return_score.ContainsKey(srcUids[i]))
                    {
                        KeyValuePair<double, double> score = new KeyValuePair<double, double>();
                        return_score.TryGetValue(srcUids[i], out score);
                        Console.WriteLine("{0}\t{1}\t{2}", score.Key, score.Value, result_urls[i]);
                    }
                }


                //long end_time = sw.ElapsedTicks;

                //Console.WriteLine("SALSA finish in {0} microseconds", end_time / 10);

                //for (int i = 0; i < scores.Length; i++)
                //{
                //    Console.WriteLine("{0}: {1}", urls[i], scores[i]);
                //}




            }
            catch (EndOfStreamException)
            {

            }

        }
    
    }


}

