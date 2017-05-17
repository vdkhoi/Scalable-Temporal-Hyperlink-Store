using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using SHS;
using System.IO.Compression;


namespace SHS
{
    

    public struct node_revision_info
    {
        //public long nodeId;
        public int number_of_revision;
        public int outlink_vector_size;
        public long[] time_duration;
        public byte[] revision_matrix;
    }

    public class RevisionData
    {
        
        //public static int NUM_RECORDS = 18444488;
        public static long TEMP_BUFF = 10000;
        //public static int NUM_RECORDS = 503101;  //From Jan-2013
        public static long NUM_RECORDS = 0;  //From Jan-to-Mar-2013
        //public static int TEMP_BUFF = 47;
        public node_revision_info[] revision_data;
        public int[] pointer = new int[NUM_RECORDS];
        public long NUM_LINKS = 0;
        public UidMap nodeMap;
        public RevisionData(string revision_file)
        {
            long[] uuid = null;
            using (var rd = new BinaryReader(new GZipStream(new BufferedStream(new FileStream(revision_file, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress)))
            {
                int i = 0;
                try
                {
                    NUM_RECORDS = rd.ReadInt64();
                    NUM_LINKS = rd.ReadInt64();
                    revision_data = new node_revision_info[NUM_RECORDS];
                    uuid = new long[NUM_RECORDS];
                    for (i = 0; i < NUM_RECORDS; i++)
                    {
                        uuid[i] = (long)rd.ReadInt64();
                        //Console.Write("{0} ", uuid[i]);
                        revision_data[i].outlink_vector_size = rd.ReadInt32();
                        //Console.Write("{0} ", revision_data[i].outlink_vector_size);
                        revision_data[i].number_of_revision = rd.ReadInt32();
                        //Console.Write("{0} ", revision_data[i].number_of_revision);

                        var bit_vector_length = revision_data[i].outlink_vector_size * revision_data[i].number_of_revision;

                        if (bit_vector_length % 8 != 0)
                        {
                            bit_vector_length += (8 - bit_vector_length % 8);
                        }

                        revision_data[i].revision_matrix = new byte[bit_vector_length];
                        revision_data[i].time_duration = new long[revision_data[i].number_of_revision];
                        //Console.WriteLine("2");
                        revision_data[i].time_duration[0] = rd.ReadInt64();
                        for (int j = 1; j < revision_data[i].number_of_revision; j++)
                        {
                            revision_data[i].time_duration[j] = revision_data[i].time_duration[j] + rd.ReadInt64();
                        }
                        //Console.WriteLine("3 {0}", j);

                        for (int j = 0; j < revision_data[i].revision_matrix.Length; j++)
                        {
                            revision_data[i].revision_matrix[j] = rd.ReadByte();
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
            DateTime d0 = Convert.ToDateTime("1998-01-01");
            Console.Write("{0}, ", info.number_of_revision);
            Console.Write("{0}, ", info.outlink_vector_size);
            Console.Write("{0}, ", d0.AddDays(info.time_duration[0]));
            for (int i = 1; i < info.number_of_revision; i ++ )
            {
                Console.Write("{0}, ", d0.AddDays(info.time_duration[i]));
            }
            Console.WriteLine();
        }

        public SortedDictionary<string, long> getOutlinkInDuration(long nodeId, long[] uids, string[] urls, DateTime start_date, DateTime end_date)
        {
            node_revision_info info = getNodeInfo(nodeId);

            //Console.Write("{0}, ", nodeId);
            //print_node_revision_info(info);
            DateTime d0 = Convert.ToDateTime("1998-01-01");
            long dist_start = (start_date - d0).Days;
            long dist_end = (end_date - d0).Days;

            if (start_date > end_date) return null;

            //DateTime first_time_stamp = Convert.ToDateTime(info.first_time_stamp);
            //long dist_start = (first_time_stamp - start_date).Milliseconds;
            //long dist_end = (first_time_stamp - end_date).Milliseconds;

 

            byte[] union_vector = new byte[info.outlink_vector_size];

            if (info.outlink_vector_size != uids.Length)
                Console.WriteLine("Some error happen!!!");

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
                        union_vector[k] = (byte)(link_at_index(info.revision_matrix, i * union_vector.Length + k) | union_vector[k]);
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
                if (union_vector[i] > 0)
                {
                    dict.Add(urls[i], uids[i]);
                }
            }

            //dict.Values.CopyTo(uids, 0);


            //for (int i = 0; i < info.outlink_vector_size; i++)
            //{
            //    if (link_at_index(union_vector, i) == 0)
            //    {
            //        dict.Remove(urls[i]);
            //    }
            //}
            return dict;
        }
    }

    public class TemporalDemo
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
                for (int i = 0; i < uids.Length; i++)
                {
                    List(uids[i], store, dist > 0 ? dist - 1 : dist + 1);
                }
            }
        }

        public static void Main(string[] args)
        {
            if (args.Length != 6)
            {
                Console.Error.WriteLine("Usage: SHS.Demo <servers.txt> <store> <dist> <urlBytes> <startDate> <endDate>");
            }
            else
            {
                int dist = Int32.Parse(args[2]);
                var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
                Console.WriteLine("URL={0}\nLinks={1}", store.NumUrls(), store.NumLinks());

                RevisionData revData = new RevisionData(args[1] + ".gz");
                long uid = store.UrlToUid(args[3]);
                //Console.WriteLine("UUID={0}", uid);
                DateTime d1 = Convert.ToDateTime(args[4]);
                DateTime d2 = Convert.ToDateTime(args[5]);
                //Console.WriteLine("Loaded time revision");
                long[] outlinks = store.GetLinks(uid, Dir.Fwd);
                string[] urls = store.BatchedUidToUrl(outlinks);

                
                Console.WriteLine("Number of outlinks: {0}", outlinks.Length);

                SortedDictionary<string, long> final_outlinks = new SortedDictionary<string, long>();
                final_outlinks = revData.getOutlinkInDuration(uid, outlinks, urls, d1, d2);

                Console.WriteLine("Number of final_outlinks: {0}", final_outlinks.Count);

                for (int i = 0; i < urls.Length; i++)
                {
                    if (final_outlinks.ContainsKey(urls[i]))
                        Console.WriteLine(urls[i]);
                }

                while (true)
                {
                    Console.Write("Input your data: ");
                    string[] input = Console.ReadLine().Split();
                    if (input[0] == "0") break;

                    uid = store.UrlToUid(input[0]);
                    outlinks = store.GetLinks(uid, Dir.Fwd);
                    urls = store.BatchedUidToUrl(outlinks);
                    Console.WriteLine("Number of outlinks: {0}", outlinks.Length);

                    d1 = Convert.ToDateTime(input[1]);
                    d2 = Convert.ToDateTime(input[2]);

                    final_outlinks = revData.getOutlinkInDuration(uid, outlinks, urls, d1, d2);

                    Console.WriteLine("Number of final_outlinks: {0}", final_outlinks.Count);
                    
                    Console.WriteLine("Press enter to read the URLs.\nUUID={0} ", uid);
                    Console.ReadLine();
                    for (int i = 0; i < urls.Length; i++)
                    {
                        if (final_outlinks.ContainsKey(urls[i]))
                            Console.WriteLine(urls[i]);
                    }

                }

            }

        }

        public static string InvertHost(string host)
        {
            if (HostUtils.IsNumeric(host))
            {
                return host;
            }
            else
            {
                int n = host.Length;
                char[] res = new char[n];
                int a = 0;
                while (a < n)
                {
                    int c = a;
                    while (c < n && host[c] != '.') c++;
                    int d = n - c;
                    while (a < c) res[d++] = host[a++];
                    res[d++] = '.';
                    a = c + 1;
                }
                return new string(res);
            }
        }

        //public static void Main(string[] args)
        //{
        //    Console.WriteLine("{0}", UrlUtils.HostOf("http://www.bbc.co.uk/index.html"));
        //    Console.WriteLine("{0}", InvertHost("www.bbc.co.uk"));
        //    Console.ReadLine();
        //}
    }
}
