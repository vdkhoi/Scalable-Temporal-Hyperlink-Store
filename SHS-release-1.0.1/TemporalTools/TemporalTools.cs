using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using SHS;
using System.Linq;

namespace SHS
{
    public struct node_revision_info
    {
        //public long nodeId;
        public int number_of_revision;
        public int outlink_vector_size;
        public string first_time_stamp;
        public long[] time_duration;
        public byte[] revision_matrix;

    }

    internal struct Revision_Links
    {
        internal long time_Stamp;
        internal long[] link_Vector;

        internal class Comparer : System.Collections.Generic.Comparer<Revision_Links>
        {
            public override int Compare(Revision_Links a, Revision_Links b)
            {

                if (a.time_Stamp < b.time_Stamp) return -1;

                if (a.time_Stamp == b.time_Stamp) return 0;

                return 1;
            }
        }
    }


    public struct TemporalUUIDLinks
    {
        internal long dest_node;
        internal DateTime time;
        internal long source_node;

        internal static TemporalUUIDLinks Read(BinaryReader rd)
        {
            var dstUid = rd.ReadInt64();
            var sTime = Convert.ToDateTime(rd.ReadString());
            var srcUid = rd.ReadInt64();
            return new TemporalUUIDLinks { dest_node = dstUid, time = sTime, source_node = srcUid };
        }

        internal static void Write(BinaryWriter wr, TemporalUUIDLinks a)
        {
            wr.Write(a.dest_node);
            wr.Write(a.time.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'"));
            wr.Write(a.source_node);
        }

        internal class Comparer : System.Collections.Generic.Comparer<TemporalUUIDLinks>
        {
            public override int Compare(TemporalUUIDLinks a, TemporalUUIDLinks b)
            {

                if (a.dest_node < b.dest_node) return -1;

                if ((a.dest_node == b.dest_node) && (a.time < b.time)) return -1;

                if ((a.dest_node == b.dest_node) && (a.time == b.time) && (a.source_node < b.source_node)) return -1;

                if ((a.dest_node == b.dest_node) && (a.time == b.time) && (a.source_node == b.source_node)) return 0;

                return 1;

            }
        }
    }


    public struct TemporalNodeIdLinks
    {
        internal long dest_node;
        internal long time;
        internal long source_node;

        internal static TemporalNodeIdLinks Read(BinaryReader rd)
        {
            var dstUid = rd.ReadInt64();
            var sTime = rd.ReadInt64();
            var srcUid = rd.ReadInt64();
            return new TemporalNodeIdLinks { dest_node = dstUid, time = sTime, source_node = srcUid };
        }

        internal static void Write(BinaryWriter wr, TemporalNodeIdLinks a)
        {
            wr.Write(a.dest_node);
            wr.Write(a.time);
            wr.Write(a.source_node);
        }

        internal class Comparer : System.Collections.Generic.Comparer<TemporalNodeIdLinks>
        {
            public override int Compare(TemporalNodeIdLinks a, TemporalNodeIdLinks b)
            {

                if (a.source_node < b.source_node) return -1;

                if ((a.source_node == b.source_node) && (a.time.CompareTo(b.time) == -1)) return -1;

                if ((a.source_node == b.source_node) && (a.time == b.time) && (a.dest_node < b.dest_node)) return -1;

                if ((a.source_node == b.source_node) && (a.time == b.time) && (a.dest_node == b.dest_node)) return 0;

                return 1;

            }
        }
    }

    public class RevisionData
    {

        public int NUM_RECORDS = 0;
        public List<node_revision_info> revision_data = new List<node_revision_info>();
        public int[] pointer;
        public UidMap nodeMap;

        public RevisionData(string revision_file)
        {
            List<long> uuids = new List<long>();
            using (var rd = new BinaryReader(new GZipStream(new BufferedStream(new FileStream(revision_file, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress)))
            {
                int i = 0;
                long uuid = -1;
                try
                {

                    while (true)
                    {
                        uuids.Add(uuid = rd.ReadInt64());
                        node_revision_info rev_data;
                        rev_data.outlink_vector_size = rd.ReadInt32();
                        rev_data.number_of_revision = rd.ReadInt32();
                        rev_data.first_time_stamp = rd.ReadString();
                        int outlink_vector_matrix_size_in_byte = 0;

                        if (rev_data.outlink_vector_size % 8 != 0)
                            outlink_vector_matrix_size_in_byte = (rev_data.outlink_vector_size + 8 - (rev_data.outlink_vector_size % 8)) / 8;
                        else
                            outlink_vector_matrix_size_in_byte = rev_data.outlink_vector_size / 8;

                        rev_data.time_duration = new long[rev_data.number_of_revision];
                        rev_data.revision_matrix = new byte[outlink_vector_matrix_size_in_byte];


                        rev_data.time_duration[0] = rd.ReadInt64(); // Read first time stamp
                        for (int j = 1; j < rev_data.number_of_revision; j++)
                        {
                            rev_data.time_duration[j] += (rd.ReadInt64() + rev_data.time_duration[j - 1]);
                        }
                        for (int k = 0; k < outlink_vector_matrix_size_in_byte; k++)
                        {
                            rev_data.revision_matrix[k] = rd.ReadByte();
                        }
                        revision_data.Add(rev_data);
                        i++;
                    }
                }
                catch (EndOfStreamException e)
                {
                    Console.Error.WriteLine("Reading error at line: {0}", i);
                    Console.Error.WriteLine(e.Message);
                }

                NUM_RECORDS = revision_data.Count;
            }
            nodeMap = new UidMap(uuids);
            pointer = new int[NUM_RECORDS];

            for (int i = 0; i < NUM_RECORDS; i++)
            {
                int pos = nodeMap[uuids[i]];
                if (pos > -1)
                    pointer[pos] = i;
            }
        }

        public node_revision_info getNodeInfo(long nodeId)
        {
            int pos = nodeMap[nodeId];
            if (pos == -1) return new node_revision_info { number_of_revision = 0 };
            int idx = pointer[pos];
            return revision_data[idx];
        }

        public IEnumerable<TemporalUUIDLinks> link_at_index(long nodeId, long[] out_links, node_revision_info data)
        {
            DateTime[] d = new DateTime[data.number_of_revision];
            d[0] = Convert.ToDateTime(data.first_time_stamp);

            for (int i = 0; i < data.number_of_revision; i++)
            {
                if (i > 1)
                    d[i] = d[i - 1].AddSeconds(data.time_duration[i]);

                for (int j = 0; j < data.outlink_vector_size; j++)
                {
                    byte mask = (byte)(1 << (7 - j % 8));
                    if (data.revision_matrix[j / 8] >> ((7 - j) % 8) == 1)
                    {

                        yield return (new TemporalUUIDLinks { dest_node = out_links[j], time = d[i], source_node = nodeId });
                    }
                }
            }
        }

        public void print_node_revision_info(node_revision_info info)
        {
            Console.Write("{0}, ", info.number_of_revision);
            Console.Write("{0}, ", info.outlink_vector_size);
            Console.Write("{0}, ", info.first_time_stamp.ToString());
            for (int i = 0; i < info.number_of_revision; i++)
            {
                //Write out something here
            }
            Console.WriteLine();
        }
    }

    public class TemporalTools
    {

        //public static int NUM_RECORDS = 503101;  //For graph from Jan-2013
        public static int TEMP_BUFF = 100000;
        public static UidMap temp_urls = new UidMap(TEMP_BUFF);

        public static void mappingURLToUUIDText(Store store, string in_filename, string out_filename)
        {

            string[] urls = new string[TEMP_BUFF];
            string[] revisions = new string[TEMP_BUFF];

            char[] delimiter = { ' ', '\t' };
            string[] currentLine = new string[2];
            string line = "";
            int i = 0;
            long[] return_set = new long[TEMP_BUFF];
            try
            {
                using (var rd = new StreamReader(new GZipStream(new BufferedStream(new FileStream(in_filename, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress)))
                using (var wr = new StreamWriter(new GZipStream(new FileStream(out_filename, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
                {
                    while ((line = rd.ReadLine()) != null)
                    {
                        currentLine = line.Split(delimiter, 2, StringSplitOptions.RemoveEmptyEntries);
                        urls[i % TEMP_BUFF] = currentLine[0];
                        revisions[i % TEMP_BUFF] = currentLine[1];
                        if (((i + 1) % TEMP_BUFF) == 0)
                        {
                            Console.WriteLine("Start writing batch to file");
                            return_set = store.BatchedUrlToUid(urls);
                            for (int k = 0; k < TEMP_BUFF; k++)
                            {
                                wr.WriteLine("{0}\t{1}", return_set[k], revisions[k].Trim());
                            }
                            Console.WriteLine("Finish {0} lines.", i + 1);
                        }
                        i++;
                    }

                    i--;

                    return_set = store.BatchedUrlToUid(urls);
                    for (int k = 0; k < i; k++)
                    {
                        wr.WriteLine("{0}\t{1}", return_set[k], revisions[k].Trim());
                    }

                    Console.WriteLine("Mapping {0} URLs to UUID", i);
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
                Console.Error.WriteLine("Stop at line {0}", i);
            }

        }

        public static void temp_write_matrix(byte[] matrix)
        {
            for (int i = 0; i < matrix.Length; i++)
            {
                Console.Write("{0} ", matrix[i]);
            }
            Console.WriteLine();
        }

        public static void mappingURLToUUIDBinary4Outlink(string in_filename, string out_filename)
        {

            var rd = new StreamReader(new GZipStream(new BufferedStream(new FileStream(in_filename, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
            var wr = new BinaryWriter(new GZipStream(new FileStream(out_filename, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress));
            char[] delimiter = { ' ', '\t' };
            string[] currentLine = null;
            string line = "";
            int i = 0;
            try
            {
                while ((line = rd.ReadLine()) != null)
                {
                    currentLine = line.Split(delimiter, StringSplitOptions.RemoveEmptyEntries);
                    long nodeId = Int64.Parse(currentLine[0]);
                    int outlink_vector_size = Int32.Parse(currentLine[1]);
                    int number_of_revision = Int32.Parse(currentLine[2]);
                    DateTime firstTimeStamp = Convert.ToDateTime(currentLine[3]);

                    wr.Write(nodeId);
                    wr.Write(outlink_vector_size);
                    wr.Write(number_of_revision);
                    wr.Write(currentLine[3]);

                    int outlink_vector_matrix_size_in_byte = 0;

                    if (outlink_vector_size % 8 != 0)
                        outlink_vector_matrix_size_in_byte = (outlink_vector_size + 8 - (outlink_vector_size % 8)) / 8;
                    else
                        outlink_vector_matrix_size_in_byte = outlink_vector_size / 8;

                    byte[] matrix_row = new byte[outlink_vector_matrix_size_in_byte];
                    //Console.WriteLine("First Time: {0}", currentLine[count]);

                    for (int k = 0; k < number_of_revision; k++)
                    {
                        wr.Write(Int64.Parse(currentLine[k + 4]));
                    }

                    //Console.Write("{0} ",currentLine[count++]);
                    for (int k = 0; k < outlink_vector_matrix_size_in_byte; k++)
                    {

                        wr.Write(Byte.Parse(currentLine[k + 4 + number_of_revision]));
                    }


                    i++;
                }
                Console.WriteLine("Wrote {0} list into binary file", i);

            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
                Console.Error.WriteLine("Stop at line {0}", i);
            }
            wr.Close();
            rd.Close();
        }


        public static void probagateBwdLinks(Store store, string bwd_file, string fwd_file)  // Have to code here
        {
            long[] temp_uuid = new long[TEMP_BUFF];
            RevisionData rev_data = new RevisionData(fwd_file);

            using (var wr = new StreamWriter(new GZipStream(new FileStream(bwd_file, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
            using (var sorter = new DiskSorter<TemporalUUIDLinks>(new TemporalUUIDLinks.Comparer(), TemporalUUIDLinks.Write, TemporalUUIDLinks.Read, 1 << 25))
            {
                int current_idx = 0;
                IEnumerator<long> uids = store.Uids().GetEnumerator();
                while (uids.MoveNext())
                {
                    temp_uuid[current_idx++] = uids.Current;
                    if (current_idx == TEMP_BUFF)
                    {
                        long[][] links = store.BatchedGetLinks(temp_uuid, Dir.Fwd);
                        for (int i = 0; i < links.Length; i++)
                        {
                            if (links[i].Length > 0)
                            {

                                node_revision_info info = rev_data.getNodeInfo(temp_uuid[i]);
                                if (info.number_of_revision > 0)
                                {
                                    foreach (TemporalUUIDLinks el in rev_data.link_at_index(temp_uuid[i], links[i], info))
                                    {
                                        sorter.Add(el);
                                    }
                                }
                            }
                        }

                        current_idx = 0;
                    }
                }

                sorter.Sort();

                while (!sorter.AtEnd())
                {
                    TemporalUUIDLinks link = sorter.Get();
                    wr.Write("{0}\t", link.dest_node);
                    wr.Write("{0}\t", link.time.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'"));
                    wr.WriteLine("{0}", link.source_node);
                }

            }
        }

        internal struct AdjDstURL
        {
            internal string srcIdx;

            internal static AdjDstURL Read(BinaryReader rd)
            {
                var srcUid = rd.ReadString();
                return new AdjDstURL { srcIdx = srcUid };
            }

            internal static void Write(BinaryWriter wr, AdjDstURL a)
            {
                wr.Write(a.srcIdx);
            }

            internal class Comparer : System.Collections.Generic.Comparer<AdjDstURL>
            {
                public override int Compare(AdjDstURL a, AdjDstURL b)
                {

                    return a.srcIdx.CompareTo(b.srcIdx);

                }
            }
        }

        public static void ConvertAdjUidList2BinaryMatrix(string args0, string args1, long num_list)
        {
            char[] delimiter = { '\t', ' ' };
            string line = "";
            long lastSrc = -1;
            SortedSet<long> allURLs = new SortedSet<long>();
            SortedSet<Revision_Links> timeStampList = new SortedSet<Revision_Links>(new Revision_Links.Comparer());
            try
            {
                using (var rd = new StreamReader(new GZipStream(new FileStream(args0, FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                using (var wr = new BinaryWriter(new GZipStream(new FileStream(args1, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
                {
                    wr.Write(num_list);

                    wr.Write(Convert.ToInt64(rd.ReadLine()));

                    while ((line = rd.ReadLine()) != null)
                    {
                        var field = line.Split(delimiter, StringSplitOptions.RemoveEmptyEntries).Select(Int64.Parse).ToArray();

                        if (field[0] == lastSrc || (lastSrc < 0))   // Current URL is last URL
                        {
                            for (int i = 2; i < field.Length; i++)
                            {
                                allURLs.Add(field[i]);
                            }
                            long[] outLinkList = new long[field.Length - 2];
                            Array.Copy(field, 2, outLinkList, 0, outLinkList.Length);
                            timeStampList.Add(new Revision_Links { time_Stamp = field[1], link_Vector = outLinkList });
                        }
                        else
                        {
                            var allLinks_Vector = allURLs.ToArray();
                            UidMap allLinks_Map = new UidMap(allLinks_Vector);
                            var pointers = new int[allLinks_Map.GetSize()];

                            for (int i = 0; i < pointers.Length; i++)
                            {
                                pointers[allLinks_Map[allLinks_Vector[i]]] = i;
                            }

                            var bit_vector_length = timeStampList.Count * allLinks_Vector.Length;

                            if (bit_vector_length % 8 != 0)
                            {
                                bit_vector_length += (8 - bit_vector_length % 8);
                            }

                            byte[] bit_matrix = new byte[bit_vector_length];
                            var vector_list_array = timeStampList.ToArray();

                            int[] time_diff = new int[timeStampList.Count];
                            time_diff[0] = (int)vector_list_array[0].time_Stamp;



                            for (int i = 0; i < vector_list_array.Length; i++)
                            {
                                if (i > 0)
                                    time_diff[i] = (int)(vector_list_array[i].time_Stamp - vector_list_array[i - 1].time_Stamp);

                                for (int j = 0; j < vector_list_array[i].link_Vector.Length; j++)
                                {
                                    if (allLinks_Map[vector_list_array[i].link_Vector[j]] > -1)
                                    {
                                        var base_index = i * allLinks_Vector.Length;
                                        bit_matrix[base_index + pointers[allLinks_Map[vector_list_array[i].link_Vector[j]]]] = 1;
                                    }
                                }

                            }


                            // Compress byte values array into bit
                            byte[] results = new byte[bit_matrix.Length / 8];

                            for (int i = 0; i < bit_matrix.Length / 8; i++)
                            {
                                for (int j = 0; j < 8; j++)
                                {
                                    if (bit_matrix[i * 8 + j] == 1)
                                    {
                                        byte curr_position = (byte)(1 << (7 - j));
                                        results[i] += curr_position;
                                    }
                                }
                            }

                            //wr.Write("{0}\t", lastSrc);                       // Source URL
                            //wr.Write("{0}\t", allLinks_Vector.Length);           // Size of link vector
                            //wr.Write("{0}\t", vector_list_array.Length);                // Number of revisions


                            wr.Write(lastSrc);                       // Source URL - Int64
                            wr.Write(allLinks_Vector.Length);        // Size of link vector - Int32
                            wr.Write(vector_list_array.Length);      // Number of revisions - Int32


                            for (int i = 0; i < time_diff.Length; i++)
                            {
                                //wr.Write("{0}\t", time_diff[i]);                 // Time difference between time stamp
                                wr.Write(time_diff[i]);                            // Int32
                            }

                            for (int i = 0; i < results.Length; i++)
                            {
                                //wr.Write("{0}\t", results[i]);                   // List of time revision bit matrix
                                wr.Write(results[i]);                              // Binary
                            }

                            //wr.WriteLine("{0}", outlink_URLs);                  // List of out URLs

                            //for (int i = 0; i < allLinks_Vector.Length - 1; i++)
                            //{
                            //    wr.Write("{0}\t", allLinks_Vector[i]);
                            //}

                            //wr.WriteLine("{0}", allLinks_Vector[allLinks_Vector.Length - 1]);

                            // Clear old data for new links
                            allURLs.Clear();
                            timeStampList.Clear();


                            for (int i = 2; i < field.Length; i++)
                            {
                                allURLs.Add(field[i]);
                            }
                            long[] outLinkList = new long[field.Length - 2];
                            Array.Copy(field, 2, outLinkList, 0, outLinkList.Length);
                            timeStampList.Add(new Revision_Links { time_Stamp = field[1], link_Vector = outLinkList });

                        }



                        lastSrc = field[0];

                    }
                }

            }
            catch (Exception e)
            {
                Console.Error.WriteLine("{0} {1}", e.Source, e.Message);
                Console.Error.WriteLine(e.StackTrace);
                Console.Error.WriteLine(lastSrc);
            }
            finally
            {

            }
        }



        public static void ConvertAdjUidList2TextMatrix(string args0, string args1, long num_list)
        {
            char[] delimiter = { '\t', ' ' };
            string line = "";
            long lastSrc = -1;
            SortedSet<long> allURLs = new SortedSet<long>();
            SortedSet<Revision_Links> timeStampList = new SortedSet<Revision_Links>(new Revision_Links.Comparer());
            try
            {
                using (var fwd = new StreamReader(new GZipStream(new FileStream(args0, FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                using (var wr = new StreamWriter(new GZipStream(new FileStream(args1, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
                {
                    while (((line = fwd.ReadLine()) != null))
                    {
                        var field = line.Split(delimiter, StringSplitOptions.RemoveEmptyEntries).Select(Int64.Parse).ToArray();

                        if (field[0] == lastSrc || (lastSrc < 0))   // Current URL is last URL
                        {
                            for (int i = 2; i < field.Length; i++)
                            {
                                allURLs.Add(field[i]);
                            }
                            long[] outLinkList = new long[field.Length - 2];
                            Array.Copy(field, 2, outLinkList, 0, outLinkList.Length);
                            timeStampList.Add(new Revision_Links { time_Stamp = field[1], link_Vector = outLinkList });
                        }
                        else
                        {
                            var allLinks_Vector = allURLs.ToArray();
                            UidMap allLinks_Map = new UidMap(allLinks_Vector);
                            var pointers = new int[allLinks_Map.GetSize()];

                            for (int i = 0; i < pointers.Length; i++)
                            {
                                pointers[allLinks_Map[allLinks_Vector[i]]] = i;
                            }

                            var bit_vector_length = timeStampList.Count * allLinks_Vector.Length;

                            if (bit_vector_length % 8 != 0)
                            {
                                bit_vector_length += (8 - bit_vector_length % 8);
                            }

                            byte[] bit_matrix = new byte[bit_vector_length];
                            var vector_list_array = timeStampList.ToArray();

                            int[] time_diff = new int[timeStampList.Count];
                            time_diff[0] = (int)vector_list_array[0].time_Stamp;



                            for (int i = 0; i < vector_list_array.Length; i++)
                            {
                                if (i > 0)
                                    time_diff[i] = (int)(vector_list_array[i].time_Stamp - vector_list_array[i - 1].time_Stamp);

                                for (int j = 0; j < vector_list_array[i].link_Vector.Length; j++)
                                {
                                    if (allLinks_Map[vector_list_array[i].link_Vector[j]] > -1)
                                    {
                                        var base_index = i * allLinks_Vector.Length;
                                        bit_matrix[base_index + pointers[allLinks_Map[vector_list_array[i].link_Vector[j]]]] = 1;
                                    }
                                }

                            }


                            // Compress byte values array into bit
                            byte[] results = new byte[bit_matrix.Length / 8];

                            for (int i = 0; i < bit_matrix.Length / 8; i++)
                            {
                                for (int j = 0; j < 8; j++)
                                {
                                    if (bit_matrix[i * 8 + j] == 1)
                                    {
                                        byte curr_position = (byte)(1 << (7 - j));
                                        results[i] += curr_position;
                                    }
                                }
                            }

                            wr.Write("{0}\t", lastSrc);                       // Source URL
                            wr.Write("{0}\t", allLinks_Vector.Length);           // Size of link vector
                            wr.Write("{0}\t", vector_list_array.Length);                // Number of revisions


                            //wr.Write(lastSrc);                       // Source URL - Int64
                            //wr.Write(allLinks_Vector.Length);        // Size of link vector - Int32
                            //wr.Write(vector_list_array.Length);      // Number of revisions - Int32


                            for (int i = 0; i < time_diff.Length; i++)
                            {
                                wr.Write("{0}\t", time_diff[i]);                 // Time difference between time stamp
                                //wr.Write(time_diff[i]);                            // Int32
                            }

                            for (int i = 0; i < results.Length; i++)
                            {
                                wr.Write("{0}\t", results[i]);                   // List of time revision bit matrix
                                //wr.Write(results[i]);                              // Binary
                            }

                            //wr.WriteLine("{0}", outlink_URLs);                  // List of out URLs

                            for (int i = 0; i < allLinks_Vector.Length - 1; i++)
                            {
                                wr.Write("{0}\t", allLinks_Vector[i]);
                            }

                            wr.WriteLine("{0}", allLinks_Vector[allLinks_Vector.Length - 1]);

                            // Clear old data for new links
                            allURLs.Clear();
                            timeStampList.Clear();


                            for (int i = 2; i < field.Length; i++)
                            {
                                allURLs.Add(field[i]);
                            }
                            long[] outLinkList = new long[field.Length - 2];
                            Array.Copy(field, 2, outLinkList, 0, outLinkList.Length);
                            timeStampList.Add(new Revision_Links { time_Stamp = field[1], link_Vector = outLinkList });

                        }



                        lastSrc = field[0];

                    }

                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("{0} {1}", e.Source, e.Message);
                Console.Error.WriteLine(e.StackTrace);
                Console.Error.WriteLine(lastSrc);
            }
            finally
            {

            }
        }

        public static void CompressTemporalData(ref long[] current_fields, StreamReader rd, StreamWriter wr)
        {
            char[] delimiter = { '\t', ' ' };
            long lastSrc = current_fields[0];
            SortedSet<long> allURLs = new SortedSet<long>();
            SortedSet<Revision_Links> timeStampList = new SortedSet<Revision_Links>(new Revision_Links.Comparer());

            while (!rd.EndOfStream && (current_fields[0] == lastSrc))
            {

                for (int i = 2; i < current_fields.Length; i++)
                {
                    allURLs.Add(current_fields[i]);
                }
                long[] outLinkList = new long[current_fields.Length - 2];
                Array.Copy(current_fields, 2, outLinkList, 0, outLinkList.Length);
                timeStampList.Add(new Revision_Links { time_Stamp = current_fields[1], link_Vector = outLinkList });


                lastSrc = current_fields[0];

                current_fields = rd.ReadLine().Split(delimiter, StringSplitOptions.RemoveEmptyEntries).Select(Int64.Parse).ToArray();
            }


            var allLinks_Vector = allURLs.ToArray();
            UidMap allLinks_Map = new UidMap(allLinks_Vector);
            var pointers = new int[allLinks_Map.GetSize()];

            for (int i = 0; i < pointers.Length; i++)
            {
                pointers[allLinks_Map[allLinks_Vector[i]]] = i;
            }

            var bit_vector_length = timeStampList.Count * allLinks_Vector.Length;

            if (bit_vector_length % 8 != 0)
            {
                bit_vector_length += (8 - bit_vector_length % 8);
            }

            byte[] bit_matrix = new byte[bit_vector_length];
            var vector_list_array = timeStampList.ToArray();

            int[] time_diff = new int[timeStampList.Count];
            time_diff[0] = (int)vector_list_array[0].time_Stamp;



            for (int i = 0; i < vector_list_array.Length; i++)
            {
                if (i > 0)
                    time_diff[i] = (int)(vector_list_array[i].time_Stamp - vector_list_array[i - 1].time_Stamp);

                for (int j = 0; j < vector_list_array[i].link_Vector.Length; j++)
                {
                    if (allLinks_Map[vector_list_array[i].link_Vector[j]] > -1)
                    {
                        var base_index = i * allLinks_Vector.Length;
                        bit_matrix[base_index + pointers[allLinks_Map[vector_list_array[i].link_Vector[j]]]] = 1;
                    }
                }

            }


            // Compress byte values array into bit
            byte[] results = new byte[bit_matrix.Length / 8];

            for (int i = 0; i < bit_matrix.Length / 8; i++)
            {
                for (int j = 0; j < 8; j++)
                {
                    if (bit_matrix[i * 8 + j] == 1)
                    {
                        byte curr_position = (byte)(1 << (7 - j));
                        results[i] += curr_position;
                    }
                }
            }

            //wr.Write("{0}\t", lastSrc);                                         // Source URL
            wr.Write("{0}\t", allLinks_Vector.Length);           // Size of link vector and direction
            wr.Write("{0}\t", vector_list_array.Length);                        // Number of revisions


            //wr.Write(lastSrc);                       // Source URL - Int64
            //wr.Write(allLinks_Vector.Length);        // Size of link vector - Int32
            //wr.Write(vector_list_array.Length);      // Number of revisions - Int32


            for (int i = 0; i < time_diff.Length; i++)
            {
                wr.Write("{0}\t", time_diff[i]);                 // Time difference between time stamp
                //wr.Write(time_diff[i]);                            // Int32
            }

            for (int i = 0; i < results.Length; i++)
            {
                wr.Write("{0}\t", results[i]);                   // List of time revision bit matrix
                //wr.Write(results[i]);                              // Binary
            }

            //wr.WriteLine("{0}", outlink_URLs);                  // List of out URLs

            for (int i = 0; i < allLinks_Vector.Length - 1; i++)
            {
                wr.Write("{0}\t", allLinks_Vector[i]);
            }

            wr.Write("{0}", allLinks_Vector[allLinks_Vector.Length - 1]);
        }

        public static void ConvertBwdFwdAdjUidList2BinaryMatrix(string args0, string args1)
        {
            char[] delimiter = { '\t', ' ' };
            try
            {
                int NUM_RECORDS = 0;
                using (var fwd = new StreamReader(new GZipStream(new FileStream(args0, FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                using (var bwd = new StreamReader(new GZipStream(new FileStream("Bwd_" + args0, FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                using (var wr = new BinaryWriter(new BufferedStream(new GZipStream(new FileStream("tmp-" + args1, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress))))
                {
                    var num_links = fwd.ReadLine();
                    num_links = bwd.ReadLine();
                    var fwd_fields = fwd.ReadLine().Split(delimiter, StringSplitOptions.RemoveEmptyEntries).Select(Int64.Parse).ToArray();
                    var bwd_fields = bwd.ReadLine().Split(delimiter, StringSplitOptions.RemoveEmptyEntries).Select(Int64.Parse).ToArray();

                    
                    
                    while (!fwd.EndOfStream && !bwd.EndOfStream)
                    {

                        if (fwd_fields[0] < bwd_fields[0])
                        {
                            wr.Write(fwd_fields[0]);
                            wr.Write((byte)1);
                            CompressTemporalDataToBinary(current_fields: ref fwd_fields, rd: fwd, wr: wr);
                        }
                        else if (fwd_fields[0] > bwd_fields[0])
                        {
                            wr.Write(bwd_fields[0]);
                            wr.Write((byte)0);
                            CompressTemporalDataToBinary(current_fields: ref bwd_fields, rd: bwd, wr: wr);
                        }
                        else
                        {
                            wr.Write(fwd_fields[0]);
                            wr.Write((byte)1);
                            CompressTemporalDataToBinary(current_fields: ref fwd_fields, rd: fwd, wr: wr);
                            CompressTemporalDataToBinary(current_fields: ref bwd_fields, rd: bwd, wr: wr);
                        }
                        NUM_RECORDS++;
                    }

                    while (!fwd.EndOfStream)
                    {
                        wr.Write(fwd_fields[0]);
                        wr.Write((byte)1);
                        CompressTemporalDataToBinary(current_fields: ref fwd_fields, rd: fwd, wr: wr);
                        NUM_RECORDS++;
                    }

                    while (!bwd.EndOfStream)
                    {
                        wr.Write(bwd_fields[0]);
                        wr.Write((byte)0);
                        CompressTemporalDataToBinary(current_fields: ref bwd_fields, rd: bwd, wr: wr);
                        NUM_RECORDS++;
                    }
                   
                }

                using (var wr = new BinaryWriter(new BufferedStream(new GZipStream(new FileStream(args1, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress))))
                using (var rd = new BinaryReader(new GZipStream(new FileStream("tmp-" + args1, FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                {
                    wr.Write(NUM_RECORDS);
                    while (true)
                    {
                        wr.Write(rd.ReadByte());
                    }
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("{0} {1}", e.Source, e.Message);
                Console.Error.WriteLine(e.StackTrace);
            }
            finally
            {

            }
        }


        public static void CompressTemporalDataToBinary(ref long[] current_fields, StreamReader rd, BinaryWriter wr)
        {
            char[] delimiter = { '\t', ' ' };
            long lastSrc = current_fields[0];
            SortedSet<long> allURLs = new SortedSet<long>();
            SortedSet<Revision_Links> timeStampList = new SortedSet<Revision_Links>(new Revision_Links.Comparer());

            while (!rd.EndOfStream && (current_fields[0] == lastSrc))
            {

                for (int i = 2; i < current_fields.Length; i++)
                {
                    allURLs.Add(current_fields[i]);
                }
                long[] outLinkList = new long[current_fields.Length - 2];
                Array.Copy(current_fields, 2, outLinkList, 0, outLinkList.Length);
                timeStampList.Add(new Revision_Links { time_Stamp = current_fields[1], link_Vector = outLinkList });


                lastSrc = current_fields[0];

                current_fields = rd.ReadLine().Split(delimiter, StringSplitOptions.RemoveEmptyEntries).Select(Int64.Parse).ToArray();
            }


            var allLinks_Vector = allURLs.ToArray();
            UidMap allLinks_Map = new UidMap(allLinks_Vector);
            var pointers = new int[allLinks_Map.GetSize()];

            for (int i = 0; i < pointers.Length; i++)
            {
                pointers[allLinks_Map[allLinks_Vector[i]]] = i;
            }

            var bit_vector_length = timeStampList.Count * allLinks_Vector.Length;

            if (bit_vector_length % 8 != 0)
            {
                bit_vector_length += (8 - bit_vector_length % 8);
            }

            byte[] bit_matrix = new byte[bit_vector_length];
            var vector_list_array = timeStampList.ToArray();

            int[] time_diff = new int[timeStampList.Count];
            time_diff[0] = (int)vector_list_array[0].time_Stamp;



            for (int i = 0; i < vector_list_array.Length; i++)
            {
                if (i > 0)
                    time_diff[i] = (int)(vector_list_array[i].time_Stamp - vector_list_array[i - 1].time_Stamp);

                for (int j = 0; j < vector_list_array[i].link_Vector.Length; j++)
                {
                    if (allLinks_Map[vector_list_array[i].link_Vector[j]] > -1)
                    {
                        var base_index = i * allLinks_Vector.Length;
                        bit_matrix[base_index + pointers[allLinks_Map[vector_list_array[i].link_Vector[j]]]] = 1;
                    }
                }

            }


            // Compress byte values array into bit
            byte[] results = new byte[bit_matrix.Length / 8];

            for (int i = 0; i < bit_matrix.Length / 8; i++)
            {
                for (int j = 0; j < 8; j++)
                {
                    if (bit_matrix[i * 8 + j] == 1)
                    {
                        byte curr_position = (byte)(1 << (7 - j));
                        results[i] += curr_position;
                    }
                }
            }

            //wr.Write("{0}\t", lastSrc);                                         // Source URL
            wr.Write(allLinks_Vector.Length);           // Size of link vector and direction
            wr.Write(vector_list_array.Length);                        // Number of revisions


            //wr.Write(lastSrc);                       // Source URL - Int64
            //wr.Write(allLinks_Vector.Length);        // Size of link vector - Int32
            //wr.Write(vector_list_array.Length);      // Number of revisions - Int32


            for (int i = 0; i < time_diff.Length; i++)
            {
                wr.Write(time_diff[i]);                 // Time difference between time stamp
                //wr.Write(time_diff[i]);                            // Int32
            }

            for (int i = 0; i < results.Length - 1; i++)
            {
                wr.Write(results[i]);                   // List of time revision bit matrix
                //wr.Write(results[i]);                              // Binary
            }

            wr.Write(results[results.Length - 1]);                  // List of out URLs

            //for (int i = 0; i < allLinks_Vector.Length - 1; i++)
            //{
            //    wr.Write("{0}\t", allLinks_Vector[i]);
            //}

            //wr.Write("{0}", allLinks_Vector[allLinks_Vector.Length - 1]);
        }

        public static void ConvertBwdFwdAdjUidList2TextMatrix(string args0, string args1)
        {
            char[] delimiter = { '\t', ' ' };
            try
            {
                using (var fwd = new StreamReader(new GZipStream(new FileStream(args0, FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                using (var bwd = new StreamReader(new GZipStream(new FileStream("Bwd_" + args0, FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                using (var wr = new StreamWriter(new BufferedStream(new GZipStream(new FileStream(args1, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress))))
                {
                    var num_links = fwd.ReadLine();
                    num_links = bwd.ReadLine();
                    var fwd_fields = fwd.ReadLine().Split(delimiter, StringSplitOptions.RemoveEmptyEntries).Select(Int64.Parse).ToArray();
                    var bwd_fields = bwd.ReadLine().Split(delimiter, StringSplitOptions.RemoveEmptyEntries).Select(Int64.Parse).ToArray();

                    
                    while (!fwd.EndOfStream && !bwd.EndOfStream)
                    {

                        if (fwd_fields[0] < bwd_fields[0])
                        {
                            wr.Write("{0}\t1\t", fwd_fields[0]);
                            CompressTemporalData(current_fields: ref fwd_fields, rd: fwd, wr: wr);
                            wr.WriteLine();
                        }
                        else if (fwd_fields[0] > bwd_fields[0])
                        {
                            wr.Write("{0}\t0\t", bwd_fields[0]);
                            CompressTemporalData(current_fields: ref bwd_fields, rd: bwd, wr: wr);
                            wr.WriteLine();
                        }
                        else
                        {
                            wr.Write("{0}\t1\t", fwd_fields[0]);
                            CompressTemporalData(current_fields: ref fwd_fields, rd: fwd, wr: wr);
                            wr.Write("\t");
                            CompressTemporalData(current_fields: ref bwd_fields, rd: bwd, wr: wr);
                            wr.WriteLine();
                        }
                        
                    }

                    while (!fwd.EndOfStream)
                    {
                        wr.Write("{0}\t1\t", fwd_fields[0]);
                        CompressTemporalData(current_fields: ref fwd_fields, rd: fwd, wr: wr);
                        wr.WriteLine();
                        
                    }

                    while (!bwd.EndOfStream)
                    {
                        wr.Write("{0}\t0\t", bwd_fields[0]);
                        CompressTemporalData(current_fields: ref bwd_fields, rd: bwd, wr: wr);
                        wr.WriteLine();
                        
                    }
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("{0} {1}", e.Source, e.Message);
                Console.Error.WriteLine(e.StackTrace);
            }
            finally
            {

            }
        }


        public static KeyValuePair<long, long> ConvertLinks2AdjListByUid(Store store, string args0, string args1)
        {
            char[] Sep = { ' ', '\t' };
            var d0 = Convert.ToDateTime("1998-01-01");
            DateTime d = d0;
            long num_links = 0;
            long fwd_num_list = 0;
            long bwd_num_list = 0;
            var bwd_sorter = new DiskSorter<TemporalNodeIdLinks>(new TemporalNodeIdLinks.Comparer(), TemporalNodeIdLinks.Write, TemporalNodeIdLinks.Read, 1 << 25);
            try
            {
                using (var rd = new StreamReader(new GZipStream(new FileStream(args0, FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                using (var wr = new StreamWriter(new GZipStream(new FileStream(args1, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
                using (var sorter = new DiskSorter<TemporalNodeIdLinks>(new TemporalNodeIdLinks.Comparer(), TemporalNodeIdLinks.Write, TemporalNodeIdLinks.Read, 1 << 25))
                {
                    string line = "";
                    int NUM_CACHE = 10000;
                    string[] links = new string[3];
                    string[] cache = new string[2 * NUM_CACHE];
                    long[] temporal = new long[NUM_CACHE];

                    int current_count = 0;
                    long[] out_link;

                    while ((line = rd.ReadLine()) != null)
                    {
                        links = line.Split(Sep, StringSplitOptions.RemoveEmptyEntries);
                        temporal[current_count] = (Convert.ToDateTime(links[2]) - d0).Days;
                        cache[2 * current_count] = links[0];
                        cache[2 * current_count + 1] = links[1];

                        if (current_count == (NUM_CACHE - 1))
                        {
                            out_link = store.BatchedUrlToUid(cache);
                            for (int i = 0; i < NUM_CACHE; i++)
                            {
                                sorter.Add(new TemporalNodeIdLinks { source_node = out_link[2 * i], time = temporal[i], dest_node = out_link[2 * i + 1] });
                                bwd_sorter.Add(new TemporalNodeIdLinks { source_node = out_link[2 * i + 1], time = temporal[i], dest_node = out_link[2 * i] });
                            }
                            current_count = -1;
                        }
                        current_count++;
                    }

                    if (current_count > 0)
                    {
                        current_count--;
                        string[] restUid = new string[current_count];
                        Array.Copy(cache, restUid, current_count);
                        out_link = store.BatchedUrlToUid(restUid);
                        for (int i = 0; i < current_count / 2; i++)
                        {
                            sorter.Add(new TemporalNodeIdLinks { source_node = out_link[2 * i], time = temporal[i], dest_node = out_link[2 * i + 1] });
                            bwd_sorter.Add(new TemporalNodeIdLinks { source_node = out_link[2 * i + 1], time = temporal[i], dest_node = out_link[2 * i] });
                        }
                    }

                    sorter.Sort();
                    num_links = sorter.Total;
                    Console.WriteLine("{0} links", num_links);

                    TemporalNodeIdLinks temporalFwdLink = sorter.Get();
                    var current = temporalFwdLink;

                    wr.WriteLine(num_links);
                    while (!sorter.AtEnd())
                    {

                        wr.Write(current.source_node + "\t" + current.time + "\t");
                        while ((current.source_node == temporalFwdLink.source_node) && (current.time == temporalFwdLink.time) && !sorter.AtEnd())
                        {
                            wr.Write(temporalFwdLink.dest_node + "\t");
                            temporalFwdLink = sorter.Get();
                        }

                        // Consider to change to UUID here

                        wr.WriteLine();
                        current = temporalFwdLink;
                        fwd_num_list++;
                    }


                    Console.WriteLine("Finished build links to adjacency list by string...");



                }

                using (var wr = new StreamWriter(new GZipStream(new FileStream("Bwd_" + args1, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
                {
                    bwd_sorter.Sort();
                    Console.WriteLine("{0} links", bwd_sorter.Total);

                    TemporalNodeIdLinks temporalFwdLink = bwd_sorter.Get();
                    TemporalNodeIdLinks current = temporalFwdLink;
                    wr.WriteLine(num_links);
                    while (!bwd_sorter.AtEnd())
                    {

                        wr.Write(current.source_node + "\t" + current.time + "\t");
                        while ((current.source_node == temporalFwdLink.source_node) && (current.time == temporalFwdLink.time) && !bwd_sorter.AtEnd())
                        {
                            wr.Write(temporalFwdLink.dest_node + "\t");
                            temporalFwdLink = bwd_sorter.Get();
                        }

                        // Consider to change to UUID here

                        wr.WriteLine();
                        current = temporalFwdLink;
                        bwd_num_list++;
                    }
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message);
            }
            finally
            {

            }

            return new KeyValuePair<long, long>(fwd_num_list, bwd_num_list);

        }


        public static void ConvertLinks2AdjListByNodeId(Store store, string args0, string args1)
        {
            char[] Sep = { '\t' };
            var d0 = Convert.ToDateTime("1998-01-01");
            DateTime d = d0;
            try
            {
                using (var rd = new StreamReader(new GZipStream(new FileStream(args0, FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                using (var wr = new StreamWriter(new GZipStream(new FileStream(args1, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
                using (var sorter = new DiskSorter<TemporalUUIDLinks>(new TemporalUUIDLinks.Comparer(), TemporalUUIDLinks.Write, TemporalUUIDLinks.Read, 1 << 25))
                {

                    string line = "";
                    string[] links = null;

                    while ((line = rd.ReadLine()) != null)
                    {
                        links = line.Split(Sep, StringSplitOptions.RemoveEmptyEntries);
                        sorter.Add(new TemporalUUIDLinks { source_node = Convert.ToInt64(links[0]), time = Convert.ToDateTime(links[1]), dest_node = Convert.ToInt64(links[2]) });
                    }
                    sorter.Sort();

                    TemporalUUIDLinks tempLink = sorter.Get();
                    var current = tempLink;

                    while (!sorter.AtEnd())
                    {

                        wr.Write(current.source_node + "\t" + current.time.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'\t"));
                        while ((current.source_node == tempLink.source_node) && (current.time == tempLink.time) && !sorter.AtEnd())
                        {
                            wr.Write(links[2] + "\t");
                            tempLink = sorter.Get();
                        }
                        wr.WriteLine();
                        current = tempLink;

                    }

                    Console.WriteLine("Finished build links to adjacency list...");

                }
            }
            catch (Exception)
            {

            }
            finally
            {

            }
        }


        public static void Main(string[] args)
        {
            if (args.Length != 4)
            {
                Console.WriteLine("Usage: SHS.TemporalTool.exe <servers> <store> <command> <in_file> <out_file> <bwd_file>");
            }
            else
            {
                Console.Write("Press Enter to start..."); Console.ReadLine();

                if (args[2] == "-t")  // Text
                {
                    var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
                    Console.WriteLine("Store opened. Number of URL in store = " + store.NumUrls() + ". Number of Links in store = " + store.NumLinks());
                    var sw = Stopwatch.StartNew();
                    mappingURLToUUIDText(store, args[3], args[4]);
                    Console.WriteLine("Mapping successfully in {0} milliseconds", sw.ElapsedMilliseconds);
                }
                else if (args[2] == "-b")  //  Binary
                {
                    var sw = Stopwatch.StartNew();
                    mappingURLToUUIDBinary4Outlink(args[3], args[4]);
                    Console.WriteLine("Mapping successfully in {0} milliseconds", sw.ElapsedMilliseconds);
                }
                else if (args[2] == "-c")  // Complete
                {
                    var sw = Stopwatch.StartNew();
                    var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
                    KeyValuePair<long, long> pair = ConvertLinks2AdjListByUid(store, args[3], "ConvertLinks2AdjListByUid.gz");
                    //ConvertAdjUidList2TextMatrix("ConvertLinks2AdjListByUid.gz", "ConvertAdjUidList2TextMatrix.gz", pair.Key);
                    //ConvertAdjUidList2BinaryMatrix("ConvertLinks2AdjListByUid.gz", "ConvertAdjUidList2BinaryMatrix.gz", pair.Key);
                    //ConvertAdjUidList2BinaryMatrix("Bwd_ConvertLinks2AdjListByUid.gz", "ConvertAdjUidList2BinaryMatrix.gz", pair.Value);
                    ConvertBwdFwdAdjUidList2BinaryMatrix("ConvertLinks2AdjListByUid.gz", "ConvertBwdFwdAdjUidList2BinaryMatrix.gz");
                    //ConvertBwdFwdAdjUidList2TextMatrix("ConvertLinks2AdjListByUid.gz", "ConvertBwdFwdAdjUidList2TextMatrix.gz");
                }
            }

            //var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
            //Console.WriteLine("Store opened. Number of URL in store = " + store.NumUrls() + ". Number of Links in store = " + store.NumLinks());
            //Console.WriteLine("Inlink max degree: {0}", store.MaxDegree(Dir.Bwd));
            //Console.WriteLine("Outlink max degree: {0}", store.MaxDegree(Dir.Fwd));

            Console.WriteLine("Program finished");
            Console.ReadLine();
        }
    }
}
