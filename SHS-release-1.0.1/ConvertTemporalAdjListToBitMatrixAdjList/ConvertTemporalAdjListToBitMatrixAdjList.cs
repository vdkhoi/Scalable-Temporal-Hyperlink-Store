using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.IO.Compression;

namespace SHS
{
    class ConvertTemporalAdjListToBitMatrixAdjList
    {
        private static readonly char[] Sep = new char[] { ' ', '\t' };

        public static void Main(string[] args)
        {

            if (args.Length != 2)
            {
                Console.Error.WriteLine("Usage: ConvertTemporalAdjListToBitMatrixAdjList <LinkGraph.gz> <out.bin.gz>");
            }
            else
            {
                char[] delimiter = {' ', '\t'};
                try
                {
                    using (var rd = new StreamReader(new GZipStream(new FileStream(args[0], FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                    {
                        using (var wr = new StreamWriter(new GZipStream(new FileStream(args[1], FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
                        {
                            string line = "", currentSrc = "";
                            SortedSet<string> allURLs = new SortedSet<string>();
                            List<Revision_Links> vector_list = new List<Revision_Links>();

                            while ((line = rd.ReadLine()) != null) {
                                var field = line.Split(delimiter, StringSplitOptions.RemoveEmptyEntries);
                                if (field[0] != currentSrc && currentSrc != "")   // Current URL is last URL
                                {
                                    var allLinks_Vector = new long[allURLs.Count];
                                    string outlink_URLs = "";
                                    for (int i = 0; i < allLinks_Vector.Length; i++)
                                    {
                                        allLinks_Vector[i] = allURLs.Min.GetHashCode();
                                        outlink_URLs += (allURLs.Min + " ");
                                        allURLs.Remove(allURLs.Min);
                                    }
                                    UidMap allLinks_Map = new UidMap(allLinks_Vector);
                                    var pointers = new int[allLinks_Map.GetSize()];

                                    for (int i = 0; i < pointers.Length; i++)
                                    {
                                        pointers[allLinks_Map[allLinks_Vector[i]]] = i;
                                    }

                                    var bit_vector_length = vector_list.Count * allLinks_Vector.Length;

                                    if (bit_vector_length % 8 != 0)
                                    {
                                        bit_vector_length += (8 - bit_vector_length % 8);
                                    }

                                    byte[] bit_matrix = new byte[bit_vector_length];
                                    var vector_list_array = vector_list.ToArray();

                                    long[] time_diff = new long[vector_list.Count];


                                    for (int i = 0; i < vector_list.Count; i++)
                                    {
                                        if (i > 1)
                                            time_diff[i] = (Convert.ToDateTime(vector_list_array[i].time_Stamp) - Convert.ToDateTime(vector_list_array[i - 1].time_Stamp)).Seconds;

                                        for (int j = 0; j < vector_list_array[i].link_Vector.Length; j++)
                                        {
                                            if (allLinks_Map[vector_list_array[i].link_Vector[j]] > -1)
                                            {
                                                var base_index = i * allLinks_Vector.Length;
                                                bit_matrix[base_index + pointers[allLinks_Map[vector_list_array[i].link_Vector[j]]]] = 1;
                                            }
                                        }

                                    }

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

                                    wr.Write("{0} ", currentSrc);                       // Source URL
                                    wr.Write("{0} ", allLinks_Vector.Length);           // Size of link vector
                                    wr.Write("{0} ", vector_list.Count);                // Number of revisions
                                    wr.Write("{0} ", vector_list_array[0].time_Stamp);  // First time stamp


                                    for (int i = 0; i < time_diff.Length; i++)
                                    {
                                        wr.Write("{0} ", time_diff[i]);                 // Time difference between time stamp
                                    }

                                    for (int i = 0; i < results.Length; i++)
                                    {
                                        wr.Write("{0} ", results[i]);                   // List of time revision bit matrix 
                                    }

                                    wr.WriteLine("{0}", outlink_URLs);                  // List of out URLs

                                    //string[] urls = outlink_URLs.Split(delimiter, StringSplitOptions.RemoveEmptyEntries);
                                    //for (int i = 0; i < urls.Length; i++)
                                    //{
                                    //    wr.Write(urls[i]);
                                    //}

                                    //wr.WriteLine("{0}", urls[urls.Length - 1]);

                                    // Clear old data for new links
                                    allURLs.Clear();
                                    vector_list.Clear();

                                }
                                

                                var revision_Vector = new long[field.Length - 2];
                                if (field.Length > 2)
                                {
                                    for (int i = 2; i < field.Length; i++)
                                    {
                                        allURLs.Add(field[i]);
                                        revision_Vector[i - 2] = field[i].GetHashCode();
                                    }
                                }
                                vector_list.Add(new Revision_Links { time_Stamp = field[1], link_Vector = revision_Vector });
                                currentSrc = field[0];
                              

                            }
                        }
                    }
                }
                catch (Exception)
                {

                }
                finally
                {

                }
            }
        }

        internal struct Revision_Links
        {
            internal string time_Stamp;
            internal long[] link_Vector;
        }
    }
 }