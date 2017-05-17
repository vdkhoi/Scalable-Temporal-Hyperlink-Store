using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.IO.Compression;

namespace SHS
{
    class ConvertLinkGraphToTemporalAdjList
    {
        private static readonly char[] Sep = new char[] { ' ', '\t' };

        public static void Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.Error.WriteLine("Usage: ConvertLinkGraphToTemporalAdjList <LinkGraph.gz> <out.gz> <in.gz>");
            }
            else
            {
                Console.WriteLine("Starting...");
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var d0 = Convert.ToDateTime("1998-01-01");
                DateTime d = d0;
                try
                {
                    using (var rd = new StreamReader(new GZipStream(new FileStream(args[0], FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                    {
                        using (var wr = new StreamWriter(new GZipStream(new FileStream(args[1], FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
                        {
                            using (var sorter = new DiskSorter<AdjDstURL>(new AdjDstURL.Comparer(), AdjDstURL.Write, AdjDstURL.Read, 1 << 25))
                            {

                                string line = "";
                                string[] links = null; ;
                                while ((line = rd.ReadLine()) != null)
                                {
                                    links = line.Split(Sep, StringSplitOptions.RemoveEmptyEntries);
                                    sorter.Add(new AdjDstURL { srcIdx = links[0] + " " + links[2] + " " + links[1] });
                                }
                                sorter.Sort();

                                line = sorter.Get().srcIdx;
                                links = line.Split(Sep, StringSplitOptions.RemoveEmptyEntries);
                                var current = links[0] + " " + links[1];

                                while (!sorter.AtEnd())
                                {

                                    wr.Write(current + " ");
                                    while ((links[0] + " " + links[1]).CompareTo(current) == 0 && !sorter.AtEnd())
                                    {
                                        wr.Write(links[2] + " ");
                                        line = sorter.Get().srcIdx;
                                        links = line.Split(Sep, StringSplitOptions.RemoveEmptyEntries);
                                    }
                                    wr.WriteLine();
                                    current = links[0] + " " + links[1];

                                }


                                Console.WriteLine("Finished forward links ...");


                            }

                        }


                    }


                    using (var rd = new StreamReader(new GZipStream(new FileStream(args[0], FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                    {
                        using (var bwr = new StreamWriter(new GZipStream(new FileStream(args[2], FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
                        {
                            using (var bwd_sorter = new DiskSorter<AdjDstURL>(new AdjDstURL.Comparer(), AdjDstURL.Write, AdjDstURL.Read, 1 << 25))
                            {

                                string line = "";
                                string[] links = null; ;
                                while ((line = rd.ReadLine()) != null)
                                {
                                    links = line.Split(Sep, StringSplitOptions.RemoveEmptyEntries);
                                    bwd_sorter.Add(new AdjDstURL { srcIdx = links[1] + " " + links[2] + " " + links[0] });
                                }


                                bwd_sorter.Sort();

                                line = bwd_sorter.Get().srcIdx;
                                links = line.Split(Sep, StringSplitOptions.RemoveEmptyEntries);
                                var current = links[0] + " " + links[1];

                                while (!bwd_sorter.AtEnd())
                                {

                                    bwr.Write(current + " ");
                                    while ((links[0] + " " + links[1]).CompareTo(current) == 0 && !bwd_sorter.AtEnd())
                                    {
                                        bwr.Write(links[2] + " ");
                                        line = bwd_sorter.Get().srcIdx;
                                        links = line.Split(Sep, StringSplitOptions.RemoveEmptyEntries);
                                    }
                                    bwr.WriteLine();
                                    current = links[0] + " " + links[1];

                                }

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
}