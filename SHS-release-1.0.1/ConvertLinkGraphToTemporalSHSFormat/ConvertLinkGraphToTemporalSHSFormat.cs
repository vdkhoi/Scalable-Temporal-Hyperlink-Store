using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;

namespace SHS
{
    public struct URLData
    {
        internal string url;

        internal static URLData Read(BinaryReader rd)
        {
            var cUrl = rd.ReadString();

            return new URLData { url = cUrl };
        }

        internal static void Write(BinaryWriter wr, URLData cUrl)
        {
            wr.Write(cUrl.url);
        }

        internal class Comparer : System.Collections.Generic.Comparer<URLData>
        {
            public override int Compare(URLData a, URLData b)
            {
                return a.url.CompareTo(b.url);
            }
        }
    }


    class ConvertLinkGraphToTemporalSHSFormat
    {
        static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.Error.WriteLine("Usage: ConvertLinkGraphToTemporalSHSFormat <LinkGraph.gz>");
            }
            else
            {
                try
                {
                    using (var rd = new StreamReader(new GZipStream(new FileStream(args[0], FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                    using (var wr = new StreamWriter(new GZipStream(new FileStream("url_" + args[0], FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
                    using (var sorter = new DiskSorter<URLData>(new URLData.Comparer(), URLData.Write, URLData.Read, 1 << 25))
                    {
                        char[] delimiter = {' ', '\t' };
                        string line ;
                        while ((line = rd.ReadLine()) != null)
                        {
                            string[] links = line.Split(delimiter, StringSplitOptions.RemoveEmptyEntries);
                            sorter.Add(new URLData { url = links[0] } );
                            sorter.Add(new URLData { url = links[1] } );
                        }
                        sorter.Sort();
                        string lastURL = "";
                        while (!sorter.AtEnd())
                        {
                            string currenURL = sorter.Get().url;
                            if (currenURL.CompareTo(lastURL) != 0)
                            {
                                wr.WriteLine(currenURL);
                                lastURL = currenURL;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine("Exception {0}", e.Message);
                }
                finally
                {
                    Console.WriteLine("Over !!!");
                }
            }
        }
    }
}
