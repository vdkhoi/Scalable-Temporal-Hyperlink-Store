using System;
using System.Linq;
using System.Text;
using SHS;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.IO.Compression;
using System.Diagnostics;

namespace SHS
{
    public class MyUrlUtils
    {
        public static string InvertHost(string url)
        {
            int a = PrefixLength(url);
            int b = HostLength(url, a);
            return url.Substring(0, a)
              + HostUtils.InvertHost(url.Substring(a, b))
              + url.Substring(a + b);
        }

        public static string HostOf(string url)
        {
            int a = PrefixLength(url);
            int b = HostLength(url, a);
            return url.Substring(a, b);
        }

        public static string DomainOf(string url)
        {
            return HostUtils.DomainOf(UrlUtils.HostOf(url));
        }

        private static int PrefixLength(string url)
        {
            if (MyExtensionMethods.SafeStartsWith(url, "http://"))
            {
                return 7;
            }
            else if (MyExtensionMethods.SafeStartsWith(url, "https://"))
            {
                return 8;
            }
            else
            {
                throw new Exception("URL " + url + " does not start with http:// or https://");
            }
        }

        private static int HostLength(string url, int prefixLen)
        {
            int i = prefixLen;
            while (i < url.Length && url[i] != ':' && url[i] != '/') i++;
            return i - prefixLen;
        }

        public static string getNormalURLFormat(string decodeURL)
        {
            char[] delimiter = { ' ', '\t', ',' };
            string first = "", later = "", revfirst = "";
            int split_pos = decodeURL.IndexOf(')');
            if (split_pos >= 4)
            {
                first = decodeURL.Substring(0, split_pos);
                string[] dn = first.Split(delimiter, StringSplitOptions.RemoveEmptyEntries);
                int len = dn.Length;
                revfirst = dn[0];
                for (int i = 1; i < len; i++)
                {
                    revfirst = dn[i] + "." + revfirst;
                }
                later = decodeURL.Substring(split_pos + 1);
                return ("http://" + revfirst + later);
            }
            return null;
        }
    }

    public static class MyExtensionMethods
    {
        public static bool SafeStartsWith(this string full, string pref)
        {
            if (full.Length < pref.Length) return false;
            for (int i = 0; i < pref.Length; i++)
            {
                if (full[i] != pref[i]) return false;
            }
            return true;
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

    public class GraphTools
    {
        public static KeyValuePair<long, long> ConvertLinks2AdjListByUid(Store store, string args0, string args1)
        {
            char[] Sep = { ' ', '\t' };
            var d0 = Convert.ToDateTime("1998-01-01");
            DateTime d = d0;
            long num_links = 0;
            long fwd_num_list = 0;
            long bwd_num_list = 0;

            string line = "";
            int NUM_CACHE = 100000;
            string[] links = new string[3];
            string[] cache = new string[2 * NUM_CACHE];
            long[] temporal = new long[NUM_CACHE];

            int current_count = 0;
            long[] out_link;

            var bwd_sorter = new DiskSorter<TemporalNodeIdLinks>(new TemporalNodeIdLinks.Comparer(), TemporalNodeIdLinks.Write, TemporalNodeIdLinks.Read, 1 << 25);
            try
            {
                using (var rd = new StreamReader(new GZipStream(new FileStream(args0, FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                using (var wr = new StreamWriter(new GZipStream(new FileStream(args1, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
                using (var sorter = new DiskSorter<TemporalNodeIdLinks>(new TemporalNodeIdLinks.Comparer(), TemporalNodeIdLinks.Write, TemporalNodeIdLinks.Read, 1 << 25))
                {

                    while ((line = rd.ReadLine()) != null)
                    {
                        links = line.Split(Sep, StringSplitOptions.RemoveEmptyEntries);
                        temporal[current_count] = (Convert.ToDateTime(links[2] + "-28") - d0).Days;
                        cache[2 * current_count] = links[0];
                        cache[2 * current_count + 1] = links[1];



                        if (current_count == (NUM_CACHE - 1))
                        {
                            out_link = store.BatchedUrlToUid(cache);
                            for (int i = 0; i < NUM_CACHE; i++)
                            {
                                if (out_link[2 * i] != -1 && out_link[2 * i + 1] != -1)
                                {
                                    sorter.Add(new TemporalNodeIdLinks { source_node = out_link[2 * i], time = temporal[i], dest_node = out_link[2 * i + 1] });
                                    bwd_sorter.Add(new TemporalNodeIdLinks { source_node = out_link[2 * i + 1], time = temporal[i], dest_node = out_link[2 * i] });
                                }
                            }
                            current_count = -1;
                            Console.WriteLine("Next 1mil links finished...");
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
                            if (out_link[2 * i] != -1 && out_link[2 * i + 1] != -1)
                            {
                                sorter.Add(new TemporalNodeIdLinks { source_node = out_link[2 * i], time = temporal[i], dest_node = out_link[2 * i + 1] });
                                bwd_sorter.Add(new TemporalNodeIdLinks { source_node = out_link[2 * i + 1], time = temporal[i], dest_node = out_link[2 * i] });
                            }
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
                Console.Error.WriteLine(e.Source);
                Console.Error.WriteLine(e.StackTrace);
                Console.Error.WriteLine(line);
            }
            finally
            {

            }

            return new KeyValuePair<long, long>(fwd_num_list, bwd_num_list);

        }

        public static SortedDictionary<string, long> buildMapping(Store store, string load_file)
        {
            SortedSet<string> url_set = new SortedSet<string>();
            SortedDictionary<string, long> mapped = new SortedDictionary<string, long>();
            char[] Sep = { ' ', '\t' };
            string line;

            int NUM_CACHE = 10000000;

            try
            {
                using (var rd = new StreamReader(new GZipStream(new FileStream(load_file, FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                {
                    while ((line = rd.ReadLine()) != null)
                    {
                        string[] links = line.Split(Sep, StringSplitOptions.RemoveEmptyEntries);
                        url_set.Add(links[0]);
                        url_set.Add(links[1]);

                        if (url_set.Count == NUM_CACHE)
                        {
                            string[] unique_urls = new string[NUM_CACHE];
                            url_set.CopyTo(unique_urls);
                            var uids = store.BatchedUrlToUid(unique_urls);
                            for (int i = 0; i < unique_urls.Length; i++)
                            {
                                try
                                {
                                    mapped.Add(unique_urls[i], uids[i]);
                                }
                                catch(Exception)
                                {

                                }
                            }
                            url_set = new SortedSet<string>();
                            Console.WriteLine("Mapped 10M links...");
                        }
                    }
                    if (url_set.Count > 0)
                    {
                        string[] unique_urls = new string[url_set.Count];
                        url_set.CopyTo(unique_urls);
                        var uids = store.BatchedUrlToUid(unique_urls);
                        for (int i = 0; i < unique_urls.Length; i++)
                        {
                            try
                            {
                                mapped.Add(unique_urls[i], uids[i]);
                            }
                            catch (Exception)
                            {

                            }
                        }
                        Console.WriteLine("Mapped {0} links...", url_set.Count);
                    }
                }
                
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message);
                Console.Error.WriteLine(e.Source);
                Console.Error.WriteLine(e.StackTrace);
            }
            finally
            {

            }
            return mapped;
        }

        public static KeyValuePair<long, long> ConvertTempLinks2AdjListByUid(Store store, string args0, string args1)
        {
            char[] Sep = { ' ', '\t' };
            var d0 = Convert.ToDateTime("1998-01-01");
            DateTime d = d0;
            long num_links = 0;
            long fwd_num_list = 0;
            long bwd_num_list = 0;

            string line = "";
            int NUM_CACHE = 10000000;
            string[] links = new string[3];
            string[] cache = new string[2 * NUM_CACHE];
            long[][] temporal = new long[NUM_CACHE][];

            int current_count = 0;


            SortedSet<string> url_set = new SortedSet<string>();
            SortedDictionary<string, long> mapped = new SortedDictionary<string, long>();

            var bwd_sorter = new DiskSorter<TemporalNodeIdLinks>(new TemporalNodeIdLinks.Comparer(), TemporalNodeIdLinks.Write, TemporalNodeIdLinks.Read, 1 << 25);
            try
            {
                using (var rd = new StreamReader(new GZipStream(new FileStream(args0, FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                using (var wr = new StreamWriter(new GZipStream(new FileStream(args1, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
                using (var sorter = new DiskSorter<TemporalNodeIdLinks>(new TemporalNodeIdLinks.Comparer(), TemporalNodeIdLinks.Write, TemporalNodeIdLinks.Read, 1 << 25))
                {

                    while ((line = rd.ReadLine()) != null)
                    {
                        links = line.Split(Sep, StringSplitOptions.RemoveEmptyEntries);
                        temporal[current_count] = new long[line.Length - 3];
                        temporal[current_count][0] = (Convert.ToDateTime(links[2]) - d0).Days;
                        
                        for (int t = 3; t < links.Length - 1; t ++ )
                        {
                            temporal[current_count][t - 2] = (Convert.ToDateTime(links[t]) - d0).Days;
                        }

                        cache[2 * current_count] = links[0];
                        cache[2 * current_count + 1] = links[1];

                        url_set.Add(links[0]);
                        url_set.Add(links[1]);

                        if (current_count == (NUM_CACHE - 1))
                        {

                            string[] unique_urls = new string[url_set.Count];
                            url_set.CopyTo(unique_urls);
                            var uids = store.BatchedUrlToUid(unique_urls);
                            for (int i = 0; i < unique_urls.Length; i++)
                            {
                                try
                                {
                                    mapped.Add(unique_urls[i], uids[i]);
                                }
                                catch (Exception)
                                {
                                    Console.WriteLine("Error mapping...");
                                }
                            }
                            
                            for (int i = 0; i < NUM_CACHE; i++)
                            {
                                long mSource, mDest;
                                mapped.TryGetValue(cache[2 * i], out mSource);
                                mapped.TryGetValue(cache[2 * i + 1], out mDest);
                                if (mSource != -1 && mDest != -1)
                                {
                                    for (int t = 0; t < temporal[i].Length; t++)
                                    {
                                        sorter.Add(new TemporalNodeIdLinks { source_node = mSource, time = temporal[i][t], dest_node = mDest });
                                        bwd_sorter.Add(new TemporalNodeIdLinks { source_node = mDest, time = temporal[i][t], dest_node = mSource });
                                    }
                                }
                            }
                            current_count = -1;
                            url_set = new SortedSet<string>();
                            mapped = new SortedDictionary<string, long>();
                            Console.WriteLine("Next {0} links finished...", NUM_CACHE);
                        }
                        current_count++;
                    }

                    if (current_count > 0)
                    {
                        current_count--;

                        string[] unique_urls = new string[url_set.Count];
                        url_set.CopyTo(unique_urls);
                        var uids = store.BatchedUrlToUid(unique_urls);
                        for (int i = 0; i < unique_urls.Length; i++)
                        {
                            try
                            {
                                mapped.Add(unique_urls[i], uids[i]);
                            }
                            catch (Exception)
                            {
                                Console.WriteLine("Error mapping...");
                            }
                        }

                        for (int i = 0; i < current_count; i++)
                        {
                            long mSource, mDest;
                            mapped.TryGetValue(cache[2 * i], out mSource);
                            mapped.TryGetValue(cache[2 * i + 1], out mDest);
                            if (mSource != -1 && mDest != -1)
                            {
                                for (int t = 0; t < temporal[i].Length; t++)
                                {
                                    sorter.Add(new TemporalNodeIdLinks { source_node = mSource, time = temporal[i][t], dest_node = mDest });
                                    bwd_sorter.Add(new TemporalNodeIdLinks { source_node = mDest, time = temporal[i][t], dest_node = mSource });
                                }
                            }
                        }
                        
                        Console.WriteLine("Next {0} links finished...", current_count);
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
                Console.Error.WriteLine(e.Source);
                Console.Error.WriteLine(e.StackTrace);
                Console.Error.WriteLine(line);
            }
            finally
            {

            }

            return new KeyValuePair<long, long>(fwd_num_list, bwd_num_list);

        }



        public static KeyValuePair<long, long> AttempConvertTempLinks2AdjListByUid(Store store, string args0, string args1)
        {
            char[] Sep = { ' ', '\t' };
            var d0 = Convert.ToDateTime("1998-01-01");
            DateTime d = d0;
            long num_links = 0;
            long fwd_num_list = 0;
            long bwd_num_list = 0;

            string line = "";
            string[] links = new string[3];
            long temporal;

            SortedDictionary<string, long> mapped = buildMapping(store, args0);

            var bwd_sorter = new DiskSorter<TemporalNodeIdLinks>(new TemporalNodeIdLinks.Comparer(), TemporalNodeIdLinks.Write, TemporalNodeIdLinks.Read, 1 << 25);
            try
            {
                using (var rd = new StreamReader(new GZipStream(new FileStream(args0, FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                using (var wr = new StreamWriter(new GZipStream(new FileStream(args1, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
                using (var sorter = new DiskSorter<TemporalNodeIdLinks>(new TemporalNodeIdLinks.Comparer(), TemporalNodeIdLinks.Write, TemporalNodeIdLinks.Read, 1 << 25))
                {

                    while ((line = rd.ReadLine()) != null)
                    {
                        links = line.Split(Sep, StringSplitOptions.RemoveEmptyEntries);

                        for (int t = 3; t < links.Length - 1; t++)
                        {
                            temporal = (Convert.ToDateTime(links[t]) - d0).Days;
                            long src;
                            mapped.TryGetValue(links[0], out src);
                            long dest;
                            mapped.TryGetValue(links[0], out dest);
                            sorter.Add(new TemporalNodeIdLinks { source_node = src, time = temporal, dest_node = dest });
                            bwd_sorter.Add(new TemporalNodeIdLinks { source_node = dest, time = temporal, dest_node = src });
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
                Console.Error.WriteLine(e.Source);
                Console.Error.WriteLine(e.StackTrace);
                Console.Error.WriteLine(line);
            }
            finally
            {

            }

            return new KeyValuePair<long, long>(fwd_num_list, bwd_num_list);

        }


        public static KeyValuePair<long, long> ConvertSURT2AdjListByUid(Store store, string args0, string args1)
        {
            char[] Sep = { ' ', '\t' };
            var d0 = Convert.ToDateTime("1998-01-01");
            DateTime d = d0;
            long num_links = 0;
            long fwd_num_list = 0;
            long bwd_num_list = 0;

            string line = "";
            int NUM_CACHE = 100000;
            string[] links = new string[3];
            string[] cache = new string[2 * NUM_CACHE];
            long[] temporal = new long[NUM_CACHE];

            int current_count = 0;
            long[] out_link;

            var bwd_sorter = new DiskSorter<TemporalNodeIdLinks>(new TemporalNodeIdLinks.Comparer(), TemporalNodeIdLinks.Write, TemporalNodeIdLinks.Read, 1 << 25);
            try
            {
                using (var rd = new StreamReader(new GZipStream(new FileStream(args0, FileMode.Open, FileAccess.Read), CompressionMode.Decompress)))
                using (var wr = new StreamWriter(new GZipStream(new FileStream(args1, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress)))
                using (var sorter = new DiskSorter<TemporalNodeIdLinks>(new TemporalNodeIdLinks.Comparer(), TemporalNodeIdLinks.Write, TemporalNodeIdLinks.Read, 1 << 25))
                {

                    while ((line = rd.ReadLine()) != null)
                    {
                        links = line.Split(Sep, StringSplitOptions.RemoveEmptyEntries);

                        temporal[current_count] = (Convert.ToDateTime(links[2] + "-28") - d0).Days;
                        cache[2 * current_count] = getNormalURLFormat(links[0]);
                        cache[2 * current_count + 1] = getNormalURLFormat(links[1]);

                        //if ((String.IsNullOrWhiteSpace(cache[2 * current_count])) || (String.IsNullOrWhiteSpace(cache[2 * current_count + 1])))
                        //{
                        //    Console.WriteLine("ConvertSURT2AdjListByUid Error {0}", cache[2 * current_count]);
                        //    Console.WriteLine("ConvertSURT2AdjListByUid Error {0}", cache[2 * current_count + 1]);
                        //}

                        if (current_count == (NUM_CACHE - 1))
                        {

                            out_link = store.BatchedUrlToUid(cache);
                            for (int i = 0; i < NUM_CACHE; i++)
                            {
                                if (out_link[2 * i] != -1 && out_link[2 * i + 1] != -1)
                                {
                                    sorter.Add(new TemporalNodeIdLinks { source_node = out_link[2 * i], time = temporal[i], dest_node = out_link[2 * i + 1] });
                                    bwd_sorter.Add(new TemporalNodeIdLinks { source_node = out_link[2 * i + 1], time = temporal[i], dest_node = out_link[2 * i] });
                                }
                            }
                            current_count = -1;
                            Console.WriteLine("Next 1mil links finished...");
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
                            if (out_link[2 * i] != -1 && out_link[2 * i + 1] != -1)
                            {
                                sorter.Add(new TemporalNodeIdLinks { source_node = out_link[2 * i], time = temporal[i], dest_node = out_link[2 * i + 1] });
                                bwd_sorter.Add(new TemporalNodeIdLinks { source_node = out_link[2 * i + 1], time = temporal[i], dest_node = out_link[2 * i] });
                            }
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
                Console.Error.WriteLine(e.Source);
                Console.Error.WriteLine(e.StackTrace);
                Console.Error.WriteLine(line);
            }
            finally
            {

            }

            return new KeyValuePair<long, long>(fwd_num_list, bwd_num_list);

        }

        public static string getNormalURLFormat(string decodeURL)
        {
            char[] delimiter = { ' ', '\t', ',' };
            string first = "", later = "", revfirst = "";
            int split_pos = decodeURL.IndexOf(')');
            if (split_pos >= 4)
            {
                first = decodeURL.Substring(0, split_pos);
                string[] dn = first.Split(delimiter, StringSplitOptions.RemoveEmptyEntries);
                int len = dn.Length;
                revfirst = dn[0];
                for (int i = 1; i < len; i++)
                {
                    revfirst = dn[i] + "." + revfirst;
                }
                later = decodeURL.Substring(split_pos + 1);
                return ("http://" + revfirst + later);
            }
            return null;
        }
        public static void Main(string[] args)
        {
            //Console.WriteLine(getNormalURLFormat("de,main-netz)/nachrichten/politik/politik-kurz/kurzberichtet01/archiv4712,20120728,0,10"));
            //Console.ReadLine();



            //Arguments:
            // server store require_convert num_files

            if (args[2].CompareTo("SURT") == 0)
            {
                var sw = Stopwatch.StartNew();
                var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
                int numFile = Convert.ToInt32(args[3]);
                for (int i = 0; i < numFile; i++)
                {
                    string file_name = "part-r-" + i.ToString("d5") + ".gz";
                    KeyValuePair<long, long> pair = ConvertSURT2AdjListByUid(store, file_name, args[4]);
                }
            }
            else if (args[2].CompareTo("URI") == 0)
            {
                var sw = Stopwatch.StartNew();
                var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
                int numFile = Convert.ToInt32(args[3]);
                for (int i = 0; i < numFile; i++)
                {
                    string file_name = "part-r-" + i.ToString("d5") + ".gz";
                    KeyValuePair<long, long> pair = ConvertLinks2AdjListByUid(store, file_name, args[4]);
                }
            }
            else if (args[2].CompareTo("TEMP_LINKS") == 0)
            {
                var sw = Stopwatch.StartNew();
                var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
                int numFile = Convert.ToInt32(args[3]);
                string file_name = args[4];
                KeyValuePair<long, long> pair = ConvertTempLinks2AdjListByUid(store, file_name, "_" + file_name);
            }
            else if (args[2].CompareTo("DEBUG") == 0)
            {
                var sw = Stopwatch.StartNew();
                var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
                int numFile = Convert.ToInt32(args[3]);
                //for (int i = 0; i < numFile; i++)
                //{
                string file_name = args[4];
                KeyValuePair<long, long> pair = ConvertLinks2AdjListByUid(store, file_name, "_" + file_name);
                //}
            }
        }
    }
}
