using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;

namespace SHS
{
    internal class PageLinksBinaryFileEnumerator : IEnumerator<PageLinks>
    {
        private readonly string fileName;
        private BinaryReader rd;
        private PageLinks current;

        internal PageLinksBinaryFileEnumerator(string fileName)
        {
            this.fileName = fileName;
            this.rd = new BinaryReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
        }

        public PageLinks Current { get { return this.current; } }

        object IEnumerator.Current { get { return this.current; } }

        public bool MoveNext()
        {
            try
            {
                this.current.pageUrl = rd.ReadString();
                this.current.linkUrls = new string[rd.ReadInt32()];
                for (int i = 0; i < this.current.linkUrls.Length; i++)
                {
                    this.current.linkUrls[i] = rd.ReadString();
                }
                return true;
            }
            catch (EndOfStreamException)
            {
                return false;
            }
        }

        public void Reset()
        {
            this.rd.Close();
            this.rd = new BinaryReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
        }

        public void Dispose()
        {
            this.rd.Close();
        }
    }

    internal class PageLinksTextFileEnumerator : IEnumerator<PageLinks>
    {
        private readonly string fileName;
        private static char[] delimiter = { ' ', '\t' };
        private TextReader rd;
        private PageLinks current;


        internal PageLinksTextFileEnumerator(string fileName)
        {
            this.fileName = fileName;
            this.rd = new StreamReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
        }

        public PageLinks Current { get { return this.current; } }

        object IEnumerator.Current { get { return this.current; } }

        public bool MoveNext()
        {
            try
            {
                string[] line = rd.ReadLine().Split(delimiter);
                this.current.pageUrl = line[0];
                this.current.linkUrls = new string[line.Length - 2];
                for (int i = 0; i < line.Length - 2; i++)
                {
                    this.current.linkUrls[i] = line[i + 2];
                }
                return true;
            }
            catch (EndOfStreamException)
            {
                return false;
            }
            catch (NullReferenceException ex)
            {
                // Get stack trace for the exception with source file information
                var st = new StackTrace(ex, true);
                // Get the top stack frame
                var frame = st.GetFrame(0);
                // Get the line number from the stack frame
                var line = frame.GetFileLineNumber();

                Console.Error.WriteLine("Exception at: " + line);
                return false;
            }
        }

        public void Reset()
        {
            this.rd.Close();
            this.rd = new StreamReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
        }

        public void Dispose()
        {
            this.rd.Close();
        }
    }


    internal class PageIndividualLinksTextFileEnumerator : IEnumerator<PairLinks>
    {
        private readonly string fileName;
        private static char[] tab_delimiter = { '\t' };
        private TextReader rd;
        private PairLinks current;

        internal PageIndividualLinksTextFileEnumerator(string fileName)
        {
            this.fileName = fileName;
            this.rd = new StreamReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
        }

        public PairLinks Current { get { return this.current; } }

        object IEnumerator.Current { get { return this.current; } }

        public bool MoveNext()
        {
            try
            {
                string[] line = rd.ReadLine().Split(tab_delimiter);

                //Console.WriteLine("Loading... {0} {1}", line[0], line[3]);

                this.current.pageSrcUrl = line[0];
                this.current.pageDstUrl = line[1];
                this.current.outDegree = Convert.ToInt32(line[3]);
                return true;
            }
            catch (EndOfStreamException)
            {
                return false;
            }
            catch (NullReferenceException ex)
            {
                // Get stack trace for the exception with source file information
                var st = new StackTrace(ex, true);
                // Get the top stack frame
                var frame = st.GetFrame(0);
                // Get the line number from the stack frame
                var line = frame.GetFileLineNumber();

                Console.Error.WriteLine("Exception at: " + line);
                return false;
            }
        }
        public void Reset()
        {
            this.rd.Close();
            this.rd = new StreamReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
        }

        public void Dispose()
        {
            this.rd.Close();
        }
    }

    internal class PageIndividualTempLinksTextFileEnumerator : IEnumerator<TempPairLinks>
    {
        private readonly string fileName;
        private static char[] tab_delimiter = { '\t', ' ' };
        private TextReader rd;
        private TempPairLinks current;

        internal PageIndividualTempLinksTextFileEnumerator(string fileName)
        {
            this.fileName = fileName;
            this.rd = new StreamReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
        }

        public TempPairLinks Current { get { return this.current; } }

        object IEnumerator.Current { get { return this.current; } }

        public bool MoveNext()
        {
            try
            {
                string[] line = rd.ReadLine().Split(tab_delimiter);

                //Console.WriteLine("Loading... {0} {1}", line[0], line[3]);

                this.current.pageSrcUrl = line[0];
                this.current.pageDstUrl = line[1];
                this.current.timeStamp = line[2];
                return true;
            }
            catch (EndOfStreamException)
            {
                current.pageSrcUrl = null;
                return false;
            }
            catch (NullReferenceException ex)
            {
                // Get stack trace for the exception with source file information
                var st = new StackTrace(ex, true);
                // Get the top stack frame
                var frame = st.GetFrame(0);
                // Get the line number from the stack frame
                var line = frame.GetFileLineNumber();

                Console.Error.WriteLine("Exception at: " + line);
                current.pageSrcUrl = null;
                return false;
            }
        }
        public void Reset()
        {
            this.rd.Close();
            this.rd = new StreamReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
        }

        public void Dispose()
        {
            this.rd.Close();
        }
    }

    internal class PageIndividualLinksSURTTextFileEnumerator : IEnumerator<PairLinks>
    {
        private readonly string fileName;
        private static char[] tab_delimiter = { '\t' };
        private TextReader rd;
        private PairLinks current;

        internal PageIndividualLinksSURTTextFileEnumerator(string fileName)
        {
            this.fileName = fileName;
            this.rd = new StreamReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
        }

        public PairLinks Current { get { return this.current; } }

        object IEnumerator.Current { get { return this.current; } }

        public bool MoveNext()
        {
            try
            {
                string[] line = rd.ReadLine().Split(tab_delimiter);

                //Console.WriteLine("Loading... {0} {1}", line[0], line[3]);

                this.current.pageSrcUrl = UrlUtils.getNormalURLFormat(line[0]);
                this.current.pageDstUrl = UrlUtils.getNormalURLFormat(line[1]);
                this.current.outDegree = Convert.ToInt32(line[3]);
                return true;
            }
            catch (EndOfStreamException)
            {
                return false;
            }
            catch (NullReferenceException ex)
            {
                // Get stack trace for the exception with source file information
                var st = new StackTrace(ex, true);
                // Get the top stack frame
                var frame = st.GetFrame(0);
                // Get the line number from the stack frame
                var line = frame.GetFileLineNumber();

                Console.Error.WriteLine("Exception at: " + line);
                return false;
            }
        }

        public void Reset()
        {
            this.rd.Close();
            this.rd = new StreamReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
        }

        public void Dispose()
        {
            this.rd.Close();
        }
    }

    internal class RevisionPageLinksTextFileEnumerator : IEnumerator<PageLinks>
    {
        private readonly string fileName;
        private static char[] tab_delimiter = { '\t' }, space_delimiter = { ' ' };
        private TextReader rd;
        private PageLinks current;

        internal RevisionPageLinksTextFileEnumerator(string fileName)
        {
            this.fileName = fileName;
            this.rd = new StreamReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
        }

        public PageLinks Current { get { return this.current; } }

        object IEnumerator.Current { get { return this.current; } }

        public bool MoveNext()
        {
            try
            {
                string[] line = rd.ReadLine().Split(tab_delimiter);
                this.current.pageUrl = line[0];
                string[] outlinks = line[2].Split(space_delimiter);
                this.current.linkUrls = new string[outlinks.Length];
                for (int i = 0; i < outlinks.Length; i++)
                {
                    this.current.linkUrls[i] = outlinks[i];
                }
                return true;
            }
            catch (EndOfStreamException)
            {
                return false;
            }
            catch (NullReferenceException ex)
            {
                // Get stack trace for the exception with source file information
                var st = new StackTrace(ex, true);
                // Get the top stack frame
                var frame = st.GetFrame(0);
                // Get the line number from the stack frame
                var line = frame.GetFileLineNumber();

                Console.Error.WriteLine("Exception at: " + line);
                return false;
            }
        }

        public void Reset()
        {
            this.rd.Close();
            this.rd = new StreamReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
        }

        public void Dispose()
        {
            this.rd.Close();
        }
    }


    internal class URLEnumerator : IEnumerator<PageURL>
    {
        private readonly string fileName;
        private static char[] tab_delimiter = { '\t' }, space_delimiter = { ' ' };
        private TextReader rd;
        private PageURL current;

        internal URLEnumerator(string fileName)
        {
            this.fileName = fileName;
            this.rd = new StreamReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
        }

        public PageURL Current { get { return this.current; } }

        object IEnumerator.Current { get { return this.current; } }

        public bool MoveNext()
        {
            try
            {
                string line = rd.ReadLine();
                if (line == null) throw new NullReferenceException();
                this.current.pageUrl = line;
                return true;
            }
            catch (EndOfStreamException)
            {
                this.current.pageUrl = null;
                return false;
            }
            catch (NullReferenceException ex)
            {
                // Get stack trace for the exception with source file information
                var st = new StackTrace(ex, true);
                // Get the top stack frame
                var frame = st.GetFrame(0);
                // Get the line number from the stack frame
                var line = frame.GetFileLineNumber();

                Console.Error.WriteLine("Exception at: " + line);
                this.current.pageUrl = null;
                return false;
            }
        }

        public void Reset()
        {
            this.rd.Close();
            this.rd = new StreamReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
        }

        public void Dispose()
        {
            this.rd.Close();
        }
    }

    class Builder
    {
        public static void Main(string[] args)
        {
            if (args.Length != 5)
            {
                Console.Error.WriteLine("Usage: SHS.Builder <leader> <file-type: -b|-t|-r> <linkfile.bin.gz> <friendly-name>");
                Environment.Exit(1);
            }

            Console.Write("Waiting...");
            Console.ReadLine();
            var sw = Stopwatch.StartNew();
            var service = new Service(args[0]);
            var numServers = service.NumServers();
            Console.WriteLine("SHS service is currently running on {0} servers", numServers);
            //var store = service.CreateStore(numServers-1, 2, args[2]);
            var store = service.CreateStore(numServers - 1, 2, args[3], 5);
            Console.WriteLine("Created store...");
            if (args[1] == "-b")  // Adjacent list in binary
            {
                var plBinEnum = new PageLinksBinaryFileEnumerator(args[2]);
                store.AddPageLinks(plBinEnum);
                store.Seal();
            }
            else if (args[1] == "-t")  // Adjacent list in text
            {
                var plTextEnum = new PageLinksTextFileEnumerator(args[2]);
                store.AddPageLinks(plTextEnum);
                store.Seal();
            }
            else if (args[1] == "-r")  // Adjacent list with timestamp
            {
                var plTextEnum = new RevisionPageLinksTextFileEnumerator(args[2]);
                store.AddPageLinks(plTextEnum);
                store.Seal();
            }
            else if (args[1] == "-p")  // Pair links with outdegree URL format
            {
                Console.WriteLine("Starting...");
                var plTextEnum = new PageIndividualLinksTextFileEnumerator(args[2]);
                store.AddIndividualLinks(plTextEnum);
                store.Seal();
            }
            else if (args[1] == "-s")  // Sample pair links in SURT form with outdegree
            {
                Console.WriteLine("Starting...");
                var plTextEnum = new PageIndividualLinksSURTTextFileEnumerator(args[2]);
                store.AddIndividualLinks(plTextEnum);
                store.Seal();
            }
            else if (args[1] == "-pt")  // Pair links with time and outdegree in URL format
            {
                Console.WriteLine("Starting...");
                var plTextEnum = new PageIndividualTempLinksTextFileEnumerator(args[2]);
                var plURLEnum = new URLEnumerator(args[3]);
                store.AddIndividualTempLinks(plTextEnum, plURLEnum);
                store.Seal();
            }



            Console.Error.WriteLine("Done. Building store {0:N} took {1} seconds.", store.ID, 0.001 * sw.ElapsedMilliseconds);
        }
    }
}
