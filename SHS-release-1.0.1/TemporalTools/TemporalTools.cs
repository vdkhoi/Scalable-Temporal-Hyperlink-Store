using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using SHS;

namespace SHS
{
    public class TemporalTools
    {

        //public static int NUM_RECORDS = 503101;  //For graph from Jan-2013
        public static int NUM_RECORDS = 1030208;    //For graph from Jan to Mar 2013
        public static int TEMP_BUFF = 10000;

        public static void mappingURLToUUIDText(Store store, string in_filename, string out_filename)
        {

            string[] urls = new string[TEMP_BUFF];
            string[] revisions = new string[TEMP_BUFF];
            var rd = new StreamReader(new GZipStream(new BufferedStream(new FileStream(in_filename, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
            var wr = new StreamWriter(new GZipStream(new FileStream(out_filename, FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress));
            char[] delimiter = { ' ', '\t' };
            string[] currentLine = new string[2];
            int i = 0;
            try
            {
                for (i = 0; i < NUM_RECORDS ; i++)
                {
                    currentLine = rd.ReadLine().Split(delimiter, 2);
                    urls[i % TEMP_BUFF] = currentLine[0];
                    revisions[i % TEMP_BUFF] = currentLine[1];
                    if (((i + 1) % TEMP_BUFF) == 0 || (i + 1) == NUM_RECORDS)
                    {
                        Console.WriteLine("Start writing batch to file");
                        long[] return_set = store.BatchedUrlToUid(urls);
                        for (int k = 0; k < TEMP_BUFF; k++)
                        {
                            wr.WriteLine("{0}\t{1}", return_set[k], revisions[k]);
                        }
                        Console.WriteLine("Finish {0} lines.", i + 1);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
                Console.Error.WriteLine("Stop at line {0}", i);
            }
            wr.Close();
            rd.Close();   
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
            int i = 0;
            try
            {
                for (i = 0; i < NUM_RECORDS; i++)
                {
                    string reading_line = rd.ReadLine();
                    currentLine = reading_line.Split(delimiter);
                    long nodeId = Int64.Parse(currentLine[0]);
                    int number_of_revision = Int32.Parse(currentLine[1]);
                    int outlink_vector_size = Int32.Parse(currentLine[2]);
                    int outlink_vector_matrix_size_in_byte = Int32.Parse(currentLine[3]);
                    wr.Write(nodeId);
                    wr.Write(number_of_revision);
                    wr.Write(outlink_vector_size);
                    int count = 4;
                    int temp = 0;
                    byte[] matrix_row = new byte[outlink_vector_matrix_size_in_byte];
                    //Console.WriteLine("First Time: {0}", currentLine[count]);
                    DateTime d = Convert.ToDateTime(currentLine[count]);
                    wr.Write(currentLine[count++]);  // DateTime value
                    //Console.Write("{0} ",currentLine[count++]); 
                    for (int k = 0; k < outlink_vector_matrix_size_in_byte; k++)
                    {
                        if (currentLine[count] == "-0")
                            currentLine[count] = "128";
                        temp = Int16.Parse(currentLine[count++]);
                        if (temp < 0)
                            matrix_row[k] = (byte)(128 - temp);
                        else
                            matrix_row[k] = (byte)temp;
                    }
                    wr.Write(matrix_row);
                    //temp_write_matrix(matrix_row);

                    //Manually write for first row

                    for (int j = 1; j < number_of_revision; j++)
                    {
                        //Console.WriteLine("Later time {0}",currentLine[count]);
                        DateTime d1 = Convert.ToDateTime(currentLine[count++]);
                        wr.Write( (long)(d1 - d).TotalSeconds );  // DateTime difference in seconds
                        //Console.Write("{0} ", (long)(d - d1).TotalSeconds);
                        matrix_row = new byte[outlink_vector_matrix_size_in_byte];
                        for (int k = 0; k < outlink_vector_matrix_size_in_byte; k++)
                        {
                            if (currentLine[count] == "-0")
                                currentLine[count] = "128";
                            temp = Int16.Parse(currentLine[count++]);
                            if (temp < 0)
                                matrix_row[k] = (byte)(128 - temp);
                            else
                                matrix_row[k] = (byte)temp;

                        }
                        wr.Write(matrix_row);
                        //temp_write_matrix(matrix_row);
                        if (i % 10000 == 0 || i == NUM_RECORDS - 1)
                            Console.WriteLine("Record {0}", i);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
                Console.Error.WriteLine("Stop at line {0}", i);
            }
            wr.Close();
            rd.Close();
        }

        public static void Main(string[] args)
        {
            if (args.Length != 5)
            {
                Console.WriteLine("Usage: SHS.Demo <servers> <store> <command> <in_file> <out_file>");
            }
            else
            {
                if (args[2] == "-t")
                {
                    var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
                    Console.WriteLine("Store opened. Number of URL in store = " + store.NumUrls() + ". Number of Links in store = " + store.NumLinks());
                    var sw = Stopwatch.StartNew();
                    mappingURLToUUIDText(store, args[3], args[4]);
                    Console.WriteLine("Mapping successfully in {0} milliseconds", sw.ElapsedMilliseconds);
                }
                else if (args[2] == "-b")
                {
                    var sw = Stopwatch.StartNew();
                    mappingURLToUUIDBinary4Outlink(args[3], args[4]);
                    Console.WriteLine("Mapping successfully in {0} milliseconds", sw.ElapsedMilliseconds);
                }
            }

            //var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
            //Console.WriteLine("Store opened. Number of URL in store = " + store.NumUrls() + ". Number of Links in store = " + store.NumLinks());
            //Console.WriteLine("Inlink max degree: {0}", store.MaxDegree(Dir.Bwd));
            //Console.WriteLine("Outlink max degree: {0}", store.MaxDegree(Dir.Fwd));
        }
    }
}
