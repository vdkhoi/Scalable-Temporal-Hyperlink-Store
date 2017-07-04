using Exception = System.Exception;
using System;
using System.Collections;
using System.Collections.Generic;

namespace SHS
{
    public class TempUtils
    {
        public static DateTime START_DATE = Convert.ToDateTime("1998-01-01");

        public static DateTime END_DATE = Convert.ToDateTime("2014-12-31");

        public static UInt32[] VALUE_AT_BIT = {     2147483648,
                                                    1073741824,
                                                    536870912,
                                                    268435456,
                                                    134217728,
                                                    67108864,
                                                    33554432,
                                                    16777216,
                                                    8388608,
                                                    4194304,
                                                    2097152,
                                                    1048576,
                                                    524288,
                                                    262144,
                                                    131072,
                                                    65536,
                                                    32768,
                                                    16384,
                                                    8192,
                                                    4096,
                                                    2048,
                                                    1024,
                                                    512,
                                                    256,
                                                    128,
                                                    64,
                                                    32,
                                                    16,
                                                    8,
                                                    4,
                                                    2,
                                                    1
                                              };

        public static byte RADIX = 32;

        public static long GetDistanceToStartingPoint(string date)
        {
            return (Convert.ToDateTime(date) - START_DATE).Days;
        }
        
        public static UInt32[] ConvertTimeSeriesToByteArray(List<List<long>> linkRevsUids, List<long> linkTotalUids)
        {

            uint length = TempUtils.getBitMatrixLength((uint)linkRevsUids.Count, (uint)linkTotalUids.Count);

            UInt32[] bit_matrix = new UInt32[length];

            for (int i = 0; i < linkRevsUids.Count; i++)
            {
                var linkRevUids = linkRevsUids[i];
                for (int j = 0; j < linkTotalUids.Count; j++)
                {
                    long id = linkTotalUids[j];
                    if (linkRevUids.BinarySearch(id) >= 0)
                    {
                        var currentPos = i * linkTotalUids.Count + j;
                        bit_matrix[currentPos / RADIX] += VALUE_AT_BIT[(RADIX - 1) - currentPos % RADIX];
                    }
                }
            }
            return bit_matrix;
        
        }

        public static uint getBitMatrixLength(uint numOfRevs, uint numOfTotalOutlink)
        {
            uint length = numOfRevs * numOfTotalOutlink;

            if (length % RADIX != 0)
            {
                length += (RADIX - length % RADIX);
            }
            return length / RADIX;
        }

        public static uint getBitValueAtPosition(uint position, uint numOfRevs, uint numOfTotalOutlink, UInt32[] bit_matrix)
        {
            UInt32 value = position / RADIX;
            return bit_matrix[value] & ((uint)1 << ((int)(position % RADIX)));
        }

        public static List<long> getListOfUidInTimeStamp(List<long> timeStamps, UInt32[] bit_matrix, List<long> linkTotalUids, long firstTime, long lastTime)
        {
            uint numOfRevs = (uint)timeStamps.Count;
            SortedSet<long> linkUids = new SortedSet<long>();
            for (uint i = 0; i < numOfRevs; i++)
            {
                if (timeStamps[(int)i] < firstTime || timeStamps[(int)i] > lastTime)
                    continue;

                for (uint j = 0; j < linkTotalUids.Count; j++)
                {
                    uint position = i * (uint)linkTotalUids.Count + j;
                    uint value = getBitValueAtPosition(position, numOfRevs, (uint)linkTotalUids.Count, bit_matrix);
                    if (value > 0)
                    {
                        linkUids.Add(linkTotalUids[(int)j]);
                    }
                }
            }
            List<long> result = new List<long>();
            while (linkUids.Count > 0) {
                result.Add(linkUids.Min);
                linkUids.Remove(linkUids.Min);
            }

            return result;
        }

    }
}