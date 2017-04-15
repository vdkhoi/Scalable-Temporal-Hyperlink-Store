using System;
using System.Diagnostics.Contracts;
using System.IO;
using System.Text;

namespace SHS {
  internal struct EncString {
    internal static readonly byte[] Empty = new byte[0];

    public static byte[] Rd(BinaryReader rd) {
      var n = rd.ReadInt32();
      return rd.ReadBytes(n);
    }

    public static void Wr(BinaryWriter wr, byte[] bytes) {
      wr.Write(bytes.Length);
      wr.Write(bytes);
    }

    public static int Compare(byte[] a, byte[] b) {
      Contract.Requires(a != null && b != null);
      int i = 0;
      while (i < a.Length && i < b.Length) {
        if (a[i] < b[i]) {
          return -1;
        } else if (a[i] > b[i]) {
          return +1;
        } else {
          i++;
        }
      }
      if (a.Length < b.Length) {
        return -1;
      } else if (a.Length > b.Length) {
        return +1;
      } else {
        return 0;
      }
    }

    internal class Comparer : System.Collections.Generic.Comparer<byte[]> {
      public override int Compare(byte[] a, byte[] b) {
        int i = 0;
        while (i < a.Length && i < b.Length) {
          if (a[i] < b[i]) {
            return -1;
          } else if (a[i] > b[i]) {
            return +1;
          } else {
            i++;
          }
        }
        if (a.Length < b.Length) {
          return -1;
        } else if (a.Length > b.Length) {
          return +1;
        } else {
          return 0;
        }
      }
    }
  }
}
