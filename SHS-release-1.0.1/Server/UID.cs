using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;

namespace SHS {
  internal static class UID {
    private const int UidDeletedBit = 62;

    [Pure]
    internal static bool HasDeletedBit(long uid) {
      return (uid & (1L << UidDeletedBit)) != 0;
    }

    [Pure]
    internal static long SetDeletedBit(long uid) {
      return uid | (1L << UidDeletedBit);
    }

    [Pure]
    internal static long ClrDeletedBit(long uid) {
      return uid & ~(1L << UidDeletedBit);
    }

    [Pure]
    internal static int CompareUidsForSort(long uid1, long uid2) {
      return ClrDeletedBit(uid1).CompareTo(ClrDeletedBit(uid2));
    }

    [Pure]
    internal static bool LinksAreSorted(List<long> uids) {
      for (int i = 1; i < uids.Count; i++) {
        if (UID.ClrDeletedBit(uids[i - 1]) >= UID.ClrDeletedBit(uids[i])) return false;
      }
      return true;
    }

    [Pure]
    internal static bool NoLinksAreDeleted(List<long> uids) {
      for (int i = 0; i < uids.Count; i++) {
        if (UID.HasDeletedBit(uids[i])) return false;
      }
      return true;
    }
  }
}
