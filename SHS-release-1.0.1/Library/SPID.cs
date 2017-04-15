using System;

namespace SHS {
  internal struct SPID : IEquatable<SPID> {
    internal readonly Guid storeID;
    internal readonly int partID;

    internal SPID(Guid storeID, int partID) {
      this.storeID = storeID;
      this.partID = partID;
    }

    public override string ToString() {
      return string.Format("SPID(storeID={0:N},partID={2})", this.storeID, this.partID);
    }

    public override int GetHashCode() {
      return this.storeID.GetHashCode() ^ this.partID.GetHashCode();
    }

    public bool Equals(SPID that) {
      return this.storeID == that.storeID && this.partID == that.partID;
    }
  }
}
