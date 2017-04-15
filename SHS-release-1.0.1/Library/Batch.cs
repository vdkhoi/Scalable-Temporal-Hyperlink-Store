using System;
using System.Diagnostics.Contracts;


namespace SHS {
  public class Batch<T> {
    private T[] vals;
    private int pos;

    [ContractInvariantMethod]
    private void ObjectInvariant() {
      Contract.Invariant(this.vals != null);
      Contract.Invariant(0 <= this.pos && this.pos <= this.vals.Length);
    }

    public Batch(int sz) {
      Contract.Requires(sz >= 0);

      this.vals = new T[sz];
      this.pos = 0;
    }

    public void Add(T val) {
      Contract.Requires(!this.Full);

      this.vals[pos++] = val;
    }

    public static implicit operator T[] (Batch<T> b) {
      if (b.Full) {
        return b.vals;
      } else {
        var res = new T[b.pos];
        Array.Copy(b.vals, res, b.pos);
        return res;
      }
    }

    public bool Full {
      get {
        return this.pos == this.vals.Length;
      }
    }

    public int Count {
      get {
        return this.pos;
      }
    }

    public void Reset() {
      this.pos = 0;
    }
  }
}
