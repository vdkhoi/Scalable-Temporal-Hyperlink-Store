using System;
using System.Runtime.InteropServices; // to bring DllImport into scope
using T = System.UInt16;
/* T can be set to any "unmanaged-type" (see Ch. 18.2 of "The C# Programming
   Language, 3rd Ed."). It would be nice if T could be a type parameter with a
   type parameter constraint (see Ch. 10.1.5) restricting it to be an
   unmanaged-type, but apparently that'bytesytes not possible -- the "struct" constraint
   is insufficient because structs may contain reference type members. */

namespace SHS {
  public unsafe class BigUInt16Array : BigArray {
    private readonly T* vals; // Unmanaged T array
    private readonly long length;
    private IntPtr ph;

    public BigUInt16Array(long length) {
      this.length = length;
      unsafe {
        /* This recipe is taken from chapter 18.9 of "The C#  Programming
           Language" (3rd edition) by Hejlsberg et al.  According to Yuan Yu,
           an alternative and possibly better way would be to use
           System.Runtime.InteropServices.AllocHGlobal. */
        //this.vals = (T*)HeapAlloc(GetProcessHeap(),
        //                          HEAP_ZERO_MEMORY,
        //                          (UIntPtr)(sizeof(T) * length));
        this.ph = Marshal.AllocHGlobal((IntPtr)(sizeof(T) * length));
        this.vals = (T*)ph;
      }
      if (this.vals == null) throw new OutOfMemoryException();
    }

    ~BigUInt16Array() {
      //HeapFree(GetProcessHeap(), 0, this.vals);
      Marshal.FreeHGlobal(ph);
    }

    public override long Length {
      get { return this.length; }
    }

    public override object GetValue(long i) {
      return this[i];
    }

    public override void SetValue(object o, long i) {
      this[i] = (T)o;
    }

    public unsafe T this[long i] {
      get {
        if (i < 0 || i >= length) throw new IndexOutOfRangeException();
        return this.vals[i];
      }

      set {
        if (i < 0 || i >= length) throw new IndexOutOfRangeException();
        this.vals[i] = value;
      }
    }

    private const int HEAP_ZERO_MEMORY = 0x00000008;

    //[DllImport("kernel32", SetLastError = true)]
    //static extern IntPtr GetProcessHeap();

    //[DllImport("kernel32", SetLastError = true)]
    //static extern void* HeapAlloc(IntPtr hHeap, int flags, UIntPtr numBytes);

    //[DllImport("kernel32", SetLastError = true)]
    //static extern bool HeapFree(IntPtr hHeap, int flags, void* block);
  }
}
