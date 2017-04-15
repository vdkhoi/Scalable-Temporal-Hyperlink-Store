using System;
using System.IO;

namespace SHS {
  internal class AtomicStream : FileStream {
    private string name;

    internal AtomicStream(string name, FileMode mode, FileAccess access)
      : base(name + ".new", mode, access) 
    {
      this.name = name;
    }

    protected override void Dispose(bool disposing) {
      if (disposing) {
        base.Dispose(disposing);
        if (File.Exists(this.name)) {
          // Maybe File.Replace is atomic than doing two File.Move calls?
          File.Move(this.name, this.name + ".old");
          File.Move(this.name + ".new", this.name);
          File.Delete(this.name + ".old");
        } else {
          File.Move(this.name + ".new", this.name);
        }
      }
    }
  }
}
