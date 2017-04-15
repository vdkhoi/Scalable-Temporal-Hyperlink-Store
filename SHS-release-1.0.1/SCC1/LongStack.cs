using System.IO;
public class LongStack {
    private string baseName;
    private long[] buf;
    private int pos;
    private int exclHiFileId;

    public LongStack(int bufSz, string prefix) {
      this.baseName = prefix + "_" +  System.DateTime.Now.Ticks.ToString("X16");
      this.buf = new long[bufSz];
      this.pos = 0;
      this.exclHiFileId = 0;
    }

    private string Name(int id) {
      return baseName + "." + id.ToString("x8");
    }

    public void Push(long item) {
      if (pos >= buf.Length) {
        // Write buffer to disk and use newly freed buffer.
        using (var wr = new BinaryWriter(new BufferedStream(new FileStream(Name(exclHiFileId++), FileMode.Create, FileAccess.Write)))) {
          for (int i = 0; i < buf.Length; i++) {
            wr.Write(buf[i]);
          }
        }
        pos = 0;
      }
      buf[pos++] = item;
    }

    public long Pop() {
      if (pos == 0) {
        if (Empty) throw new System.Exception("Stack is empty");
        string name = Name(--exclHiFileId);
        using (var rd = new BinaryReader(new BufferedStream(new FileStream(name, FileMode.Open, FileAccess.Read)))) {
          for (int i = 0; i < buf.Length; i++) {
            buf[i] = rd.ReadInt64();
          }
        }
        System.IO.File.Delete(name);
        pos = buf.Length;
      }
      return buf[--pos];
    }

    public bool Empty {
      get {
        return pos == 0 && exclHiFileId == 0;
      }
    }
  }
