using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace SHS {
  internal class TempFileNames {
    private readonly List<string> names;

    internal static void CleanAll() {
      var tmpFileRegex = new Regex(@"^tmp-([\dabcdef]{32}).shs$", RegexOptions.IgnoreCase);
      var tmpFiles = Directory.GetFiles(Directory.GetCurrentDirectory())
                              .Select(x => Path.GetFileName(x))
                              .Where(x => tmpFileRegex.IsMatch(x))
                              .ToList();
      foreach (var x in tmpFiles) {
        File.Delete(x);
      }
    }

    internal TempFileNames() {
      this.names = new List<string>();
    }

    internal void CleanThis() {
      foreach (var name in this.names) {
        if (File.Exists(name)) {
          File.Delete(name);
        }
      }
      this.names.Clear();
    }

    internal string New() {
      var name = string.Format("tmp-{0:N}.shs", Guid.NewGuid());
      names.Add(name);
      return name;
    }
  }
}
