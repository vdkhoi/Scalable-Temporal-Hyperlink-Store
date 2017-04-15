using System;
using System.Collections.Generic;
using System.Text;

namespace SHS {
  class ManageStores {
    static void Main(string[] args) {
      if (args.Length == 2 && args[1] == "list") {
        foreach (var si in new Service(args[0]).ListStores()) {
          Console.WriteLine("{0:N} {1,3} {2,2} {3,2} {4,2} {5} {6} {7}", 
            si.StoreID, 
            si.NumPartitions, 
            si.NumReplicas, 
            si.NumPartitionBits,
            si.NumRelativeBits,
            si.IsSealed ? "S" : "O",
            si.IsAvailable ? "+" : "-",
            si.FriendlyName);
        }
      } else if (args.Length == 3 && args[1] == "delete") {
        new Service(args[0]).DeleteStore(Guid.Parse(args[2]));
      } else {
        Console.WriteLine("Usage: SHS.ManageStores <leader> <command>");
        Console.WriteLine("where command can be:");
        Console.WriteLine("    list");
        Console.WriteLine("    delete <storename>");
      }
    }
  }
}
