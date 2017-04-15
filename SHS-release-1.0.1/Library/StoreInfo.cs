using System;

namespace SHS {
  public class StoreInfo {
    public readonly Guid StoreID;
    public readonly string FriendlyName;
    public readonly int NumPartitions;
    public readonly int NumReplicas;
    public readonly int NumPartitionBits;
    public readonly int NumRelativeBits;
    public readonly bool IsSealed;
    public readonly bool IsAvailable;

    internal StoreInfo(
      Guid storeID,
      string friendlyName,
      int numPartitions,
      int numReplicas,
      int numPartitionBits,
      int numRelativeBits,
      bool isSealed,
      bool isAvailable) 
    {
      this.StoreID = storeID;
      this.FriendlyName = friendlyName;
      this.NumPartitions = numPartitions;
      this.NumReplicas = numReplicas;
      this.NumPartitionBits = numPartitionBits;
      this.NumRelativeBits = numRelativeBits;
      this.IsSealed = isSealed;
      this.IsAvailable = isAvailable;
    }
  }
}
