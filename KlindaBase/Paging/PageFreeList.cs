namespace KlindaBase.Paging;

public class PageFreeList
{
    public const int PageId = 1; // Rett etter MetadataPage (0)

    public List<int> FreePageIds { get; set; } = new();

    public byte[] Serialize()
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(FreePageIds.Count);
        foreach (var id in FreePageIds)
            writer.Write(id);
        return ms.ToArray();
    }

    public static PageFreeList Deserialize(byte[] data)
    {
        using var ms = new MemoryStream(data);
        using var reader = new BinaryReader(ms);
        int count = reader.ReadInt32();
        var list = new List<int>();
        for (int i = 0; i < count; i++)
            list.Add(reader.ReadInt32());
        return new PageFreeList { FreePageIds = list };
    }
}