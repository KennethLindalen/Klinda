namespace KlindaBase.SchemaDefinition;

public class SchemaPage
{
    public const int PageId = 2;

    public List<TableDefinition> Tables { get; set; } = new();

    public byte[] Serialize()
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(Tables.Count);
        foreach (var table in Tables)
        {
            writer.Write(table.TableId);
            writer.Write(table.Name);
            writer.Write(table.RootPageId);
        }
        return ms.ToArray();
    }

    public static SchemaPage Deserialize(byte[] data)
    {
        using var ms = new MemoryStream(data);
        using var reader = new BinaryReader(ms);
        int count = reader.ReadInt32();
        var tables = new List<TableDefinition>();
        for (int i = 0; i < count; i++)
        {
            var id = reader.ReadInt32();
            var name = reader.ReadString();
            var root = reader.ReadInt32();
            tables.Add(new TableDefinition { TableId = id, Name = name, RootPageId = root });
        }
        return new SchemaPage { Tables = tables };
    }
}
