namespace KlindaBase.SchemaDefinition;

public class TableDefinition
{
    public int TableId { get; set; }
    public string Name { get; set; }
    public int MetadataPageId { get; set; }
    public int RootPageId { get; set; } // optional, men nyttig for visning
}
