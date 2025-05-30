using KlindaBase.Buffer;
using KlindaBase.Paging;
using KlindaBase.Paging.PagedCore;
using KlindaBase.SchemaDefinition;
using KlindaBase.WriteAhead;

namespace KlindaBase.Engine;

public class DbEngine : IDisposable
{
    private readonly PageManager _pageManager;
    private readonly WALLog _wal;
    private readonly BufferManager _buffer;
    private readonly Dictionary<string, PagedBPlusTree> _tables = new Dictionary<string, PagedBPlusTree>();
    private SchemaPage _schema;

    /// <summary>
    /// Creates a new database engine instance.
    /// </summary>
    /// <param name="pageManager">The page manager instance.</param>
    /// <param name="wal">The write-ahead log instance.</param>
    public DbEngine(PageManager pageManager, WALLog wal)
    {
        _pageManager = pageManager;
        _wal = wal;
        _buffer = new BufferManager(pageManager, wal);

        try
        {
            // Attempt to read the schema from disk
            var raw = pageManager.ReadPage(SchemaPage.PageId);
            _schema = SchemaPage.Deserialize(raw.Data);
        }
        catch
        {
            // If the schema does not exist, create a new one
            _schema = new SchemaPage();
            // Save the new schema to disk
            SaveSchema();
        }

        // Create B+ tree instances for each table in the schema
        foreach (var table in _schema.Tables)
        {
            var tree = new PagedBPlusTree(pageManager, wal, _buffer, degree: 4, metadataPageId: table.MetadataPageId);
            _tables[table.Name] = tree;
        }
    }


    /// <summary>
    /// Creates a new table with the specified name.
    /// </summary>
    /// <param name="name">The name of the table to create.</param>
    /// <exception cref="InvalidOperationException">Thrown if the table already exists.</exception>
    public void CreateTable(string name)
    {
        // Check if a table with the given name already exists
        if (_tables.ContainsKey(name))
            throw new InvalidOperationException("Table already exists");

        // Allocate a new metadata page ID for the table
        var metadataPageId = _pageManager.AllocatePageId();

        // Create a new B+ tree for the table with the allocated metadata page ID
        var tree = new PagedBPlusTree(_pageManager, _wal, _buffer, degree: 4, metadataPageId: metadataPageId);
        var rootId = tree.RootPageId;

        // Define the new table with its ID, name, metadata page ID, and root page ID
        var tableDefinition = new TableDefinition
        {
            TableId = _schema.Tables.Count + 1,
            Name = name,
            MetadataPageId = metadataPageId,
            RootPageId = rootId
        };

        // Add the table definition to the schema and the table to the dictionary
        _schema.Tables.Add(tableDefinition);
        _tables[name] = tree;

        // Save the updated schema to disk
        SaveSchema();
    }


    /// <summary>
    /// Retrieves the B+ tree associated with the given table name.
    /// </summary>
    /// <param name="name">The name of the table.</param>
    /// <returns>The B+ tree corresponding to the table.</returns>
    /// <exception cref="KeyNotFoundException">Thrown if the table does not exist.</exception>
    public PagedBPlusTree GetTable(string name)
    {
        // Retrieve the tree from the dictionary of tables
        return _tables[name];
    }

    /// <summary>
    /// Saves the schema to the page with the given page ID.
    /// </summary>
    private void SaveSchema()
    {
        // Save the serialized schema to the page with the given page ID
        _pageManager.WritePage(new Page
        {
            PageId = SchemaPage.PageId,
            Type = PageType.Internal,
            Data = _schema.Serialize()
        });
    }

    /// <summary>
    /// Disposes of the resources used by the DbEngine.
    /// </summary>
    public void Dispose()
    {
        // Dispose each B+ tree in the tables
        foreach (var tree in _tables.Values)
            tree.Dispose();

        // Flush the buffer to persist any changes
        _buffer.Flush();

        // Write the free list to disk
        _pageManager.WriteFreeList();
    }
}