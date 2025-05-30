namespace KlindaBase.Engine;

public class DbService
{
    private readonly DbEngine _db;

    /// <summary>
    /// Creates a new instance of the <see cref="DbService"/> class, which provides a simple interface
    /// for interacting with the database.
    /// </summary>
    /// <param name="db">The <see cref="DbEngine"/> that this service will interact with.</param>
    public DbService(DbEngine db)
    {
        _db = db;
    }

    /// <summary>
    /// Inserts a key-value pair into the specified table.
    /// </summary>
    /// <param name="tableName">The name of the table to insert into.</param>
    /// <param name="key">The key of the entry to insert.</param>
    /// <param name="value">The value associated with the key.</param>
    public void Insert(string tableName, int key, string value)
    {
        // Retrieve the B+ tree associated with the specified table
        var tree = _db.GetTable(tableName);
        
        // Insert the key-value pair into the B+ tree
        tree.Insert(key, value);
    }

    /// <summary>
    /// Searches for a key-value pair in the specified table.
    /// </summary>
    /// <param name="tableName">The name of the table to search in.</param>
    /// <param name="key">The key of the entry to search for.</param>
    /// <returns>The value associated with the key if found, otherwise <c>null</c>.</returns>
    public string? Search(string tableName, int key)
    {
        var tree = _db.GetTable(tableName);
        return tree.Search(key);
    }

    /// <summary>
    /// Deletes a key-value pair from the specified table.
    /// </summary>
    /// <param name="tableName">The name of the table to delete from.</param>
    /// <param name="key">The key of the entry to delete.</param>
    public void Delete(string tableName, int key)
    {
        // Retrieve the B+ tree associated with the specified table
        var tree = _db.GetTable(tableName);
        
        // Delete the key-value pair from the B+ tree
        tree.Delete(key);
    }
    
}
