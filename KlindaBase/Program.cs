using KlindaBase.Core;
using KlindaBase.Paging;
using KlindaBase.Paging.PagedCore;
using KlindaBase.WriteAhead;

namespace KlindaBase;

class Program
{
    static void Main()
    {
        // Create a new database file
        var filePath = "test.db";
        if (File.Exists(filePath))
            File.Delete(filePath);
        var pageManager = new PageManager(filePath);
        var wal = new WALLog("wal.log");
        var tree = new PagedBPlusTree(pageManager, wal, degree: 4);

        // Insert some values into the tree
        for (int i = 1; i <= 30; i++)
        {
            tree.Insert(i, $"Value-{i}");
        }

        // Perform a range search
        var results = tree.RangeSearch(10, 20);
        Console.WriteLine("RangeSearch(10, 20):");
        foreach (var kvp in results)
        {
            Console.WriteLine($"{kvp.Key} => {kvp.Value}");
        }

        // Delete some values from the tree
        for (int i = 5; i <= 10; i++)
        {
            tree.Delete(i);
        }

        // Clean up
        tree.Dispose();
    }
}