using KlindaBase.Core;

namespace KlindaBase;

class Program
{
    static void Main()
    {
        var tree = new BPlusTree(degree: 3);

        Console.WriteLine("=== INSERT ===");
        var data = new Dictionary<int, string>
        {
            { 10, "A" }, { 20, "B" }, { 5, "C" }, { 6, "D" },
            { 12, "E" }, { 30, "F" }, { 7, "G" }, { 17, "H" }
        };

        foreach (var kvp in data)
        {
            Console.WriteLine($"Insert: {kvp.Key} -> {kvp.Value}");
            tree.Insert(kvp.Key, kvp.Value);
        }

        Console.WriteLine("\n=== SEARCH ===");
        foreach (var key in new[] { 6, 10, 17, 99 })
        {
            var result = tree.Search(key);
            Console.WriteLine($"Search {key}: {result ?? "not found"}");
        }

        Console.WriteLine("\n=== RANGE SEARCH (6 to 20) ===");
        var range = tree.RangeSearch(6, 20);
        foreach (var kvp in range)
        {
            Console.WriteLine($"{kvp.Key} => {kvp.Value}");
        }

        Console.WriteLine("\n=== DELETE ===");
        foreach (var key in new[] { 6, 7, 10, 12 })
        {
            Console.WriteLine($"Delete: {key}");
            tree.Delete(key);
        }

        Console.WriteLine("\n=== POST-DELETE RANGE SEARCH (5 to 30) ===");
        var postRange = tree.RangeSearch(5, 30);
        foreach (var kvp in postRange)
        {
            Console.WriteLine($"{kvp.Key} => {kvp.Value}");
        }

        Console.WriteLine("\n=== DONE ===");
        Console.WriteLine("\n=== PRINT TREE ===");
        tree.PrintTree();

    }
}