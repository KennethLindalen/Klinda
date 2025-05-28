using KlindaBase.Services;

namespace KlindaBase.Core;

public class BPlusTree
{
    public BPlusNode Root { get; set; }
    public int Degree { get; }

    private readonly InsertionService _insertionService;
    private readonly SearchService _searchService;
    private readonly DeletionService _deletionService;
    private readonly TreeSerializationService _serializer;

    public BPlusTree(int degree)
    {
        Degree = degree;
        
        Root = new LeafNode();

        var finder = new NodeFinderService();
        _insertionService = new InsertionService(finder);
        _searchService = new SearchService(finder);
        _deletionService = new DeletionService(finder);
        
        _serializer = new TreeSerializationService();
    }

    public void Insert(int key, string value) => _insertionService.Insert(this, key, value);

    public string Search(int key) => _searchService.Search(this, key);
    public void Delete(int key) => _deletionService.Delete(this, key);

    public List<KeyValuePair<int, string>> RangeSearch(int start, int end) =>
        _searchService.RangeSearch(this, start, end);
    
    public void SaveToFile(string path) => _serializer.SaveToFile(Root, path);

    public void LoadFromFile(string path) => Root = _serializer.LoadFromFile(path);
    
    public void PrintTree()
    {
        Console.WriteLine("=== Tree Structure ===");

        var queue = new Queue<(BPlusNode Node, int Level)>();
        queue.Enqueue((Root, 0));
        int currentLevel = -1;

        while (queue.Count > 0)
        {
            var (node, level) = queue.Dequeue();

            if (level != currentLevel)
            {
                currentLevel = level;
                Console.WriteLine($"\nLevel {level}:");
            }

            if (node.IsLeaf)
            {
                var leaf = (LeafNode)node;
                string content = string.Join(", ", leaf.Keys.Select(k => $"{k}:{leaf.Values[leaf.Keys.IndexOf(k)]}"));
                Console.Write($"[Leaf: {content}]  ");
            }
            else
            {
                var internalNode = (InternalNode)node;
                Console.Write($"[Internal: {string.Join(", ", internalNode.Keys)}]  ");

                foreach (var child in internalNode.Children)
                    queue.Enqueue((child, level + 1));
            }
        }

        Console.WriteLine("\n=========================\n");
    }

}