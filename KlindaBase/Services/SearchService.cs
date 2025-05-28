using KlindaBase.Core;

namespace KlindaBase.Services;

public class SearchService
{
    private readonly NodeFinderService _finder;

    public SearchService(NodeFinderService finder)
    {
        _finder = finder;
    }

    public string Search(BPlusTree tree, int key)
    {
        var node = _finder.FindLeafNode(tree.Root, key);
        var leaf = (LeafNode)node;

        int index = leaf.Keys.BinarySearch(key);
        return index >= 0 ? leaf.Values[index] : null;
    }

    public List<KeyValuePair<int, string>> RangeSearch(BPlusTree tree, int start, int end)
    {
        var results = new List<KeyValuePair<int, string>>();
        var node = _finder.FindLeafNode(tree.Root, start);
        var leaf = (LeafNode)node;

        while (leaf != null)
        {
            for (int i = 0; i < leaf.Keys.Count; i++)
            {
                int k = leaf.Keys[i];
                if (k > end) return results;
                if (k >= start)
                {
                    results.Add(new KeyValuePair<int, string>(k, leaf.Values[i]));
                }
            }

            leaf = leaf.Next;
        }

        return results;
    }
}