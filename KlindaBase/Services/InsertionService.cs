using KlindaBase.Core;

namespace KlindaBase.Services;

public class InsertionService
{
    private readonly NodeFinderService _finder;

    public InsertionService(NodeFinderService finder)
    {
        _finder = finder;
    }

    public void Insert(BPlusTree tree, int key, string value)
    {
        var leaf = _finder.FindLeafNode(tree.Root, key);
        InsertInLeaf(tree, (LeafNode)leaf, key, value);
    }

    private void InsertInLeaf(BPlusTree tree, LeafNode leaf, int key, string value)
    {
        int index = leaf.Keys.BinarySearch(key);
        if (index >= 0)
            leaf.Values[index] = value;
        else
        {
            index = ~index;
            leaf.Keys.Insert(index, key);
            leaf.Values.Insert(index, value);
        }

        if (leaf.Keys.Count > tree.Degree)
            SplitLeaf(tree, leaf);
    }

    private void SplitLeaf(BPlusTree tree, LeafNode leaf)
    {
        int mid = leaf.Keys.Count / 2;
        var newLeaf = new LeafNode();
        newLeaf.Keys.AddRange(leaf.Keys.GetRange(mid, leaf.Keys.Count - mid));
        newLeaf.Values.AddRange(leaf.Values.GetRange(mid, leaf.Values.Count - mid));

        leaf.Keys.RemoveRange(mid, leaf.Keys.Count - mid);
        leaf.Values.RemoveRange(mid, leaf.Values.Count - mid);

        newLeaf.Next = leaf.Next;
        leaf.Next = newLeaf;

        InsertInParent(tree, leaf, newLeaf.Keys[0], newLeaf);
    }

    private void InsertInParent(BPlusTree tree, BPlusNode left, int keyToLift, BPlusNode right)
    {
        if (left == tree.Root)
        {
            var newRoot = new InternalNode();
            newRoot.Keys.Add(keyToLift);
            newRoot.Children.Add(left);
            newRoot.Children.Add(right);
            tree.Root = newRoot;
            return;
        }

        var parent = _finder.FindParent(tree.Root, left);
        int index = parent.Children.IndexOf(left);
        parent.Keys.Insert(index, keyToLift);
        parent.Children.Insert(index + 1, right);

        if (parent.Keys.Count > tree.Degree)
            SplitInternalNode(tree, parent);
    }

    private void SplitInternalNode(BPlusTree tree, InternalNode node)
    {
        int mid = node.Keys.Count / 2;
        int keyToLift = node.Keys[mid];

        var newNode = new InternalNode();
        newNode.Keys.AddRange(node.Keys.GetRange(mid + 1, node.Keys.Count - mid - 1));
        newNode.Children.AddRange(node.Children.GetRange(mid + 1, node.Children.Count - mid - 1));

        node.Keys.RemoveRange(mid, node.Keys.Count - mid);
        node.Children.RemoveRange(mid + 1, node.Children.Count - mid - 1);

        InsertInParent(tree, node, keyToLift, newNode);
    }
}
