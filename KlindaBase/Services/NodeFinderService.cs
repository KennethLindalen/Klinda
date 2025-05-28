namespace KlindaBase.Services;

public class NodeFinderService
{
    public BPlusNode FindLeafNode(BPlusNode node, int key)
    {
        if (node.IsLeaf) return node;

        var internalNode = (InternalNode)node;
        for (int i = 0; i < internalNode.Keys.Count; i++)
        {
            if (key < internalNode.Keys[i])
                return FindLeafNode(internalNode.Children[i], key);
        }

        return FindLeafNode(internalNode.Children[^1], key);
    }

    public InternalNode FindParent(BPlusNode current, BPlusNode child)
    {
        if (current.IsLeaf) return null;

        var internalNode = (InternalNode)current;
        foreach (var node in internalNode.Children)
        {
            if (node == child) return internalNode;

            var potential = FindParent(node, child);
            if (potential != null)
                return potential;
        }

        return null;
    }
}