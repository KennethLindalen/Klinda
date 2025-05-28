using KlindaBase.Core;

namespace KlindaBase.Services;

public class DeletionService
{
    private readonly NodeFinderService _finder;

    public DeletionService(NodeFinderService finder)
    {
        _finder = finder;
    }

    public void Delete(BPlusTree tree, int key)
    {
        var node = _finder.FindLeafNode(tree.Root, key);
        var leaf = (LeafNode)node;

        int index = leaf.Keys.IndexOf(key);
        if (index == -1)
            return;

        leaf.Keys.RemoveAt(index);
        leaf.Values.RemoveAt(index);


        if (leaf == tree.Root || leaf.Keys.Count >= Math.Ceiling((tree.Degree / 2.0)))
            return;

        HandleUnderflow(tree, leaf);
    }

    private void HandleUnderflow(BPlusTree tree, LeafNode leaf)
    {
        var parent = _finder.FindParent(tree.Root, leaf);
        if (parent == null)
            return;

        int index = parent.Children.IndexOf(leaf);


        if (index > 0 && parent.Children[index - 1] is LeafNode leftSibling &&
            leftSibling.Keys.Count > tree.Degree / 2)
        {
            BorrowFromLeftLeaf(parent, index, leftSibling, leaf);
            return;
        }


        if (index < parent.Children.Count - 1 && parent.Children[index + 1] is LeafNode rightSibling &&
            rightSibling.Keys.Count > tree.Degree / 2)
        {
            BorrowFromRightLeaf(parent, index, leaf, rightSibling);
            return;
        }


        if (index > 0 && parent.Children[index - 1] is LeafNode mergeLeft)
        {
            MergeLeafNodes(tree, parent, index - 1, mergeLeft, leaf);
        }
        else if (index < parent.Children.Count - 1 && parent.Children[index + 1] is LeafNode mergeRight)
        {
            MergeLeafNodes(tree, parent, index, leaf, mergeRight);
        }
    }

    private void BorrowFromLeftLeaf(InternalNode parent, int index, LeafNode left, LeafNode target)
    {
        var lastKey = left.Keys[^1];
        var lastValue = left.Values[^1];

        left.Keys.RemoveAt(left.Keys.Count - 1);
        left.Values.RemoveAt(left.Values.Count - 1);

        target.Keys.Insert(0, lastKey);
        target.Values.Insert(0, lastValue);

        parent.Keys[index - 1] = target.Keys[0];
    }

    private void BorrowFromRightLeaf(InternalNode parent, int index, LeafNode target, LeafNode right)
    {
        var firstKey = right.Keys[0];
        var firstValue = right.Values[0];

        right.Keys.RemoveAt(0);
        right.Values.RemoveAt(0);

        target.Keys.Add(firstKey);
        target.Values.Add(firstValue);

        parent.Keys[index] = right.Keys[0];
    }

    private void MergeLeafNodes(BPlusTree tree, InternalNode parent, int index, LeafNode left, LeafNode right)
    {
        left.Keys.AddRange(right.Keys);
        left.Values.AddRange(right.Values);
        left.Next = right.Next;

        parent.Keys.RemoveAt(index);
        parent.Children.RemoveAt(index + 1);

        if (parent == tree.Root && parent.Keys.Count == 0)
        {
            tree.Root = left;
            return;
        }

        if (parent.Keys.Count < Math.Ceiling(tree.Degree / 2.0))
        {
            HandleInternalUnderflow(tree, parent);
        }
    }

    private void HandleInternalUnderflow(BPlusTree tree, InternalNode node)
    {
        var parent = _finder.FindParent(tree.Root, node);
        if (parent == null) return;

        int index = parent.Children.IndexOf(node);


        if (index > 0 && parent.Children[index - 1] is InternalNode leftSibling &&
            leftSibling.Keys.Count > tree.Degree / 2)
        {
            BorrowFromLeftInternal(parent, index, leftSibling, node);
            return;
        }


        if (index < parent.Children.Count - 1 && parent.Children[index + 1] is InternalNode rightSibling &&
            rightSibling.Keys.Count > tree.Degree / 2)
        {
            BorrowFromRightInternal(parent, index, node, rightSibling);
            return;
        }


        if (index > 0 && parent.Children[index - 1] is InternalNode mergeLeft)
        {
            MergeInternalNodes(tree, parent, index - 1, mergeLeft, node);
        }
        else if (index < parent.Children.Count - 1 && parent.Children[index + 1] is InternalNode mergeRight)
        {
            MergeInternalNodes(tree, parent, index, node, mergeRight);
        }
    }

    private void BorrowFromLeftInternal(InternalNode parent, int index, InternalNode left, InternalNode target)
    {
        int sepKey = parent.Keys[index - 1];

        var borrowedKey = left.Keys[^1];
        var borrowedChild = left.Children[^1];

        left.Keys.RemoveAt(left.Keys.Count - 1);
        left.Children.RemoveAt(left.Children.Count - 1);

        target.Keys.Insert(0, sepKey);
        target.Children.Insert(0, borrowedChild);

        parent.Keys[index - 1] = borrowedKey;
    }

    private void BorrowFromRightInternal(InternalNode parent, int index, InternalNode target, InternalNode right)
    {
        int sepKey = parent.Keys[index];

        var borrowedKey = right.Keys[0];
        var borrowedChild = right.Children[0];

        right.Keys.RemoveAt(0);
        right.Children.RemoveAt(0);

        target.Keys.Add(sepKey);
        target.Children.Add(borrowedChild);

        parent.Keys[index] = borrowedKey;
    }

    private void MergeInternalNodes(BPlusTree tree, InternalNode parent, int index, InternalNode left,
        InternalNode right)
    {
        int sepKey = parent.Keys[index];

        left.Keys.Add(sepKey);
        left.Keys.AddRange(right.Keys);
        left.Children.AddRange(right.Children);

        parent.Keys.RemoveAt(index);
        parent.Children.RemoveAt(index + 1);

        if (parent == tree.Root && parent.Keys.Count == 0)
        {
            tree.Root = left;
            return;
        }

        if (parent.Keys.Count < Math.Ceiling(tree.Degree / 2.0) - 1)
        {
            HandleInternalUnderflow(tree, parent);
        }
    }
}