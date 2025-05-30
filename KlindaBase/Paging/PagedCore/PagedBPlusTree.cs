using KlindaBase.Buffer;
using KlindaBase.Services.PagedServices;
using KlindaBase.WriteAhead;

namespace KlindaBase.Paging.PagedCore;

public class PagedBPlusTree
{
    private readonly PageManager _pageManager;
    private readonly BufferManager _buffer;
    private readonly WALLog _wal;
    private MetadataPage _metadata;
    private int _rootPageId;
    private readonly int _degree;


    /// <summary>
    /// Creates a new PagedBPlusTree with the given degree.
    /// </summary>
    /// <param name="pageManager">The page manager to use for storing pages.</param>
    /// <param name="wal">The Write-Ahead Log to use for logging inserts and deletes.</param>
    /// <param name="degree">The degree of the B+ tree.</param>
    public PagedBPlusTree(PageManager pageManager, WALLog wal, int degree)
    {
        _pageManager = pageManager;
        _wal = wal;
        _buffer = new BufferManager(pageManager, wal);
        _degree = degree;

        // Perform recovery by replaying the log
        _buffer.Recover();

        // Read the metadata page, which contains the id of the root page
        _metadata = _pageManager.ReadMetadata();

        if (_metadata == null)
        {
            // If the metadata page is missing, create a new root page and write it to disk
            var root = new PagedLeafNode { PageId = 1 };
            SaveNode(root);

            // Create a new metadata page with the id of the root page
            _metadata = new MetadataPage { RootPageId = root.PageId };
            _pageManager.WriteMetadata(_metadata);
        }

        // Set the root page id
        _rootPageId = _metadata.RootPageId;
    }
    
    
    /// <summary>
    /// Flushes the buffer manager and persists the free list.
    /// </summary>
    public void Dispose()
    {
        _buffer.Flush();
        _pageManager.WriteFreeList(); 
        _pageManager.WriteMetadata(_metadata); 
    }

    /// <summary>
    /// Inserts a key-value pair into the B+ tree.
    /// </summary>
    /// <param name="key">The key to insert.</param>
    /// <param name="value">The value associated with the key.</param>
    public void Insert(int key, string value)
    {
        // Load the root node from the page manager
        var root = LoadNode(_rootPageId);
        // Recursively insert the key-value pair
        var result = InsertRecursive(root, key, value);

        // Check if the root needs to be split
        if (result.NeedsSplit)
        {
            // The root was split, create a new root node
            var newRoot = new PagedInternalNode
            {
                PageId = _pageManager.AllocatePageId(),
                Keys = new() { result.SplitKey },
                ChildrenPageIds = new() { root.PageId, result.NewPage.PageId }
            };

            // Save the new page and new root node
            SaveNode(result.NewPage);
            SaveNode(newRoot);
            // Update the runtime root page ID
            _rootPageId = newRoot.PageId;

            // Important: Save the new root in the metadata
            _metadata.RootPageId = _rootPageId;
            _pageManager.WriteMetadata(_metadata);
        }
    }

    /// <summary>
    /// Deletes the specified key from the B+ tree.
    /// </summary>
    /// <param name="key">The key to delete.</param>
    public void Delete(int key)
    {
        // Find the leaf node that contains the key
        var node = LoadNode(_rootPageId);

        while (node is PagedInternalNode internalNode)
        {
            // Find the index in the internal node where the key should be inserted
            int i = 0;
            while (i < internalNode.Keys.Count && key >= internalNode.Keys[i]) i++;
            // Recursively traverse the tree to find the leaf node
            node = LoadNode(internalNode.ChildrenPageIds[i]);
        }

        // The node is now a leaf node
        var leaf = (PagedLeafNode)node;
        int index = leaf.Keys.IndexOf(key);
        if (index == -1) return;

        // Remove the key and its associated value from the leaf node
        leaf.Keys.RemoveAt(index);
        leaf.Values.RemoveAt(index);
        _wal.LogDelete(node.PageId);
        _pageManager.FreePage(node.PageId);
        if (leaf.Keys.Count < Math.Ceiling(_degree / 2.0))
        {
            HandleLeafUnderflow(leaf);
        }


        // Log the changes to the leaf node
        var updatedPage = new Page
        {
            PageId = leaf.PageId,
            Type = leaf.Type,
            Data = PagedNodeSerializer.Serialize(leaf)
        };

        // Log the updated page to the Write-Ahead Log
        _wal.LogInsert(leaf.PageId, updatedPage.Data);
        // Save the updated page to the page manager
        _buffer.PutPage(updatedPage);
    }

    private void HandleLeafUnderflow(PagedLeafNode leaf)
    {
        var parent = PagedSearchService.FindParent(_rootPageId, leaf.PageId, LoadNode);
        if (parent == null) return; // leaf is root

        int index = parent.ChildrenPageIds.IndexOf(leaf.PageId);

        // Venstre søsken
        if (index > 0)
        {
            var left = (PagedLeafNode)LoadNode(parent.ChildrenPageIds[index - 1]);
            if (left.Keys.Count > _degree / 2)
            {
                // Lån fra venstre
                var lastKey = left.Keys[^1];
                var lastVal = left.Values[^1];

                left.Keys.RemoveAt(left.Keys.Count - 1);
                left.Values.RemoveAt(left.Values.Count - 1);

                leaf.Keys.Insert(0, lastKey);
                leaf.Values.Insert(0, lastVal);

                parent.Keys[index - 1] = leaf.Keys[0];

                SaveNode(left);
                SaveNode(leaf);
                SaveNode(parent);
                return;
            }
        }

        // Høyre søsken
        if (index < parent.ChildrenPageIds.Count - 1)
        {
            var right = (PagedLeafNode)LoadNode(parent.ChildrenPageIds[index + 1]);
            if (right.Keys.Count > _degree / 2)
            {
                // Lån fra høyre
                var firstKey = right.Keys[0];
                var firstVal = right.Values[0];

                right.Keys.RemoveAt(0);
                right.Values.RemoveAt(0);

                leaf.Keys.Add(firstKey);
                leaf.Values.Add(firstVal);

                parent.Keys[index] = right.Keys[0];

                SaveNode(right);
                SaveNode(leaf);
                SaveNode(parent);
                return;
            }
        }

        // Ingen å låne fra → merge
        if (index > 0)
        {
            var left = (PagedLeafNode)LoadNode(parent.ChildrenPageIds[index - 1]);
            left.Keys.AddRange(leaf.Keys);
            left.Values.AddRange(leaf.Values);
            left.NextLeafPageId = leaf.NextLeafPageId;

            parent.Keys.RemoveAt(index - 1);
            parent.ChildrenPageIds.RemoveAt(index);

            SaveNode(left);
            SaveNode(parent);
            _wal.LogDelete(leaf.PageId);
        }
        else if (index < parent.ChildrenPageIds.Count - 1)
        {
            var right = (PagedLeafNode)LoadNode(parent.ChildrenPageIds[index + 1]);
            leaf.Keys.AddRange(right.Keys);
            leaf.Values.AddRange(right.Values);
            leaf.NextLeafPageId = right.NextLeafPageId;

            parent.Keys.RemoveAt(index);
            parent.ChildrenPageIds.RemoveAt(index + 1);

            SaveNode(leaf);
            SaveNode(parent);
            _wal.LogDelete(right.PageId);
        }

        // Recurse om parent får underflyt
        if (parent != null && parent.Keys.Count < Math.Ceiling(_degree / 2.0) - 1)
        {
            HandleInternalUnderflow(parent);
        }
    }

    /// <summary>
    /// Handles an internal node that has too few keys.
    /// </summary>
    /// <param name="node">The internal node to handle.</param>
    private void HandleInternalUnderflow(PagedInternalNode node)
    {
        // If the root has only one child, make it the new root
        if (node.PageId == _rootPageId)
        {
            if (node.ChildrenPageIds.Count == 1)
            {
                // Make the only child the new root
                _rootPageId = node.ChildrenPageIds[0];
                _metadata.RootPageId = _rootPageId;
                _pageManager.WriteMetadata(_metadata);
                // Log the deletion of the old root
                _wal.LogDelete(node.PageId);
            }

            return;
        }

        // Find the parent of the internal node
        var parent = PagedSearchService.FindParent(_rootPageId, node.PageId, LoadNode);
        int index = parent.ChildrenPageIds.IndexOf(node.PageId);

        // Check if the node can borrow from its left sibling
        if (index > 0)
        {
            var left = (PagedInternalNode)LoadNode(parent.ChildrenPageIds[index - 1]);
            if (left.Keys.Count > (_degree / 2) - 1)
            {
                // Move the separator from the parent down to the right
                var separator = parent.Keys[index - 1];

                // Borrow a key and its child from the left sibling
                var borrowedKey = left.Keys[^1];
                var borrowedChild = left.ChildrenPageIds[^1];

                left.Keys.RemoveAt(left.Keys.Count - 1);
                left.ChildrenPageIds.RemoveAt(left.ChildrenPageIds.Count - 1);

                // Insert the separator and the borrowed key and child into the right node
                node.Keys.Insert(0, separator);
                node.ChildrenPageIds.Insert(0, borrowedChild);

                // Update the parent
                parent.Keys[index - 1] = borrowedKey;

                // Save the updated nodes
                SaveNode(left);
                SaveNode(node);
                SaveNode(parent);
                return;
            }
        }

        // Check if the node can borrow from its right sibling
        if (index < parent.ChildrenPageIds.Count - 1)
        {
            var right = (PagedInternalNode)LoadNode(parent.ChildrenPageIds[index + 1]);
            if (right.Keys.Count > (_degree / 2) - 1)
            {
                // Move the separator from the parent down to the left
                var separator = parent.Keys[index];

                // Borrow a key and its child from the right sibling
                var borrowedKey = right.Keys[0];
                var borrowedChild = right.ChildrenPageIds[0];

                right.Keys.RemoveAt(0);
                right.ChildrenPageIds.RemoveAt(0);

                // Insert the separator and the borrowed key and child into the left node
                node.Keys.Add(separator);
                node.ChildrenPageIds.Add(borrowedChild);

                // Update the parent
                parent.Keys[index] = borrowedKey;

                // Save the updated nodes
                SaveNode(right);
                SaveNode(node);
                SaveNode(parent);
                return;
            }
        }

        // If the node can't borrow from either sibling, merge with one of them
        if (index > 0)
        {
            var left = (PagedInternalNode)LoadNode(parent.ChildrenPageIds[index - 1]);
            var separator = parent.Keys[index - 1];

            // Add the separator and all the keys and children of the right node to the left node
            left.Keys.Add(separator);
            left.Keys.AddRange(node.Keys);
            left.ChildrenPageIds.AddRange(node.ChildrenPageIds);

            // Remove the right node from the parent
            parent.Keys.RemoveAt(index - 1);
            parent.ChildrenPageIds.RemoveAt(index);

            // Save the updated nodes
            SaveNode(left);
            SaveNode(parent);
            // Log the deletion of the right node
            _wal.LogDelete(node.PageId);
        }
        else if (index < parent.ChildrenPageIds.Count - 1)
        {
            var right = (PagedInternalNode)LoadNode(parent.ChildrenPageIds[index + 1]);
            var separator = parent.Keys[index];

            // Add the separator and all the keys and children of the right node to the left node
            node.Keys.Add(separator);
            node.Keys.AddRange(right.Keys);
            node.ChildrenPageIds.AddRange(right.ChildrenPageIds);

            // Remove the right node from the parent
            parent.Keys.RemoveAt(index);
            parent.ChildrenPageIds.RemoveAt(index + 1);

            // Save the updated nodes
            SaveNode(node);
            SaveNode(parent);
            // Log the deletion of the right node
            _wal.LogDelete(right.PageId);
        }

        // Recursively handle the parent if it has too few keys
        if (parent.Keys.Count < (_degree / 2) - 1)
        {
            HandleInternalUnderflow(parent);
        }
    }


    public string Search(int key)
    {
        var node = LoadNode(_rootPageId);

        while (true)
        {
            if (node is PagedLeafNode leaf)
            {
                int idx = leaf.Keys.IndexOf(key);
                return idx >= 0 ? leaf.Values[idx] : null;
            }

            var internalNode = (PagedInternalNode)node;
            int i = 0;
            while (i < internalNode.Keys.Count && key >= internalNode.Keys[i]) i++;
            node = LoadNode(internalNode.ChildrenPageIds[i]);
        }
    }

    /// <summary>
    /// Searches the B+ tree for all key-value pairs that have a key
    /// in the range [start, end].
    /// </summary>
    /// <param name="start">The start of the range (inclusive).</param>
    /// <param name="end">The end of the range (inclusive).</param>
    /// <returns>A list of key-value pairs that match the search criteria.</returns>
    public List<KeyValuePair<int, string>> RangeSearch(int start, int end)
    {
        var results = new List<KeyValuePair<int, string>>();

        // Navigate down to the first leaf node that may contain "start"
        var node = LoadNode(_rootPageId);
        while (node is PagedInternalNode internalNode)
        {
            int i = 0;
            while (i < internalNode.Keys.Count && start >= internalNode.Keys[i]) i++;
            node = LoadNode(internalNode.ChildrenPageIds[i]);
        }

        // The node is now a leaf node
        var leaf = (PagedLeafNode)node;

        // Iterate over the leaf nodes, adding key-value pairs that are in the range
        while (leaf != null)
        {
            for (int i = 0; i < leaf.Keys.Count; i++)
            {
                int k = leaf.Keys[i];
                if (k > end) return results;
                if (k >= start)
                    results.Add(new KeyValuePair<int, string>(k, leaf.Values[i]));
            }

            // If the next leaf node is null, we can break out of the loop
            if (leaf.NextLeafPageId == null) break;
            // Otherwise, load the next leaf node and continue the loop
            leaf = (PagedLeafNode)LoadNode(leaf.NextLeafPageId.Value);
        }

        return results;
    }


    private InsertResult InsertRecursive(PagedBPlusNode node, int key, string value)
    {
        if (node is PagedLeafNode leaf)
        {
            int idx = leaf.Keys.BinarySearch(key);
            if (idx >= 0)
                leaf.Values[idx] = value;
            else
            {
                idx = ~idx;
                leaf.Keys.Insert(idx, key);
                leaf.Values.Insert(idx, value);
            }

            if (leaf.Keys.Count <= _degree)
            {
                SaveNode(leaf);
                return InsertResult.NoSplit();
            }

            // Split
            int mid = leaf.Keys.Count / 2;
            var newLeaf = new PagedLeafNode
            {
                PageId = _pageManager.AllocatePageId(),
                Keys = leaf.Keys.GetRange(mid, leaf.Keys.Count - mid),
                Values = leaf.Values.GetRange(mid, leaf.Values.Count - mid),
                NextLeafPageId = leaf.NextLeafPageId
            };

            leaf.Keys.RemoveRange(mid, leaf.Keys.Count - mid);
            leaf.Values.RemoveRange(mid, leaf.Values.Count - mid);
            leaf.NextLeafPageId = newLeaf.PageId;

            SaveNode(leaf);
            SaveNode(newLeaf);
            return InsertResult.Split(newLeaf.Keys[0], newLeaf);
        }

        var internalNode = (PagedInternalNode)node;
        int pos = internalNode.Keys.FindIndex(k => key < k);
        if (pos == -1) pos = internalNode.Keys.Count;

        var child = LoadNode(internalNode.ChildrenPageIds[pos]);
        var result = InsertRecursive(child, key, value);

        if (!result.NeedsSplit)
        {
            SaveNode(internalNode);
            return InsertResult.NoSplit();
        }

        // Insert split result into current internal node
        internalNode.Keys.Insert(pos, result.SplitKey);
        internalNode.ChildrenPageIds.Insert(pos + 1, result.NewPage.PageId);

        if (internalNode.Keys.Count <= _degree)
        {
            SaveNode(internalNode);
            SaveNode(result.NewPage);
            return InsertResult.NoSplit();
        }

        // Split internal node
        int midIdx = internalNode.Keys.Count / 2;
        int midKey = internalNode.Keys[midIdx];

        var newInternal = new PagedInternalNode
        {
            PageId = _pageManager.AllocatePageId(),
            Keys = internalNode.Keys.GetRange(midIdx + 1, internalNode.Keys.Count - midIdx - 1),
            ChildrenPageIds =
                internalNode.ChildrenPageIds.GetRange(midIdx + 1, internalNode.ChildrenPageIds.Count - midIdx - 1)
        };

        internalNode.Keys.RemoveRange(midIdx, internalNode.Keys.Count - midIdx);
        internalNode.ChildrenPageIds.RemoveRange(midIdx + 1, internalNode.ChildrenPageIds.Count - midIdx - 1);

        SaveNode(internalNode);
        SaveNode(newInternal);
        return InsertResult.Split(midKey, newInternal);
    }

    private void SaveNode(PagedBPlusNode node)
    {
        var data = PagedNodeSerializer.Serialize(node);
        var page = new Page
        {
            PageId = node.PageId,
            Type = node.Type,
            Data = data
        };

        _buffer.PutPage(page);
    }

    private PagedBPlusNode LoadNode(int pageId)
    {
        var page = _buffer.GetPage(pageId);
        return PagedNodeSerializer.Deserialize(page);
    }

    private class InsertResult
    {
        public bool NeedsSplit { get; private set; }
        public int SplitKey { get; private set; }
        public PagedBPlusNode NewPage { get; private set; }

        public static InsertResult NoSplit() => new() { NeedsSplit = false };

        public static InsertResult Split(int key, PagedBPlusNode page) =>
            new() { NeedsSplit = true, SplitKey = key, NewPage = page };
    }
}