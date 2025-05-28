using KlindaBase.Buffer;
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
            ChildrenPageIds = internalNode.ChildrenPageIds.GetRange(midIdx + 1, internalNode.ChildrenPageIds.Count - midIdx - 1)
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
