using KlindaBase.Buffer;
using KlindaBase.WriteAhead;

namespace KlindaBase.Paging.PagedCore;

public class PagedBPlusTree
{
    private readonly PageManager _pageManager;
    private readonly BufferManager _buffer;
    private readonly WALLog _wal;
    private int _rootPageId;
    private readonly int _degree;
    

    public PagedBPlusTree(PageManager pageManager, WALLog wal, int degree)
    {
        _pageManager = pageManager;
        _wal = wal;
        _buffer = new BufferManager(pageManager, wal);
        _buffer.Recover();

        _degree = degree;
        _buffer.Recover();

        if (_pageManager.AllocatePageId() == 0)
        {
            var root = new PagedLeafNode { PageId = 0 };
            SaveNode(root);
            _rootPageId = 0;
        }
        else
        {
            _rootPageId = 0; // TODO: ved reell recovery, last dette fra metadata
        }
    }


    public void Insert(int key, string value)
    {
        var root = LoadNode(_rootPageId);
        var result = InsertRecursive(root, key, value);

        if (result.NeedsSplit)
        {
            var newRoot = new PagedInternalNode
            {
                PageId = _pageManager.AllocatePageId(),
                Keys = new() { result.SplitKey },
                ChildrenPageIds = new() { root.PageId, result.NewPage.PageId }
            };

            SaveNode(result.NewPage);
            SaveNode(newRoot);
            _rootPageId = newRoot.PageId;
        }
    }
    public void Delete(int key)
    {
        var node = LoadNode(_rootPageId);

        while (node is PagedInternalNode internalNode)
        {
            int i = 0;
            while (i < internalNode.Keys.Count && key >= internalNode.Keys[i]) i++;
            node = LoadNode(internalNode.ChildrenPageIds[i]);
        }

        var leaf = (PagedLeafNode)node;
        int index = leaf.Keys.IndexOf(key);
        if (index == -1) return; // finnes ikke

        leaf.Keys.RemoveAt(index);
        leaf.Values.RemoveAt(index);

        // Log sletting av hele noden – eller spesifikk sletting
        var updatedPage = new Page
        {
            PageId = leaf.PageId,
            Type = leaf.Type,
            Data = PagedNodeSerializer.Serialize(leaf)
        };

        _wal.LogInsert(leaf.PageId, updatedPage.Data); // logg ny versjon
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
