using System.Text;
using KlindaBase.Paging.PagedCore;

namespace KlindaBase.Paging;

public static class PagedNodeSerializer
{
    public static byte[] Serialize(PagedBPlusNode node)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        writer.Write(node.Keys.Count);
        foreach (var key in node.Keys)
            writer.Write(key);

        switch (node)
        {
            case PagedLeafNode leaf:
                writer.Write(leaf.Values.Count);
                foreach (var val in leaf.Values)
                    writer.Write(val);
                writer.Write(leaf.NextLeafPageId ?? -1);
                break;

            case PagedInternalNode internalNode:
                writer.Write(internalNode.ChildrenPageIds.Count);
                foreach (var id in internalNode.ChildrenPageIds)
                    writer.Write(id);
                break;
        }

        return ms.ToArray();
    }

    public static PagedBPlusNode Deserialize(Page page)
    {
        using var ms = new MemoryStream(page.Data);
        using var reader = new BinaryReader(ms, Encoding.UTF8);

        int keyCount = reader.ReadInt32();
        var keys = new List<int>();
        for (int i = 0; i < keyCount; i++)
            keys.Add(reader.ReadInt32());

        if (page.Type == PageType.Leaf)
        {
            int valCount = reader.ReadInt32();
            var values = new List<string>();
            for (int i = 0; i < valCount; i++)
                values.Add(reader.ReadString());

            int nextId = reader.ReadInt32();
            return new PagedLeafNode
            {
                PageId = page.PageId,
                Keys = keys,
                Values = values,
                NextLeafPageId = nextId == -1 ? null : nextId
            };
        }
        int childCount = reader.ReadInt32();
        var children = new List<int>();
        for (int i = 0; i < childCount; i++)
            children.Add(reader.ReadInt32());

        return new PagedInternalNode
        {
            PageId = page.PageId,
            Keys = keys,
            ChildrenPageIds = children
        };
    }
}