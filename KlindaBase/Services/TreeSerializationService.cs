using System.Text.Json;

namespace KlindaBase.Services;

public class TreeSerializationService
{
    private readonly JsonSerializerOptions _options;

    public TreeSerializationService()
    {
        _options = new JsonSerializerOptions
        {
            WriteIndented = true
        };
    }

    public void SaveToFile(BPlusNode root, string path)
    {
        var json = JsonSerializer.Serialize(root, _options);
        File.WriteAllText(path, json);
    }

    public BPlusNode LoadFromFile(string path)
    {
        var json = File.ReadAllText(path);
        return JsonSerializer.Deserialize<BPlusNode>(json, _options)
               ?? throw new InvalidOperationException("Deserialization failed");
    }
    
    public void SaveToBinary(BPlusNode root, Stream stream)
    {
        using var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, leaveOpen: true);
        WriteNode(writer, root);
    }

    public BPlusNode LoadFromBinary(Stream stream)
    {
        using var reader = new BinaryReader(stream, System.Text.Encoding.UTF8, leaveOpen: true);
        var leafList = new List<LeafNode>();
        var root = ReadNode(reader, leafList);
        LinkLeafNodes(leafList);
        return root;
    }
    
    private void WriteNode(BinaryWriter writer, BPlusNode node)
    {
        writer.Write((byte)(node.IsLeaf ? 0 : 1));

        writer.Write(node.Keys.Count);
        foreach (var key in node.Keys)
            writer.Write(key);

        if (node is LeafNode leaf)
        {
            writer.Write(leaf.Values.Count);
            foreach (var val in leaf.Values)
                writer.Write(val);
        }
        else if (node is InternalNode internalNode)
        {
            writer.Write(internalNode.Children.Count);
            foreach (var child in internalNode.Children)
                WriteNode(writer, child);
        }
    }
    
    private BPlusNode ReadNode(BinaryReader reader, List<LeafNode> leafList)
    {
        byte type = reader.ReadByte();
        int keyCount = reader.ReadInt32();

        var keys = new List<int>();
        for (int i = 0; i < keyCount; i++)
            keys.Add(reader.ReadInt32());

        if (type == 0) // Leaf
        {
            int valueCount = reader.ReadInt32();
            var values = new List<string>();
            for (int i = 0; i < valueCount; i++)
                values.Add(reader.ReadString());

            var leaf = new LeafNode
            {
                Keys = keys,
                Values = values
            };

            leafList.Add(leaf);
            return leaf;
        }

        // Internal
        int childCount = reader.ReadInt32();
        var children = new List<BPlusNode>();
        for (int i = 0; i < childCount; i++)
            children.Add(ReadNode(reader, leafList));

        return new InternalNode
        {
            Keys = keys,
            Children = children
        };
    }

    private void LinkLeafNodes(List<LeafNode> leaves)
    {
        for (int i = 0; i < leaves.Count - 1; i++)
        {
            leaves[i].Next = leaves[i + 1];
        }
    }

}