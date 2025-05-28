namespace KlindaBase.Services;

public class LeafNode : BPlusNode
{
    public List<string> Values { get; set; } = new();
    public LeafNode Next { get; set; }
    public override bool IsLeaf => true;
}