namespace KlindaBase.Services;

public class InternalNode : BPlusNode
{
    public List<BPlusNode> Children { get; set; } = new();
    public override bool IsLeaf => false;
}