namespace KlindaBase.Services;

public abstract class BPlusNode
{
    public List<int> Keys { get; set; } = new();
    public abstract bool IsLeaf { get; }
}