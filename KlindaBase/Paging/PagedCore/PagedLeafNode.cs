namespace KlindaBase.Paging.PagedCore;

public class PagedLeafNode : PagedBPlusNode
{
    public List<string> Values { get; set; } = new();
    public int? NextLeafPageId { get; set; } = null;
    public override PageType Type => PageType.Leaf;
}