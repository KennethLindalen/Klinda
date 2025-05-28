namespace KlindaBase.Paging.PagedCore;

public class PagedInternalNode : PagedBPlusNode
{
    public List<int> ChildrenPageIds { get; set; } = new();
    public override PageType Type => PageType.Internal;
}