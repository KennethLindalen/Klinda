namespace KlindaBase.Paging.PagedCore;


public abstract class PagedBPlusNode
{
    public int PageId { get; set; }
    public List<int> Keys { get; set; } = new();
    public abstract PageType Type { get; }
}
