namespace KlindaBase.Paging;

public class Page
{
    public int PageId { get; set; }
    public PageType Type { get; set; }
    public byte[] Data { get; set; } // Serialisert nodeinnhold

    public const int PageSize = 4096; // 4KB
}