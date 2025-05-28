using System.Text;

namespace KlindaBase.Paging;

public class PageManager
{
    private readonly string _filePath;
    private FileStream _stream;
    private int _nextPageId;

    public PageManager(string filePath)
    {
        _filePath = filePath;
        _stream = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
        _nextPageId = (int)(_stream.Length / Page.PageSize);
    }

    public int AllocatePageId() => _nextPageId++;

    public void WritePage(Page page)
    {
        if (page.Data.Length > Page.PageSize - 5)
            throw new InvalidOperationException("Page data exceeds maximum size.");

        long offset = (long)page.PageId * Page.PageSize;
        _stream.Seek(offset, SeekOrigin.Begin);

        using var writer = new BinaryWriter(_stream, Encoding.UTF8, leaveOpen: true);
        writer.Write((byte)page.Type);
        writer.Write(page.Data.Length);
        writer.Write(page.Data);

        // Pad resten av siden
        int padding = Page.PageSize - 5 - page.Data.Length;
        writer.Write(new byte[padding]);
    }

    public Page ReadPage(int pageId)
    {
        long offset = (long)pageId * Page.PageSize;
        _stream.Seek(offset, SeekOrigin.Begin);

        using var reader = new BinaryReader(_stream, Encoding.UTF8, leaveOpen: true);
        var type = (PageType)reader.ReadByte();
        int length = reader.ReadInt32();
        byte[] data = reader.ReadBytes(length);

        return new Page
        {
            PageId = pageId,
            Type = type,
            Data = data
        };
    }

    public void Close() => _stream.Close();
}