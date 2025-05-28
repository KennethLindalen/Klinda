using System.Text;

namespace KlindaBase.Paging;

public class PageManager
{
    private readonly string _filePath;
    private FileStream _stream;
    private int _nextPageId;

    /// <summary>
    /// Creates a new PageManager with the given file path.
    /// </summary>
    /// <param name="filePath">The path to the file where the pages are stored.</param>
    public PageManager(string filePath)
    {
        _filePath = filePath;
        _stream = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
        // Calculate the next available page id based on the current stream length.
        // The stream length should be a multiple of PageSize, which is the length of a page in bytes.
        _nextPageId = (int)(_stream.Length / Page.PageSize);
    }

    /// <summary>
    /// Allocates a new page id.
    /// </summary>
    /// <returns>The new page id.</returns>
    public int AllocatePageId()
    {
        // The next page id is the current value of _nextPageId plus 1.
        // This is done to ensure that the page ids are unique and that
        // the allocation is thread-safe.
        return Interlocked.Increment(ref _nextPageId) - 1;
    }

    /// <summary>
    /// Writes a page to the file.
    /// </summary>
    /// <param name="page">The page to write.</param>
    public void WritePage(Page page)
    {
        if (page.Data.Length > Page.PageSize - 5)
            throw new InvalidOperationException("Page data exceeds maximum size.");

        long offset = (long)page.PageId * Page.PageSize;
        _stream.Seek(offset, SeekOrigin.Begin);

        using var writer = new BinaryWriter(_stream, Encoding.UTF8, leaveOpen: true);
        // Write the type of the page (leaf or internal)
        writer.Write((byte)page.Type);
        // Write the length of the page data
        writer.Write(page.Data.Length);
        // Write the page data
        writer.Write(page.Data);

        // Pad the rest of the page with zeros
        int padding = Page.PageSize - 5 - page.Data.Length;
        writer.Write(new byte[padding]);
    }

    /// <summary>
    /// Reads a page from the file.
    /// </summary>
    /// <param name="pageId">The id of the page to read.</param>
    /// <returns>The read page.</returns>
    public Page ReadPage(int pageId)
    {
        long offset = (long)pageId * Page.PageSize;
        _stream.Seek(offset, SeekOrigin.Begin);

        using var reader = new BinaryReader(_stream, Encoding.UTF8, leaveOpen: true);
        // Read the type of the page (leaf or internal)
        var type = (PageType)reader.ReadByte();
        // Read the length of the page data
        int length = reader.ReadInt32();
        // Read the page data
        byte[] data = reader.ReadBytes(length);

        return new Page
        {
            PageId = pageId,
            Type = type,
            Data = data
        };
    }

    /// <summary>
    /// Writes the metadata to the page file.
    /// </summary>
    /// <param name="metadata">The metadata page to write.</param>
    public void WriteMetadata(MetadataPage metadata)
    {
        // Create a new page with the specified page id, type, and serialized data
        WritePage(new Page
        {
            PageId = MetadataPage.PageId, // The id of the metadata page
            Type = PageType.Internal, // Symbolic type for metadata
            Data = metadata.Serialize() // Serialize the metadata to a byte array
        });
    }

    /// <summary>
    /// Reads the metadata page from the file.
    /// </summary>
    /// <returns>The deserialized metadata page if the page exists, otherwise null.</returns>
    public MetadataPage ReadMetadata()
    {
        try
        {
            // Read the page with page id 0 (the metadata page)
            var page = ReadPage(MetadataPage.PageId);
            // Deserialize the page data to a metadata page
            return MetadataPage.Deserialize(page.Data);
        }
        catch
        {
            // If the page does not exist, return null
            return null;
        }
    }

    /// <summary>
    /// Closes the file stream associated with the page manager.
    /// </summary>
    public void Close()
    {
        // Close the file stream to release the file handle.
        _stream.Close();
    }
}