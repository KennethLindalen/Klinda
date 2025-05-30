using System.Text;

namespace KlindaBase.Paging;

public class PageManager
{
    private readonly string _filePath;
    private PageFreeList _freeList;
    
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
        _freeList = TryReadFreeList() ?? new PageFreeList();
        _nextPageId = Math.Max(2, GetMaxPageIdOnDisk() + 1);
    }
    
    
    
    /// <summary>
    /// Tries to read the free list from the file.
    /// </summary>
    /// <returns>The free list if it exists, otherwise null.</returns>
    private PageFreeList? TryReadFreeList()
    {
        try
        {
            // The free list is stored in the page with page id 1.
            var page = ReadPage(PageFreeList.PageId);
            // Deserialize the page data to a free list.
            return PageFreeList.Deserialize(page.Data);
        }
        catch
        {
            // If the page does not exist, return null.
            return null;
        }
    }
    /// <summary>
    /// Calculates the maximum page id currently on disk.
    /// </summary>
    /// <returns>The maximum page id.</returns>
    public int GetMaxPageIdOnDisk()
    {
        int maxPageId = -1;

        _stream.Seek(0, SeekOrigin.Begin);
        using var reader = new BinaryReader(_stream, Encoding.UTF8, leaveOpen: true);

        // Iterate over the file and find the maximum page id.
        while (_stream.Position < _stream.Length)
        {
            long pos = _stream.Position;

            int pageId = reader.ReadInt32();
            byte pageType = reader.ReadByte();
            int dataLength = reader.ReadInt32();

            // Skip over the data for this page.
            reader.BaseStream.Seek(dataLength, SeekOrigin.Current);

            if (pageId > maxPageId)
                maxPageId = pageId;
        }

        return maxPageId;
    }

    /// <summary>
    /// Writes the free list to the file.
    /// </summary>
    /// <remarks>
    /// The free list is stored in the page with page id 1.
    /// </remarks>
    public void WriteFreeList()
    {
        // Create a new page with the free list data.
        var page = new Page
        {
            PageId = PageFreeList.PageId,
            Type = PageType.Internal, // symbolsk
            Data = _freeList.Serialize()
        };

        // Write the page to the file.
        WritePage(page);
    }

    /// <summary>
    /// Allocates a new page id.
    /// </summary>
    /// <returns>The new page id.</returns>
    public int AllocatePageId()
    {
        if (_freeList.FreePageIds.Count > 0)
        {
            int reused = _freeList.FreePageIds[^1];
            _freeList.FreePageIds.RemoveAt(_freeList.FreePageIds.Count - 1);
            return reused;
        }

        return _nextPageId++;
    }
    public void FreePage(int pageId)
    {
        _freeList.FreePageIds.Add(pageId);
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
    /// Writes the metadata to the page file using the specified page id.
    /// </summary>
    /// <param name="pageId">The id of the page to write the metadata to.</param>
    /// <param name="metadata">The metadata page to write.</param>
    public void WriteMetadata(int pageId, MetadataPage metadata)
    {
        // Create a new page with the specified page id, type, and serialized data
        WritePage(new Page
        {
            PageId = pageId, // The id of the metadata page
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
/// Reads the metadata page from the file using the specified page id.
/// </summary>
/// <param name="pageId">The id of the metadata page to read.</param>
/// <returns>The deserialized metadata page.</returns>
public MetadataPage ReadMetadata(int pageId)
{
    // Read the page with the given page id
    var page = ReadPage(pageId);

    // Deserialize the page data to a metadata page
    return MetadataPage.Deserialize(page.Data);
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