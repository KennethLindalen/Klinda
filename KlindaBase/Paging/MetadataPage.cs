namespace KlindaBase.Paging;

public class MetadataPage
{
    public const int PageId = 0; // ALWAYS FIRST PAGE

    public int RootPageId { get; set; }

    /// <summary>
    /// Serializes the metadata page to a byte array.
    /// </summary>
    /// <returns>The serialized metadata page.</returns>
    public byte[] Serialize()
    {
        // Create a memory stream to write the data to
        using var ms = new MemoryStream();
        // Create a binary writer to write the data
        using var writer = new BinaryWriter(ms);
        // Write the root page id to the stream
        writer.Write(RootPageId);
        // Return the serialized data
        return ms.ToArray();
    }

    /// <summary>
    /// Deserializes a byte array to a metadata page.
    /// </summary>
    /// <param name="data">The byte array to deserialize.</param>
    /// <returns>The deserialized metadata page.</returns>
    public static MetadataPage Deserialize(byte[] data)
    {
        // Create a memory stream from the byte array
        using var ms = new MemoryStream(data);
        // Create a binary reader to read the data
        using var reader = new BinaryReader(ms);
        // Read the root page id from the stream
        var rootPageId = reader.ReadInt32();
        // Create a new metadata page with the read root page id
        return new MetadataPage
        {
            RootPageId = rootPageId
        };
    }
}