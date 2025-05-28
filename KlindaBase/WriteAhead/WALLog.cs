namespace KlindaBase.WriteAhead;

public class WALLog : IDisposable
{
    private readonly string _logFile;
    private readonly FileStream _stream;
    private readonly BinaryWriter _writer;

    public WALLog(string logFile)
    {
        _logFile = logFile;
        _stream = new FileStream(_logFile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        _writer = new BinaryWriter(_stream);
    }

    public void LogInsert(int pageId, byte[] pageData)
    {
        _writer.Write((byte)1); // 1 = insert
        _writer.Write(pageId);
        _writer.Write(pageData.Length);
        _writer.Write(pageData);
        _writer.Flush();
    }

    public void LogDelete(int pageId)
    {
        _writer.Write((byte)2); // 2 = delete
        _writer.Write(pageId);
        _writer.Flush();
    }

    public void Flush() => _writer.Flush();

    public IEnumerable<LogEntry> ReadAll()
    {
        _stream.Seek(0, SeekOrigin.Begin);
        using var reader = new BinaryReader(_stream, System.Text.Encoding.UTF8, leaveOpen: true);

        while (_stream.Position < _stream.Length)
        {
            byte type = reader.ReadByte();
            int pageId = reader.ReadInt32();

            if (type == 1) // Insert/update
            {
                int len = reader.ReadInt32();
                byte[] data = reader.ReadBytes(len);
                yield return new LogEntry { Type = LogEntryType.Insert, PageId = pageId, Data = data };
            }
            else if (type == 2) // Delete
            {
                yield return new LogEntry { Type = LogEntryType.Delete, PageId = pageId };
            }
        }
    }

    public void Truncate()
    {
        _writer.Flush();
        _stream.SetLength(0);
        _stream.Seek(0, SeekOrigin.Begin);
    }

    public void Dispose()
    {
        _writer.Dispose();
        _stream.Dispose();
    }

    public class LogEntry
    {
        public LogEntryType Type { get; set; }
        public int PageId { get; set; }
        public byte[] Data { get; set; }
    }

    public enum LogEntryType : byte
    {
        Insert = 1,
        Delete = 2
    }
}
