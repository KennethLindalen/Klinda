using KlindaBase.Paging;
using KlindaBase.WriteAhead;

namespace KlindaBase.Buffer;

public class BufferManager : IDisposable
{
    private readonly PageManager _pageManager;
    private readonly WALLog _wal;
    private readonly int _capacity;

    private readonly Dictionary<int, Page> _cache = new();
    private readonly HashSet<int> _dirtyPages = new();
    private readonly LinkedList<int> _lruList = new(); // front = nyligst brukt

    public BufferManager(PageManager pageManager, WALLog wal,  int capacity = 100)
    {
        _pageManager = pageManager;
        _wal = wal;
        _capacity = capacity;
    }

    public Page GetPage(int pageId)
    {
        if (_cache.TryGetValue(pageId, out var page))
        {
            Touch(pageId);
            return page;
        }

        // Ikke i buffer – last fra disk
        if (_cache.Count >= _capacity)
            Evict();

        var loaded = _pageManager.ReadPage(pageId);
        _cache[pageId] = loaded;
        _lruList.AddFirst(pageId);
        return loaded;
    }

    public void PutPage(Page page)
    {
        if (_cache.Count >= _capacity && !_cache.ContainsKey(page.PageId))
            Evict();

        _cache[page.PageId] = page;
        _dirtyPages.Add(page.PageId);
        Touch(page.PageId);
        _wal.LogInsert(page.PageId, page.Data);
    }


    public void MarkDirty(int pageId)
    {
        _dirtyPages.Add(pageId);
    }

    public void Flush()
    {
        foreach (var pageId in _dirtyPages)
        {
            if (_cache.TryGetValue(pageId, out var page))
            {
                _pageManager.WritePage(page);
            }
        }

        _dirtyPages.Clear();
        _wal.Truncate();
    }

    
    public void Dispose()
    {
        Flush();
    }
    
    //TODO: Move to own RecoveryManager
    public void Recover()
    {
        foreach (var entry in _wal.ReadAll())
        {
            if (entry.Type == WALLog.LogEntryType.Insert)
            {
                var page = new Page
                {
                    PageId = entry.PageId,
                    Data = entry.Data,
                    Type = DetectPageType(entry.Data)
                };
                _pageManager.WritePage(page); // replay insert/update
            }
            else if (entry.Type == WALLog.LogEntryType.Delete)
            {
                // For enkelhets skyld: overskriv siden med 0-byte "deleted marker"
                var tombstone = new Page
                {
                    PageId = entry.PageId,
                    Type = PageType.Leaf, // default
                    Data = Array.Empty<byte>()
                };
                _pageManager.WritePage(tombstone);
            }
        }
    }

    private PageType DetectPageType(byte[] data)
    {
        // Anta at første byte i serialisert data er type (Leaf=0, Internal=1)
        return data.Length > 0 && data[0] == 1 ? PageType.Internal : PageType.Leaf;
    }


    private void Evict()
    {
        int pageIdToEvict = _lruList.Last.Value;
        _lruList.RemoveLast();

        if (_cache.TryGetValue(pageIdToEvict, out var page))
        {
            if (_dirtyPages.Contains(pageIdToEvict))
            {
                _pageManager.WritePage(page);
                _dirtyPages.Remove(pageIdToEvict);
            }

            _cache.Remove(pageIdToEvict);
        }
    }

    private void Touch(int pageId)
    {
        _lruList.Remove(pageId);
        _lruList.AddFirst(pageId);
    }
}