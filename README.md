# Klinda – A Lightweight Local B+Tree-Based Database Engine in C#

**Klinda** is a lightweight embedded database engine implemented in C#. It is designed around a paged B+Tree structure with full disk persistence, buffer management, write-ahead logging (WAL), and support for multiple schema-defined tables. The long-term goal is to develop a fast, embeddable, fully queryable, and transaction-safe local database core.

---

## 🚀 Features

- ✅ Paged B+Tree with insert/search/delete
- ✅ Buffer manager with eviction and auto-flush
- ✅ Write-Ahead Log (WAL) with compression support
- ✅ FreeList for PageId reuse
- ✅ MetadataPage for root node tracking
- ✅ Multi-table support via `DbEngine`
- ✅ API wrapper via `DbService` (Insert, Search, etc.)
- ✅ Range queries (`RangeSearch`)
- ✅ Schema persistence (`SchemaPage`)
- ✅ Recovery from WAL at startup
- ✅ Node & metadata serialization to disk


---

## 🧪 Example Usage

```csharp
var stream = new FileStream("test.db", FileMode.OpenOrCreate, FileAccess.ReadWrite);
var pageManager = new PageManager(stream);
var wal = new WriteAheadLog("wal.log");

var db = new DbEngine(pageManager, wal);
var service = new DbService(db);

db.CreateTable("users");
service.Insert("users", 1, "Alice");
service.Insert("users", 2, "Bob");

Console.WriteLine(service.Search("users", 1)); // → Alice

service.Delete("users", 1);

var results = service.RangeSearch("users", 0, 10);
foreach (var r in results)
    Console.WriteLine($"{r.Key} → {r.Value}");

db.Dispose();
```
📌 Roadmap
**Phase 1 – Schema & Table Management (🟢 In Progress)**

- Store and load TableDefinition using SchemaPage
- Assign a unique MetadataPageId per table
- Track RootPageId per table internally
- Implement DbEngine to manage tables and B+Trees

> Implement DbService to wrap table operations

**Phase 2 – Column Definitions & Types**

- Add ColumnDefinition with name + type (int, string, etc.)
- Validate typed input on Insert
- Serialize and store column definitions in schema

> Add basic row validation using schema

**Phase 3 – Transactions**

- Support BeginTransaction, Commit, Rollback
- Track TransactionId in WAL
- Write undo/redo records to log

> Add recovery logic for unfinished transactions

**Phase 4 – Query Language & Parser**

- Create simple SQL-like lexer/parser
- Translate queries to DbService calls
- Add support for WHERE filters
> Add string pattern filters and boolean logic

**Phase 5 – Performance & Resilience**

- Replace FIFO eviction with LRU in BufferManager
- Implement optional page compression (e.g. GZip)
- Add performance counters (buffer hits, flush count)
- Write checkpointing/snapshot mechanism

> Write fuzz + crash tests

**Phase 6 – CLI / REPL**

- Add interactive shell with table/query support
- Live insert/search/delete

> Optional SQLite-like command line
