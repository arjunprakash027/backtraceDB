# BacktraceDB

> **Note:** This is a toy project.  
> BacktraceDB is something I built to understand database internals better. It’s not production-ready, hasn’t been security-audited, and shouldn’t be used for anything serious. Think of it as a lab notebook you can run.

---

## What This Is

BacktraceDB is a small column-oriented storage engine written to explore how real databases work under the hood.

I wanted something concrete to experiment with:
- how write-ahead logs actually make in-memory state durable,
- how columnar layouts change query performance,
- how metadata (like min/max stats) lets engines skip huge chunks of data,
- and what happens when your dataset is bigger than RAM.

This project is intentionally simple, sometimes naive, and very opinionated. That’s the point.

---

## Design Overview

### Hybrid Storage Model

Recent (“hot”) data lives in memory for fast writes and reads.  
Once a block reaches a size threshold, it’s frozen and flushed to disk as an **Apache Parquet** file.

After that, the block is immutable.

### Write-Ahead Logging (WAL)

Every row append is written to a WAL before it’s visible to the table.  
On startup, the database:
1. Scans existing Parquet blocks
2. Loads their metadata (min/max stats)
3. Replays the WAL to rebuild any in-memory state that wasn’t flushed yet

The filesystem is treated as the source of truth.

### Columnar Layout

Data is stored column-wise, not row-wise. This makes:
- scans cheaper,
- aggregates faster,
- and compression (dictionary encoding for strings) straightforward.

### Block Pruning via Metadata

Each Parquet block stores min/max statistics per column.  
During a query, these stats are checked first so entire blocks can be skipped without being read from disk.

---

## Example Usage

### Opening a Database and Table

```go
import (
    "backtraceDB/internal/db"
    "backtraceDB/internal/schema"
)

func main() {
    storage, _ := db.Open("trading_db")
    defer storage.Close()

    s := schema.Schema{
        Name: "orders",
        TimeColumn: "ts",
        Columns: []schema.Column{
            {Name: "ts",     Type: schema.Int64},
            {Name: "symbol", Type: schema.String},
            {Name: "price",  Type: schema.Float64},
            {Name: "qty",    Type: schema.Int64},
        },
    }

    tbl, _ := storage.CreateTable(s)

    tbl.MaxBlockSize = 100_000
    tbl.UseDiskStorage = true
}
````

If the table already exists on disk, `CreateTable` will recover it instead of creating a new one.

---

### Querying Data

Filters are chained. The engine applies predicate pushdown so only relevant blocks are scanned.

```go
reader := tbl.Reader().
    Filter("symbol", "==", "BTC").
    Filter("qty", ">", int64(100)).
    Filter("price", ">", 45000.0)

for {
    row, ok := reader.Next()
    if !ok {
        break
    }
    // process row
}
```

---

## Recovery Model

On startup:

1. All Parquet files are discovered
2. Their metadata is loaded into memory
3. The WAL is replayed to restore rows that were still in RAM

If the process crashes mid-ingestion, the worst case is re-reading some WAL entries.

---

## Code Layout

| Package           | Purpose                                    |
| ----------------- | ------------------------------------------ |
| `internal/db`     | Database and table lifecycle management    |
| `internal/table`  | Core engine logic, block rotation, readers |
| `internal/wal`    | Binary WAL encoding and replay             |
| `internal/schema` | Type system, validation, column indexing   |

---

## Testing

```bash
# Correctness tests
./test.sh

# Basic performance benchmarks
./benchmark.sh
```

There’s also a `stress_test.go` that pushes ingestion past available RAM to exercise block rotation.

---

## Why This Exists

This project exists so I can answer questions like:

* “What actually happens when a database crashes?”
* “Why does columnar storage help analytics?”
* “How much work can metadata save during a scan?”

If you’re curious about those things too, feel free to poke around.

---

## Contributing

If you want to experiment:

1. Fork it
2. Break it
3. Fix it
4. Open a PR

Ideas like basic aggregations, better indexing, or smarter pruning are all fair game.
