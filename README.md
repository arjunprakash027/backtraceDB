# 🗄️ BacktraceDB

**⚠️ Disclaimer: This is a toy project.**
BacktraceDB is a side project I'm using to tinker with database internals. It is **not** production-ready, hasn't been audited for security, and will likely eat your data if you push it too hard. 

---

## 🛠️ Come and Tinker
If you've ever wondered how a database *actually* works under the hood like how it flushes memory to disk, how it recovers from a crash using a WAL, or how columnar storage makes queries faster, this project is for you.

I'd love for people to:
- **Take the code and break it**: See where the logical flaws are.
- **Optimize the internals**: Can we make the Parquet reader faster? Can we improve the vectorized filter application?
- **Suggest Features**: What's missing? Indices? Compression algorithms?

---

## 🚀 Key Features
*   **Hybrid Memory-Disk Architecture**: Keep recent data in RAM for speed, and older data in **Parquet** blocks for scale.
*   **Write-Ahead Log (WAL)**: Ensures durability. If the process dies, the WAL replays your unflushed data on the next boot.
*   **Columnar Storage**: Optimized for analytical queries using dictionary encoding and internal block rotation.
*   **Push-down Filtering**: Filters are applied inside the reader, skipping unrelated data.

---

## 📖 Extended Usage Guide

### 1. Basic Setup & Ingestion
```go
package main

import (
	"fmt"
	"backtraceDB/internal/db"
	"backtraceDB/internal/schema"
)

func main() {
	// Open (or create) a database directory
	database, _ := db.Open("my_trading_engine")
	defer database.Close()

	// Define your schema
	s := schema.Schema{
		Name:       "trades",
		TimeColumn: "ts",
		Columns: []schema.Column{
			{Name: "ts",     Type: schema.Int64},
			{Name: "symbol", Type: schema.String},
			{Name: "price",  Type: schema.Float64},
			{Name: "volume", Type: schema.Int64},
		},
	}

	// Create table with WAL for safety
	tbl, _ := database.CreateTable(s, &db.CreateTableOptions{EnableWal: true})

	// Add some data
	tbl.AppendRow(map[string]any{
		"ts":     int64(1673628000),
		"symbol": "BTC",
		"price":  23450.5,
		"volume": int64(10),
	})
}
```

### 2. Enabling Persistence (Disk Storage)
By default, Parquet blocks are kept in memory. To scale beyond RAM, enable disk storage. This will automatically flush data to the `_data_internal` directory when a block reaches its size limit.

```go
tbl.MaxBlockSize = 5000   // Flush every 5000 rows
tbl.UseDiskStorage = true // Write .parquet files to disk
```

### 3. Recovery Across Restarts
This is where it gets interesting. If you close your app and restart it, you don't call `CreateTable`. Instead, you use `OpenTable`. This triggers the discovery logic:
1. It scans `_data_internal` for existing Parquet files.
2. It re-builds the table's state (row count, last timestamp).
3. It replays the WAL to recover any rows that weren't flushed to disk yet.

```go
// Next time your app starts:
database, _ := db.Open("my_trading_engine")
tbl, err := database.OpenTable(s) // Automatically discovers and reloads state
if err == nil {
    fmt.Printf("Recovered %d rows!\n", tbl.RowCount())
}
```

### 4. Advanced Filter Querying
The reader supports chained filters. These skip the expensive de-serialization of blocks that don't match the criteria.

```go
// Get all BTC trades where volume > 5 and price is between two values
r := tbl.Reader().
    Filter("symbol", "==", "BTC").
    Filter("volume", ">", int64(5)).
    Filter("price", ">", 20000.0).
    Filter("price", "<", 25000.0)

for {
    row, ok := r.Next()
    if !ok { break }
    fmt.Println(row)
}
```

---

## 🧬 Project Structure
- `internal/db`: The top-level API for managing tables.
- `internal/table`: The core logic for block management and columnar storage.
- `internal/wal`: Write-Ahead Log implementation for crash recovery.
- `internal/schema`: Data typing and validation.

---

## 🧪 Testing
Check out how the internals work by running the tests:
```bash
./test.sh      # Core logic tests
./benchmark.sh # Speed tests (Memory vs Disk)
```
