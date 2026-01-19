# BacktraceDB

BacktraceDB is a high-performance, columnar time-series database written in Go. It is designed to handle high-throughput ingestion and efficient storage by utilizing a hybrid memory-disk architecture with **Parquet** for persistence and **Write-Ahead Logging (WAL)** for durability.

## 🚀 Key Features

*   **Columnar Storage**: Optimized for analytical queries and high compression ratios.
*   **Hybrid Storage Engine**: Seamlessly handles data in-memory for speed and on-disk (Parquet) for scale.
*   **Auto-Flushing**: Automatically moves data from memory to disk when configured thresholds are reached.
*   **Durability**: Write-Ahead Log (WAL) ensures no data loss in the event of a crash.
*   **Efficient Querying**:
    *   Push-down predicate filtering (Equality, Range, etc.) to minimize data scanning.
    *   Dictionary encoding for string columns.
*   **Schema Enforcement**: Strictly typed schema definition.

## ️ Usage

### Installation

```bash
git clone https://github.com/arjunprakash027/backtraceDB.git
cd backtraceDB
go mod tidy
```

### Example: Embedding DB in your Go App

```go
package main

import (
	"fmt"
	"backtraceDB/internal/db"
	"backtraceDB/internal/schema"
)

func main() {
	// 1. Open the Database
	database, _ := db.Open("my_db")
	defer database.Close()

	// 2. Define Schema
	s := schema.Schema{
		Name:       "stock_prices",
		TimeColumn: "timestamp",
		Columns: []schema.Column{
			{Name: "timestamp", Type: schema.Int64},
			{Name: "symbol",    Type: schema.String},
			{Name: "price",     Type: schema.Float64},
		},
	}

	// 3. Create Table with WAL enabled
	opts := &db.CreateTableOptions{EnableWal: true}
	tbl, _ := database.CreateTable(s, opts)

	// 4. Ingest Data
	tbl.AppendRow(map[string]any{
		"timestamp": int64(1673628000),
		"symbol":    "AAPL",
		"price":     150.5,
	})

	// 5. Query Data
	// Filter: symbol == "AAPL" AND price > 100.0
	r := tbl.Reader().
		Filter("symbol", "==", "AAPL").
		Filter("price", ">", 100.0)

	for {
		row, ok := r.Next()
		if !ok { break }
		fmt.Printf("Row: %v\n", row)
	}
}
```

## 🧪 Testing & Benchmarking

The project includes a comprehensive suite of tests and benchmarks.

### Run Unit & Integration Tests
Uses the `internal` package tests to verify flushing, WAL recovery, and filtering logic.
```bash
./test.sh
```

### Run Benchmarks
Performance tests for ingestion (with/without WAL) and retrieval speeds.
```bash
./benchmark.sh
```

### Run Stress Test
Simulates OOM (Out of Memory) conditions and validates persistence under load.
```bash
go run stress_test.go
```
