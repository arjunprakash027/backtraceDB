package table

import (
	"backtraceDB/internal/schema"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func setupTestTable() (*Table, error) {
	s := schema.Schema{
		Name:       "test_table",
		TimeColumn: "ts",
		Columns: []schema.Column{
			{Name: "ts", Type: schema.Int64},
			{Name: "symbol", Type: schema.String},
			{Name: "price", Type: schema.Float64},
			{Name: "volume", Type: schema.Int64},
		},
	}

	tbl, err := CreateTable(s, nil, "test_db")
	if err != nil {
		return nil, err
	}

	rows := []map[string]any{
		{"ts": int64(100), "symbol": "AAPL", "price": 150.0, "volume": int64(100)},
		{"ts": int64(200), "symbol": "GOOG", "price": 2800.0, "volume": int64(200)},
		{"ts": int64(300), "symbol": "MSFT", "price": 300.0, "volume": int64(300)},
		{"ts": int64(400), "symbol": "AAPL", "price": 155.0, "volume": int64(400)},
		{"ts": int64(500), "symbol": "GOOG", "price": 2810.0, "volume": int64(500)},
	}

	for _, r := range rows {
		if err := tbl.AppendRow(r); err != nil {
			return nil, err
		}
	}

	return tbl, nil
}

func TestTimeFilters(t *testing.T) {
	tbl, _ := setupTestTable()

	tests := []struct {
		name     string
		op       string
		val      int64
		expected []int64
	}{
		{"GT", ">", 300, []int64{400, 500}},
		{"GTE", ">=", 300, []int64{300, 400, 500}},
		{"LT", "<", 300, []int64{100, 200}},
		{"LTE", "<=", 300, []int64{100, 200, 300}},
		{"EQ", "==", 300, []int64{300}},
		{"NEQ", "!=", 300, []int64{100, 200, 400, 500}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tbl.Reader().Filter("ts", tt.op, tt.val)
			var results []int64
			for {
				row, ok := r.Next()
				if !ok {
					break
				}
				results = append(results, row["ts"].(int64))
			}

			if len(results) != len(tt.expected) {
				t.Fatalf("expected %d results, got %d", len(tt.expected), len(results))
			}
			for i, v := range results {
				if v != tt.expected[i] {
					t.Errorf("at index %d: expected %d, got %d", i, tt.expected[i], v)
				}
			}
		})
	}
}

func TestChainedFilters(t *testing.T) {
	tbl, _ := setupTestTable()

	// ts > 150 AND price < 1000
	// 200 (GOOG 2800) - NO (price looks wrong, 2800 > 1000)
	// 300 (MSFT 300)  - YES
	// 400 (AAPL 155)  - YES
	// 500 (GOOG 2810) - NO

	r := tbl.Reader().
		Filter("ts", ">", int64(150)).
		Filter("price", "<", 1000.0)

	count := 0
	for {
		row, ok := r.Next()
		if !ok {
			break
		}
		ts := row["ts"].(int64)
		if ts != 300 && ts != 400 {
			t.Errorf("unexpected row in chain: ts=%v", ts)
		}
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 matches, got %d", count)
	}
}

func TestMultiTimestampEquality(t *testing.T) {
	s := schema.Schema{
		Name: "multi_ts", TimeColumn: "ts",
		Columns: []schema.Column{{Name: "ts", Type: schema.Int64}},
	}
	tbl, _ := CreateTable(s, nil, "test_db")
	tbl.AppendRow(map[string]any{"ts": int64(100)})
	tbl.AppendRow(map[string]any{"ts": int64(200)})
	tbl.AppendRow(map[string]any{"ts": int64(200)}) // Duplicate
	tbl.AppendRow(map[string]any{"ts": int64(200)}) // Duplicate
	tbl.AppendRow(map[string]any{"ts": int64(300)})

	t.Run("EQ", func(t *testing.T) {
		r := tbl.Reader().Filter("ts", "==", int64(200))
		count := 0
		for {
			if _, ok := r.Next(); !ok {
				break
			}
			count++
		}
		if count != 3 {
			t.Errorf("expected 3 rows for ts==200, got %d", count)
		}
	})

	t.Run("GT", func(t *testing.T) {
		r := tbl.Reader().Filter("ts", ">", int64(200))
		count := 0
		for {
			row, ok := r.Next()
			if !ok {
				break
			}
			if row["ts"].(int64) != 300 {
				t.Errorf("expected only ts 300, got %v", row["ts"])
			}
			count++
		}
		if count != 1 {
			t.Errorf("expected 1 row for ts > 200, got %d", count)
		}
	})
}

func TestStringFilters(t *testing.T) {
	tbl, _ := setupTestTable()

	t.Run("EQ", func(t *testing.T) {
		r := tbl.Reader().Filter("symbol", "==", "AAPL")
		count := 0
		for {
			row, ok := r.Next()
			if !ok {
				break
			}
			if row["symbol"] != "AAPL" {
				t.Errorf("expected AAPL, got %v", row["symbol"])
			}
			count++
		}
		if count != 2 {
			t.Errorf("expected 2 AAPL rows, got %d", count)
		}
	})

	t.Run("NEQ", func(t *testing.T) {
		r := tbl.Reader().Filter("symbol", "!=", "GOOG")
		count := 0
		for {
			row, ok := r.Next()
			if !ok {
				break
			}
			if row["symbol"] == "GOOG" {
				t.Errorf("did not expect GOOG, got it anyway")
			}
			count++
		}
		if count != 3 { // AAPL, MSFT, AAPL
			t.Errorf("expected 3 non-GOOG rows, got %d", count)
		}
	})
}

func TestComplexFilterCombinations(t *testing.T) {
	tbl, _ := setupTestTable()

	// ts >= 200 AND ts <= 400 AND symbol != MSFT
	// 200 (GOOG) - YES
	// 300 (MSFT) - NO (symbol filter)
	// 400 (AAPL) - YES

	r := tbl.Reader().
		Filter("ts", ">=", int64(200)).
		Filter("ts", "<=", int64(400)).
		Filter("symbol", "!=", "MSFT")

	count := 0
	for {
		row, ok := r.Next()
		if !ok {
			break
		}
		ts := row["ts"].(int64)
		if ts != 200 && ts != 400 {
			t.Errorf("unexpected ts in complex chain: %v", ts)
		}
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 matches in complex chain, got %d", count)
	}
}

func TestBlockFlushing(t *testing.T) {
	s := schema.Schema{
		Name:       "flush_test",
		TimeColumn: "ts",
		Columns: []schema.Column{
			{Name: "ts", Type: schema.Int64},
			{Name: "val", Type: schema.Float64},
		},
	}

	tests := []struct {
		name    string
		useDisk bool
	}{
		{"DiskPersistence", true},
		{"MemoryPersistence", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer os.RemoveAll(filepath.Join("_data_internal", "test_db"))

			tbl, err := CreateTable(s, nil, "test_db")
			if err != nil {
				t.Fatal(err)
			}

			const testBlockSize = 50
			tbl.MaxBlockSize = testBlockSize
			tbl.UseDiskStorage = tt.useDisk

			// Ingest rows to trigger flush
			for i := 0; i < testBlockSize; i++ {
				row := map[string]any{
					"ts":  int64(i),
					"val": float64(i),
				}
				if err := tbl.AppendRow(row); err != nil {
					t.Fatalf("failed at row %d: %v", i, err)
				}
			}

			if len(tbl.coldBlocks) != 1 {
				t.Fatalf("expected 1 cold block, got %d", len(tbl.coldBlocks))
			}

			coldBlock := tbl.coldBlocks[0]

			// Verify RAM purging
			if coldBlock.Storage != nil {
				t.Error("expected cold block Storage to be nil (purged from RAM)")
			}

			if tt.useDisk {
				if !coldBlock.isOnDisk {
					t.Error("expected cold block to be flagged as on disk")
				}
				if _, err := os.Stat(coldBlock.Path); os.IsNotExist(err) {
					t.Errorf("parquet file not found at %s", coldBlock.Path)
				}
			} else {
				if coldBlock.isOnDisk {
					t.Error("expected cold block to NOT be flagged as on disk")
				}
				if len(coldBlock.inMemoryData) == 0 {
					t.Error("expected in-memory parquet data to be present")
				}
			}

			// Core verification: Can we read it back?
			r := tbl.Reader()
			count := 0
			for {
				row, ok := r.Next()
				if !ok {
					break
				}
				if row["ts"].(int64) != int64(count) {
					t.Errorf("data mismatch: expected %d, got %v", count, row["ts"])
				}
				count++
			}

			if count != testBlockSize {
				t.Errorf("expected %d rows read back, got %d", testBlockSize, count)
			}
		})
	}
}

func TestBlockLoadInto(t *testing.T) {
	// 1. Setup
	s := schema.Schema{
		Name:       "load_test",
		TimeColumn: "ts",
		Columns: []schema.Column{
			{Name: "ts", Type: schema.Int64},
			{Name: "val", Type: schema.Float64},
			{Name: "sym", Type: schema.String},
		},
	}

	// Manual setup for test
	colTypes := []schema.ColumnType{schema.Int64, schema.Float64, schema.String}
	block, locations, err := NewBlock(colTypes)
	if err != nil {
		t.Fatal(err)
	}

	// Use Storage directly for this unit test
	block.Storage.Int64Cols[locations[0].Index] = append(block.Storage.Int64Cols[locations[0].Index], 10)
	block.Storage.Float64Cols[locations[1].Index] = append(block.Storage.Float64Cols[locations[1].Index], 10.5)

	// String A
	block.Storage.StringReads[locations[2].Index] = append(block.Storage.StringReads[locations[2].Index], "A")
	block.Storage.StringCols[locations[2].Index] = append(block.Storage.StringCols[locations[2].Index], 0)
	block.Storage.StringDicts[locations[2].Index]["A"] = 0

	// Row 2
	block.Storage.Int64Cols[locations[0].Index] = append(block.Storage.Int64Cols[locations[0].Index], 20)
	block.Storage.Float64Cols[locations[1].Index] = append(block.Storage.Float64Cols[locations[1].Index], 20.5)

	block.Storage.StringReads[locations[2].Index] = append(block.Storage.StringReads[locations[2].Index], "B")
	block.Storage.StringCols[locations[2].Index] = append(block.Storage.StringCols[locations[2].Index], 1)
	block.Storage.StringDicts[locations[2].Index]["B"] = 1

	block.RowCount = 2

	// 3. Flush to Disk
	path := "_data_internal/test_db/load_test/test.parquet"
	if err := block.Rotate(true, path, s, locations); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	defer os.RemoveAll(filepath.Join("_data_internal", "test_db"))

	// 4. Test LoadInto
	// Create a FRESH storage destination
	destStorage, _, _ := NewColumnStorage(colTypes)

	if err := block.LoadInto(destStorage, s, locations); err != nil {
		t.Fatalf("LoadInto failed: %v", err)
	}

	// 5. Verify Content
	// Check Row Count (Implicitly via slice len)
	if len(destStorage.Int64Cols[locations[0].Index]) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(destStorage.Int64Cols[locations[0].Index]))
	}

	// Check Values
	val1 := destStorage.Float64Cols[locations[1].Index][0]
	if val1 != 10.5 {
		t.Errorf("Expected 10.5, got %f", val1)
	}

	// Check String Reconstruction
	strID := destStorage.StringCols[locations[2].Index][1] // Row 2 ("B")
	strVal := destStorage.StringReads[locations[2].Index][strID]
	if strVal != "B" {
		t.Errorf("Expected 'B', got '%s'", strVal)
	}

	fmt.Println("✅ LoadInto Test Passed!")
}

func TestEndToEndIntegration(t *testing.T) {
	// Setup
	s := schema.Schema{
		Name:       "e2e_test",
		TimeColumn: "ts",
		Columns: []schema.Column{
			{Name: "ts", Type: schema.Int64},
			{Name: "val", Type: schema.Int64},
		},
	}

	dbName := "e2e_test_db"
	defer os.RemoveAll(filepath.Join("_data_internal", dbName))

	// Phase 1: Create, write, and close
	{
		tbl, err := CreateTable(s, nil, dbName)
		if err != nil {
			t.Fatal(err)
		}

		tbl.MaxBlockSize = 100
		tbl.UseDiskStorage = true

		for i := 0; i < 250; i++ {
			row := map[string]any{
				"ts":  int64(i * 10),
				"val": int64(i),
			}
			if err := tbl.AppendRow(row); err != nil {
				t.Fatalf("failed to append row %d: %v", i, err)
			}
		}

		if err := tbl.Close(); err != nil {
			t.Fatalf("failed to close table: %v", err)
		}
	}

	// Phase 2: Open fresh table and load from disk
	tbl, err := CreateTable(s, nil, dbName)
	if err != nil {
		t.Fatal(err)
	}

	if err := tbl.LoadFromDisk(); err != nil {
		t.Fatalf("failed to load table from disk: %v", err)
	}

	t.Run("ReadAll", func(t *testing.T) {
		r := tbl.Reader()
		count := 0
		lastVal := int64(-1)
		for {
			row, ok := r.Next()
			if !ok {
				break
			}
			val := row["val"].(int64)
			if val != lastVal+1 {
				t.Errorf("expected val %d, got %d", lastVal+1, val)
			}
			lastVal = val
			count++
		}
		if count != 250 {
			t.Errorf("expected 250 rows, got %d", count)
		}
	})

	t.Run("FilterCrossBoundary", func(t *testing.T) {
		// Filter val >= 180
		// Should match:
		// Disk Block 2: 180-199 (20 rows)
		// Mem Block 3:  200-249 (50 rows)
		// Total: 70

		r := tbl.Reader().Filter("val", ">=", int64(180))

		count := 0
		// We expect values from 180 to 249
		expectedStart := int64(180)
		expectedCount := 70

		for {
			row, ok := r.Next()
			if !ok {
				break
			}
			val := row["val"].(int64)

			expectedVal := expectedStart + int64(count)
			if val != expectedVal {
				t.Errorf("expected val %d at index %d, got %d", expectedVal, count, val)
			}
			count++
		}

		if count != expectedCount {
			t.Errorf("expected %d rows, got %d", expectedCount, count)
		}
	})
}
