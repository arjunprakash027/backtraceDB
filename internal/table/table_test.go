package table

import (
	"backtraceDB/internal/schema"
	"testing"
	"os"
	"fmt"
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

	tbl, err := CreateTable(s, nil)
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
	tbl, _ := CreateTable(s, nil)
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

	tbl, err := CreateTable(s, nil)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("Ingesting %d rows to trigger flush...\n", MAX_BLOCK_SIZE)

	for i := 0; i < MAX_BLOCK_SIZE; i++ {
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

	if coldBlock.Storage != nil {
		t.Error("expected cold block Storage to be nil (purged from RAM)")
	}

	if !coldBlock.isOnDisk {
		t.Error("expected cold block to be flagged as on disk")
	}
	if _, err := os.Stat(coldBlock.Path); os.IsNotExist(err) {
		t.Errorf("parquet file not found at %s", coldBlock.Path)
	} else {
		fmt.Printf("✅ Success! Parquet file created at: %s\n", coldBlock.Path)
	}

	os.RemoveAll("data_internal")
}
