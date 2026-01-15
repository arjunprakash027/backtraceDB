package wal_test

import (
	"backtraceDB/internal/wal"
	"backtraceDB/internal/schema"
	"backtraceDB/internal/table"
	"os"
	"testing"
)

func TestWAL(t *testing.T) {
	tmpDir := "test_wal"
	defer os.RemoveAll(tmpDir)

	s := schema.Schema{
		Name:       "test_table",
		TimeColumn: "ts",
		Columns: []schema.Column{
			{Name: "ts", Type: schema.Int64},
			{Name: "val", Type: schema.Float64},
		},
	}

	// 1. Create WAL
	w, err := wal.NewWAL(tmpDir, s)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Populate WAL
	row1 := map[string]any{"ts": int64(1), "val": 1.1}
	row2 := map[string]any{"ts": int64(2), "val": 2.2}

	if err := w.AppendRow(row1); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendRow(row2); err != nil {
		t.Fatal(err)
	}

	// 3. Replay into a fresh Table
	tbl, err := table.CreateTable(s, w)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.ReplayTable(tbl); err != nil {
		t.Fatal(err)
	}

	// Check table contents
	if tbl.RowCount() != 2 {
		t.Errorf("Expected 2 rows after replay, got %d", tbl.RowCount())
	}

	// 4. Close
	if err := w.Reset(); err != nil {
		t.Fatal(err)
	}
}
