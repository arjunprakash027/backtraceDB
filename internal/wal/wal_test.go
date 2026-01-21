package wal_test

import (
	"backtraceDB/internal/schema"
	"backtraceDB/internal/table"
	"backtraceDB/internal/wal"
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

	t.Run("AppendAndReplay", func(t *testing.T) {
		defer os.RemoveAll(tmpDir)
		w, err := wal.NewWAL(tmpDir, s)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()

		row1 := map[string]any{"ts": int64(1), "val": 1.1}
		row2 := map[string]any{"ts": int64(2), "val": 2.2}

		if err := w.AppendRow(row1); err != nil {
			t.Fatal(err)
		}
		if err := w.AppendRow(row2); err != nil {
			t.Fatal(err)
		}

		tbl, _ := table.CreateTable(s, w, "test_db")
		if err := w.ReplayTable(tbl); err != nil {
			t.Fatal(err)
		}

		if tbl.RowCount() != 2 {
			t.Errorf("Expected 2 rows after replay, got %d", tbl.RowCount())
		}
	})

	t.Run("ResetBehavior", func(t *testing.T) {
		defer os.RemoveAll(tmpDir)
		w, err := wal.NewWAL(tmpDir, s)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()

		// 1. Append rows
		w.AppendRow(map[string]any{"ts": int64(1), "val": 1.1})

		// 2. Reset
		if err := w.Reset(); err != nil {
			t.Fatal(err)
		}

		// 3. Replay into fresh table - should be empty
		tbl, _ := table.CreateTable(s, w, "test_db")
		if err := w.ReplayTable(tbl); err != nil {
			t.Fatal(err)
		}
		if tbl.RowCount() != 0 {
			t.Errorf("Expected 0 rows after reset, got %d", tbl.RowCount())
		}

		// 4. Append more after reset
		w.AppendRow(map[string]any{"ts": int64(2), "val": 2.2})

		// 5. Replay again - should have only the new row
		tbl2, _ := table.CreateTable(s, w, "test_db")
		if err := w.ReplayTable(tbl2); err != nil {
			t.Fatal(err)
		}
		if tbl2.RowCount() != 1 {
			t.Errorf("Expected 1 row after append-post-reset, got %d", tbl2.RowCount())
		}
	})
}
