package table

import (
	"backtraceDB/internal/schema"
	"testing"
)

func TestTableReaderSimple(t *testing.T) {
	s := schema.Schema{
		Name: "test", TimeColumn: "ts",
		Columns: []schema.Column{{Name: "ts", Type: schema.Int64}},
	}
	tbl, _ := CreateTable(s, nil)
	tbl.AppendRow(map[string]any{"ts": int64(1)})
	tbl.AppendRow(map[string]any{"ts": int64(2)})

	r := tbl.Reader()
	row, ok := r.Next()
	if !ok || row["ts"] != int64(1) {
		t.Errorf("Expected ts 1, got %v", row["ts"])
	}
}

func TestTableFilterMinimal(t *testing.T) {
	s := schema.Schema{
		Name: "test", TimeColumn: "ts",
		Columns: []schema.Column{{Name: "ts", Type: schema.Int64}},
	}
	tbl, _ := CreateTable(s, nil)
	tbl.AppendRow(map[string]any{"ts": int64(10)})
	tbl.AppendRow(map[string]any{"ts": int64(20)})

	// Filter for ts > 15
	r := tbl.Reader().Filter("ts", ">", int64(15))

	row, ok := r.Next()
	if !ok || row["ts"] != int64(20) {
		t.Fatal("Filter failed to find matching row")
	}

	if _, ok := r.Next(); ok {
		t.Error("Should only have 1 matching row")
	}
}
