package table

import (
	"backtraceDB/internal/schema"
	"fmt"
	"os"
	"testing"
)

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
