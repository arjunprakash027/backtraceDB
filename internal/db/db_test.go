package db

import (
	"backtraceDB/internal/schema"
	"testing"
)

func TestDBWorkFlow(t *testing.T) {
	// 1. Open DB
	database, err := Open()
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	// 2. Define Schema
	s := schema.Schema{
		Name:       "logs",
		TimeColumn: "timestamp",
		Columns: []schema.Column{
			{Name: "timestamp", Type: schema.Int64},
			{Name: "message", Type: schema.String},
			{Name: "level", Type: schema.Float64}, // Using float for logs just to test all types
		},
	}

	// 3. Create Table
	tbl, err := database.CreateTable(s)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 4. Append Rows
	rows := []map[string]any{
		{"timestamp": int64(100), "message": "error found", "level": 1.0},
		{"timestamp": int64(200), "message": "system restart", "level": 2.0},
		{"timestamp": int64(300), "message": "error found", "level": 1.5}, // Reuse "error found" string
	}

	for _, row := range rows {
		if err := tbl.AppendRow(row); err != nil {
			t.Errorf("Failed to append row: %v", err)
		}
	}

	// 5. Verification
	if tbl.RowCount() != 3 {
		t.Errorf("Expected row count 3, got %d", tbl.RowCount())
	}

	tables := database.ListAllTables()
	found := false
	for _, name := range tables {
		if name == "logs" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Table 'logs' not found in ListAllTables")
	}

	// 6. Test Error Handling: Duplicate Table
	_, err = database.CreateTable(s)
	if err == nil {
		t.Error("Expected error when creating duplicate table, got nil")
	}

	// 7. Test Error Handling: Out of order timestamp
	badRow := map[string]any{"timestamp": int64(50), "message": "old log", "level": 0.5}
	if err := tbl.AppendRow(badRow); err == nil {
		t.Error("Expected error for out-of-order timestamp, got nil")
	}
}
