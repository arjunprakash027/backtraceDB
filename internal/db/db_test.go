package db

import (
	"backtraceDB/internal/schema"
	"os"
	"testing"
)

func TestDBWorkFlow(t *testing.T) {
	dbName := "workflow_test"
	defer os.RemoveAll(dbName)

	// 1. Open DB
	database, err := Open(dbName)
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
			{Name: "level", Type: schema.Float64},
		},
	}

	// 3. Create Table

	opts := &CreateTableOptions{}

	tbl, err := database.CreateTable(s, opts)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 4. Append Rows
	rows := []map[string]any{
		{"timestamp": int64(100), "message": "error found", "level": 1.0},
		{"timestamp": int64(200), "message": "system restart", "level": 2.0},
	}

	for _, row := range rows {
		if err := tbl.AppendRow(row); err != nil {
			t.Errorf("Failed to append row: %v", err)
		}
	}

	// 5. Verification
	if tbl.RowCount() != 2 {
		t.Errorf("Expected row count 2, got %d", tbl.RowCount())
	}
}

func TestRecoveryWorkflow(t *testing.T) {
	dbName := "recovery_test"
	tableName := "metrics"
	defer os.RemoveAll(dbName)

	s := schema.Schema{
		Name:       tableName,
		TimeColumn: "timestamp",
		Columns: []schema.Column{
			{Name: "timestamp", Type: schema.Int64},
			{Name: "value", Type: schema.Float64},
		},
	}

	// PHASE 1: Write data and "close"
	{
		database, err := Open(dbName)
		if err != nil {
			t.Fatal(err)
		}

		opts := &CreateTableOptions{
			EnableWal: true,
		}

		tbl, err := database.CreateTable(s, opts)
		if err != nil {
			t.Fatal(err)
		}

		_ = tbl.AppendRow(map[string]any{"timestamp": int64(10), "value": 1.1})
		_ = tbl.AppendRow(map[string]any{"timestamp": int64(20), "value": 2.2})

		// In a real database we'd have a Close method,
		// but our WAL is synced on every write!
	}

	// PHASE 2: Open and Recover
	{
		database, err := Open(dbName)
		if err != nil {
			t.Fatal(err)
		}

		// OpenTable triggers the WAL Replay
		tbl, err := database.OpenTable(s)
		if err != nil {
			t.Fatalf("Failed to recover table: %v", err)
		}

		if tbl.RowCount() != 2 {
			t.Errorf("Recovery failed! Expected 2 rows, got %d", tbl.RowCount())
		}
	}
}
