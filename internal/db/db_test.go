package db

import (
	"backtraceDB/internal/schema"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestDBWorkFlow(t *testing.T) {
	dbName := "workflow_test"
	defer os.RemoveAll(filepath.Join("_data_internal", dbName))

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

	tbl, err := database.CreateTable(s)
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
	defer os.RemoveAll(filepath.Join("_data_internal", dbName))

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

		tbl, err := database.CreateTable(s)
		if err != nil {
			t.Fatal(err)
		}

		tbl.UseDiskStorage = true
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

func TestFullPersistenceAndRecovery(t *testing.T) {
	dbName := "full_recovery_test"
	tableName := "sensor_data"
	dataRoot := "_data_internal"

	// Clean up only the specific test database
	testDbPath := filepath.Join(dataRoot, dbName)
	os.RemoveAll(testDbPath)
	defer os.RemoveAll(testDbPath)

	s := schema.Schema{
		Name:       tableName,
		TimeColumn: "ts",
		Columns: []schema.Column{
			{Name: "ts", Type: schema.Int64},
			{Name: "value", Type: schema.Float64},
			{Name: "status", Type: schema.String},
		},
	}

	// PHASE 1: Write data and trigger multiple blocks
	expectedRows := 12
	{
		database, err := Open(dbName)
		if err != nil {
			t.Fatalf("Phase 1: Failed to open DB: %v", err)
		}

		tbl, err := database.CreateTable(s)
		if err != nil {
			t.Fatalf("Phase 1: Failed to create table: %v", err)
		}

		// Force multiple blocks
		tbl.MaxBlockSize = 5
		tbl.UseDiskStorage = true

		for i := 0; i < expectedRows; i++ {
			row := map[string]any{
				"ts":     int64(i * 100),
				"value":  float64(i) * 1.5,
				"status": fmt.Sprintf("msg_%d", i),
			}
			if err := tbl.AppendRow(row); err != nil {
				t.Fatalf("Phase 1: Failed to append row %d: %v", i, err)
			}
		}

		// Close DB to ensure active block and metadata are persisted
		if err := database.Close(); err != nil {
			t.Fatalf("Phase 1: Failed to close DB: %v", err)
		}
	}

	// PHASE 2: Check Disk Structure
	tableDir := filepath.Join(dataRoot, dbName, tableName)
	files, err := os.ReadDir(tableDir)
	if err != nil {
		t.Fatalf("Phase 2: Could not read table directory: %v", err)
	}

	parquetCount := 0
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".parquet" {
			parquetCount++
			// Verify name format: Ts<ts>R<rc>i<iter>.parquet
			if f.Name()[0:2] != "Ts" {
				t.Errorf("Phase 2: Unexpected filename format: %s", f.Name())
			}
		}
	}

	// Expected blocks: 12 rows with size 5 -> 5 (i0), 5 (i1), 2 (i2) = 3 blocks
	if parquetCount != 3 {
		t.Errorf("Phase 2: Expected 3 parquet blocks on disk, found %d", parquetCount)
	}

	// PHASE 3: Recovery
	{
		database, err := Open(dbName)
		if err != nil {
			t.Fatalf("Phase 3: Failed to open DB: %v", err)
		}
		defer database.Close()

		// OpenTable should trigger LoadFromDisk
		tbl, err := database.OpenTable(s)
		if err != nil {
			t.Fatalf("Phase 3: Failed to open table: %v", err)
		}

		if tbl.RowCount() != expectedRows {
			t.Errorf("Phase 3: Row count mismatch! Expected %d, got %d", expectedRows, tbl.RowCount())
		}

		// Read back all data to verify content
		reader := tbl.Reader()
		count := 0
		for {
			row, ok := reader.Next()
			if !ok {
				break
			}

			expectedTs := int64(count * 100)
			if row["ts"].(int64) != expectedTs {
				t.Errorf("Phase 3: Data mismatch at row %d. Expected ts %d, got %d", count, expectedTs, row["ts"])
			}

			expectedStatus := fmt.Sprintf("msg_%d", count)
			if row["status"].(string) != expectedStatus {
				t.Errorf("Phase 3: Data mismatch at row %d. Expected status %s, got %s", count, expectedStatus, row["status"])
			}
			count++
		}

		if count != expectedRows {
			t.Errorf("Phase 3: Only read %d rows back from storage, expected %d", count, expectedRows)
		}
	}
}
