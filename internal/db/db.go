package db

import (
	"backtraceDB/internal/schema"
	"backtraceDB/internal/table"
	"backtraceDB/internal/wal"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type DB struct {
	mu sync.RWMutex
	name string
	tables map[string]*table.Table
}

type CreateTableOptions struct { //options while creating a table, simple for now, will add as database improves with more options
	EnableWal bool
}

func Open(name string) (*DB, error) {
	return &DB{
		name: name,
		tables: make(map[string]*table.Table),
	}, nil
}

func (db *DB) CreateTable(s schema.Schema, opts *CreateTableOptions) (*table.Table, error) {

	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.tables[s.Name]; ok {
		return nil, fmt.Errorf("table %s already exists", s.Name)
	}

	var w *wal.WAL

	if opts.EnableWal {
		tablePath := filepath.Join(db.name, s.Name)
		walPath := filepath.Join(tablePath, "wal")
		var err error
		w, err = wal.NewWAL(walPath, s)
		if err != nil {
			return nil, err
		}
	}
	
	t, err := table.CreateTable(s, w)
	if err != nil {
		return nil, err
	}

	db.tables[s.Name] = t
	return t, nil
}

func (db *DB) OpenTable(s schema.Schema) (*table.Table, error) {
	
	db.mu.Lock()
	defer db.mu.Unlock()

	if tbl, ok := db.tables[s.Name]; ok {
		return tbl, nil
	}

	tablePath := filepath.Join(db.name, s.Name)
	walPath := filepath.Join(tablePath, "wal")

	wal, err := wal.NewWAL(walPath, s)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %v", err)
	}

	t, err := table.CreateTable(s, wal)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}

	if err := wal.ReplayTable(t); err != nil {
		return nil, fmt.Errorf("failed to replay WAL: %v", err)
	}

	db.tables[s.Name] = t
	return t, nil
}

func (db *DB) getTableName(name string) (*table.Table, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if tbl, ok := db.tables[name]; ok {
		return tbl, true
	}

	return nil, false
}

func (db *DB) ListAllTables() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	names := make([]string, 0, len(db.tables))
	for name := range db.tables {
		names = append(names, name)
	}
	return names
}

func (db *DB) Close() error {
	return os.RemoveAll("data_internal")
}
