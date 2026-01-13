package db

import (
	"fmt"
	"sync"
	"backtraceDB/internal/table"
	"backtraceDB/internal/schema"
)

type DB struct {
	mu sync.RWMutex
	tables map[string]*table.Table
}

func Open() (*DB, error) {
	return &DB{
		tables: make(map[string]*table.Table),
	}, nil
}

func (db *DB) CreateTable(s schema.Schema) (*table.Table, error) {

	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.tables[s.Name]; ok {
		return nil, fmt.Errorf("table %s already exists", s.Name)
	}

	t, err := table.CreateTable(s)
	if err != nil {
		return nil, err
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
