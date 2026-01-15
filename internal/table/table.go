// possible optimizations
package table

import (
	"backtraceDB/internal/schema"
	"backtraceDB/internal/wal"
	"fmt"
)

type Table struct {
	schema     schema.Schema
	storage    *ColumnStorage
	locations  []ColumnLocation
	timeColIdx int
	lastTs     int
	rowCount   int
	wal        *wal.WAL
}

func CreateTable(s schema.Schema, w *wal.WAL) (*Table, error) {

	if err := s.Validate(); err != nil {
		return nil, err
	}

	colTypes := make([]schema.ColumnType, len(s.Columns))
	timeIdx := -1

	for i, col := range s.Columns {
		colTypes[i] = col.Type

		if col.Name == s.TimeColumn {
			timeIdx = i
		}
	}

	if timeIdx == -1 {
		return nil, fmt.Errorf("time column %s not found", s.TimeColumn)
	}

	storage, locations, err := NewColumnStorage(colTypes)
	if err != nil {
		return nil, err
	}

	return &Table{
		schema:     s,
		storage:    storage,
		locations:  locations,
		timeColIdx: timeIdx,
		lastTs:     -1,
		rowCount:   0,
		wal:        w,
	}, nil
}

func (t *Table) AppendHelper(row map[string]any) error {
	if len(row) != len(t.schema.Columns) {
			return fmt.Errorf("row must have %d columns, got %d", len(t.schema.Columns), len(row))
		}

		timeColName := t.schema.Columns[t.timeColIdx].Name

		rowTs, ok := row[timeColName]
		if !ok {
			return fmt.Errorf("row must have time column %s", timeColName)
		}

		ts, ok := rowTs.(int64)
		if !ok {
			return fmt.Errorf("time column %s must be of type int64", timeColName)
		}

		if ts <= int64(t.lastTs) {
			return fmt.Errorf("time column %s must be in increasing order", timeColName)
		}

		for logicalIdx, col := range t.schema.Columns {

			val, ok := row[col.Name]
			if !ok {
				return fmt.Errorf("row must have column %s", col.Name)
			}

			loc := t.locations[logicalIdx]

			switch loc.Type {
			case schema.Int64:
				v, ok := val.(int64)
				if !ok {
					return fmt.Errorf("column %s must be of type int64", col.Name)
				}
				t.storage.Int64Cols[loc.Index] = append(t.storage.Int64Cols[loc.Index], v)

			case schema.Float64:
				v, ok := val.(float64)
				if !ok {
					return fmt.Errorf("column %s must be of type float64", col.Name)
				}
				t.storage.Float64Cols[loc.Index] = append(t.storage.Float64Cols[loc.Index], v)

			case schema.String:
				v, ok := val.(string)
				if !ok {
					return fmt.Errorf("column %s must be of type string", col.Name)
				}

				dict := t.storage.StringDicts[loc.Index]
				id, exists := dict[v]
				if !exists {
					id = len(dict)
					dict[v] = id
				}
				t.storage.StringCols[loc.Index] = append(t.storage.StringCols[loc.Index], id)

			default:
				return fmt.Errorf("unsupported column type: %v", loc.Type)
			}
		}

		t.rowCount++
		t.lastTs = int(ts)

		return nil
}
func (t *Table) AppendRow(row map[string]any) error {
	
	if t.wal != nil {
		if err := t.wal.AppendRow(row); err != nil {
			return fmt.Errorf("failed to append row to WAL: %v", err)
		}
	}
	return t.AppendHelper(row)
}

func (t *Table) LoadRowNoWAL(row map[string]any) error {
	return t.AppendHelper(row)
}

func (t *Table) RowCount() int {
	return t.rowCount
}
