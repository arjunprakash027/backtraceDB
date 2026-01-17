// possible optimizations
package table

import (
	"backtraceDB/internal/schema"
	"backtraceDB/internal/wal"
	"fmt"
	"sort"
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

type TableReader struct {
	table *Table
	cursor int
	endIdx int
	mask []bool
}

func (t *Table) Reader() *TableReader {
	return &TableReader{
		table: t,
		cursor: 0,
		endIdx: t.rowCount,
	}
}

func (tr *TableReader) Next() (map[string]any, bool) {

	for tr.cursor < tr.endIdx && tr.mask != nil && !tr.mask[tr.cursor] {
		tr.cursor++
	}

	if tr.cursor >= tr.endIdx {
		return nil, false
	}

	row := make(map[string]any)
	t := tr.table

	for logicalIdx, col := range t.schema.Columns {
		loc := t.locations[logicalIdx]

		switch loc.Type {
		case schema.Int64:
			row[col.Name] = t.storage.Int64Cols[loc.Index][tr.cursor]
		case schema.Float64:
			row[col.Name] = t.storage.Float64Cols[loc.Index][tr.cursor]
		case schema.String:
			strID := t.storage.StringCols[loc.Index][tr.cursor]
			row[col.Name] = t.storage.StringReads[loc.Index][strID]
		}
	}

	tr.cursor++

	return row, true

}

func (tr *TableReader) ReadColumn(colName string) (any, error) { //we are not returning string column data for now

	for i, col := range tr.table.schema.Columns {
		if col.Name == colName {
			loc := tr.table.locations[i]

			switch col.Type {
			case schema.Int64:
				return tr.table.storage.Int64Cols[loc.Index], nil
			case schema.Float64:
				return tr.table.storage.Float64Cols[loc.Index], nil
			}
		}
	}

	return nil, fmt.Errorf("column %s not found", colName)
}

func (tr *TableReader) evalInt64(a int64, op string, b int64) bool {
	switch op {
	case "==": return a == b
	case ">":  return a > b
	case "<":  return a < b
	case ">=": return a >= b
	case "<=": return a <= b
	case "!=": return a != b
	}
	return false
}
func (tr *TableReader) evalFloat64(a float64, op string, b float64) bool {
    switch op {
    case "==": return a == b
	case ">":  return a > b
	case "<":  return a < b
	case ">=": return a >= b
	case "<=": return a <= b
	case "!=": return a != b
    }
    return false
}

func (tr *TableReader) Filter(colName string, op string, value any) *TableReader {

	if tr.mask == nil {
		tr.mask = make([]bool, tr.table.rowCount)
		for i := range tr.mask {
			tr.mask[i] = true
		}
	}

	if colName == tr.table.schema.TimeColumn {
		Col, err := tr.ReadColumn(colName)
		if err != nil {
			return tr
		}
		slice := Col.([]int64)
		targetTs := value.(int64)

		// first idx where slice is equal to target and increasing 
		idx := sort.Search(len(slice), func(i int) bool {
			return slice[i] >= targetTs
		})
		
		// first idx where slice is greater than target and increasing
		upperIdx := sort.Search(len(slice), func(i int) bool {
			return slice[i] > targetTs
		})
		
		if op == "==" {

			if idx > tr.cursor { tr.cursor = idx }
			if upperIdx < tr.endIdx { tr.endIdx = upperIdx }

			for i := 0; i < idx; i++ { tr.mask[i] = false }
			for i := upperIdx; i < len(slice); i++ { tr.mask[i] = false }

		} else if op == "!=" {
			for i := idx; i < upperIdx; i++ {
				tr.mask[i] = false
			}

		} else if op == ">" {

			if upperIdx > tr.cursor { tr.cursor = upperIdx }
			for i := 0; i < upperIdx; i++ {
				tr.mask[i] = false
			}
		} else if op == ">=" {

			if idx > tr.cursor { tr.cursor = idx }
			for i := 0; i < idx; i++ {
				tr.mask[i] = false
			}
		} else if op == "<" {

			if idx < tr.endIdx { tr.endIdx = idx }
			for i := idx; i < len(slice); i++ {
				tr.mask[i] = false
			}
		} else if op == "<=" {

			if upperIdx < tr.endIdx { tr.endIdx = upperIdx }
			for i := upperIdx; i < len(slice); i++ {
				tr.mask[i] = false
			}
		}

		return tr

	}

	switch targetVal := value.(type) {
	case int64:
		Col, err := tr.ReadColumn(colName)
		if err != nil {
			return tr
		}

		slice := Col.([]int64)

		for i, v := range slice {
			if !tr.evalInt64(v, op, targetVal){
				tr.mask[i] = false
			}
		}
		
	case float64:
		Col, err := tr.ReadColumn(colName)
		if err != nil {
			return tr
		}

		slice := Col.([]float64)

		for i, v := range slice {
			if !tr.evalFloat64(v, op, targetVal){
				tr.mask[i] = false
			}
		}
			
	}

	return tr
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
					t.storage.StringReads[loc.Index] = append(t.storage.StringReads[loc.Index], v)
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
