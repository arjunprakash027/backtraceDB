// possible optimizations
package table

import (
	"backtraceDB/internal/schema"
	"backtraceDB/internal/wal"
	"fmt"
)

type Table struct {
	schema       schema.Schema
	activeBlock  *Block
	coldBlocks   []*Block
	locations    []ColumnLocation
	timeColIdx   int
	lastTs       int
	rowCount     int
	wal          *wal.WAL
	MaxBlockSize int
}

type Predicate struct {
	ColName string
	Op      string
	Value   any
}

type TableReader struct {
	table           *Table
	blocks          []*Block
	currentBlockIdx int
	currentStorage  *ColumnStorage
	predicates      []Predicate

	localMask   []bool
	localCursor int
}

func (t *Table) Reader() *TableReader {
	allBlocks := make([]*Block, 0, len(t.coldBlocks)+1)
	allBlocks = append(allBlocks, t.coldBlocks...)
	allBlocks = append(allBlocks, t.activeBlock)

	return &TableReader{
		table:           t,
		blocks:          allBlocks,
		currentBlockIdx: 0,
		currentStorage:  nil,
		predicates:      []Predicate{},
		localMask:       nil,
		localCursor:     0,
	}
}

func (tr *TableReader) Next() (map[string]any, bool) {

	for {
		if tr.currentStorage == nil || tr.localCursor >= len(tr.localMask) {

			if tr.currentStorage != nil {
				tr.currentBlockIdx++
				tr.currentStorage = nil
				tr.localMask = nil
				tr.localCursor = 0
			}

			if err := tr.LoadNextBlock(); err != nil {
				return nil, false
			}
		}

		for tr.localCursor < len(tr.localMask) && !tr.localMask[tr.localCursor] {
			tr.localCursor++
		}

		if tr.localCursor < len(tr.localMask) {
			row := make(map[string]any)

			for logicalIdx, col := range tr.table.schema.Columns {
				loc := tr.table.locations[logicalIdx]
				switch col.Type {
				case schema.Int64:
					row[col.Name] = tr.currentStorage.Int64Cols[loc.Index][tr.localCursor]
				case schema.Float64:
					row[col.Name] = tr.currentStorage.Float64Cols[loc.Index][tr.localCursor]
				case schema.String:
					strID := tr.currentStorage.StringCols[loc.Index][tr.localCursor]
					row[col.Name] = tr.currentStorage.StringReads[loc.Index][strID]
				}
			}

			tr.localCursor++
			return row, true
		}

	}

}

func (tr *TableReader) evalInt64(a int64, op string, b int64) bool {
	switch op {
	case "==":
		return a == b
	case ">":
		return a > b
	case "<":
		return a < b
	case ">=":
		return a >= b
	case "<=":
		return a <= b
	case "!=":
		return a != b
	}
	return false
}

func (tr *TableReader) evalFloat64(a float64, op string, b float64) bool {
	switch op {
	case "==":
		return a == b
	case ">":
		return a > b
	case "<":
		return a < b
	case ">=":
		return a >= b
	case "<=":
		return a <= b
	case "!=":
		return a != b
	}
	return false
}

func (tr *TableReader) evalString(a string, op string, b string) bool {
	switch op {
	case "==":
		return a == b
	case "!=":
		return a != b
	}
	return false
}

func (tr *TableReader) Filter(colName string, op string, value any) *TableReader {

	tr.predicates = append(tr.predicates,
		Predicate{ColName: colName,
			Op:    op,
			Value: value,
		})

	tr.currentBlockIdx = 0
	tr.currentStorage = nil
	tr.localCursor = 0
	tr.localMask = nil

	return tr
}

func (tr *TableReader) LoadNextBlock() error {

	if tr.currentBlockIdx >= len(tr.blocks) {
		return fmt.Errorf("no more blocks to load")
	}

	block := tr.blocks[tr.currentBlockIdx]

	if !block.isOnDisk {
		tr.currentStorage = block.Storage
	} else {
		colTypes := make([]schema.ColumnType, len(tr.table.schema.Columns))

		for i, col := range tr.table.schema.Columns {
			colTypes[i] = col.Type
		}

		storage, _, err := NewColumnStorage(colTypes)
		if err != nil {
			return err
		}

		if err := block.LoadInto(storage, tr.table.schema, tr.table.locations); err != nil {
			return err
		}

		tr.currentStorage = storage
	}

	tr.localMask = make([]bool, block.RowCount)
	for i := range tr.localMask {
		tr.localMask[i] = true
	}

	for _, pred := range tr.predicates {
		tr.applyPredicates(pred)
	}

	return nil

}

func (tr *TableReader) applyPredicates(p Predicate) error {
	var loc ColumnLocation
	var found bool

	for i, col := range tr.table.schema.Columns {
		if col.Name == p.ColName {
			loc = tr.table.locations[i]
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("column %s not found", p.ColName)
	}

	count := len(tr.localMask)

	for i := 0; i < count; i++ {
		if !tr.localMask[i] {
			continue
		}

		match := false

		switch loc.Type {
		case schema.Int64:
			val := tr.currentStorage.Int64Cols[loc.Index][i]

			if target, ok := p.Value.(int64); ok {
				match = tr.evalInt64(val, p.Op, target)
			} else if target, ok := p.Value.(int); ok {
				match = tr.evalInt64(val, p.Op, int64(target))
			} else {
				return fmt.Errorf("invalid value type for int64 column %s: %T", p.ColName, p.Value)
			}
		case schema.Float64:
			val := tr.currentStorage.Float64Cols[loc.Index][i]
			if target, ok := p.Value.(float64); ok {
				match = tr.evalFloat64(val, p.Op, target)
			} else {
				return fmt.Errorf("invalid value type for float64 column %s: %T", p.ColName, p.Value)
			}
		case schema.String:
			strID := tr.currentStorage.StringCols[loc.Index][i]
			val := tr.currentStorage.StringReads[loc.Index][strID]
			if target, ok := p.Value.(string); ok {
				match = tr.evalString(val, p.Op, target)
			} else {
				return fmt.Errorf("invalid value type for string column %s: %T", p.ColName, p.Value)
			}
		}

		if !match {
			tr.localMask[i] = false
		}
	}

	return nil
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

	block, locations, err := NewBlock(colTypes)
	if err != nil {
		return nil, err
	}

	return &Table{
		schema:       s,
		activeBlock:  block,
		coldBlocks:   []*Block{},
		locations:    locations,
		timeColIdx:   timeIdx,
		lastTs:       -1,
		rowCount:     0,
		wal:          w,
		MaxBlockSize: 10_000_000,
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

	if ts < int64(t.lastTs) {
		return fmt.Errorf("time column %s must be in non-decreasing order", timeColName)
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
			t.activeBlock.Storage.Int64Cols[loc.Index] = append(t.activeBlock.Storage.Int64Cols[loc.Index], v)

		case schema.Float64:
			v, ok := val.(float64)
			if !ok {
				return fmt.Errorf("column %s must be of type float64", col.Name)
			}
			t.activeBlock.Storage.Float64Cols[loc.Index] = append(t.activeBlock.Storage.Float64Cols[loc.Index], v)

		case schema.String:
			v, ok := val.(string)
			if !ok {
				return fmt.Errorf("column %s must be of type string", col.Name)
			}

			dict := t.activeBlock.Storage.StringDicts[loc.Index]
			id, exists := dict[v]
			if !exists {
				id = len(dict)
				dict[v] = id
				t.activeBlock.Storage.StringReads[loc.Index] = append(t.activeBlock.Storage.StringReads[loc.Index], v)
			}
			t.activeBlock.Storage.StringCols[loc.Index] = append(t.activeBlock.Storage.StringCols[loc.Index], id)

		default:
			return fmt.Errorf("unsupported column type: %v", loc.Type)
		}
	}

	t.activeBlock.RowCount++
	t.rowCount++
	t.lastTs = int(ts)

	if t.activeBlock.RowCount >= t.MaxBlockSize {
		path := fmt.Sprintf("data_internal/%s/%d_block.parquet", t.schema.Name, t.lastTs)
		if err := t.activeBlock.Flush(path, t.schema, t.locations); err != nil {
			return fmt.Errorf("failed to flush block: %v", err)
		}

		t.coldBlocks = append(t.coldBlocks, t.activeBlock)
		colTypes := make([]schema.ColumnType, len(t.schema.Columns))
		for i, col := range t.schema.Columns {
			colTypes[i] = col.Type
		}
		nextBlock, _, err := NewBlock(colTypes)
		if err != nil {
			return fmt.Errorf("failed to create new block: %v", err)
		}
		t.activeBlock = nextBlock
	}

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
