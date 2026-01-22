// possible optimizations
package table

import (
	"backtraceDB/internal/schema"
	"backtraceDB/internal/wal"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/parquet-go/parquet-go"
)

type Table struct {
	schema         schema.Schema
	activeBlock    *Block
	coldBlocks     []*Block
	locations      []ColumnLocation
	timeColIdx     int
	lastTs         int
	rowCount       int
	wal            *wal.WAL
	MaxBlockSize   int
	UseDiskStorage bool
	dbName         string
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

func (tr *TableReader) Next() (map[string]any, bool) { // L1 optimization : instead of retruning single row, return vectorized values at single next pass

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

func (t *Table) getColumnLocation(colName string) (ColumnLocation, bool) {
	for i, col := range t.schema.Columns {
		if col.Name == colName {
			return t.locations[i], true
		}
	}
	return ColumnLocation{}, false
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

	for tr.currentBlockIdx < len(tr.blocks) {
		block := tr.blocks[tr.currentBlockIdx]
		
		if block.Storage != nil {
			break
		}

		skipBlock := false
		for _, pred := range tr.predicates {
			skip, err := tr.CanSkip(block, pred)
			if err != nil {
				return err
			}
			if skip {
				skipBlock = true
				break
			}
		}

		if !skipBlock {
			break
		}

		tr.currentBlockIdx++
	}

	block := tr.blocks[tr.currentBlockIdx]

	if !block.isOnDisk && block.Storage != nil {
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

func (tr *TableReader) CanSkip(block *Block, predicate Predicate) (bool, error) {
	loc, found := tr.table.getColumnLocation(predicate.ColName)
	if !found {
		return false, fmt.Errorf("column %s not found", predicate.ColName)
	}

	var min, max, target float64
	
	switch loc.Type {
	case schema.Int64:
		target = float64(0)
		if v, ok := predicate.Value.(int64); ok {
			target = float64(v)
		} else if v, ok := predicate.Value.(int); ok {
			target = float64(v)
		} else {
			return false, fmt.Errorf("invalid value type for int64 column: %T", predicate.Value)
		}

		min = float64(block.IntMin[loc.Index])
		max = float64(block.IntMax[loc.Index])

	case schema.Float64:
		target = float64(0)
		if v, ok := predicate.Value.(float64); ok {
			target = v
		} else if v, ok := predicate.Value.(int); ok {
			target = float64(v)
		} else {
			return false, fmt.Errorf("invalid value type for float64 column: %T", predicate.Value)
		}

		min = block.FloatMin[loc.Index]
		max = block.FloatMax[loc.Index]
	default:
		return false, nil
	}

	switch predicate.Op {
	case "==":
		return target < min || target > max, nil
	case "!=":
		return min == target && max == target, nil
	case ">":
		return max <= target, nil
	case ">=":
		return max < target, nil
	case "<":
		return min >= target, nil
	case "<=":
		return min > target, nil
	default:
		return false, nil
	}

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

func CreateTable(s schema.Schema, w *wal.WAL, dbName string) (*Table, error) {

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

	t := &Table{
		schema:       s,
		activeBlock:  block,
		coldBlocks:   []*Block{},
		locations:    locations,
		timeColIdx:   timeIdx,
		lastTs:       -1,
		rowCount:     0,
		wal:          w,
		MaxBlockSize: 10_000_000,
		dbName:       dbName,
	}

	return t, nil
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
	t.activeBlock.MaxTs = ts

	if t.activeBlock.RowCount >= t.MaxBlockSize {
		path := ""

		if t.UseDiskStorage {
			path = filepath.Join("_data_internal", t.dbName, t.schema.Name, fmt.Sprintf("Ts%dR%di%d.parquet", t.activeBlock.MaxTs, t.activeBlock.RowCount, len(t.coldBlocks)))
		}

		if err := t.activeBlock.Rotate(t.UseDiskStorage, path, t.schema, t.locations); err != nil {
			return fmt.Errorf("failed to flush block: %v", err)
		}

		if t.wal != nil && t.UseDiskStorage {
			if err := t.wal.Reset(); err != nil {
				return fmt.Errorf("failed to reset WAL after rotation: %v", err)
			}
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

	if t.UseDiskStorage && t.wal == nil {
		tablePath := filepath.Join("_data_internal", t.dbName, t.schema.Name)
		walPath := filepath.Join(tablePath, "wal")
		var err error
		t.wal, err = wal.NewWAL(walPath, t.schema)
		if err != nil {
			return fmt.Errorf("failed to create WAL: %v", err)
		}
	}

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

func (t *Table) Close() error {
	if t.activeBlock.RowCount > 0 {
		path := filepath.Join("_data_internal", t.dbName, t.schema.Name, fmt.Sprintf("Ts%dR%di%d.parquet", t.activeBlock.MaxTs, t.activeBlock.RowCount, len(t.coldBlocks)))
		if err := t.activeBlock.Persist(path, t.schema, t.locations); err != nil {
			return fmt.Errorf("failed to persist active block: %v", err)
		}

		if t.wal != nil {
			if err := t.wal.Reset(); err != nil {
				return fmt.Errorf("failed to reset WAL on close: %v", err)
			}
		}
	}

	for i, block := range t.coldBlocks {
		if !block.isOnDisk {
			path := filepath.Join("_data_internal", t.dbName, t.schema.Name, fmt.Sprintf("Ts%dR%di%d.parquet", block.MaxTs, block.RowCount, i))
			if err := block.Persist(path, t.schema, t.locations); err != nil {
				return fmt.Errorf("failed to persist cold block %d: %v", i, err)
			}
		}
	}

	return nil
}

func (t *Table) LoadFromDisk() error {
	dirPath := filepath.Join("_data_internal", t.dbName, t.schema.Name)

	info, err := os.Stat(dirPath)

	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to check table directory: %v", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("path %s is not a directory", dirPath)
	}

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read table directory: %v", err)
	}

	var loadedBlocks []*Block

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".parquet") {
			continue
		}

		fullPath := filepath.Join(dirPath, name)

		var maxTs int64
		var rowCount int
		var iter int
		_, err := fmt.Sscanf(name, "Ts%dR%di%d.parquet", &maxTs, &rowCount, &iter)
		if err != nil {
			continue
		}

		block := &Block{
			Path:     fullPath,
			MaxTs:    maxTs,
			RowCount: rowCount,
			isOnDisk: true,
			isClosed: true,
		}
		
		// fill the statistics of the loaded blocks for filter to be efficient
		f, err := os.Open(fullPath)
		if err != nil {
			continue // or handle error
		}
		defer f.Close()
		stat, _ := f.Stat()
		pf, err := parquet.OpenFile(f, stat.Size())
		if err != nil {
			continue
		}
		numInt, numFloat := 0, 0
		for _, col := range t.schema.Columns {
			if col.Type == schema.Int64 {
				numInt++
			}
			if col.Type == schema.Float64 {
				numFloat++
			}
		}
		block.IntMin = make([]int64, numInt)
		block.IntMax = make([]int64, numInt)
		block.FloatMin = make([]float64, numFloat)
		block.FloatMax = make([]float64, numFloat)

		intCount, floatCount := 0, 0
		rowGroup := pf.RowGroups()[0]
		for i, col := range t.schema.Columns {

			if i >= len(rowGroup.ColumnChunks()) {
				break
			}

			chunk := rowGroup.ColumnChunks()[i]
			idx, err := chunk.ColumnIndex()

			if err == nil && idx != nil && idx.NumPages() > 0 {
				switch col.Type {
				case schema.Int64:
					block.IntMin[intCount] = idx.MinValue(0).Int64()
					block.IntMax[intCount] = idx.MaxValue(0).Int64()
					intCount++
				case schema.Float64:
					block.FloatMin[floatCount] = idx.MinValue(0).Double()
					block.FloatMax[floatCount] = idx.MaxValue(0).Double()
					floatCount++
				}
			}
		}

		f.Close()
		loadedBlocks = append(loadedBlocks, block)
		t.rowCount += rowCount
	}

	sort.Slice(loadedBlocks, func(i, j int) bool {
		return loadedBlocks[i].MaxTs < loadedBlocks[j].MaxTs
	})

	t.coldBlocks = append(t.coldBlocks, loadedBlocks...)

	if len(loadedBlocks) > 0 {
		lastBlock := loadedBlocks[len(loadedBlocks)-1]
		if int(lastBlock.MaxTs) > t.lastTs {
			t.lastTs = int(lastBlock.MaxTs)
		}
	}

	t.UseDiskStorage = true //when using opentable if we find a evidence of disk storage, we set this to true
	return nil
}
