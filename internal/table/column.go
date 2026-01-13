package table

import (
	"backtraceDB/internal/schema"
)

type ColumnLocation struct {
	Type  schema.ColumnType
	Index int
}

type ColumnStorage struct {
	Int64Cols   [][]int64
	Float64Cols [][]float64
	StringCols  [][]int
	StringDicts []map[string]int
}

func NewColumnStorage(colTypes []schema.ColumnType) (*ColumnStorage, []ColumnLocation, error) {
	storage := &ColumnStorage{}
	location := make([]ColumnLocation, len(colTypes))

	intIdx := 0
	floatIdx := 0
	stringIdx := 0

	for i, t := range colTypes {

		switch t {

		case schema.Int64:
			storage.Int64Cols = append(storage.Int64Cols, []int64{})
			location[i] = ColumnLocation{Type: t, Index: intIdx}
			intIdx++

		case schema.Float64:
			storage.Float64Cols = append(storage.Float64Cols, []float64{})
			location[i] = ColumnLocation{Type: t, Index: floatIdx}
			floatIdx++

		case schema.String:
			storage.StringCols = append(storage.StringCols, []int{})
			storage.StringDicts = append(storage.StringDicts, map[string]int{})
			location[i] = ColumnLocation{Type: t, Index: stringIdx}
			stringIdx++
		
		default:
			return nil, nil, fmt.Errorf("unsupported column type: %v", t)
		}

	}
	
	return storage, nil, nil
}
