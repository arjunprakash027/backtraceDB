package schema

import "fmt"

type ColumnType int

const (
	Int64 ColumnType = iota
	Float64
	String
	Boolean
)

type Column struct {
	Name string
	Type ColumnType
}

type Schema struct {
	Name       string
	TimeColumn string
	Columns    []Column
}

func (s Schema) Validate() error {

	if s.Name == "" {
		return fmt.Errorf("schema name cannot be empty")
	}

	if len(s.Columns) == 0 {
		return fmt.Errorf("schema must have at least one column")
	}

	columnNames := make(map[string]struct{})
	timeColumnFound := false

	for _, col := range s.Columns {
		if col.Name == "" {
			return fmt.Errorf("column name cannot be empty")
		}

		if _, ok := columnNames[col.Name]; ok {
			return fmt.Errorf("Duplicate column name %s is not allowed", col.Name)
		}
		columnNames[col.Name] = struct{}{}

		if col.Name == s.TimeColumn {
			timeColumnFound = true

			if col.Type != Int64 {
				return fmt.Errorf("time column %s must be of type int64", col.Name)
			}
		}
	}

	if !timeColumnFound {
		return fmt.Errorf("time column %s not found", s.TimeColumn)
	}

	return nil
}
