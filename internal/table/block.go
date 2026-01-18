package table

import (
	"backtraceDB/internal/schema"
	"fmt"
	"os"
	"github.com/parquet-go/parquet-go"
	"path/filepath"
)

type Block struct {
	Storage    *ColumnStorage
	RowCount   int
	Path       string
	isOnDisk bool
}

func NewBlock(colTypes []schema.ColumnType) (*Block, []ColumnLocation, error) {
	storage, locations, err := NewColumnStorage(colTypes)

	return &Block{
		Storage:    storage,
		RowCount:   0,
		Path:       "",
		isOnDisk: false,
	}, locations, err
}

func (b *Block) Flush(filePath string, s schema.Schema, locations []ColumnLocation) error {

	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return err 
	}
	defer f.Close()

	pqFields := make(map[string]parquet.Node)

	for _, col := range s.Columns {
		switch col.Type {
		case schema.Int64:
			pqFields[col.Name] = parquet.Leaf(parquet.Int64Type)
		case schema.Float64:
			pqFields[col.Name] = parquet.Leaf(parquet.DoubleType)
		case schema.String:
			pqFields[col.Name] = parquet.Leaf(parquet.ByteArrayType)
		}
	}

	pqSchema := parquet.NewSchema(s.Name, parquet.Group(pqFields))

	writer := parquet.NewGenericWriter[any](f,pqSchema)

	for i := 0; i < b.RowCount; i++ {
		row := make(map[string]any)
		for logicalIdx, col := range s.Columns {
			loc := locations[logicalIdx]
			switch col.Type {
			case schema.Int64:
				row[col.Name] = b.Storage.Int64Cols[loc.Index][i]
			case schema.Float64:
				row[col.Name] = b.Storage.Float64Cols[loc.Index][i]
			case schema.String:
				strID := b.Storage.StringCols[loc.Index][i]
				row[col.Name] = b.Storage.StringReads[loc.Index][strID]
			}
		}

		if _, err := writer.Write([]any{row}); err != nil {
			return fmt.Errorf("failed to write row %d: %v", i, err)
		}
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %v", err)
	}

	b.Path = filePath
	b.isOnDisk = true
	b.Storage = nil

	return nil
}

