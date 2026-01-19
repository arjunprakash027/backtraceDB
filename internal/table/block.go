package table

import (
	"backtraceDB/internal/schema"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"
)

type Block struct {
	Storage  *ColumnStorage
	RowCount int
	Path     string
	isOnDisk bool
}

func NewBlock(colTypes []schema.ColumnType) (*Block, []ColumnLocation, error) {
	storage, locations, err := NewColumnStorage(colTypes)

	return &Block{
		Storage:  storage,
		RowCount: 0,
		Path:     "",
		isOnDisk: false,
	}, locations, err
}

func (b *Block) LoadInto(dest *ColumnStorage, s schema.Schema, locations []ColumnLocation) error {
	if !b.isOnDisk {
		return fmt.Errorf("LoadCurrentBlock is called on non disk block")
	}

	f, err := os.Open(b.Path)
	if err != nil {
		return fmt.Errorf("failed to open block file: %v", err)
	}
	defer f.Close()

	stat, _ := f.Stat()
	pf, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		return err
	}

	fileSchema := pf.Schema()
	parquetColIndices := make(map[string]int)
	for i, col := range fileSchema.Fields() {
		parquetColIndices[col.Name()] = i
	}

	valueBuffer := make([]parquet.Value, 256)

	for logicalIdx, col := range s.Columns {

		pIdx, exists := parquetColIndices[col.Name]
		loc := locations[logicalIdx]

		if !exists {
			return fmt.Errorf("column %s not found in parquet file", col.Name)
		}
		for _, rowGroup := range pf.RowGroups() {
			columnChunk := rowGroup.ColumnChunks()[pIdx]
			pages := columnChunk.Pages()

			for {
				page, err := pages.ReadPage()
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("failed to read page: %v", err)
				}

				values := page.Values()

				for {
					n, err := values.ReadValues(valueBuffer)

					if n > 0 {
						for i := 0; i < n; i++ {
							v := valueBuffer[i]

							if v.IsNull() {
								continue
							}

							switch loc.Type {
							case schema.Int64:
								dest.Int64Cols[loc.Index] = append(dest.Int64Cols[loc.Index], v.Int64())
							case schema.Float64:
								dest.Float64Cols[loc.Index] = append(dest.Float64Cols[loc.Index], v.Double())
							case schema.String:
								stringVal := v.String()
								dict := dest.StringDicts[loc.Index]
								id, exists := dict[stringVal]
								if !exists {
									id = len(dict)
									dict[stringVal] = id
									dest.StringReads[loc.Index] = append(dest.StringReads[loc.Index], stringVal)
								}
								dest.StringCols[loc.Index] = append(dest.StringCols[loc.Index], id)
							}
						}
					}
					if err == io.EOF {
						break
					}
					if err != nil {
						return fmt.Errorf("failed to read values: %v", err)
					}
				}
			}
		}
	}
	return nil
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

	writer := parquet.NewGenericWriter[any](f, pqSchema)

	for i := 0; i < b.RowCount; i++ {
		row := make(map[string]any) //Optimization : reuse single map to avoid GC overhead
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
