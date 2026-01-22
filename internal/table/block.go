package table

import (
	"backtraceDB/internal/schema"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/parquet-go/parquet-go"
)

type Block struct {
	Storage  *ColumnStorage
	RowCount int
	Path     string
	isOnDisk bool
	MaxTs    int64

	isClosed     bool
	inMemoryData []byte

	IntMin   []int64
	IntMax   []int64
	FloatMin []float64
	FloatMax []float64
}

func NewBlock(colTypes []schema.ColumnType) (*Block, []ColumnLocation, error) {
	storage, locations, err := NewColumnStorage(colTypes)

	numInt, numFloat := 0, 0
	for _, t := range colTypes {
		switch t {
		case schema.Int64:
			numInt++
		case schema.Float64:
			numFloat++
		}
	}

	return &Block{
		Storage:  storage,
		RowCount: 0,
		Path:     "",
		isOnDisk: false,
		MaxTs:    0,
		IntMin:   make([]int64, numInt),
		IntMax:   make([]int64, numInt),
		FloatMin: make([]float64, numFloat),
		FloatMax: make([]float64, numFloat),
	}, locations, err
}

func (b *Block) LoadInto(dest *ColumnStorage, s schema.Schema, locations []ColumnLocation) error {

	var pf *parquet.File
	var err error

	if len(b.inMemoryData) > 0 {
		pf, err = parquet.OpenFile(bytes.NewReader(b.inMemoryData), int64(len(b.inMemoryData)))
		if err != nil {
			return fmt.Errorf("failed to open block file: %v", err)
		}
	} else if b.isOnDisk {
		f, err := os.Open(b.Path)
		if err != nil {
			return fmt.Errorf("failed to open block file: %v", err)
		}
		defer f.Close()

		stat, _ := f.Stat()
		pf, err = parquet.OpenFile(f, stat.Size())
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("block is not on disk and has no in memory data")
	}

	fileSchema := pf.Schema()
	parquetColIndices := make(map[string]int)
	for i, col := range fileSchema.Fields() {
		parquetColIndices[col.Name()] = i
	}

	valueBuffer := make([]parquet.Value, 256) //256 is small enough to fit into l1/l2 cache

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

func (b *Block) WriteParquetTo(w io.Writer, s schema.Schema, locations []ColumnLocation) error {
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

	writer := parquet.NewGenericWriter[any](w, pqSchema)

	row := make(map[string]any)

	for i := 0; i < b.RowCount; i++ {
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

	return writer.Close()
}

func (b *Block) UpdateStats() {
	if b.Storage == nil {
		return
	}

	for i, col := range b.Storage.Int64Cols {
		if len(col) == 0 {
			continue
		}
		b.IntMax[i] = slices.Max(col)
		b.IntMin[i] = slices.Min(col)
	}

	for i, col := range b.Storage.Float64Cols {
		if len(col) == 0 {
			continue
		}
		b.FloatMax[i] = slices.Max(col)
		b.FloatMin[i] = slices.Min(col)
	}

}
func (b *Block) Rotate(useDisk bool, filePath string, s schema.Schema, locations []ColumnLocation) error {

	b.UpdateStats()

	if useDisk {
		if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
			return fmt.Errorf("failed to create directory: %v", err)
		}

		f, err := os.Create(filePath)
		if err != nil {
			return err
		}
		defer f.Close()

		if err := b.WriteParquetTo(f, s, locations); err != nil {
			return err
		}

		b.inMemoryData = nil
		b.Path = filePath
		b.isOnDisk = true

	} else {

		var buf bytes.Buffer
		if err := b.WriteParquetTo(&buf, s, locations); err != nil {
			return err
		}
		b.inMemoryData = buf.Bytes()
		b.Path = ""
		b.isOnDisk = false
	}

	b.Storage = nil
	b.isClosed = true

	return nil
}

func (b *Block) Persist(path string, s schema.Schema, locations []ColumnLocation) error {
	b.UpdateStats()

	if b.isOnDisk {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if len(b.inMemoryData) > 0 {
		_, err = io.Copy(f, bytes.NewReader(b.inMemoryData))
		if err != nil {
			return err
		}
	} else if b.Storage != nil {
		if err := b.WriteParquetTo(f, s, locations); err != nil {
			return err
		}
	}

	b.Path = path
	b.isOnDisk = true
	b.inMemoryData = nil
	b.Storage = nil
	return nil
}