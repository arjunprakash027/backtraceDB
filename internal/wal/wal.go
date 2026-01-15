//possible optimizations

package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"backtraceDB/internal/schema"
	"backtraceDB/internal/table"
)

type WAL struct {
	mu     sync.Mutex
	file   *os.File
	path   string
	schema schema.Schema
}

func NewWAL(path string, schema schema.Schema) (*WAL, error) {

	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	p := filepath.Join(path, "wal.dat")

	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

	if err != nil {
		return nil, err
	}

	return &WAL{
		file:   f,
		path:   p,
		schema: schema,
	}, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	if err := w.file.Close(); err != nil {
		return err
	}

	w.file = nil

	return nil
}

// encode rows first before dumping them into wal log
func (w *WAL) encodeRow(row map[string]any) ([]byte, error) {
	var buf bytes.Buffer

	for _, col := range w.schema.Columns {
		val, ok := row[col.Name]
		if !ok {
			return nil, fmt.Errorf("column %s not found in row", col.Name)
		}

		switch col.Type {
		case schema.Int64:
			v, ok := val.(int64)
			if !ok {
				return nil, fmt.Errorf("column %s must be of type int64", col.Name)
			}
			err := binary.Write(&buf, binary.LittleEndian, v)
			if err != nil {
				return nil, err
			}
		case schema.Float64:
			v, ok := val.(float64)
			bits := math.Float64bits(v)
			if !ok {
				return nil, fmt.Errorf("column %s must be of type float64", col.Name)
			}
			err := binary.Write(&buf, binary.LittleEndian, bits)
			if err != nil {
				return nil, err
			}
		case schema.String:
			v, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("column %s must be of type string", col.Name)
			}

			bs := []byte(v)

			if err := binary.Write(&buf, binary.LittleEndian, uint32(len(bs))); err != nil {
				return nil, err
			}

			if _, err := buf.Write(bs); err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("unsupported column type: %v", col.Type)
		}
	}

	return buf.Bytes(), nil
}

func (w *WAL) AppendRow(row map[string]any) error {
	payload, err := w.encodeRow(row)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return fmt.Errorf("wal is closed")
	}

	if err := binary.Write(w.file, binary.LittleEndian, uint32(len(payload))); err != nil {
		return err
	}

	if _, err := w.file.Write(payload); err != nil {
		return err
	}

	//potential optimization by making sync interval based
	if err := w.file.Sync(); err != nil {
		return err
	}

	return nil

}

func (w *WAL) DecodePayload(payload []byte) (map[string]any, error) {
	r := bytes.NewReader(payload)

	out := make(map[string]any, len(w.schema.Columns))

	for _, col := range w.schema.Columns {
		switch col.Type {
		case schema.Int64:
			var v int64
			if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
				return nil, err
			}
			out[col.Name] = v
		case schema.Float64:
			var v float64
			if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
				return nil, err
			}
			out[col.Name] = v
		case schema.String:
			var len uint32
			if err := binary.Read(r, binary.LittleEndian, &len); err != nil {
				return nil, err
			}
			bs := make([]byte, len)
			if _, err := io.ReadFull(r, bs); err != nil {
				return nil, err
			}
			out[col.Name] = string(bs)
		default:
			return nil, fmt.Errorf("unsupported column type: %v", col.Type)
		}
	}

	return out, nil
}

// potential optimization by making this faster
func (w *WAL) ReplayTable(tbl *table.Table) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return fmt.Errorf("wal is closed")
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	for {
		var len uint32
		if err := binary.Read(w.file, binary.LittleEndian, &len); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if len == 0 {
			continue
		}

		payload := make([]byte, len)
		if _, err := io.ReadFull(w.file, payload); err != nil {
			return err
		}

		row, err := w.DecodePayload(payload)
		if err != nil {
			return err
		}

		if err := tbl.AppendRow(row); err != nil {
			return err
		}

	}
}

func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
		w.file = nil
	}
	if err := os.Remove(w.path); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	w.file = f
	return nil
}
