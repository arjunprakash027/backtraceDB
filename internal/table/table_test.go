package table

import (
	"backtraceDB/internal/schema"
	"testing"
)

func TestTableReader(t *testing.T) {
	s := schema.Schema{
		Name:       "test_table",
		TimeColumn: "ts",
		Columns: []schema.Column{
			{Name: "ts", Type: schema.Int64},
			{Name: "message", Type: schema.String},
			{Name: "price", Type: schema.Float64},
		},
	}

	tbl, err := CreateTable(s, nil)
	if err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{
		{"ts": int64(1), "message": "first", "price": 10.5},
		{"ts": int64(2), "message": "second", "price": 20.5},
		{"ts": int64(3), "message": "third", "price": 30.5},
	}

	for _, r := range rows {
		if err := tbl.AppendRow(r); err != nil {
			t.Fatal(err)
		}
	}

	// Test Next()
	reader := tbl.Reader()
	count := 0
	for {
		row, ok := reader.Next()
		if !ok {
			break
		}

		expected := rows[count]
		if row["ts"] != expected["ts"] {
			t.Errorf("row %d: expected ts %v, got %v", count, expected["ts"], row["ts"])
		}
		if row["message"] != expected["message"] {
			t.Errorf("row %d: expected message %v, got %v", count, expected["message"], row["message"])
		}
		if row["price"] != expected["price"] {
			t.Errorf("row %d: expected price %v, got %v", count, expected["price"], row["price"])
		}
		count++
	}

	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}

	// Test ReadColumn
	tsCol, err := reader.ReadColumn("ts")
	if err != nil {
		t.Fatal(err)
	}
	tsSlice := tsCol.([]int64)
	if len(tsSlice) != 3 || tsSlice[0] != 1 || tsSlice[2] != 3 {
		t.Errorf("ReadColumn(ts) returned wrong data: %v", tsSlice)
	}

	priceCol, err := reader.ReadColumn("price")
	if err != nil {
		t.Fatal(err)
	}
	priceSlice := priceCol.([]float64)
	if len(priceSlice) != 3 || priceSlice[0] != 10.5 || priceSlice[2] != 30.5 {
		t.Errorf("ReadColumn(price) returned wrong data: %v", priceSlice)
	}
}
