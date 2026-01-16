package backtracedb

import (
	"backtraceDB/internal/schema"
	"backtraceDB/internal/db"
	"math/rand"
	"testing"
	"os"
)

func generateStockData(count int) []map[string]any {
	symbols := []string{"AAPL", "GOOG", "MSFT", "TSLA", "AMZN", "META", "NVDA", "AMD"}

	rows := make([]map[string]any, count)
	baseTime := int64(1673628000000)
	for i := 0; i < count; i++ {
		symbol := symbols[rand.Intn(len(symbols))]

		rows[i] = map[string]any{
			"timestamp": baseTime + int64(i),
			"symbol":    symbol,
			"price":     100.0 + rand.Float64()*1000.0, // Price between 100 and 1100
			"volume":    int64(rand.Intn(10000) + 1),   // Volume between 1 and 10001
		}
	}
	return rows
}

func SetupTableforBenchmark(count int) (*db.DB, schema.Schema, error) {
	
	dbName := "benchmark_test_db"
	os.RemoveAll(dbName)

	database, err := db.Open(dbName)
	if err != nil {
		return nil, schema.Schema{}, err
	}

	s := schema.Schema{
		Name:       "Testing",
		TimeColumn: "timestamp",
		Columns: []schema.Column{
			{Name: "timestamp", Type: schema.Int64},
			{Name: "symbol", Type: schema.String},
			{Name: "price", Type: schema.Float64},
			{Name: "volume", Type: schema.Int64},
		},
	}

	opts := &db.CreateTableOptions{
		EnableWal: true,
	}

	tbl, err := database.CreateTable(s, opts)
	if err != nil {
		return nil, schema.Schema{}, err
	}

	stockData := generateStockData(count)

	for _, row := range stockData {
		_ = tbl.AppendRow(row)
	}

	return database, s, nil
	
}

func BenchmarkCreationWithWal(b *testing.B) {

	rowCount := 100_000
	stockData := generateStockData(rowCount)

	b.SetBytes(int64(rowCount))

	defer os.RemoveAll("benchmark_test_db")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		b.StartTimer()

		s := schema.Schema{
			Name:       "Testing",
			TimeColumn: "timestamp",
			Columns: []schema.Column{
				{Name: "timestamp", Type: schema.Int64},
				{Name: "symbol", Type: schema.String},
				{Name: "price", Type: schema.Float64},
				{Name: "volume", Type: schema.Int64},
			},
		}

		database, err := db.Open("benchmark_test_db")
		if err != nil {
			b.Fatal(err)
		}

		opts := &db.CreateTableOptions{
			EnableWal: true,
		}
	
		tbl, err := database.CreateTable(s, opts)
		if err != nil {
			b.Fatal(err)
		}

		for _, row := range stockData {
			if err := tbl.AppendRow(row); err != nil {
				b.Fatal(err)
			}
		}

	}
}

func BenchmarkCreationWithoutWal(b *testing.B) {

	rowCount := 100_000
	stockData := generateStockData(rowCount)

	b.SetBytes(int64(rowCount))

	defer os.RemoveAll("benchmark_test_db")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		b.StartTimer()

		s := schema.Schema{
			Name:       "Testing",
			TimeColumn: "timestamp",
			Columns: []schema.Column{
				{Name: "timestamp", Type: schema.Int64},
				{Name: "symbol", Type: schema.String},
				{Name: "price", Type: schema.Float64},
				{Name: "volume", Type: schema.Int64},
			},
		}

		database, err := db.Open("benchmark_test_db")
		if err != nil {
			b.Fatal(err)
		}

		opts := &db.CreateTableOptions{
			EnableWal: false,
		}
		
		tbl, err := database.CreateTable(s, opts)
		if err != nil {
			b.Fatal(err)
		}

		for _, row := range stockData {
			if err := tbl.AppendRow(row); err != nil {
				b.Fatal(err)
			}
		}

	}
}
func BenchmarkWalReplay(b *testing.B) {
	
	rowCount := 100_000
	_, s, err := SetupTableforBenchmark(rowCount)
	b.SetBytes(int64(rowCount))

	defer os.RemoveAll("benchmark_test_db")
	 
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		database, _ := db.Open("benchmark_test_db")
		b.StartTimer()

		_, err := database.OpenTable(s)
		if err != nil {
			b.Fatal(err)
		}
	}


}

