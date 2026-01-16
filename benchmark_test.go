package backtracedb

import (
	"backtraceDB/internal/db"
	"backtraceDB/internal/schema"
	"math/rand"
	"os"
	"testing"
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

func BenchmarkRetreivalSpeed(b *testing.B) {

	rowCount := 100_000
	stockData := generateStockData(rowCount)

	b.SetBytes(int64(rowCount))

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

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		b.StartTimer()

		tblReader := tbl.Reader()

		for {
			_, ok := tblReader.Next()
			if !ok {
				break
			}
		}
	}
}

func BenchmarkRetreivalSpeedWithFilter(b *testing.B) {

	rowCount := 100_000
	stockData := generateStockData(rowCount)

	b.SetBytes(int64(rowCount))

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

	b.ResetTimer()

	b.Run("Filter-On-Volume-Int", func(b *testing.B) {
		b.SetBytes(int64(rowCount))
		for i := 0; i < b.N; i++ {
			r := tbl.Reader().Filter("volume", "<=", int64(1000))
			for {
				if _, ok := r.Next(); !ok {
					break
				}
			}
		}
	})

	b.Run("Filter-On-Price-Float", func(b *testing.B) {
		b.SetBytes(int64(rowCount))
		for i := 0; i < b.N; i++ {
			r := tbl.Reader().Filter("price", ">", 50.5)
			for {
				if _, ok := r.Next(); !ok {
					break
				}
			}
		}
	})

	b.Run("Chained-Volume-AND-Price", func(b *testing.B) {
		b.SetBytes(int64(rowCount))
		for i := 0; i < b.N; i++ {
			r := tbl.Reader().
				Filter("volume", "<=", int64(5000)).
				Filter("price", ">", 10.0)
			for {
				if _, ok := r.Next(); !ok {
					break
				}
			}
		}
	})

	b.Run("High-Selectivity-Match-Nothing", func(b *testing.B) {
		b.SetBytes(int64(rowCount))
		for i := 0; i < b.N; i++ {
			r := tbl.Reader().Filter("volume", "<", int64(0)) // Matches 0 rows
			for {
				if _, ok := r.Next(); !ok {
					break
				}
			}
		}
	})
}
