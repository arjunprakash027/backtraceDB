package backtracedb

import (
	"backtraceDB/internal/db"
	"backtraceDB/internal/schema"
	"math/rand"
	"os"
	"path/filepath"
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
			"price":     100.0 + rand.Float64()*1000.0,
			"volume":    int64(rand.Intn(10000) + 1),
		}
	}
	return rows
}

func SetupTableforBenchmark(count int) (*db.DB, schema.Schema, error) {

	dbName := "benchmark_test_db"
	os.RemoveAll(filepath.Join("_data_internal", dbName))

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

	tbl, err := database.CreateTable(s)
	if err != nil {
		return nil, schema.Schema{}, err
	}

	tbl.MaxBlockSize = 1000
	stockData := generateStockData(count)

	for _, row := range stockData {
		_ = tbl.AppendRow(row)
	}

	return database, s, nil

}

func BenchmarkCreation(b *testing.B) {

	rowCount := 10_000
	stockData := generateStockData(rowCount)

	b.Run("Memory-Parquet", func(b *testing.B) {
		b.SetBytes(int64(rowCount))

		defer os.RemoveAll(filepath.Join("_data_internal", "benchmark_test_db"))

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
			defer database.Close()

			tbl, err := database.CreateTable(s)
			if err != nil {
				b.Fatal(err)
			}
			tbl.MaxBlockSize = 1000

			for _, row := range stockData {
				if err := tbl.AppendRow(row); err != nil {
					b.Fatal(err)
				}
			}

		}
	})

	b.Run("InDisk-Parquet-With-WAL", func(b *testing.B) {
		b.SetBytes(int64(rowCount))
		defer os.RemoveAll(filepath.Join("_data_internal", "benchmark_test_db"))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			os.RemoveAll(filepath.Join("_data_internal", "benchmark_test_db"))

			database, err := db.Open("benchmark_test_db")
			if err != nil {
				b.Fatal(err)
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

			tbl, err := database.CreateTable(s)
			if err != nil {
				database.Close()
				b.Fatal(err)
			}

			// Enabling disk storage automatically enables the WAL lazily on first append
			tbl.UseDiskStorage = true
			tbl.MaxBlockSize = 1000

			for _, row := range stockData {
				if err := tbl.AppendRow(row); err != nil {
					database.Close()
					b.Fatal(err)
				}
			}

			database.Close()
		}
	})
}

func BenchmarkRetreivalSpeed(b *testing.B) {
	rowCount := 10_000
	stockData := generateStockData(rowCount)

	s := schema.Schema{
		Name:       "RetreivalBench",
		TimeColumn: "timestamp",
		Columns: []schema.Column{
			{Name: "timestamp", Type: schema.Int64},
			{Name: "symbol", Type: schema.String},
			{Name: "price", Type: schema.Float64},
			{Name: "volume", Type: schema.Int64},
		},
	}

	b.Run("Memory-Parquet", func(b *testing.B) {
		b.SetBytes(int64(rowCount))
		dbPath := "bench_mem_retreival"
		database, _ := db.Open(dbPath)
		defer os.RemoveAll(filepath.Join("_data_internal", dbPath))
		defer database.Close()

		tbl, _ := database.CreateTable(s)
		tbl.MaxBlockSize = 1000
		tbl.UseDiskStorage = false

		for _, r := range stockData {
			_ = tbl.AppendRow(r)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r := tbl.Reader()
			for {
				if _, ok := r.Next(); !ok {
					break
				}
			}
		}
	})

	b.Run("InDisk-Parquet-With-WAL", func(b *testing.B) {
		b.SetBytes(int64(rowCount))
		dbPath := "bench_disk_retreival"
		database, _ := db.Open(dbPath)
		defer os.RemoveAll(filepath.Join("_data_internal", dbPath))
		defer database.Close()

		tbl, _ := database.CreateTable(s)
		tbl.MaxBlockSize = 1000
		tbl.UseDiskStorage = true

		for _, r := range stockData {
			_ = tbl.AppendRow(r)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r := tbl.Reader()
			for {
				if _, ok := r.Next(); !ok {
					break
				}
			}
		}
	})
}

func BenchmarkLoadSpeedFromDisk(b *testing.B) {
	rowCount := 10_000
	b.SetBytes(int64(rowCount))
	stockData := generateStockData(rowCount)
	s := schema.Schema{
		Name:       "LoadBench",
		TimeColumn: "timestamp",
		Columns: []schema.Column{
			{Name: "timestamp", Type: schema.Int64},
			{Name: "symbol", Type: schema.String},
			{Name: "price", Type: schema.Float64},
			{Name: "volume", Type: schema.Int64},
		},
	}

	dbPath := "bench_load_speed"
	database, _ := db.Open(dbPath)
	defer os.RemoveAll(filepath.Join("_data_internal", dbPath))

	tbl, _ := database.CreateTable(s)
	tbl.MaxBlockSize = 1000
	tbl.UseDiskStorage = true

	for _, r := range stockData {
		_ = tbl.AppendRow(r)
	}
	database.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Measure just the discovery + initial reading
		dbNew, _ := db.Open(dbPath)

		// This measures discovery (naming convention parsing)
		tblNew, _ := dbNew.OpenTable(s)

		// If we want to measure the ACTUAL loading of data into memory, we must read
		r := tblNew.Reader()
		for {
			if _, ok := r.Next(); !ok {
				break
			}
		}
		dbNew.Close()
	}
}

func BenchmarkRetreivalSpeedWithFilter(b *testing.B) {

	rowCount := 10_000
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
	defer database.Close()

	tbl, err := database.CreateTable(s)
	if err != nil {
		b.Fatal(err)
	}
	tbl.MaxBlockSize = 1000

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

	b.Run("Filter-On-String", func(b *testing.B) {
		b.SetBytes(int64(rowCount))
		for i := 0; i < b.N; i++ {
			r := tbl.Reader().Filter("symbol", "==", "AAPL")
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

	b.Run("TimeFilter", func(b *testing.B) {
		b.SetBytes(int64(rowCount))
		for i := 0; i < b.N; i++ {
			r := tbl.Reader().Filter("timestamp", ">", int64(1673628000000+5000))
			for {
				if _, ok := r.Next(); !ok {
					break
				}
			}
		}
	})

	b.Run("TimeFilterSlice", func(b *testing.B) {
		b.SetBytes(int64(rowCount))
		for i := 0; i < b.N; i++ {
			r := tbl.Reader().Filter("timestamp", ">", int64(1673628000000+5000)).Filter("timestamp", "<", int64(1673628000000+5000+3000))
			for {
				if _, ok := r.Next(); !ok {
					break
				}
			}
		}
	})
}
