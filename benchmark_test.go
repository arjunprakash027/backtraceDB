package backtracedb

import (
	"backtraceDB/internal/schema"
	"backtraceDB/internal/db"
	"math/rand"
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

func BenchmarkCreation(b *testing.B) {

	stockData := generateStockData(100000)

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

		database, err := db.Open("test")
		if err != nil {
			b.Fatal(err)
		}

		tbl, err := database.CreateTable(s)
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

