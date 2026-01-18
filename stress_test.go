package backtracedb

import (
	"fmt"
	"testing"
	"backtraceDB/internal/db"
	"backtraceDB/internal/schema"
)

func TestOOMStress(t *testing.T) {
	s := schema.Schema{
		Name: "stress_test", TimeColumn: "ts",
		Columns: []schema.Column{
			{Name: "ts", Type: schema.Int64},
			{Name: "val", Type: schema.Float64},
		},
	}
	
	database, _ := db.Open("stress_db")
	defer database.Close()

	tbl, _ := database.CreateTable(s, &db.CreateTableOptions{EnableWal: false})
	
	fmt.Println("🚀 Starting OOM Stress Test. Watch your RAM!")
	
	row := map[string]any{"ts": int64(0), "val": 0.0}
	count := 0
	for i := 1; ; i++ {
		row["ts"] = int64(i)
		row["val"] = float64(i)
		
		if err := tbl.AppendRow(row); err != nil {
			t.Fatal(err)
		}

		if i % 10_000_000 == 0 {
			fmt.Printf("🔥 Total Rows: %d Million\n", i/1_000_000)
			count ++
		}

		if count >= 4 {
			break
		}
	}
}