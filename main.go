package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	// Define command line parameters
	host := flag.String("host", "127.0.0.1", "ClickHouse host")
	port := flag.Int("port", 9000, "ClickHouse port")
	batchSize := flag.Int("batch", 3000, "Batch size")
	database := flag.String("database", "default", "ClickHouse database")
	user := flag.String("user", "default", "ClickHouse user")
	password := flag.String("password", "", "ClickHouse password")
	table := flag.String("table", "", "Target table name (required)")
	csvFilePath := flag.String("csv", "", "Path to CSV file (required)")
	flag.Parse()

	// Check required parameters
	if *table == "" || *csvFilePath == "" {
		log.Fatalf("Both --table and --csv are required parameters")
	}

	// ClickHouse configuration
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", *host, *port)},
		Auth: clickhouse.Auth{
			Database: *database,
			Username: *user,
			Password: *password,
		},
		DialTimeout:  time.Second,
		MaxOpenConns: 10,
	})
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer conn.Close()

	// Open CSV file
	file, err := os.Open(*csvFilePath)
	if err != nil {
		log.Fatalf("Failed to open CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	headers, err := reader.Read()
	if err != nil {
		if err.Error() == "EOF" {
			log.Fatalf("Failed to read record: %v", err)
		}
		log.Fatalf("Failed to read record: %v", err)
	}

	var batch [][]string

	start := time.Now()

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break // File is read
			}
			log.Fatalf("Failed to read record: %v", err)
		}
		batch = append(batch, record)

		if len(batch) >= *batchSize {
			if err := insertBatch(context.Background(), conn, *table, headers, batch); err != nil {
				log.Fatalf("Failed to insert batch: %v", err)
			}
			batch = nil
		}
	}

	if len(batch) > 0 {
		if err := insertBatch(context.Background(), conn, *table, headers, batch); err != nil {
			log.Fatalf("Failed to insert final batch: %v", err)
		}
	}

	fmt.Printf("Data successfully inserted into ClickHouse. Time taken: %s\n", time.Since(start))
}

func insertBatch(ctx context.Context, conn driver.Conn, table string, headers []string, records [][]string) error {

	// Build SQL Insert statement
	columnList := fmt.Sprintf("(%s)", joinHeaders(headers, ","))
	insertQuery := fmt.Sprintf("INSERT INTO %s %s VALUES", table, columnList)

	// Batch insert data
	batch, err := conn.PrepareBatch(ctx, insertQuery)
	if err != nil {
		log.Fatalf("Failed to prepare batch: %v", err)
	}

	for _, record := range records {
		row := make([]any, len(record))
		for i, value := range record {

			convertedValue, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				row[i] = value
			} else {
				row[i] = convertedValue
			}

		}
		if err := batch.Append(row...); err != nil {
			log.Fatalf("Failed to append record: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		log.Fatalf("Failed to send batch: %v", err)
	}
	return nil
}

// joinHeaders joins headers into a comma-separated string
func joinHeaders(headers []string, sep string) string {
	return joinStrings(headers, sep)
}

// joinStrings joins an array of strings into a comma-separated string
func joinStrings(strings []string, sep string) string {
	result := ""
	for i, s := range strings {
		if i > 0 {
			result += sep
		}
		result += fmt.Sprintf("`%s`", s)
	}
	return result
}
