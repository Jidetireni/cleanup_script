package main

import (
	"database/sql"
	"encoding/csv"
	"io"
	"log"
	"os"
	"time"

	"github.com/lib/pq"
)

func ProcessFile(db *sql.DB, filePath string, query string, actionName string) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Could not open file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)

	stmt, err := db.Prepare(query)
	if err != nil {
		log.Fatalf("Could not prepare SQL statement for %s: %v", actionName, err)
	}
	defer stmt.Close()

	const batchSize = 500
	var idBatch []string

	totalProcessed := 0
	headerSkipped := false

	log.Printf("Starting %s with batch size %d...", actionName, batchSize)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV line: %v", err)
			continue
		}

		if !headerSkipped {
			headerSkipped = true
			continue
		}

		id := record[0]
		if id == "" {
			continue
		}

		idBatch = append(idBatch, id)

		if len(idBatch) >= batchSize {
			executeBatch(stmt, idBatch)
			totalProcessed += len(idBatch)

			idBatch = nil

			log.Println("Batch limit reached. Sleeping for 2 seconds...")
			time.Sleep(2 * time.Second)
		}
	}

	if len(idBatch) > 0 {
		executeBatch(stmt, idBatch)
		totalProcessed += len(idBatch)
	}

	log.Printf("Finished %s. Total records processed: %d", actionName, totalProcessed)
}

func executeBatch(stmt *sql.Stmt, ids []string) {
	_, err := stmt.Exec(pq.Array(ids))
	if err != nil {
		log.Printf("Failed to process batch of %d IDs: %v", len(ids), err)
	} else {
		log.Printf("Successfully processed batch of %d IDs", len(ids))
	}
}
