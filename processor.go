package main

import (
	"database/sql"
	"encoding/csv"
	"io"
	"log"
	"os"
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

	count := 0
	headerSkipped := false

	log.Printf("Starting %s...", actionName)

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

		_, err = stmt.Exec(id)
		if err != nil {
			log.Printf("Failed to process ID %s: %v", id, err)
		} else {
			count++
		}
	}

	log.Printf("Finished %s. Total records processed: %d", actionName, count)
}
