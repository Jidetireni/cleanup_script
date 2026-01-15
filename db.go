package main

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

func ConnectDB(dbUrl string) *sql.DB {
	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		log.Fatal("Error opening database connection: ", err)
	}

	if err = db.Ping(); err != nil {
		log.Fatal("Error pinging database: ", err)
	}

	log.Println("Connected to the database successfully.")
	return db
}
