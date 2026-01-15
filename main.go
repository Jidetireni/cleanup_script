package main

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, relying on system environment variables")
	}

	dbUrl := os.Getenv("DB_URL")
	fileInvalid := os.Getenv("FILE_INVALID")
	fileUnsub := os.Getenv("FILE_UNSUB")

	if dbUrl == "" || fileInvalid == "" || fileUnsub == "" {
		log.Fatal("Missing required environment variables. Please check .env")
	}

	db := ConnectDB(dbUrl)
	defer db.Close()

	deleteQuery := `DELETE FROM pseudo_users WHERE id = $1`
	ProcessFile(db, fileInvalid, deleteQuery, "DELETING Invalid Users")

	updateQuery := `UPDATE pseudo_users SET unsubscribed_at = NOW() WHERE id = $1`
	ProcessFile(db, fileUnsub, updateQuery, "UPDATING Unsubscribed Users")
}
