package main

import (
	"database/sql"
	"dblock/dblock"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

const (
	dbUser  = "user"
	dbPass  = "password"
	dbHost  = "localhost"
	dbPort  = "5432"
	timeout = 5 * time.Minute
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <schema_version>", os.Args[0])
	}

	targetVersion, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid schema version: %v", err)
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s?sslmode=disable", dbUser, dbPass, dbHost, dbPort)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to initialize database connection handle: %v", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}

	if err := dblock.UpgradeIfNeeded(db, targetVersion, exampleUpgrade, timeout); err != nil {
		log.Fatalf("Upgrade failed: %v", err)
	} else {
		log.Println("Schema is up to date!")
	}
}

func exampleUpgrade(tx *sql.Tx) error {
	_, err := tx.Exec("SELECT 1") // Add ALTER and other stuff here
	time.Sleep(5 * time.Second)
	return err
}
