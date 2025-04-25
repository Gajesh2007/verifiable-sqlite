package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gaj/verifiable-sqlite/pkg/config"
	"github.com/gaj/verifiable-sqlite/pkg/vsqlite"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	// Remove example database if it exists
	os.Remove("./simple_example.db")

	// Initialize VSQLite with verification enabled
	cfg := config.DefaultConfig()
	vsqlite.InitVerification(cfg)
	defer vsqlite.Shutdown()

	// Open database
	db, err := vsqlite.Open("sqlite3", "./simple_example.db")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT UNIQUE,
		created_at DATETIME
	)`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Start a transaction
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert a user
	now := time.Now().Format(time.RFC3339)
	result, err := tx.Exec(
		"INSERT INTO users (name, email, created_at) VALUES (?, ?, ?)",
		"John Doe", "john@example.com", now,
	)
	if err != nil {
		log.Fatalf("Failed to insert: %v", err)
	}

	// Get last insert ID
	id, err := result.LastInsertId()
	if err != nil {
		log.Fatalf("Failed to get last insert ID: %v", err)
	}
	fmt.Printf("Inserted user with ID: %d\n", id)

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}

	// Update the user in a new transaction
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.Exec("UPDATE users SET name = ? WHERE id = ?", "Jane Doe", id)
	if err != nil {
		log.Fatalf("Failed to update: %v", err)
	}

	// Commit the update
	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify data was updated
	var name string
	err = db.QueryRow("SELECT name FROM users WHERE id = ?", id).Scan(&name)
	if err != nil {
		log.Fatalf("Failed to query: %v", err)
	}
	fmt.Printf("Updated name: %s\n", name)

	// Clean up - comment this out if you want to inspect the DB file
	// os.Remove("./simple_example.db")

	// Wait for async verification to complete
	fmt.Println("Waiting for async verification to complete...")
	time.Sleep(300 * time.Millisecond)
	fmt.Println("Done.")
}
