package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gaj/verifiable-sqlite/pkg/config"
	"github.com/gaj/verifiable-sqlite/pkg/vsqlite"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	// Initialize the verification engine with default configuration
	cfg := config.DefaultConfig()
	vsqlite.InitVerification(cfg)
	defer vsqlite.Shutdown()

	// Open a SQLite database (creates a new file if it doesn't exist)
	db, err := vsqlite.Open("sqlite3", "./example.db")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create tables
	createTables(db)

	// Perform a transaction with multiple operations
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert a product
	productID, err := insertProduct(tx, "Laptop", 1200.00, 10)
	if err != nil {
		tx.Rollback()
		log.Fatalf("Failed to insert product: %v", err)
	}
	fmt.Printf("Inserted product with ID: %d\n", productID)

	// Insert an order
	orderID, err := insertOrder(tx, "John Doe", "123 Main St", time.Now())
	if err != nil {
		tx.Rollback()
		log.Fatalf("Failed to insert order: %v", err)
	}
	fmt.Printf("Inserted order with ID: %d\n", orderID)

	// Add order item
	err = insertOrderItem(tx, orderID, productID, 2)
	if err != nil {
		tx.Rollback()
		log.Fatalf("Failed to insert order item: %v", err)
	}

	// Update product inventory
	err = updateProductInventory(tx, productID, -2)
	if err != nil {
		tx.Rollback()
		log.Fatalf("Failed to update inventory: %v", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}
	fmt.Println("Transaction committed successfully")

	// Query the database for verification
	verifyData(db, productID, orderID)

	// Allow time for asynchronous verification to complete
	fmt.Println("Waiting for async verification to complete...")
	time.Sleep(1 * time.Second)
	fmt.Println("Example completed.")
}

func createTables(db *vsqlite.DB) {
	// Create products table
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS products (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		price REAL NOT NULL,
		inventory INTEGER NOT NULL
	)`)
	if err != nil {
		log.Fatalf("Failed to create products table: %v", err)
	}

	// Create orders table
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS orders (
		id INTEGER PRIMARY KEY,
		customer_name TEXT NOT NULL,
		shipping_address TEXT NOT NULL,
		order_date DATETIME NOT NULL
	)`)
	if err != nil {
		log.Fatalf("Failed to create orders table: %v", err)
	}

	// Create order_items table
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS order_items (
		id INTEGER PRIMARY KEY,
		order_id INTEGER NOT NULL,
		product_id INTEGER NOT NULL,
		quantity INTEGER NOT NULL,
		FOREIGN KEY (order_id) REFERENCES orders(id),
		FOREIGN KEY (product_id) REFERENCES products(id)
	)`)
	if err != nil {
		log.Fatalf("Failed to create order_items table: %v", err)
	}
}

func insertProduct(tx *vsqlite.Tx, name string, price float64, inventory int) (int64, error) {
	result, err := tx.Exec("INSERT INTO products (name, price, inventory) VALUES (?, ?, ?)",
		name, price, inventory)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func insertOrder(tx *vsqlite.Tx, customerName, shippingAddress string, orderDate time.Time) (int64, error) {
	result, err := tx.Exec("INSERT INTO orders (customer_name, shipping_address, order_date) VALUES (?, ?, ?)",
		customerName, shippingAddress, orderDate.Format(time.RFC3339))
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func insertOrderItem(tx *vsqlite.Tx, orderID, productID int64, quantity int) error {
	_, err := tx.Exec("INSERT INTO order_items (order_id, product_id, quantity) VALUES (?, ?, ?)",
		orderID, productID, quantity)
	return err
}

func updateProductInventory(tx *vsqlite.Tx, productID int64, change int) error {
	_, err := tx.Exec("UPDATE products SET inventory = inventory + ? WHERE id = ?",
		change, productID)
	return err
}

func verifyData(db *vsqlite.DB, productID, orderID int64) {
	// Check product inventory
	var inventory int
	err := db.QueryRow("SELECT inventory FROM products WHERE id = ?", productID).Scan(&inventory)
	if err != nil {
		log.Fatalf("Failed to query product: %v", err)
	}
	fmt.Printf("Product inventory after transaction: %d\n", inventory)

	// Check order items
	var itemCount int
	err = db.QueryRow("SELECT COUNT(*) FROM order_items WHERE order_id = ?", orderID).Scan(&itemCount)
	if err != nil {
		log.Fatalf("Failed to query order items: %v", err)
	}
	fmt.Printf("Order has %d items\n", itemCount)
}