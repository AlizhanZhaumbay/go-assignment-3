package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/redis/go-redis/v9"
	"log"
	"time"
)

const (
	dbHost     = "localhost"
	dbPort     = 5432
	dbUser     = "postgres"
	dbPassword = "password"
	dbDbname   = "postgres"

	redisHost     = "localhost"
	redisPort     = 6379
	redisPassword = ""
	redisDatabase = 0
	cacheKey      = "cache_products:%v"
)

type DB struct {
	pgConn *pgx.Conn
	rdb    *redis.Client
}

func NewDB(ctx context.Context) (*DB, error) {
	// Connect to PostgreSQL DB
	pgConn, err := connectToPostgres(ctx)
	if err != nil {
		return nil, err
	}

	// Connect to Redis DB
	rdb := connectToRedis()

	return &DB{
		pgConn: pgConn,
		rdb:    rdb,
	}, nil
}

func connectToPostgres(ctx context.Context) (*pgx.Conn, error) {
	connString := fmt.Sprintf("postgresql://%v:%v@%v:%v/%v", dbUser, dbPassword, dbHost, dbPort, dbDbname)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func connectToRedis() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%v:%v", redisHost, redisPort),
		Password: redisPassword,
		DB:       redisDatabase,
	})
	return rdb
}

func (db DB) Close() {
	if db.pgConn != nil {
		db.pgConn.Close(context.Background())
	}
	if db.rdb != nil {
		db.rdb.Close()
	}
}

type Product struct {
	ID          int
	Name        string
	Description string
	Price       float64
}

func HandleError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func (db DB) CreateProduct(ctx context.Context, product Product) error {
	transaction, err := db.pgConn.Begin(ctx)
	HandleError(err)

	_, err = transaction.Exec(ctx,
		"INSERT INTO products (name, description, price) VALUES ($1, $2, $3)",
		product.Name, product.Description, product.Price)

	if err != nil {
		err = transaction.Rollback(ctx)
	}
	return err
}

func (db DB) GetAllProducts(ctx context.Context) ([]Product, error) {
	rows, err := db.pgConn.Query(ctx, "SELECT id, name, description, price FROM products")
	HandleError(err)
	defer rows.Close()

	var products []Product
	for rows.Next() {
		var p Product
		if err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.Price); err != nil {
			return nil, err
		}
		products = append(products, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	fmt.Println("Fetched from Postgresql database!")
	return products, nil
}

func (db DB) UpdateProduct(ctx context.Context, product Product) error {
	transaction, err := db.pgConn.Begin(ctx)
	HandleError(err)
	_, err = transaction.Exec(ctx,
		"UPDATE products SET name = $1, description = $2, price = $3 WHERE id = $4",
		product.Name, product.Description, product.Price, product.ID)
	if err != nil {
		err = transaction.Rollback(ctx)
	}
	return err
}

func (db DB) GetProductByID(ctx context.Context, id int) (Product, error) {
	var product Product

	exists, err := db.existsInRedis(ctx, id)
	HandleError(err)

	if exists {
		return db.fetchFromRedis(ctx, id)
	}

	err = db.pgConn.QueryRow(ctx,
		"SELECT id, name, description, price FROM products WHERE id = $1", id).
		Scan(&product.ID, &product.Name, &product.Description, &product.Price)
	if err != nil {
		return Product{}, err
	}
	fmt.Println("Fetched from Postgresql database!")

	//Storing to cache
	db.saveToCache(ctx, product)
	return product, nil
}

func (db DB) DeleteProduct(ctx context.Context, id int) error {
	transaction, err := db.pgConn.Begin(ctx)
	_, err = transaction.Exec(ctx,
		"DELETE FROM products WHERE id = $1", id)
	if err != nil {
		err = transaction.Rollback(ctx)
	}
	return err
}

func (db DB) existsInRedis(ctx context.Context, id int) (bool, error) {
	key := fmt.Sprintf(cacheKey, id)
	exists, err := db.rdb.HExists(ctx, key, "data").Result()
	HandleError(err)
	if exists == false {
		fmt.Println("Product not stored in cache")
		return false, err
	}
	fmt.Println("Product found in cache!")
	return exists, nil
}

func (db DB) fetchFromRedis(ctx context.Context, id int) (Product, error) {
	key := fmt.Sprintf(cacheKey, id)
	jsonString, err := db.rdb.HGet(ctx, key, "data").Result()
	HandleError(err)

	var product Product
	err = json.Unmarshal([]byte(jsonString), &product)
	if err != nil {
		HandleError(err)
		return Product{}, err
	}
	fmt.Println("Fetched from Cache!")
	return product, nil
}

func (db DB) saveToCache(ctx context.Context, product Product) {
	//Saving only for 5 minutes
	key := fmt.Sprintf(cacheKey, product.ID)
	jsonString, err := json.Marshal(product)
	HandleError(err)
	db.rdb.HSet(ctx, key, "data", jsonString)
	db.rdb.Expire(ctx, key, 5*time.Minute)
	fmt.Println("Stored to cache!")
}

func PrintProduct(product Product) {
	fmt.Printf("ID: %d, Name: %s, Description: %s, Price: $%.2f\n",
		product.ID, product.Name, product.Description, product.Price)
}
