package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v4"
	"net/http"
	"strconv"
)

func main() {
	http.HandleFunc("/product/", getProductByIDHandler)

	err := http.ListenAndServe("localhost:8080", nil)
	HandleError(err)
}

func getProductByIDHandler(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Path[len("/product/"):]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid product ID", http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	db, err := NewDB(ctx)

	// Retrieve the product from the database
	product, err := db.GetProductByID(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "Product not found", http.StatusNotFound)
		} else {
			http.Error(w, "Failed to retrieve product", http.StatusInternalServerError)
		}
		return
	}

	// Convert the product to JSON
	productJSON, err := json.Marshal(product)
	if err != nil {
		http.Error(w, "Failed to serialize product", http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")

	_, err = w.Write(productJSON)
	if err != nil {
		panic(err)
	}
}
