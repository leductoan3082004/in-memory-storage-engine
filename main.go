package main

import (
	"context"
	"in-memory-storage-engine/storage_engine/storage"
)

func main() {
	store := storage.NewMemStore()
	store.StartTransaction(context.Background())
}
