package main

import (
	"in-memory-storage-engine/cronjob"
	"in-memory-storage-engine/storage_engine/storage"
	"log"
)

func main() {
	store := storage.NewMemStore()
	if err := cronjob.RemoveOldVersionKey(store); err != nil {
		log.Fatal(err)
	}
}
