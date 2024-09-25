package gomemdb

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-memdb"
)

// Initialize a basic MemDB for key-value storage
func NewMemDB() (*memdb.MemDB, error) {
	// Define the schema
	schema := &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"kv": {
				Name: "kv",
				Indexes: map[string]*memdb.IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "Key"},
					},
				},
			},
		},
	}

	return memdb.NewMemDB(schema)
}

type KeyValue struct {
	Key   string
	Value string
}

func BenchmarkMemDB_KeyValueStore(b *testing.B) {
	storage, err := NewMemDB()
	if err != nil {
		log.Fatal("Failed to create memdb: ", err)
	}

	// Pre-generate some keys
	keys := make([]string, 10)
	for i := 0; i < 10; i++ {
		keys[i] = "key" + strconv.Itoa(i)
	}

	// Concurrency levels to test
	concurrencyLevels := []int{10, 50, 100, 500, 1000}
	numOperationsPerGoroutine := 100

	for _, concurrency := range concurrencyLevels {
		b.Run("concurrency_"+strconv.Itoa(concurrency), func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup

				for j := 0; j < concurrency; j++ {
					wg.Add(1)

					go func(j int) {
						defer wg.Done()

						tx := storage.Txn(true)
						defer tx.Abort() // In case of failure

						for op := 0; op < numOperationsPerGoroutine; op++ {
							key := keys[j%len(keys)]
							value := fmt.Sprintf("value-%d", time.Now().UnixNano())

							// Set operation
							err := tx.Insert("kv", &KeyValue{Key: key, Value: value})
							if err != nil {
								b.Fatalf("Set failed: %v", err)
							}

							// Get operation
							_, err = tx.First("kv", "id", key)
							if err != nil && err != memdb.ErrNotFound {
								b.Fatalf("Get failed: %v", err)
							}

							// Delete operation
							err = tx.Delete("kv", &KeyValue{Key: key})
							if err != nil {
								b.Fatalf("Delete failed: %v", err)
							}
						}

						// Commit the transaction
						tx.Commit()
						if err != nil {
							b.Fatalf("Commit failed: %v", err)
						}
					}(j)
				}

				wg.Wait()
			}
		})
	}
}
