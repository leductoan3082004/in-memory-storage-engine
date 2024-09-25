package gomemdb

import (
	"fmt"
	"github.com/hashicorp/go-memdb"
	"strconv"
	"sync"
	"testing"
)

func BenchmarkMemDB_SetGetDelete(b *testing.B) {
	storage, _ := NewMemDB()
	keys := make([]string, 10)
	for i := 0; i < 10; i++ {
		keys[i] = "key" + strconv.Itoa(i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		for j := 0; j < 10000; j++ {
			wg.Add(1)

			go func(j int) {
				defer wg.Done()

				for op := 0; op < 100; op++ {
					key := keys[op%10]
					value := fmt.Sprintf("value-%d", i*100+op)

					tx := storage.Txn(true)

					if err := tx.Insert("kv", &KeyValue{Key: key, Value: value}); err != nil {
						b.Fatalf("Set failed: %v", err)
					}

					_, err := tx.First("kv", "id", key)
					if err != nil && err != memdb.ErrNotFound {
					}

					if err := tx.Delete("kv", &KeyValue{Key: key}); err != nil {
					}

					tx.Commit()
				}
			}(j)
		}

		wg.Wait()
	}
}
