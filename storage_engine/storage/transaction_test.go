package storage

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMemStorage_Transactions(t *testing.T) {
	ctx := context.Background()
	storage := NewMemStore()

	t.Run("Set and Get within a transaction", func(t *testing.T) {
		txID := storage.Tx()

		err := txID.Set(ctx, "key1", "txValue1")
		assert.NoError(t, err)

		value, err := txID.Get(ctx, "key1")
		assert.NoError(t, err)
		assert.Equal(t, "txValue1", value)

		globalValue, _ := storage.Get(ctx, "key1")
		assert.Nil(t, globalValue)
	})

	t.Run("Commit Transaction", func(t *testing.T) {
		txID := storage.Tx()

		err := txID.Set(ctx, "key2", "committedValue")
		assert.NoError(t, err)

		err = txID.Commit(ctx)
		assert.NoError(t, err)

		globalValue, _ := storage.Get(ctx, "key2")
		assert.Equal(t, "committedValue", globalValue)
	})

	t.Run("Abort Transaction", func(t *testing.T) {
		txID := storage.Tx()

		err := txID.Set(ctx, "key3", "abortedValue")
		assert.NoError(t, err)

		err = txID.Abort(ctx)
		assert.NoError(t, err)

		globalValue, _ := storage.Get(ctx, "key3")
		assert.Nil(t, globalValue)
	})

	t.Run("Repeatable Read Isolation", func(t *testing.T) {
		txID1 := storage.Tx()
		err := txID1.Set(ctx, "key4", "valueInTx1")
		assert.NoError(t, err)

		txID2 := storage.Tx()

		valueInTx2, _ := txID2.Get(ctx, "key4")
		assert.Nil(t, valueInTx2)

		err = txID1.Commit(ctx)
		assert.NoError(t, err)

		globalValue, _ := storage.Get(ctx, "key4")
		assert.Equal(t, "valueInTx1", globalValue)

		valueInTx2AfterCommit, _ := txID2.Get(ctx, "key4")
		assert.Nil(t, valueInTx2AfterCommit)
	})

	t.Run("Set, Delete, and Commit in a Transaction", func(t *testing.T) {
		txID := storage.Tx()

		err := txID.Set(ctx, "key5", "tempValue")
		assert.NoError(t, err)

		err = txID.Delete(ctx, "key5")
		assert.NoError(t, err)

		err = txID.Commit(ctx)
		assert.NoError(t, err)

		globalValue, _ := storage.Get(ctx, "key5")
		assert.Nil(t, globalValue)
	})
}

func BenchmarkMemStore_ConcurrentTransactionScaling(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()
	storage := NewMemStore()
	keys := make([]string, 10)
	for i := 0; i < 10; i++ {
		keys[i] = "key" + strconv.Itoa(i)
	}

	concurrencyLevels := []int{10, 50, 100, 500, 1000, 10000}
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

						tx := storage.Tx()
						for op := 0; op < numOperationsPerGoroutine; op++ {
							key := keys[j%len(keys)]
							value := time.Now().UnixNano()

							if err := tx.Set(ctx, key, value); err != nil {
							}
							if _, err := tx.Get(ctx, key); err != nil {
							}
							if err := tx.Delete(ctx, key); err != nil {
							}
						}

						if err := tx.Commit(ctx); err != nil {
						}
					}(j)
				}

				wg.Wait()
			}
		})
	}
}
