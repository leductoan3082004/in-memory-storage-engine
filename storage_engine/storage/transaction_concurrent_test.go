package storage

import (
	"context"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

// Test cases for concurrent transactions in MemStorage with MVCC
func TestMemStorage_ConcurrentTransactions(t *testing.T) {
	ctx := context.Background()
	storage := NewMemStore() // Assuming NewMemStore creates an instance of your MemStorage

	t.Run("Concurrent Set in Multiple Transactions with Controlled Commit Order", func(t *testing.T) {
		var wg sync.WaitGroup

		// Transaction 1 sets key6
		wg.Add(1)
		c := make(chan struct{})

		go func() {
			defer wg.Done()
			txID1 := storage.Tx()
			c <- struct{}{}
			t.Logf("Transaction 1 with txID %d", txID1)
			err := txID1.Set(ctx, "key6", "tx1Value")
			assert.NoError(t, err, "Transaction 1: Setting Value should not fail")

			// sleep for 10 ms to wait for goroutine 2 to start
			time.Sleep(time.Millisecond * 10)

			err = txID1.Commit(ctx)
			assert.NoError(t, err, "Transaction 1: Commit should not fail")
			c <- struct{}{}
		}()

		// Transaction 2 sets the same key to a different value
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-c
			txID2 := storage.Tx()

			t.Logf("Transaction 2 with txID %d", txID2)
			// Now Transaction 2 proceeds
			err := txID2.Set(ctx, "key6", "tx2Value")
			assert.NoError(t, err, "Transaction 2: Setting Value should not fail")

			<-c
			// Commit Transaction 2 after Transaction 1 is done
			err = txID2.Commit(ctx)
			assert.Error(t, err, "Transaction 2: Commit should fail due to transaction 1 commit earlier")
		}()

		// Wait for both transactions to complete
		wg.Wait()

		// Now we verify that the last committed value should be "tx1Value" because Transaction 1 was committed first
		globalValue, err := storage.Get(ctx, "key6")
		assert.NoError(t, err, "Transaction 6: Getting value should not fail")
		assert.Equal(t, "tx1Value", globalValue, "Expected the last committed value to be 'tx1Value'")
	})

	t.Run("Multiple Concurrent Transactions on Different Keys", func(t *testing.T) {
		var wg sync.WaitGroup

		// Transaction 1 sets key9
		wg.Add(1)
		go func() {
			defer wg.Done()
			txID1 := storage.Tx()
			err := storage.Set(ctx, "key9", "tx1Value")
			assert.NoError(t, err, "Transaction 1: Setting Value should not fail")

			// Commit transaction 1
			err = txID1.Commit(ctx)
			assert.NoError(t, err, "Transaction 1: Commit should not fail")
		}()

		// Transaction 2 sets key10
		wg.Add(1)
		go func() {
			defer wg.Done()
			txID2 := storage.Tx()
			err := txID2.Set(ctx, "key10", "tx2Value")
			assert.NoError(t, err, "Transaction 2: Setting Value should not fail")

			// Commit transaction 2
			err = txID2.Commit(ctx)
			assert.NoError(t, err, "Transaction 2: Commit should not fail")
		}()

		// Wait for both transactions to complete
		wg.Wait()

		// Now we verify both keys are correctly committed and their values are separate
		globalValueKey9, err := storage.Get(ctx, "key9")
		assert.NoError(t, err, "Transaction 9: Getting value should not fail")

		globalValueKey10, err := storage.Get(ctx, "key10")
		assert.NoError(t, err, "Transaction 10: Getting value should not fail")

		assert.Equal(t, "tx1Value", globalValueKey9, "Expected the value for key9 to be 'tx1Value'")
		assert.Equal(t, "tx2Value", globalValueKey10, "Expected the value for key10 to be 'tx2Value'")
	})
}
