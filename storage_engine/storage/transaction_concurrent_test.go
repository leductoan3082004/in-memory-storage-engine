package storage

import (
	"context"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
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
			txID1 := storage.StartTransaction(ctx)
			c <- struct{}{}
			t.Logf("Transaction 1 with txID %d", txID1)
			err := storage.SetValueForTransaction(ctx, txID1, "key6", "tx1Value")
			assert.NoError(t, err, "Transaction 1: Setting Value should not fail")

			err = storage.CommitTransaction(ctx, txID1)
			c <- struct{}{}
			assert.NoError(t, err, "Transaction 1: Commit should not fail")
		}()

		// Transaction 2 sets the same key to a different value
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-c
			txID2 := storage.StartTransaction(ctx)

			t.Logf("Transaction 2 with txID %d", txID2)
			// Now Transaction 2 proceeds
			err := storage.SetValueForTransaction(ctx, txID2, "key6", "tx2Value")
			assert.NoError(t, err, "Transaction 2: Setting Value should not fail")

			<-c
			// Commit Transaction 2 after Transaction 1 is done
			err = storage.CommitTransaction(ctx, txID2)
			assert.NoError(t, err, "Transaction 2: Commit should not fail")
		}()

		// Wait for both transactions to complete
		wg.Wait()

		// Now we verify that the last committed value should be "tx2Value" because Transaction 2 was committed later
		globalValue := storage.Get(ctx, "key6")
		assert.Equal(t, "tx2Value", globalValue, "Expected the last committed value to be 'tx2Value'")
	})

	t.Run("Concurrent Read and Write Transactions", func(t *testing.T) {
		var wg sync.WaitGroup

		// Transaction 1 sets key7
		wg.Add(1)
		go func() {
			defer wg.Done()
			txID1 := storage.StartTransaction(ctx)
			err := storage.SetValueForTransaction(ctx, txID1, "key7", "tx1Value")
			assert.NoError(t, err, "Transaction 1: Setting Value should not fail")

			// Commit transaction 1
			err = storage.CommitTransaction(ctx, txID1)
			assert.NoError(t, err, "Transaction 1: Commit should not fail")
		}()

		// Transaction 2 reads key7 before Transaction 1 commits, ensuring isolation
		wg.Add(1)
		go func() {
			defer wg.Done()
			txID2 := storage.StartTransaction(ctx)

			// Should not see Transaction 1's changes because txID1 is not committed yet
			value, err := storage.GetValueForTransaction(ctx, txID2, "key7")
			assert.Nil(t, value, "Transaction 2: Should not see uncommitted value")
			assert.NoError(t, err, "Transaction 2: GetValue should not fail")

			// Commit transaction 2
			err = storage.CommitTransaction(ctx, txID2)
			assert.NoError(t, err, "Transaction 2: Commit should not fail")
		}()

		// Wait for both transactions to complete
		wg.Wait()

		// Now the value should be visible globally
		globalValue := storage.Get(ctx, "key7")
		assert.Equal(t, "tx1Value", globalValue, "Expected the committed value from Transaction 1 to be visible")
	})

	t.Run("Multiple Concurrent Transactions on Different Keys", func(t *testing.T) {
		var wg sync.WaitGroup

		// Transaction 1 sets key9
		wg.Add(1)
		go func() {
			defer wg.Done()
			txID1 := storage.StartTransaction(ctx)
			err := storage.SetValueForTransaction(ctx, txID1, "key9", "tx1Value")
			assert.NoError(t, err, "Transaction 1: Setting Value should not fail")

			// Commit transaction 1
			err = storage.CommitTransaction(ctx, txID1)
			assert.NoError(t, err, "Transaction 1: Commit should not fail")
		}()

		// Transaction 2 sets key10
		wg.Add(1)
		go func() {
			defer wg.Done()
			txID2 := storage.StartTransaction(ctx)
			err := storage.SetValueForTransaction(ctx, txID2, "key10", "tx2Value")
			assert.NoError(t, err, "Transaction 2: Setting Value should not fail")

			// Commit transaction 2
			err = storage.CommitTransaction(ctx, txID2)
			assert.NoError(t, err, "Transaction 2: Commit should not fail")
		}()

		// Wait for both transactions to complete
		wg.Wait()

		// Now we verify both keys are correctly committed and their values are separate
		globalValueKey9 := storage.Get(ctx, "key9")
		globalValueKey10 := storage.Get(ctx, "key10")

		assert.Equal(t, "tx1Value", globalValueKey9, "Expected the value for key9 to be 'tx1Value'")
		assert.Equal(t, "tx2Value", globalValueKey10, "Expected the value for key10 to be 'tx2Value'")
	})
}
