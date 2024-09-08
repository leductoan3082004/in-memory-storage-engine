package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test cases for the transaction handling in MemStorage with MVCC
func TestMemStorage_Transactions(t *testing.T) {
	ctx := context.Background()
	storage := NewMemStore() // Assuming NewMemStore creates an instance of your MemStorage

	// Test Start Transaction
	t.Run("Start Transaction", func(t *testing.T) {
		txID := storage.StartTransaction(ctx)
		assert.Equal(t, 1, txID, "Expected txID to be 1 for the first transaction")

		txID2 := storage.StartTransaction(ctx)
		assert.Equal(t, 2, txID2, "Expected txID to be 2 for the second transaction")
	})

	// Test Set and Get within a transaction
	t.Run("Set and Get within a transaction", func(t *testing.T) {
		txID := storage.StartTransaction(ctx)

		// Set a key within a transaction
		err := storage.SetValueForTransaction(ctx, txID, "key1", "txValue1")
		assert.NoError(t, err, "Setting Value in transaction should not fail")

		// Get the Value within the same transaction
		value, err := storage.GetValueForTransaction(ctx, txID, "key1")
		assert.NoError(t, err, "Getting Value in transaction should not fail")
		assert.Equal(t, "txValue1", value, "Expected 'txValue1' within transaction")

		// The Value should not be visible globally before commit
		globalValue := storage.Get(ctx, "key1")
		assert.Nil(t, globalValue, "The Value should not be visible globally before commit")
	})

	// Test Commit Transaction
	t.Run("Commit Transaction", func(t *testing.T) {
		txID := storage.StartTransaction(ctx)

		err := storage.SetValueForTransaction(ctx, txID, "key2", "committedValue")
		assert.NoError(t, err, "Setting Value in transaction should not fail")

		err = storage.CommitTransaction(ctx, txID)
		assert.NoError(t, err, "Committing transaction should not fail")

		globalValue := storage.Get(ctx, "key2")
		assert.Equal(t, "committedValue", globalValue, "Expected the Value to be visible globally after commit")
	})

	t.Run("Abort Transaction", func(t *testing.T) {
		txID := storage.StartTransaction(ctx)

		// Set a Value within the transaction
		err := storage.SetValueForTransaction(ctx, txID, "key3", "abortedValue")
		assert.NoError(t, err, "Setting Value in transaction should not fail")

		// Abort the transaction
		err = storage.AbortTransaction(ctx, txID)
		assert.NoError(t, err, "Aborting transaction should not fail")

		// The Value should not be visible globally after abort
		globalValue := storage.Get(ctx, "key3")
		assert.Nil(t, globalValue, "The Value should not be visible globally after abort")
	})

	// Test Repeatable Read Isolation
	t.Run("Repeatable Read Isolation", func(t *testing.T) {
		// Start the first transaction and set a Value
		txID1 := storage.StartTransaction(ctx)
		err := storage.SetValueForTransaction(ctx, txID1, "key4", "valueInTx1")
		assert.NoError(t, err, "Setting Value in txID1 should not fail")

		// Start the second transaction, which should see the initial state of the world (not txID1's changes)
		txID2 := storage.StartTransaction(ctx)

		// txID2 should not see txID1's changes before commit
		valueInTx2, err := storage.GetValueForTransaction(ctx, txID2, "key4")
		assert.Nil(t, valueInTx2, "txID2 should not see txID1's uncommitted changes")

		// Commit the first transaction
		err = storage.CommitTransaction(ctx, txID1)
		assert.NoError(t, err, "Committing txID1 should not fail")

		// Now, globally, the Value should be visible
		globalValue := storage.Get(ctx, "key4")
		assert.Equal(t, "valueInTx1", globalValue, "The Value should be visible globally after txID1 commits")

		// txID2 should still not see the change due to repeatable read isolation
		valueInTx2AfterCommit, err := storage.GetValueForTransaction(ctx, txID2, "key4")
		assert.Nil(t, valueInTx2AfterCommit, "txID2 should not see txID1's committed changes due to repeatable read")
	})

	// Test Set, Delete, and Commit in a Transaction
	t.Run("Set, Delete, and Commit in a Transaction", func(t *testing.T) {
		txID := storage.StartTransaction(ctx)

		// Set a Value within the transaction
		err := storage.SetValueForTransaction(ctx, txID, "key5", "tempValue")
		assert.NoError(t, err, "Setting Value in transaction should not fail")

		// Delete the Value within the transaction
		err = storage.DeleteValueForTransaction(ctx, txID, "key5")
		assert.NoError(t, err, "Deleting Value in transaction should not fail")

		// Commit the transaction
		err = storage.CommitTransaction(ctx, txID)
		assert.NoError(t, err, "Committing transaction should not fail")

		// The Value should not be visible globally because it was deleted within the transaction
		globalValue := storage.Get(ctx, "key5")
		assert.Nil(t, globalValue, "The Value should not exist globally after deletion in a transaction and commit")
	})
}
