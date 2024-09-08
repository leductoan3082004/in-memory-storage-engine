package storage

import (
	"context"
	"reflect"
	"testing"
)

func TestMemStorage_TransactionMVCC(t *testing.T) {
	ctx := context.Background()
	storage := NewMemStore()

	txID1 := storage.StartTransaction(ctx)

	storage.Set(ctx, "key1", "initialValue")

	tests := []struct {
		name              string
		txID              int
		key               string
		setValue          interface{}
		expectedGet       interface{}
		withinTransaction bool
		commitTransaction bool
	}{
		{
			name:              "Set and Get within a transaction (txID1)",
			txID:              txID1,
			key:               "txKey1",
			setValue:          "txValue1",
			expectedGet:       "txValue1",
			withinTransaction: true,
			commitTransaction: true,
		},
		{
			name:              "Set and Get integer within a transaction (txID1)",
			txID:              txID1,
			key:               "txKey2",
			setValue:          123,
			expectedGet:       123,
			withinTransaction: true,
			commitTransaction: true,
		},
		{
			name:              "Set and Get nil value within a transaction (txID1)",
			txID:              txID1,
			key:               "txKey3",
			setValue:          nil,
			expectedGet:       nil,
			withinTransaction: true,
			commitTransaction: true,
		},
		{
			name:              "Get value from before transaction (Repeatable Read)",
			txID:              txID1,
			key:               "key1", // Value is "initialValue"
			expectedGet:       "initialValue",
			withinTransaction: true,
			commitTransaction: false, // No need to commit
		},
		{
			name:              "Set new value in txID1, should not be visible in txID2 before commit",
			txID:              txID1,
			key:               "key4",
			setValue:          "newValueBeforeCommit",
			expectedGet:       nil, // Transaction 2 should not see this change before commit
			withinTransaction: true,
			commitTransaction: false,
		},
		{
			name:              "Commit transaction txID1, now txID2 should see new value",
			txID:              txID1,
			key:               "key4",
			setValue:          "newValueAfterCommit",
			expectedGet:       "newValueAfterCommit", // After commit, it should be visible to txID2
			withinTransaction: false,
			commitTransaction: true,
		},
		{
			name:              "Set value in txID2, should not be visible to txID1",
			txID:              storage.StartTransaction(ctx), // Starting txID2
			key:               "key5",
			setValue:          "txID2Value",
			expectedGet:       nil, // txID1 should not see this change until txID2 commits
			withinTransaction: true,
			commitTransaction: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.withinTransaction {
				// Test Set within a transaction
				if tt.setValue != nil {
					err := storage.SetValueForTransaction(ctx, tt.txID, tt.key, tt.setValue)
					if err != nil {
						t.Fatalf("SetValueForTransaction failed: %v", err)
					}
				}

				// Test Get within a transaction (before commit)
				gotValue, err := storage.GetValueForTransaction(ctx, tt.txID, tt.key)
				if err != nil {
					t.Fatalf("GetValueForTransaction failed: %v", err)
				}
				if !reflect.DeepEqual(gotValue, tt.expectedGet) {
					t.Errorf("Expected Get %v, got %v", tt.expectedGet, gotValue)
				}
			}

			// Test commit if needed
			if tt.commitTransaction {
				// Commit the transaction and check visibility
				commitTxID := tt.txID
				storage.C(ctx, commitTxID)

				// Check if the new value is now visible globally (after commit)
				globalValue := storage.Get(ctx, tt.key)
				if !reflect.DeepEqual(globalValue, tt.expectedGet) {
					t.Errorf("After commit, expected %v, but got %v", tt.expectedGet, globalValue)
				}
			}
		})
	}
}
