package operation_test

import (
	"in-memory-storage-engine/appCommon"
	"in-memory-storage-engine/storage_engine/operation"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Table-driven tests for operationsKeyStore component
func TestOperationsKeyStore(t *testing.T) {
	store := operation.NewOperationsKeyStore()

	tests := []struct {
		name        string
		action      string
		key         string
		value       interface{}
		expectErr   error
		expectValue interface{}
		expectExist bool
	}{
		// Set operation tests
		{"Set key1", "set", "key1", "value1", nil, "value1", true},
		{"Set key2", "set", "key2", 123, nil, 123, true},
		{"Set key3", "set", "key3", true, nil, true, true},

		// Get operation tests
		{"Get existing key1", "get", "key1", nil, nil, "value1", true},
		{"Get existing key2", "get", "key2", nil, nil, 123, true},
		{"Get non-existent key", "get", "keyX", nil, nil, nil, false},

		// Delete operation tests (value becomes nil, key still exists)
		{"Delete existing key1", "delete", "key1", nil, nil, nil, true},                           // Expect value to be nil, but key exists
		{"Delete non-existent key", "delete", "keyX", nil, appCommon.KeyDoesNotExist, nil, false}, // Key does not exist

		// Check if key exists (even after deletion)
		{"Check existing key2", "check", "key2", nil, nil, nil, true},
		{"Check deleted key1", "check", "key1", nil, nil, nil, true}, // Still exists after delete, but value is nil
		{"Check non-existent key", "check", "keyX", nil, nil, nil, false},

		// Set after delete (replace the nil value)
		{"Set key1 after delete", "set", "key1", "newValue1", nil, "newValue1", true},

		// Delete key then check if exists
		{"Delete key3", "delete", "key3", nil, nil, nil, true}, // Key still exists, but value is nil
		{"Check deleted key3", "check", "key3", nil, nil, nil, true},

		// Set multiple values
		{"Set key4", "set", "key4", "value4", nil, "value4", true},
		{"Set key5", "set", "key5", 456, nil, 456, true},

		// Get all operations
		{"Get all operations after setting", "getAll", "", nil, nil, nil, true},

		// Set duplicate key
		{"Set duplicate key2", "set", "key2", 789, nil, 789, true},

		// Set nil value
		{"Set nil value for key6", "set", "key6", nil, nil, nil, true},
		{"Get nil value key6", "get", "key6", nil, nil, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.action {
			case "set":
				store.Set(tt.key, tt.value)
				getValue := store.Get(tt.key)
				assert.Equal(t, tt.expectValue, getValue)
			case "get":
				getValue := store.Get(tt.key)
				assert.Equal(t, tt.expectValue, getValue)
			case "delete":
				err := store.Delete(tt.key)
				if tt.expectErr != nil {
					assert.Equal(t, tt.expectErr, err)
				} else {
					assert.NoError(t, err)
					getValue := store.Get(tt.key)
					assert.Nil(t, getValue) // Value should be nil after delete, but key still exists
				}
			case "check":
				exists := store.CheckIfKeyExists(tt.key)
				assert.Equal(t, tt.expectExist, exists)
			case "getAll":
				allOperations := store.GetAllOperation()
				assert.NotNil(t, allOperations)
			}
		})
	}
}
