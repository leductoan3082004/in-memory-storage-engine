package storage

import (
	"context"
	"in-memory-storage-engine/storage_engine/appCommon"
	"reflect" // Import reflect for deep comparison
	"testing"
)

func TestMemStorage_SetGetDelete(t *testing.T) {
	ctx := context.Background()
	storage := NewMemStore() // Assuming this is your MemStore implementation

	tests := []struct {
		name         string
		key          string
		setValue     interface{}
		expectedGet  interface{}
		deleteKey    string
		expectDelete error
	}{
		{
			name:        "Set and Get string",
			key:         "key1",
			setValue:    "value1",
			expectedGet: "value1",
		},
		{
			name:        "Set and Get integer",
			key:         "key2",
			setValue:    123,
			expectedGet: 123,
		},
		{
			name:        "Set and Get nil value",
			key:         "key3",
			setValue:    nil,
			expectedGet: nil,
		},
		{
			name:        "Get non-existent key",
			key:         "key4",
			expectedGet: nil,
		},
		{
			name:         "Delete existing key",
			key:          "key5",
			setValue:     "deleteMe",
			expectedGet:  "deleteMe",
			deleteKey:    "key5",
			expectDelete: nil,
		},
		{
			name:         "Delete non-existent key",
			deleteKey:    "key6",
			expectDelete: appCommon.KeyDoesNotExist, // Replace with the actual error for non-existent key
		},
		{
			name:         "Delete key twice",
			key:          "key7",
			setValue:     "deleteTwice",
			expectedGet:  "deleteTwice",
			deleteKey:    "key7",
			expectDelete: nil, // First delete should succeed
		},
		{
			name:         "Delete already deleted key",
			deleteKey:    "key7",                    // Trying to delete the key again
			expectDelete: appCommon.KeyDoesNotExist, // Expecting key does not exist
		},
		{
			name:        "Set new value to previously deleted key",
			key:         "key7",
			setValue:    "newValueAfterDelete",
			expectedGet: "newValueAfterDelete",
		},
		{
			name:        "Set and Get complex object",
			key:         "key8",
			setValue:    map[string]interface{}{"name": "John", "age": 30},
			expectedGet: map[string]interface{}{"name": "John", "age": 30},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test Set
			if tt.setValue != nil {
				storage.Set(ctx, tt.key, tt.setValue)
			}

			// Test Get
			gotValue := storage.Get(ctx, tt.key)

			// Use reflect.DeepEqual for comparing complex types like maps
			if !reflect.DeepEqual(gotValue, tt.expectedGet) {
				t.Errorf("Expected Get %v, got %v", tt.expectedGet, gotValue)
			}

			// Test Delete if deleteKey is set
			if tt.deleteKey != "" {
				err := storage.Delete(ctx, tt.deleteKey)
				if err != tt.expectDelete {
					t.Errorf("Expected Delete error %v, got %v", tt.expectDelete, err)
				}

				// Test Get after Delete
				gotValueAfterDelete := storage.Get(ctx, tt.deleteKey)
				if gotValueAfterDelete != nil {
					t.Errorf("Expected nil after delete, got %v", gotValueAfterDelete)
				}
			}
		})
	}
}
