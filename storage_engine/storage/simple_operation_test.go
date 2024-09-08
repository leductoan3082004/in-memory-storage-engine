package storage

import (
	"context"
	"in-memory-storage-engine/storage_engine/appCommon"
	"reflect"
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
			name:        "Set and Get negative integer",
			key:         "key3",
			setValue:    -456,
			expectedGet: -456,
		},
		{
			name:        "Set and Get float",
			key:         "key4",
			setValue:    3.14159,
			expectedGet: 3.14159,
		},
		{
			name:        "Set and Get boolean true",
			key:         "key5",
			setValue:    true,
			expectedGet: true,
		},
		{
			name:        "Set and Get boolean false",
			key:         "key6",
			setValue:    false,
			expectedGet: false,
		},
		{
			name:        "Set and Get nil value",
			key:         "key7",
			setValue:    nil,
			expectedGet: nil,
		},
		{
			name:        "Get non-existent key",
			key:         "key8",
			expectedGet: nil,
		},
		{
			name:         "Delete existing key",
			key:          "key9",
			setValue:     "deleteMe",
			expectedGet:  "deleteMe",
			deleteKey:    "key9",
			expectDelete: nil,
		},
		{
			name:         "Delete non-existent key",
			deleteKey:    "key10",
			expectDelete: appCommon.KeyDoesNotExist, // Replace with the actual error for non-existent key
		},
		{
			name:        "Set and Get empty string",
			key:         "key11",
			setValue:    "",
			expectedGet: "",
		},
		{
			name:        "Set and Get zero value",
			key:         "key12",
			setValue:    0,
			expectedGet: 0,
		},
		{
			name:        "Set and Get complex object (map)",
			key:         "key13",
			setValue:    map[string]interface{}{"name": "John", "age": 30},
			expectedGet: map[string]interface{}{"name": "John", "age": 30},
		},
		{
			name:        "Set and Get complex object (slice)",
			key:         "key14",
			setValue:    []int{1, 2, 3, 4},
			expectedGet: []int{1, 2, 3, 4},
		},
		{
			name:         "Delete same key twice",
			key:          "key15",
			setValue:     "deleteTwice",
			expectedGet:  "deleteTwice",
			deleteKey:    "key15",
			expectDelete: nil, // First delete should succeed
		},
		{
			name:         "Delete already deleted key",
			deleteKey:    "key15",                   // Trying to delete the key again
			expectDelete: appCommon.KeyDoesNotExist, // Expecting key does not exist
		},
		{
			name:        "Set new value to previously deleted key",
			key:         "key15",
			setValue:    "newValueAfterDelete",
			expectedGet: "newValueAfterDelete",
		},
		{
			name:        "Set and Get large number",
			key:         "key16",
			setValue:    999999999999,
			expectedGet: 999999999999,
		},
		{
			name:        "Set and Get float zero",
			key:         "key17",
			setValue:    0.0,
			expectedGet: 0.0,
		},
		{
			name:        "Set and Get string with special characters",
			key:         "key18",
			setValue:    "This is a test! @#$%^&*()",
			expectedGet: "This is a test! @#$%^&*()",
		},
		{
			name:        "Set and Get negative float",
			key:         "key19",
			setValue:    -0.1234,
			expectedGet: -0.1234,
		},
		{
			name:        "Set and Get nested map",
			key:         "key20",
			setValue:    map[string]interface{}{"outer": map[string]interface{}{"inner": "value"}},
			expectedGet: map[string]interface{}{"outer": map[string]interface{}{"inner": "value"}},
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
